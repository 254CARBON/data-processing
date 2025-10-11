"""
Hydro cascade preprocessor for IRP modeling.
"""

from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import structlog

from ..shared.storage import ClickHouseClient
from .mass_balance import MassBalanceCalculator, ReservoirState


logger = structlog.get_logger("hydro.preprocessor")


class HydroPreprocessor:
    """Preprocess hydro cascade data and persist mass-balance states."""

    def __init__(self, clickhouse_client: ClickHouseClient):
        self.clickhouse = clickhouse_client
        self.mass_balance = MassBalanceCalculator()
        self.cascade_metadata: Dict[str, Dict[str, Any]] = {}
        self.generation_units: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.time_step_hours: float = 1.0

    async def preprocess_hydro_cascades(
        self,
        cascade_ids: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        case_id: str = "default",
        scenario_id: Optional[str] = None,
        market: str = "wecc",
        weather_scenario: Optional[str] = None,
        data_source: str = "hydro_preprocessor",
        overwrite_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Preprocess hydro cascades, calculate reservoir trajectories, and store inputs.

        Args:
            cascade_ids: Optional list restricting cascades to process.
            start_date: Inclusive start of processing window (defaults to one year back).
            end_date: Exclusive end of processing window (defaults to one year forward).
            case_id: IRP case identifier for stored records.
            scenario_id: Optional scenario identifier tied to the inputs.
            market: Market identifier (stored with each record).
            weather_scenario: Optional weather scenario tag.
            data_source: Descriptor stored with the generated inputs.
            overwrite_existing: Delete prior rows for the case before inserting new ones.
        """
        logger.info("Starting hydro cascade preprocessing", case_id=case_id)

        start_dt, end_dt = self._determine_window(start_date, end_date)
        await self._load_cascade_topologies(cascade_ids)
        await self._load_generation_units(cascade_ids)

        results: Dict[str, Any] = {}
        staged_records: List[Dict[str, Any]] = []

        for cascade_id in list(self.cascade_metadata.keys()):
            if cascade_ids and cascade_id not in cascade_ids:
                continue

            try:
                summary, records = await self._process_cascade(
                    cascade_id=cascade_id,
                    start_date=start_dt,
                    end_date=end_dt,
                    case_id=case_id,
                    scenario_id=scenario_id,
                    market=market,
                    weather_scenario=weather_scenario,
                    data_source=data_source,
                )
                results[cascade_id] = summary
                staged_records.extend(records)
                logger.info(
                    "Cascade processed",
                    cascade_id=cascade_id,
                    records=len(records),
                    inflow_source=summary.get("inflow_source"),
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Failed to process cascade", cascade_id=cascade_id, error=str(exc))
                results[cascade_id] = {"status": "error", "error": str(exc)}

        stored_rows = 0
        if staged_records:
            stored_rows = await self._store_hydro_inputs(
                case_id=case_id,
                records=staged_records,
                overwrite_existing=overwrite_existing,
            )

        success = [r for r in results.values() if r.get("status") == "success"]
        failures = [r for r in results.values() if r.get("status") == "error"]

        return {
            "status": "completed" if not failures else "completed_with_errors",
            "processed_cascades": len(success),
            "failed_cascades": len(failures),
            "stored_rows": stored_rows,
            "time_window": {
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
            },
            "results": results,
        }

    async def _load_cascade_topologies(self, cascade_ids: Optional[List[str]]) -> None:
        """Load cascade metadata and register with the mass-balance calculator."""
        query = """
        SELECT
            cascade_id,
            cascade_name,
            river_system,
            ba_id,
            region,
            reservoirs,
            seasonal_rules,
            water_rights
        FROM markets.wecc_topology_cascades
        """
        if cascade_ids:
            formatted_ids = ",".join(f"'{cid}'" for cid in cascade_ids)
            query += f" WHERE cascade_id IN ({formatted_ids})"

        try:
            rows = await self.clickhouse.query(query)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.error("Failed to load cascade topologies", error=str(exc))
            return

        for row in rows:
            try:
                reservoirs = json.loads(row["reservoirs"]) if row.get("reservoirs") else []
                seasonal_rules = json.loads(row["seasonal_rules"]) if row.get("seasonal_rules") else {}
                water_rights = json.loads(row["water_rights"]) if row.get("water_rights") else {}

                for reservoir in reservoirs:
                    reservoir.setdefault("max_generation_mw", 0.0)
                    reservoir.setdefault("reservoir_type", "storage")

                cascade_meta = {
                    "cascade_id": row["cascade_id"],
                    "cascade_name": row.get("cascade_name"),
                    "river_system": row.get("river_system"),
                    "ba_id": row.get("ba_id"),
                    "region": row.get("region"),
                    "reservoirs": reservoirs,
                    "seasonal_rules": seasonal_rules,
                    "water_rights": water_rights,
                }
                reservoir_lookup = {res["reservoir_id"]: res for res in reservoirs}
                cascade_meta["reservoir_lookup"] = reservoir_lookup

                self.cascade_metadata[row["cascade_id"]] = cascade_meta
                self.mass_balance.load_cascade_topology(cascade_meta)
            except Exception as exc:
                logger.error(
                    "Failed to load cascade topology",
                    cascade_id=row.get("cascade_id"),
                    error=str(exc),
                )

    async def _load_generation_units(self, cascade_ids: Optional[List[str]]) -> None:
        """Load hydro generation units and attach capacity metadata to reservoirs."""
        query = """
        SELECT
            unit_id,
            unit_name,
            cascade_id,
            reservoir_id,
            capacity_mw,
            min_generation_mw,
            max_generation_mw,
            efficiency_percent,
            head_loss_factor
        FROM markets.wecc_hydro_generation_units
        """
        if cascade_ids:
            formatted_ids = ",".join(f"'{cid}'" for cid in cascade_ids)
            query += f" WHERE cascade_id IN ({formatted_ids})"

        try:
            rows = await self.clickhouse.query(query)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.warning("Failed to load hydro generation units", error=str(exc))
            return

        for row in rows:
            cascade_id = row.get("cascade_id")
            reservoir_id = row.get("reservoir_id")
            if not cascade_id or not reservoir_id:
                continue

            unit = {
                "unit_id": row.get("unit_id"),
                "unit_name": row.get("unit_name"),
                "capacity_mw": row.get("capacity_mw"),
                "min_generation_mw": row.get("min_generation_mw"),
                "max_generation_mw": row.get("max_generation_mw"),
                "efficiency_percent": row.get("efficiency_percent"),
                "head_loss_factor": row.get("head_loss_factor"),
            }

            cascade_units = self.generation_units.setdefault(cascade_id, {})
            cascade_units.setdefault(reservoir_id, []).append(unit)

            reservoir_lookup = self.cascade_metadata.get(cascade_id, {}).get("reservoir_lookup", {})
            reservoir_meta = reservoir_lookup.get(reservoir_id)
            if reservoir_meta is None:
                continue

            capacity = row.get("max_generation_mw") or row.get("capacity_mw") or 0.0
            reservoir_meta["max_generation_mw"] = (reservoir_meta.get("max_generation_mw") or 0.0) + capacity
            reservoir_meta.setdefault("generation_units", []).append(unit)

    async def _process_cascade(
        self,
        cascade_id: str,
        start_date: datetime,
        end_date: datetime,
        *,
        case_id: str,
        scenario_id: Optional[str],
        market: str,
        weather_scenario: Optional[str],
        data_source: str,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process a single cascade and return summary plus staged records."""
        cascade_meta = self.cascade_metadata.get(cascade_id)
        if cascade_meta is None:
            raise ValueError(f"Cascade metadata missing for {cascade_id}")

        inflow_data, inflow_source = await self._get_inflow_data(cascade_id, start_date, end_date)
        if inflow_data.empty:
            inflow_data = self._build_synthetic_inflow(cascade_id, start_date, end_date)
            inflow_source = "synthetic"

        initial_states = await self._get_initial_states(cascade_id, start_date)

        states = self.mass_balance.calculate_mass_balance(
            cascade_id=cascade_id,
            inflow_data=inflow_data,
            initial_states=initial_states,
            time_step_hours=self.time_step_hours,
        )

        records = self._states_to_records(
            cascade_id=cascade_id,
            states=states,
            case_id=case_id,
            scenario_id=scenario_id,
            market=market,
            weather_scenario=weather_scenario,
            data_source=data_source,
            inflow_source=inflow_source,
        )

        summary = {
            "status": "success",
            "reservoir_count": len({state.reservoir_id for state in states}),
            "time_steps": len(states),
            "records_staged": len(records),
            "inflow_source": inflow_source,
        }
        return summary, records

    async def _get_inflow_data(
        self,
        cascade_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Tuple[pd.DataFrame, str]:
        """Fetch inflow data for a cascade."""
        query = f"""
        SELECT
            timestamp,
            reservoir_id,
            inflow_cfs
        FROM hydro_inflow_data
        WHERE cascade_id = '{cascade_id}'
          AND timestamp >= '{start_date.isoformat()}'
          AND timestamp < '{end_date.isoformat()}'
        ORDER BY timestamp, reservoir_id
        """

        try:
            rows = await self.clickhouse.query(query)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.warning("Failed to load inflow data", cascade_id=cascade_id, error=str(exc))
            return pd.DataFrame(columns=["timestamp", "reservoir_id", "inflow_cfs"]), "error"

        if not rows:
            return pd.DataFrame(columns=["timestamp", "reservoir_id", "inflow_cfs"]), "missing"

        df = pd.DataFrame(rows)
        if "timestamp" not in df or "reservoir_id" not in df or "inflow_cfs" not in df:
            return pd.DataFrame(columns=["timestamp", "reservoir_id", "inflow_cfs"]), "missing"

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_localize(None)
        df = df.dropna(subset=["timestamp"])
        df = df[(df["timestamp"] >= start_date) & (df["timestamp"] < end_date)]
        df["reservoir_id"] = df["reservoir_id"].astype(str)
        df["inflow_cfs"] = df["inflow_cfs"].astype(float).fillna(0.0)
        df.sort_values(["timestamp", "reservoir_id"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df, "observed"

    async def _get_initial_states(
        self,
        cascade_id: str,
        start_date: datetime,
    ) -> Dict[str, ReservoirState]:
        """Obtain initial reservoir states prior to the processing window."""
        query = f"""
        SELECT
            reservoir_id,
            storage_af,
            elevation_feet,
            inflow_cfs,
            release_cfs,
            generation_mw,
            head_feet,
            efficiency_percent
        FROM markets.irp_inputs_hydro_cascades
        WHERE cascade_id = '{cascade_id}'
          AND timestamp < '{start_date.isoformat()}'
        ORDER BY timestamp DESC
        """

        initial_states: Dict[str, ReservoirState] = {}
        try:
            rows = await self.clickhouse.query(query)
        except Exception as exc:  # pragma: no cover - external dependency
            logger.warning("Failed to fetch initial states", cascade_id=cascade_id, error=str(exc))
            rows = []

        for row in rows:
            reservoir_id = row.get("reservoir_id")
            if reservoir_id is None or reservoir_id in initial_states:
                continue

            initial_states[reservoir_id] = ReservoirState(
                reservoir_id=reservoir_id,
                timestamp=start_date,
                storage_af=float(row.get("storage_af") or 0.0),
                elevation_feet=float(row.get("elevation_feet") or 0.0),
                inflow_cfs=float(row.get("inflow_cfs") or 0.0),
                release_cfs=float(row.get("release_cfs") or 0.0),
                generation_mw=float(row.get("generation_mw") or 0.0),
                head_feet=float(row.get("head_feet") or 0.0),
                efficiency_percent=float(row.get("efficiency_percent") or 0.0),
            )

        cascade_meta = self.cascade_metadata.get(cascade_id, {})
        reservoir_lookup = cascade_meta.get("reservoir_lookup", {})
        for reservoir_id, reservoir_meta in reservoir_lookup.items():
            if reservoir_id not in initial_states:
                initial_states[reservoir_id] = self.mass_balance._default_reservoir_state(
                    reservoir_id,
                    start_date,
                    reservoir_meta,
                )

        return initial_states

    async def _store_hydro_inputs(
        self,
        *,
        case_id: str,
        records: List[Dict[str, Any]],
        overwrite_existing: bool,
    ) -> int:
        """Persist generated hydro inputs into ClickHouse."""
        if not records:
            return 0

        if overwrite_existing:
            delete_query = f"ALTER TABLE markets.irp_inputs_hydro_cascades DELETE WHERE case_id = '{case_id}'"
            try:
                await self.clickhouse.execute(delete_query)
            except Exception as exc:  # pragma: no cover - external dependency
                logger.warning("Failed to delete existing case rows", case_id=case_id, error=str(exc))

        total_inserted = 0
        for batch in self._chunk_records(records, size=5_000):
            await self.clickhouse.insert("markets.irp_inputs_hydro_cascades", batch)
            total_inserted += len(batch)

        logger.info("Stored hydro inputs", case_id=case_id, rows=total_inserted)
        return total_inserted

    def _states_to_records(
        self,
        *,
        cascade_id: str,
        states: Iterable[ReservoirState],
        case_id: str,
        scenario_id: Optional[str],
        market: str,
        weather_scenario: Optional[str],
        data_source: str,
        inflow_source: str,
    ) -> List[Dict[str, Any]]:
        """Convert reservoir states into ClickHouse-compatible records."""
        cascade_meta = self.cascade_metadata.get(cascade_id, {})
        reservoir_lookup = cascade_meta.get("reservoir_lookup", {})

        records: List[Dict[str, Any]] = []
        metadata_template = {
            "inflow_source": inflow_source,
            "preprocessor_version": "1.0.0",
        }

        for state in states:
            reservoir_meta = reservoir_lookup.get(state.reservoir_id, {})
            metadata = dict(metadata_template)
            metadata.update(
                {
                    "reservoir_type": reservoir_meta.get("reservoir_type"),
                    "generation_units": [
                        unit.get("unit_id")
                        for unit in reservoir_meta.get("generation_units", [])
                        if unit.get("unit_id")
                    ],
                }
            )

            record = {
                "case_id": case_id,
                "scenario_id": scenario_id,
                "market": market,
                "ba_id": cascade_meta.get("ba_id"),
                "cascade_id": cascade_id,
                "cascade_name": cascade_meta.get("cascade_name"),
                "reservoir_id": state.reservoir_id,
                "reservoir_name": reservoir_meta.get("reservoir_name"),
                "timestamp": state.timestamp,
                "inflow_cfs": state.inflow_cfs,
                "storage_af": state.storage_af,
                "elevation_feet": state.elevation_feet,
                "release_cfs": state.release_cfs,
                "generation_mw": state.generation_mw,
                "head_feet": state.head_feet,
                "efficiency_percent": state.efficiency_percent,
                "weather_scenario": weather_scenario,
                "data_source": data_source,
                "metadata": json.dumps(metadata),
            }
            records.append(record)

        return records

    def _build_synthetic_inflow(
        self,
        cascade_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """Generate synthetic inflow traces when empirical data is unavailable."""
        cascade_meta = self.cascade_metadata.get(cascade_id, {})
        reservoir_lookup = cascade_meta.get("reservoir_lookup", {})

        freq_hours = int(self.time_step_hours)
        timestamps = pd.date_range(
            start=start_date,
            end=end_date,
            freq=f"{freq_hours}H",
            inclusive="left",
        )
        if timestamps.empty:
            timestamps = pd.date_range(start=start_date, periods=1, freq="H")

        records: List[Dict[str, Any]] = []
        for ts in timestamps:
            day_factor = math.sin(2.0 * math.pi * (ts.timetuple().tm_yday / 365.0))
            hour_factor = 0.1 * math.sin(2.0 * math.pi * (ts.hour / 24.0))

            for reservoir_id, reservoir_meta in reservoir_lookup.items():
                storage_af = (reservoir_meta.get("active_storage_mcm") or 0.0) * 1233.48
                base_cfs = storage_af / (24.0 * 30.0) if storage_af else 500.0
                inflow_cfs = base_cfs * (1.0 + 0.15 * day_factor + hour_factor)
                records.append(
                    {
                        "timestamp": ts.to_pydatetime().replace(tzinfo=None),
                        "reservoir_id": reservoir_id,
                        "inflow_cfs": max(inflow_cfs, 0.0),
                    }
                )

        df = pd.DataFrame.from_records(records, columns=["timestamp", "reservoir_id", "inflow_cfs"])
        df.sort_values(["timestamp", "reservoir_id"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def _determine_window(
        self,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> Tuple[datetime, datetime]:
        """Normalise the processing window to sensible defaults."""
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        default_start = now - timedelta(days=365)

        start_dt = self._normalise_datetime(start_date) if start_date else default_start
        end_dt = self._normalise_datetime(end_date) if end_date else start_dt + timedelta(days=365)

        if end_dt <= start_dt:
            end_dt = start_dt + timedelta(days=1)

        return start_dt, end_dt

    def _normalise_datetime(self, value: datetime) -> datetime:
        """Convert datetimes to naive UTC aligned to the hour."""
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.replace(minute=0, second=0, microsecond=0)

    @staticmethod
    def _chunk_records(records: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        """Yield successive chunks from a list."""
        for index in range(0, len(records), size):
            yield records[index : index + size]


async def run_hydro_preprocessing(
    clickhouse_url: str = "http://localhost:8123",
    database: str = "markets",
    cascade_ids: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    case_id: str = "default",
    scenario_id: Optional[str] = None,
    market: str = "wecc",
    weather_scenario: Optional[str] = None,
    data_source: str = "hydro_preprocessor",
    overwrite_existing: bool = True,
) -> Dict[str, Any]:
    """Entry point used by pipelines or CLI tooling."""
    client = ClickHouseClient(url=clickhouse_url, database=database)
    await client.connect()

    try:
        preprocessor = HydroPreprocessor(client)

        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        results = await preprocessor.preprocess_hydro_cascades(
            cascade_ids=cascade_ids,
            start_date=start_dt,
            end_date=end_dt,
            case_id=case_id,
            scenario_id=scenario_id,
            market=market,
            weather_scenario=weather_scenario,
            data_source=data_source,
            overwrite_existing=overwrite_existing,
        )
        return results
    finally:
        await client.disconnect()


if __name__ == "__main__":
    # Example usage for local debugging
    outcome = asyncio.run(run_hydro_preprocessing())
    print("Hydro preprocessing completed:", outcome)
