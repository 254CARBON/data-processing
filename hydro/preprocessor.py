"""
Hydro cascade preprocessor for IRP modeling.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import structlog

from ..shared.storage.clickhouse_client import ClickHouseClient
from .mass_balance import MassBalanceCalculator, ReservoirState, CascadeTopology


logger = structlog.get_logger("hydro.preprocessor")


class HydroPreprocessor:
    """Preprocesses hydro cascade data for IRP modeling."""

    def __init__(self, clickhouse_client: ClickHouseClient):
        self.clickhouse = clickhouse_client
        self.mass_balance = MassBalanceCalculator()

    async def preprocess_hydro_cascades(
        self,
        cascade_ids: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Preprocess hydro cascade data for IRP modeling.

        Args:
            cascade_ids: List of cascade IDs to process (None for all)
            start_date: Start date for processing
            end_date: End date for processing

        Returns:
            Processing results summary
        """
        logger.info("Starting hydro cascade preprocessing")

        # Default date range if not provided
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now() + timedelta(days=365)

        # Load cascade topologies
        await self._load_cascade_topologies(cascade_ids)

        # Process each cascade
        results = {}
        for cascade_id in self.mass_balance.cascade_topologies.keys():
            try:
                cascade_results = await self._process_cascade(
                    cascade_id, start_date, end_date
                )
                results[cascade_id] = cascade_results
                logger.info("Processed cascade", cascade_id=cascade_id)
            except Exception as e:
                logger.error("Failed to process cascade", cascade_id=cascade_id, error=str(e))
                results[cascade_id] = {"status": "error", "error": str(e)}

        # Store results in ClickHouse
        await self._store_hydro_inputs(results)

        return {
            "status": "completed",
            "processed_cascades": len([r for r in results.values() if r.get("status") == "success"]),
            "failed_cascades": len([r for r in results.values() if r.get("status") == "error"]),
            "results": results
        }

    async def _load_cascade_topologies(self, cascade_ids: Optional[List[str]] = None) -> None:
        """Load cascade topologies from ClickHouse."""
        logger.info("Loading cascade topologies")

        # Query hydro system data
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
            cascade_ids_str = ",".join(f"'{cid}'" for cid in cascade_ids)
            query += f" WHERE cascade_id IN ({cascade_ids_str})"

        try:
            rows = await self.clickhouse.query(query)
        except Exception as e:
            logger.error("Failed to load cascade topologies", error=str(e))
            return

        for row in rows:
            try:
                reservoirs = json.loads(row["reservoirs"]) if row["reservoirs"] else []
                seasonal_rules = json.loads(row["seasonal_rules"]) if row["seasonal_rules"] else {}
                water_rights = json.loads(row["water_rights"]) if row["water_rights"] else {}

                cascade_data = {
                    "cascade_id": row["cascade_id"],
                    "cascade_name": row["cascade_name"],
                    "river_system": row["river_system"],
                    "ba_id": row["ba_id"],
                    "region": row["region"],
                    "reservoirs": reservoirs,
                    "seasonal_rules": seasonal_rules,
                    "water_rights": water_rights
                }

                self.mass_balance.load_cascade_topology(cascade_data)

            except Exception as e:
                logger.error("Failed to load cascade topology", cascade_id=row["cascade_id"], error=str(e))

    async def _process_cascade(
        self,
        cascade_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Process a single cascade."""
        logger.info("Processing cascade", cascade_id=cascade_id)

        # Get inflow data for the cascade
        inflow_data = await self._get_inflow_data(cascade_id, start_date, end_date)

        if inflow_data.empty:
            logger.warning("No inflow data found", cascade_id=cascade_id)
            return {"status": "no_data"}

        # Get initial reservoir states
        initial_states = await self._get_initial_states(cascade_id, start_date)

        # Calculate mass balance
        states = self.mass_balance.calculate_mass_balance(
            cascade_id=cascade_id,
            inflow_data=inflow_data,
            initial_states=initial_states,
            time_step_hours=1.0  # Hourly time steps
        )

        # Convert states to ClickHouse format
        hydro_inputs = []
        for state in states:
            hydro_input = {
                "case_id": "default",  # Will be set when used in specific cases
                "cascade_id": cascade_id,
                "reservoir_id": state.reservoir_id,
                "timestamp": state.timestamp,
                "inflow_cfs": state.inflow_cfs,
                "storage_af": state.storage_af,
                "elevation_feet": state.elevation_feet,
                "release_cfs": state.release_cfs,
                "generation_mw": state.generation_mw,
                "head_feet": state.head_feet,
                "efficiency_percent": state.efficiency_percent
            }
            hydro_inputs.append(hydro_input)

        return {
            "status": "success",
            "reservoir_count": len(set(s.reservoir_id for s in states)),
            "time_steps": len(states),
            "inputs_count": len(hydro_inputs)
        }

    async def _get_inflow_data(
        self,
        cascade_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Get inflow data for a cascade."""
        # Query NOAA hydro data or other inflow sources
        # For now, return empty dataframe - would integrate with actual data sources
        query = f"""
        SELECT
            timestamp,
            reservoir_id,
            inflow_cfs
        FROM hydro_inflow_data
        WHERE cascade_id = '{cascade_id}'
        AND timestamp BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY timestamp, reservoir_id
        """

        try:
            rows = await self.clickhouse.query(query)
            return pd.DataFrame(rows)
        except Exception:
            # Return empty dataframe if no data available
            return pd.DataFrame(columns=["timestamp", "reservoir_id", "inflow_cfs"])

    async def _get_initial_states(
        self,
        cascade_id: str,
        start_date: datetime
    ) -> Dict[str, ReservoirState]:
        """Get initial reservoir states for a cascade."""
        # Query latest reservoir states before start_date
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
        AND timestamp < '{start_date}'
        ORDER BY timestamp DESC
        """

        initial_states = {}
        try:
            rows = await self.clickhouse.query(query)
            for row in rows:
                if row["reservoir_id"] not in initial_states:  # Take latest state per reservoir
                    initial_states[row["reservoir_id"]] = ReservoirState(
                        reservoir_id=row["reservoir_id"],
                        timestamp=start_date,
                        storage_af=row["storage_af"],
                        elevation_feet=row["elevation_feet"],
                        inflow_cfs=row["inflow_cfs"],
                        release_cfs=row["release_cfs"],
                        generation_mw=row["generation_mw"],
                        head_feet=row["head_feet"],
                        efficiency_percent=row["efficiency_percent"]
                    )
        except Exception as e:
            logger.warning("Could not load initial states, using defaults", cascade_id=cascade_id, error=str(e))

        return initial_states

    async def _store_hydro_inputs(self, results: Dict[str, Any]) -> None:
        """Store hydro cascade inputs in ClickHouse."""
        # This would batch insert hydro inputs for each cascade
        # For now, just log that we would store them
        logger.info("Would store hydro inputs", cascades_processed=len(results))

        # In a real implementation, this would:
        # 1. Batch the hydro inputs from all cascades
        # 2. Insert into markets.irp_inputs_hydro_cascades table
        # 3. Handle any conflicts or updates


async def run_hydro_preprocessing(
    clickhouse_url: str = "http://localhost:8123",
    database: str = "markets",
    cascade_ids: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """Run hydro cascade preprocessing."""
    client = ClickHouseClient(url=clickhouse_url, database=database)
    await client.connect()

    try:
        preprocessor = HydroPreprocessor(client)

        # Parse dates
        if start_date:
            start_date = datetime.fromisoformat(start_date)
        if end_date:
            end_date = datetime.fromisoformat(end_date)

        results = await preprocessor.preprocess_hydro_cascades(
            cascade_ids=cascade_ids,
            start_date=start_date,
            end_date=end_date
        )

        return results

    finally:
        await client.disconnect()


if __name__ == "__main__":
    import asyncio

    # Example usage
    results = asyncio.run(run_hydro_preprocessing())
    print("Hydro preprocessing completed:", results)
