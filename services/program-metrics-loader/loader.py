"""
Utility for loading IRP / RA / RPS / GHG snapshot datasets into ClickHouse.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import structlog

from shared.storage.clickhouse import ClickHouseClient
from data_processing.service_topology.app.wecc_topology import WECC_TOPOLOGY_DATA


logger = structlog.get_logger("program-metrics-loader")


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    if trimmed.endswith("Z"):
        trimmed = trimmed[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(trimmed)
    except ValueError:
        logger.warning("Failed to parse datetime, skipping value", value=value)
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_date(value: Optional[str]) -> Optional[date]:
    dt = _parse_datetime(value)
    if dt is None:
        return None
    return dt.date()


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    candidate = value.strip().replace(",", "")
    if candidate in {"", "NA", "N/A", "null", "None"}:
        return None
    try:
        return float(candidate)
    except ValueError:
        logger.warning("Failed to parse float, skipping value", value=value)
        return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return int(candidate)
    except ValueError:
        logger.warning("Failed to parse int, skipping value", value=value)
        return None


def _parse_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    candidate = value.strip()
    return candidate or None


def _parse_json(value: Optional[str]) -> Optional[str]:
    """Ensure JSON strings are normalized for ClickHouse ingestion."""
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        logger.warning("Failed to parse JSON value; storing raw string", value=value)
        return candidate
    return json.dumps(parsed, separators=(",", ":"))


def _dump_json(value: Any) -> str:
    """Render JSON with compact separators for ClickHouse string columns."""
    return json.dumps(value, separators=(",", ":"))


Converter = Callable[[Optional[str]], Any]


@dataclass(frozen=True)
class TableSpec:
    """Table loading specification."""

    name: str
    filename: str
    converters: Dict[str, Converter]
    optional_columns: Sequence[str] = ()
    post_process: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None

    @property
    def required_columns(self) -> Sequence[str]:
        return tuple(set(self.converters.keys()) - set(self.optional_columns))


TABLE_SPECS: Sequence[TableSpec] = (
    # WECC Topology Tables
    TableSpec(
        name="markets.wecc_topology_ba",
        filename="wecc_topology_ba.json",
        converters={
            "ba_id": _parse_string,
            "ba_name": _parse_string,
            "ba_type": _parse_string,
            "iso_rto": _parse_string,
            "timezone": _parse_string,
            "dst_rules": _parse_json,
            "load_forecast_zone": _parse_string,
            "tags": _parse_json,
            "metadata": _parse_json,
        },
        optional_columns=("iso_rto", "load_forecast_zone", "tags", "metadata"),
    ),
    TableSpec(
        name="markets.wecc_topology_zones",
        filename="wecc_topology_zones.json",
        converters={
            "zone_id": _parse_string,
            "zone_name": _parse_string,
            "ba_id": _parse_string,
            "zone_type": _parse_string,
            "load_serving_entity": _parse_string,
            "weather_zone": _parse_string,
            "peak_demand_mw": _parse_float,
            "planning_area": _parse_string,
            "metadata": _parse_json,
        },
        optional_columns=(
            "load_serving_entity",
            "weather_zone",
            "peak_demand_mw",
            "planning_area",
            "metadata",
        ),
    ),
    TableSpec(
        name="markets.wecc_topology_nodes",
        filename="wecc_topology_nodes.json",
        converters={
            "node_id": _parse_string,
            "node_name": _parse_string,
            "zone_id": _parse_string,
            "node_type": _parse_string,
            "voltage_level_kv": _parse_float,
            "latitude": _parse_float,
            "longitude": _parse_float,
            "capacity_mw": _parse_float,
            "resource_potential": _parse_json,
            "metadata": _parse_json,
        },
        optional_columns=(
            "voltage_level_kv",
            "latitude",
            "longitude",
            "capacity_mw",
            "metadata",
        ),
    ),
    TableSpec(
        name="markets.wecc_topology_paths",
        filename="wecc_topology_paths.json",
        converters={
            "path_id": _parse_string,
            "path_name": _parse_string,
            "from_node": _parse_string,
            "to_node": _parse_string,
            "path_type": _parse_string,
            "capacity_mw": _parse_float,
            "losses_factor": _parse_float,
            "hurdle_rate_usd_per_mwh": _parse_float,
            "wheel_rate_usd_per_mwh": _parse_float,
            "wecc_path_number": _parse_string,
            "operating_limit_mw": _parse_float,
            "emergency_limit_mw": _parse_float,
            "contingency_limit_mw": _parse_float,
            "metadata": _parse_json,
        },
        optional_columns=(
            "hurdle_rate_usd_per_mwh",
            "wheel_rate_usd_per_mwh",
            "wecc_path_number",
            "operating_limit_mw",
            "emergency_limit_mw",
            "contingency_limit_mw",
            "metadata",
        ),
    ),
    TableSpec(
        name="markets.wecc_topology_interties",
        filename="wecc_topology_interties.json",
        converters={
            "intertie_id": _parse_string,
            "intertie_name": _parse_string,
            "from_ba": _parse_string,
            "to_ba": _parse_string,
            "capacity_mw": _parse_float,
            "atc_ttc_ntc": _parse_json,
            "deliverability_flags": _parse_json,
            "edam_eligible": lambda x: str(x).lower() == "true" if x is not None else False,
            "hurdle_rates": _parse_json,
            "metadata": _parse_json,
        },
        optional_columns=("hurdle_rates", "metadata"),
    ),
    TableSpec(
        name="markets.wecc_topology_hubs",
        filename="wecc_topology_hubs.json",
        converters={
            "hub_id": _parse_string,
            "hub_name": _parse_string,
            "zone_id": _parse_string,
            "hub_type": _parse_string,
            "lmp_weighted_nodes": _parse_json,
            "metadata": _parse_json,
        },
        optional_columns=("metadata",),
    ),
    TableSpec(
        name="markets.wecc_interfaces",
        filename="wecc_interfaces.json",
        converters={
            "interface_id": _parse_string,
            "interface_name": _parse_string,
            "interface_type": _parse_string,
            "from_entity": _parse_string,
            "to_entity": _parse_string,
            "directionality": _parse_string,
            "from_zone": _parse_string,
            "to_zone": _parse_string,
            "transfer_capabilities": _parse_json,
            "loss_factors": _parse_json,
            "hurdle_rates": _parse_json,
            "seasonal_variations": _parse_json,
            "flowgate_ids": _parse_json,
            "operational_constraints": _parse_json,
            "regulatory_constraints": _parse_json,
            "edam_participation": _parse_json,
            "metadata": _parse_json,
        },
        optional_columns=(
            "from_zone",
            "to_zone",
            "flowgate_ids",
            "operational_constraints",
            "regulatory_constraints",
            "metadata",
        ),
    ),
    TableSpec(
        name="markets.wecc_path_definitions",
        filename="wecc_path_definitions.json",
        converters={
            "path_number": _parse_string,
            "path_name": _parse_string,
            "path_type": _parse_string,
            "from_location": _parse_string,
            "to_location": _parse_string,
            "rated_mw": _parse_float,
            "operating_mw": _parse_float,
            "emergency_mw": _parse_float,
            "wecc_interfaces": _parse_json,
            "monitored_elements": _parse_json,
            "controlling_entities": _parse_json,
            "notes": _parse_string,
            "metadata": _parse_json,
        },
        optional_columns=("path_type", "notes", "metadata"),
    ),
    TableSpec(
        name="markets.contract_path_definitions",
        filename="contract_path_definitions.json",
        converters={
            "contract_path_id": _parse_string,
            "contract_path_name": _parse_string,
            "source": _parse_string,
            "sink": _parse_string,
            "contract_capacity_mw": _parse_float,
            "transmission_owner": _parse_string,
            "contract_type": _parse_string,
            "rate_schedule": _parse_string,
            "wheeling_rate_usd_per_mwh": _parse_float,
            "service_type": _parse_string,
            "loss_factor": _parse_float,
            "effective_date": _parse_date,
            "termination_date": _parse_date,
            "priority_rights": _parse_json,
            "notes": _parse_string,
            "metadata": _parse_json,
        },
        optional_columns=(
            "rate_schedule",
            "wheeling_rate_usd_per_mwh",
            "loss_factor",
            "effective_date",
            "termination_date",
            "priority_rights",
            "notes",
            "metadata",
        ),
    ),
    # Program Metrics Tables
    TableSpec(
        name="markets.load_demand",
        filename="load_demand.csv",
        converters={
            "timestamp": _parse_datetime,
            "market": _parse_string,
            "ba": _parse_string,
            "zone": _parse_string,
            "demand_mw": _parse_float,
            "data_source": _parse_string,
        },
        optional_columns=("data_source",),
    ),
    TableSpec(
        name="markets.generation_actual",
        filename="generation_actual.csv",
        converters={
            "timestamp": _parse_datetime,
            "market": _parse_string,
            "ba": _parse_string,
            "zone": _parse_string,
            "resource_id": _parse_string,
            "resource_name": _parse_string,
            "resource_type": _parse_string,
            "fuel": _parse_string,
            "output_mw": _parse_float,
            "output_mwh": _parse_float,
            "data_source": _parse_string,
        },
        optional_columns=("resource_name", "data_source", "output_mwh"),
        post_process=lambda row: _ensure_output_mwh(row),
    ),
    TableSpec(
        name="markets.generation_capacity",
        filename="generation_capacity.csv",
        converters={
            "effective_date": _parse_date,
            "market": _parse_string,
            "ba": _parse_string,
            "zone": _parse_string,
            "resource_id": _parse_string,
            "resource_name": _parse_string,
            "resource_type": _parse_string,
            "fuel": _parse_string,
            "nameplate_mw": _parse_float,
            "ucap_factor": _parse_float,
            "ucap_mw": _parse_float,
            "availability_factor": _parse_float,
            "cost_curve": _parse_json,
            "data_source": _parse_string,
        },
        optional_columns=("ba", "zone", "resource_name", "fuel", "availability_factor", "cost_curve", "data_source"),
    ),
    TableSpec(
        name="markets.rec_ledger",
        filename="rec_ledger.csv",
        converters={
            "vintage_year": _parse_int,
            "market": _parse_string,
            "lse": _parse_string,
            "certificate_id": _parse_string,
            "resource_id": _parse_string,
            "mwh": _parse_float,
            "status": _parse_string,
            "retired_year": _parse_int,
            "data_source": _parse_string,
        },
        optional_columns=("resource_id", "retired_year", "data_source"),
    ),
    TableSpec(
        name="markets.emission_factors",
        filename="emission_factors.csv",
        converters={
            "fuel": _parse_string,
            "scope": _parse_string,
            "kg_co2e_per_mwh": _parse_float,
            "source": _parse_string,
            "effective_date": _parse_date,
            "expires_at": _parse_date,
        },
        optional_columns=("expires_at",),
    ),
)


def _ensure_output_mwh(row: Dict[str, Any]) -> Dict[str, Any]:
    """If a row lacks output_mwh, assume 1-hour interval and reuse MW."""
    if row.get("output_mwh") is None and row.get("output_mw") is not None:
        row["output_mwh"] = row["output_mw"]
    return row


def _chunk(records: List[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(records), batch_size):
        yield records[idx : idx + batch_size]


class ProgramMetricsSnapshotLoader:
    """Loads normalized program data snapshots into ClickHouse."""

    def __init__(
        self,
        clickhouse_client: ClickHouseClient,
        snapshot_dir: Path,
        batch_size: int = 5_000,
    ) -> None:
        self.client = clickhouse_client
        self.snapshot_dir = snapshot_dir
        self.batch_size = batch_size

    async def load_all(self) -> None:
        for spec in TABLE_SPECS:
            await self._load_table(spec)

    async def _load_table(self, spec: TableSpec) -> None:
        file_path = self.snapshot_dir / spec.filename
        records: List[Dict[str, Any]] = []
        suffix = file_path.suffix.lower()
        is_json_spec = suffix == ".json"

        if file_path.exists():
            if is_json_spec:
                records = self._read_json_snapshot(file_path, spec)
            elif suffix == ".csv":
                records = self._read_csv(file_path, spec)
            else:
                logger.warning(
                    "Unsupported file type",
                    table=spec.name,
                    path=str(file_path),
                    suffix=file_path.suffix,
                )
                return
        else:
            if not is_json_spec:
                logger.info(
                    "Skipping table; snapshot file not found",
                    table=spec.name,
                    path=str(file_path),
                )

        if not records and is_json_spec:
            records = self._generate_wecc_json_data(spec)
            if records:
                logger.info(
                    "Generated topology data for table",
                    table=spec.name,
                    path=str(file_path),
                )

        if not records:
            logger.warning("No records parsed for table", table=spec.name, path=str(file_path))
            return

        logger.info("Loading records", table=spec.name, path=str(file_path), count=len(records))
        for chunk in _chunk(records, self.batch_size):
            await self.client.insert(spec.name, chunk)

    def _read_csv(self, path: Path, spec: TableSpec) -> List[Dict[str, Any]]:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            missing = [column for column in spec.required_columns if column not in reader.fieldnames]
            if missing:
                logger.warning("Missing required columns; skipping file", path=str(path), missing=missing)
                return []

            parsed_rows: List[Dict[str, Any]] = []
            for row in reader:
                parsed = {}
                for column, converter in spec.converters.items():
                    raw_value = row.get(column)
                    parsed[column] = converter(raw_value)
                if spec.post_process:
                    parsed = spec.post_process(parsed)
                parsed_rows.append(parsed)

        return parsed_rows

    def _read_json_snapshot(self, path: Path, spec: TableSpec) -> List[Dict[str, Any]]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Failed to read JSON snapshot", path=str(path), error=str(exc))
            return []

        if isinstance(payload, list):
            raw_records = payload
        elif isinstance(payload, dict):
            records_value = payload.get("records")
            if isinstance(records_value, list):
                raw_records = records_value
            else:
                raw_records = [payload]
        else:
            logger.warning("Unsupported JSON payload shape", path=str(path), type=type(payload).__name__)
            return []

        parsed_rows: List[Dict[str, Any]] = []
        for entry in raw_records:
            if not isinstance(entry, dict):
                logger.warning("Skipping non-object JSON entry", path=str(path), entry=entry)
                continue

            parsed: Dict[str, Any] = {}
            for column, converter in spec.converters.items():
                raw_value = entry.get(column)
                if raw_value is None:
                    parsed[column] = None
                    continue

                if isinstance(raw_value, (dict, list)):
                    parsed[column] = converter(_dump_json(raw_value))
                elif isinstance(raw_value, (int, float, bool)):
                    parsed[column] = converter(str(raw_value))
                else:
                    parsed[column] = converter(raw_value)

            if spec.post_process:
                parsed = spec.post_process(parsed)
            parsed_rows.append(parsed)

        return parsed_rows

    def _generate_wecc_json_data(self, spec: TableSpec) -> List[Dict[str, Any]]:
        """Generate topology hydration data from the embedded WECC dataset."""
        if "wecc_topology_ba" in spec.filename:
            return self._generate_wecc_ba_data()
        elif "wecc_topology_zones" in spec.filename:
            return self._generate_wecc_zones_data()
        elif "wecc_topology_nodes" in spec.filename:
            return self._generate_wecc_nodes_data()
        elif "wecc_topology_paths" in spec.filename:
            return self._generate_wecc_paths_data()
        elif "wecc_topology_interties" in spec.filename:
            return self._generate_wecc_interties_data()
        elif "wecc_topology_hubs" in spec.filename:
            return self._generate_wecc_hubs_data()
        elif "wecc_interfaces" in spec.filename:
            return self._generate_wecc_interfaces_data()
        elif "wecc_path_definitions" in spec.filename:
            return self._generate_wecc_path_definitions_data()
        elif "contract_path_definitions" in spec.filename:
            return self._generate_contract_path_definitions_data()
        else:
            logger.warning("No topology data generator for file", filename=spec.filename)
            return []

    def _generate_wecc_ba_data(self) -> List[Dict[str, Any]]:
        """Generate WECC balancing authority data."""
        data = []
        for ba in WECC_TOPOLOGY_DATA.get("balancing_authorities", []):
            row = {
                "ba_id": ba["ba_id"],
                "ba_name": ba["ba_name"],
                "ba_type": ba.get("ba_type") or ("iso" if ba.get("iso_rto") else "utility"),
                "iso_rto": ba.get("iso_rto"),
                "timezone": ba["timezone"],
                "dst_rules": _dump_json(ba.get("dst_rules", {})),
                "load_forecast_zone": ba.get("load_forecast_zone"),
                "tags": _dump_json(ba.get("tags", [])),
                "metadata": _dump_json(ba.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_zones_data(self) -> List[Dict[str, Any]]:
        """Generate WECC zones data."""
        data = []
        for zone in WECC_TOPOLOGY_DATA.get("zones", []):
            row = {
                "zone_id": zone["zone_id"],
                "zone_name": zone["zone_name"],
                "ba_id": zone["ba_id"],
                "zone_type": zone["zone_type"],
                "load_serving_entity": zone.get("load_serving_entity"),
                "weather_zone": zone.get("weather_zone"),
                "peak_demand_mw": zone.get("peak_demand_mw"),
                "planning_area": zone.get("planning_area"),
                "metadata": _dump_json(zone.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_nodes_data(self) -> List[Dict[str, Any]]:
        """Generate WECC nodes data."""
        data = []
        for node in WECC_TOPOLOGY_DATA.get("nodes", []):
            row = {
                "node_id": node["node_id"],
                "node_name": node["node_name"],
                "zone_id": node["zone_id"],
                "node_type": node["node_type"],
                "voltage_level_kv": node.get("voltage_level_kv"),
                "latitude": node.get("latitude"),
                "longitude": node.get("longitude"),
                "capacity_mw": node.get("capacity_mw"),
                "resource_potential": _dump_json(node.get("resource_potential", {})),
                "metadata": _dump_json(node.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_paths_data(self) -> List[Dict[str, Any]]:
        """Generate WECC paths data."""
        data = []
        for path in WECC_TOPOLOGY_DATA.get("paths", []):
            row = {
                "path_id": path["path_id"],
                "path_name": path["path_name"],
                "from_node": path["from_node"],
                "to_node": path["to_node"],
                "path_type": path["path_type"],
                "capacity_mw": path["capacity_mw"],
                "losses_factor": path["losses_factor"],
                "hurdle_rate_usd_per_mwh": path.get("hurdle_rate_usd_per_mwh"),
                "wheel_rate_usd_per_mwh": path.get("wheel_rate_usd_per_mwh"),
                "wecc_path_number": path.get("wecc_path_number"),
                "operating_limit_mw": path.get("operating_limit_mw"),
                "emergency_limit_mw": path.get("emergency_limit_mw"),
                "contingency_limit_mw": path.get("contingency_limit_mw"),
                "metadata": _dump_json(path.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_interties_data(self) -> List[Dict[str, Any]]:
        """Generate WECC interties data."""
        data = []
        for intertie in WECC_TOPOLOGY_DATA.get("interties", []):
            row = {
                "intertie_id": intertie["intertie_id"],
                "intertie_name": intertie["intertie_name"],
                "from_ba": intertie.get("from_ba") or intertie.get("from_market"),
                "to_ba": intertie.get("to_ba") or intertie.get("to_market"),
                "capacity_mw": intertie["capacity_mw"],
                "atc_ttc_ntc": _dump_json(intertie.get("atc_ttc_ntc", {})),
                "deliverability_flags": _dump_json(intertie.get("deliverability_flags", [])),
                "edam_eligible": bool(intertie.get("edam_eligible", False)),
                "hurdle_rates": _dump_json(intertie.get("hurdle_rates", {})),
                "metadata": _dump_json(intertie.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_hubs_data(self) -> List[Dict[str, Any]]:
        """Generate WECC hubs data."""
        data = []
        for hub in WECC_TOPOLOGY_DATA.get("hubs", []):
            row = {
                "hub_id": hub["hub_id"],
                "hub_name": hub["hub_name"],
                "zone_id": hub["zone_id"],
                "hub_type": hub["hub_type"],
                "lmp_weighted_nodes": _dump_json(hub.get("lmp_weighted_nodes", [])),
                "metadata": _dump_json(hub.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_interfaces_data(self) -> List[Dict[str, Any]]:
        """Generate WECC interfaces data."""
        data = []
        for interface in WECC_TOPOLOGY_DATA.get("interfaces", []):
            row = {
                "interface_id": interface["interface_id"],
                "interface_name": interface["interface_name"],
                "interface_type": interface["interface_type"],
                "from_entity": interface["from_entity"],
                "to_entity": interface["to_entity"],
                "directionality": interface.get("directionality", "bi_directional"),
                "from_zone": interface.get("from_zone"),
                "to_zone": interface.get("to_zone"),
                "transfer_capabilities": _dump_json(interface.get("transfer_capabilities", {})),
                "loss_factors": _dump_json(interface.get("loss_factors", {})),
                "hurdle_rates": _dump_json(interface.get("hurdle_rates", {})),
                "seasonal_variations": _dump_json(interface.get("seasonal_variations", [])),
                "flowgate_ids": _dump_json(interface.get("flowgate_ids", [])),
                "operational_constraints": _dump_json(interface.get("operational_constraints", {})),
                "regulatory_constraints": _dump_json(interface.get("regulatory_constraints", {})),
                "edam_participation": _dump_json(interface.get("edam_participation", {})),
                "metadata": _dump_json(interface.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_wecc_path_definitions_data(self) -> List[Dict[str, Any]]:
        """Generate WECC path definitions data."""
        data = []
        for path_def in WECC_TOPOLOGY_DATA.get("wecc_paths", []):
            row = {
                "path_number": path_def["path_number"],
                "path_name": path_def["path_name"],
                "path_type": path_def.get("path_type"),
                "from_location": path_def["from_location"],
                "to_location": path_def["to_location"],
                "rated_mw": path_def["rated_mw"],
                "operating_mw": path_def["operating_mw"],
                "emergency_mw": path_def["emergency_mw"],
                "wecc_interfaces": _dump_json(path_def.get("wecc_interfaces", [])),
                "monitored_elements": _dump_json(path_def.get("monitored_elements", [])),
                "controlling_entities": _dump_json(path_def.get("controlling_entities", [])),
                "notes": path_def.get("notes"),
                "metadata": _dump_json(path_def.get("metadata", {})),
            }
            data.append(row)
        return data

    def _generate_contract_path_definitions_data(self) -> List[Dict[str, Any]]:
        """Generate contract path definitions data."""
        data = []
        for contract in WECC_TOPOLOGY_DATA.get("contract_paths", []):
            effective_date = contract.get("effective_date")
            termination_date = contract.get("termination_date")
            row = {
                "contract_path_id": contract["contract_path_id"],
                "contract_path_name": contract["contract_path_name"],
                "source": contract["source"],
                "sink": contract["sink"],
                "contract_capacity_mw": contract["contract_capacity_mw"],
                "transmission_owner": contract["transmission_owner"],
                "contract_type": contract["contract_type"],
                "rate_schedule": contract.get("rate_schedule"),
                "wheeling_rate_usd_per_mwh": contract.get("wheeling_rate_usd_per_mwh"),
                "service_type": contract.get("service_type", "point_to_point"),
                "loss_factor": contract.get("loss_factor"),
                "effective_date": (
                    datetime.fromisoformat(effective_date).date() if effective_date else None
                ),
                "termination_date": (
                    datetime.fromisoformat(termination_date).date() if termination_date else None
                ),
                "priority_rights": _dump_json(contract.get("priority_rights", [])),
                "notes": contract.get("notes"),
                "metadata": _dump_json(contract.get("metadata", {})),
            }
            data.append(row)
        return data


async def load_snapshot_directory(
    snapshot_dir: Path,
    clickhouse_url: str,
    database: str = "markets",
    batch_size: int = 5_000,
) -> None:
    client = ClickHouseClient(url=clickhouse_url, database=database)
    await client.connect()
    try:
        loader = ProgramMetricsSnapshotLoader(client, snapshot_dir, batch_size=batch_size)
        await loader.load_all()
    finally:
        await client.disconnect()


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load program metrics snapshots into ClickHouse.")
    parser.add_argument("--snapshot-dir", type=Path, required=True, help="Directory containing CSV snapshots.")
    parser.add_argument("--clickhouse-url", type=str, default="http://localhost:8123", help="ClickHouse HTTP URL.")
    parser.add_argument("--database", type=str, default="markets", help="Target ClickHouse database.")
    parser.add_argument("--batch-size", type=int, default=5_000, help="Insert batch size.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    if not args.snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot directory not found: {args.snapshot_dir}")
    asyncio.run(
        load_snapshot_directory(
            snapshot_dir=args.snapshot_dir,
            clickhouse_url=args.clickhouse_url,
            database=args.database,
            batch_size=args.batch_size,
        )
    )


if __name__ == "__main__":  # pragma: no cover - CLI invocation
    main()
