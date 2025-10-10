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
        if not file_path.exists():
            logger.info("Skipping table; snapshot file not found", table=spec.name, path=str(file_path))
            return

        records = self._read_csv(file_path, spec)
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
