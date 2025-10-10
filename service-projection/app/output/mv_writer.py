"""Materialized view writer for ClickHouse."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog

from shared.schemas.models import ProjectionData
from shared.storage.clickhouse import ClickHouseClient, ClickHouseConfig
from shared.utils.errors import DataProcessingError


logger = structlog.get_logger(__name__)


def _serialise_timestamp(raw_ts: Any, fallback: datetime) -> str:
    """Convert incoming timestamp payloads to ClickHouse-friendly UTC strings."""
    if isinstance(raw_ts, datetime):
        candidate = raw_ts
    elif isinstance(raw_ts, str):
        normalised = raw_ts.replace("Z", "+00:00")
        try:
            candidate = datetime.fromisoformat(normalised)
        except ValueError:
            logger.warning("Invalid timestamp string for served projection, falling back", value=raw_ts)
            candidate = fallback
    else:
        candidate = fallback

    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)
    else:
        candidate = candidate.astimezone(timezone.utc)

    return candidate.strftime("%Y-%m-%d %H:%M:%S.%f")


def _ensure_list(value: Any) -> list[Any]:
    """Normalise optional list inputs (quality flags) to a ClickHouse-friendly list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _merge_metadata(projection: ProjectionData, payload: Dict[str, Any]) -> str:
    """Merge projection-level metadata with payload metadata and serialise as JSON."""
    merged: Dict[str, Any] = {}
    if isinstance(projection.metadata, dict):
        merged.update(projection.metadata)

    payload_metadata = payload.get("metadata")
    if isinstance(payload_metadata, dict):
        merged.update(payload_metadata)

    return json.dumps(merged, separators=(",", ":"))


class MaterializedViewWriter:
    """Writer for materialized view updates."""

    def __init__(self, config):
        self.config = config
        clickhouse_url = f"http://{config.clickhouse_host}:{config.clickhouse_port}"
        self.client = ClickHouseClient(
            ClickHouseConfig(
                url=clickhouse_url,
                database=config.clickhouse_database,
                username=config.clickhouse_user or None,
                password=config.clickhouse_password or None,
                timeout=config.processing_timeout,
                max_connections=config.clickhouse_pool_max,
            )
        )

    async def start(self) -> None:
        """Start the materialized view writer."""
        await self.client.connect()
        logger.info("Materialized view writer started")

    async def stop(self) -> None:
        """Stop the materialized view writer."""
        await self.client.close()
        logger.info("Materialized view writer stopped")

    async def write_latest_price(self, projection: Optional[ProjectionData]) -> None:
        """Write latest price projection to materialized view."""
        if projection is None:
            return

        try:
            data = projection.data
            price = data.get("price")
            if price is None:
                logger.warning(
                    "Skipping latest price projection with missing price",
                    instrument_id=projection.instrument_id,
                    tenant_id=projection.tenant_id,
                )
                return

            snapshot_at = _serialise_timestamp(
                data.get("timestamp"),
                projection.last_updated,
            )
            metadata_json = _merge_metadata(projection, data)
            source = data.get("source") or projection.metadata.get("source") if isinstance(projection.metadata, dict) else None

            row = {
                "tenant_id": projection.tenant_id,
                "instrument_id": projection.instrument_id,
                "price": price,
                "volume": data.get("volume", 0),
                "quality_flags": _ensure_list(data.get("quality_flags")),
                "source": source or "unknown",
                "snapshot_at": snapshot_at,
                "projection_type": projection.projection_type or "latest_price",
                "metadata": metadata_json,
            }

            await self.client.insert("served_latest", [row])
            logger.debug(
                "Updated materialized view for latest price",
                instrument_id=projection.instrument_id,
                tenant_id=projection.tenant_id,
            )
        except Exception as exc:
            logger.error("Error writing latest price to materialized view", error=str(exc))
            raise DataProcessingError(f"Failed to write latest price to materialized view: {exc}") from exc

    async def write_curve_snapshot(self, projection: Optional[ProjectionData]) -> None:
        """Write curve snapshot projection to materialized view."""
        if projection is None:
            return

        try:
            data = projection.data
            horizon = data.get("horizon")
            curve_points = data.get("curve_points", [])

            if not horizon or not curve_points:
                logger.warning(
                    "Skipping curve snapshot projection with missing fields",
                    instrument_id=projection.instrument_id,
                    tenant_id=projection.tenant_id,
                    horizon=horizon,
                    has_curve_points=bool(curve_points),
                )
                return

            snapshot_at = _serialise_timestamp(
                data.get("timestamp"),
                projection.last_updated,
            )
            metadata_json = _merge_metadata(projection, data)

            if not isinstance(curve_points, (list, tuple)):
                logger.warning(
                    "Curve points payload is not iterable; coercing to empty list",
                    instrument_id=projection.instrument_id,
                    tenant_id=projection.tenant_id,
                    type=type(curve_points).__name__,
                )
                curve_points = []

            row = {
                "tenant_id": projection.tenant_id,
                "instrument_id": projection.instrument_id,
                "horizon": str(horizon),
                "curve_points": json.dumps(curve_points, separators=(",", ":")),
                "interpolation_method": data.get("interpolation_method", "linear"),
                "quality_flags": _ensure_list(data.get("quality_flags")),
                "snapshot_at": snapshot_at,
                "projection_type": projection.projection_type or "curve_snapshot",
                "metadata": metadata_json,
            }

            await self.client.insert("served_curve_snapshots", [row])
            logger.debug(
                "Updated materialized view for curve snapshot",
                instrument_id=projection.instrument_id,
                tenant_id=projection.tenant_id,
                horizon=row["horizon"],
            )
        except Exception as exc:
            logger.error("Error writing curve snapshot to materialized view", error=str(exc))
            raise DataProcessingError(f"Failed to write curve snapshot to materialized view: {exc}") from exc
