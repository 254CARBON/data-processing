"""
Optional ClickHouse repository used for backfill enrichment.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import structlog

from shared.storage.clickhouse import ClickHouseClient

from .config import NormalizeTicksConfig

logger = structlog.get_logger(__name__)


class ClickHouseBackfillRepository:
    """Fetches previously normalized ticks to hydrate missing attributes."""

    def __init__(self, config: NormalizeTicksConfig) -> None:
        self._config = config
        self._enabled = bool(config.database.clickhouse_url) and config.backfill_enabled
        self._client: Optional[ClickHouseClient] = None

        if self._enabled:
            self._client = ClickHouseClient(config.database.clickhouse_url)

    @property
    def enabled(self) -> bool:
        """Return True when ClickHouse backfill is active."""
        return self._enabled and self._client is not None

    async def startup(self) -> None:
        """Initialize the ClickHouse client."""
        if not self.enabled:
            return

        assert self._client is not None  # For mypy
        try:
            await self._client.connect()
            logger.info(
                "Backfill repository connected",
                table=self._config.backfill_table,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Failed to connect to ClickHouse", error=str(exc))
            self._enabled = False

    async def shutdown(self) -> None:
        """Tear down ClickHouse client."""
        if not self.enabled:
            return

        assert self._client is not None
        try:
            await self._client.disconnect()
            logger.info("Backfill repository disconnected")
        finally:
            self._client = None

    async def get_latest_tick(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve the latest normalized tick for the provided instrument."""
        if not self.enabled:
            return None

        assert self._client is not None
        query = f"""
            SELECT
                price,
                volume,
                bid_price,
                ask_price,
                currency,
                unit
            FROM {self._config.backfill_table}
            WHERE instrument_id = {{instrument_id:String}}
            ORDER BY normalized_at DESC
            LIMIT 1
        """

        try:
            rows = await self._client.query(
                query,
                params={"instrument_id": instrument_id},
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "Backfill query failed",
                error=str(exc),
                instrument_id=instrument_id,
            )
            return None

        if not rows:
            return None

        return rows[0]

