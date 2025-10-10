"""
Normalization pipeline for converting raw ticks into canonical events.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol

import structlog
from pydantic import ValidationError

from .config import NormalizeTicksConfig
from .models import (
    NormalizedMarketTick,
    RawMarketTick,
    TickNormalizationQuality,
    build_normalized_envelope,
)

logger = structlog.get_logger(__name__)


class NormalizationError(Exception):
    """Base class for normalization pipeline errors."""


class SchemaValidationError(NormalizationError):
    """Raised when the inbound message fails contract validation."""


class ContractViolationError(NormalizationError):
    """Raised when the inbound payload violates business rules."""

    def __init__(self, message: str, issues: Optional[List[str]] = None) -> None:
        super().__init__(message)
        self.issues = issues or []


class BackfillRepository(Protocol):
    """Protocol for repositories that can hydrate missing payload attributes."""

    async def get_latest_tick(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Return the most recent normalized tick for the instrument."""


@dataclass(slots=True)
class NormalizationResult:
    """Structured result returned by the normalization processor."""

    tick: NormalizedMarketTick
    was_fallback: bool = False
    issues: Optional[List[str]] = None


class NormalizedTickProcessor:
    """Encapsulates the normalization business logic."""

    def __init__(
        self,
        config: NormalizeTicksConfig,
        backfill_repository: Optional[BackfillRepository] = None,
    ) -> None:
        self._config = config
        self._backfill_repository = backfill_repository
        self._producer_id = config.producer_id

    async def normalize(self, raw_message: Dict[str, Any]) -> NormalizationResult:
        """
        Normalize a raw ingestion message into canonical form.

        Raises:
            SchemaValidationError: If the message payload fails schema validation.
            ContractViolationError: If the payload violates business rules.
        """
        try:
            raw_event = RawMarketTick.model_validate(raw_message)
        except ValidationError as exc:
            raise SchemaValidationError(str(exc)) from exc

        payload = raw_event.payload
        source_payload = self._decode_payload(payload.raw_payload, payload.encoding)

        normalized_at = datetime.now(timezone.utc)
        processing_lag_ms = max(
            0,
            int((normalized_at - raw_event.ingested_at).total_seconds() * 1000),
        )

        instrument_id = (
            payload.metadata.get("instrument_id")
            or source_payload.get("instrument_id")
            or payload.symbol
        )
        if not instrument_id:
            raise ContractViolationError(
                "Missing instrument identifier", issues=["missing_instrument_id"]
            )

        market = payload.market.strip().upper()
        symbol = payload.symbol.strip()
        price, price_source, issues = await self._resolve_price(
            source_payload,
            instrument_id=instrument_id,
        )
        volume = self._coerce_float(source_payload.get("volume"))
        bid_price = self._coerce_float(
            source_payload.get("bid_price") or source_payload.get("bid")
        )
        ask_price = self._coerce_float(
            source_payload.get("ask_price") or source_payload.get("ask")
        )
        currency = source_payload.get("currency") or payload.metadata.get("currency", "USD")
        unit = source_payload.get("unit") or payload.metadata.get("unit", "ton_CO2e")

        if price is None:
            raise ContractViolationError(
                "Price is required for normalization",
                issues=(issues or []) + ["missing_price"],
            )

        tick_id = self._compute_tick_id(
            symbol=symbol,
            occurred_at=raw_event.occurred_at,
            source_system=payload.source_system,
        )

        quality_status = "canonical" if price_source == "primary" else "fallback"
        all_issues = issues or []
        if volume is None:
            all_issues.append("missing_volume")
        quality = TickNormalizationQuality(status=quality_status, issues=all_issues)

        normalized_payload = {
            "tick_id": tick_id,
            "source_event_id": raw_event.event_id,
            "symbol": symbol,
            "market": market,
            "instrument_id": instrument_id,
            "price": price,
            "volume": volume,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "currency": currency,
            "unit": unit,
            "normalized_at": normalized_at,
            "processing_lag_ms": processing_lag_ms,
            "source_system": payload.source_system,
            "quality": quality,
            "metadata": {
                "sequence": str(payload.sequence or ""),
                "retry_count": str(payload.retry_count),
                **payload.metadata,
            },
            "tags": source_payload.get("tags", []),
        }

        normalized_event = build_normalized_envelope(
            trace_id=raw_event.trace_id,
            tenant_id=raw_event.tenant_id,
            producer=self._producer_id,
            occurred_at=raw_event.occurred_at,
            ingested_at=raw_event.ingested_at,
            payload=normalized_payload,
        )

        return NormalizationResult(
            tick=normalized_event,
            was_fallback=price_source != "primary",
            issues=issues,
        )

    def _decode_payload(self, raw_payload: str, encoding: str) -> Dict[str, Any]:
        """Decode the raw payload according to the provided encoding."""
        if encoding.lower() == "json":
            try:
                decoded = json.loads(raw_payload)
                if not isinstance(decoded, dict):
                    raise ContractViolationError(
                        "Raw payload must decode to an object", issues=["invalid_payload"]
                    )
                return decoded
            except json.JSONDecodeError as exc:
                raise ContractViolationError(
                    f"Invalid JSON payload: {exc.msg}",
                    issues=["invalid_json"],
                ) from exc

        # Non-JSON encodings are currently unsupported
        raise ContractViolationError(
            f"Unsupported payload encoding: {encoding}",
            issues=["unsupported_encoding"],
        )

    async def _resolve_price(
        self,
        source_payload: Dict[str, Any],
        *,
        instrument_id: str,
    ) -> tuple[Optional[float], str, List[str]]:
        """
        Resolve price using primary payload or optional backfill.

        Returns (price, source, issues).
        """
        candidate = self._coerce_float(
            source_payload.get("price")
            or source_payload.get("last_price")
            or source_payload.get("value")
        )
        if candidate is not None:
            return candidate, "primary", []

        if not self._config.backfill_enabled or not self._backfill_repository:
            return None, "missing", ["missing_price_primary"]

        backfilled = await self._backfill_repository.get_latest_tick(instrument_id)
        if not backfilled:
            return None, "missing", ["missing_price_primary", "backfill_not_found"]

        candidate = self._coerce_float(backfilled.get("price"))
        if candidate is None:
            return None, "missing", ["missing_price_primary", "backfill_invalid"]

        issues = ["price_backfilled"]
        return candidate, "backfill", issues

    def _coerce_float(self, value: Any) -> Optional[float]:
        """Convert value to float when possible."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _compute_tick_id(
        self,
        *,
        symbol: str,
        occurred_at: datetime,
        source_system: str,
    ) -> str:
        """Create a deterministic tick identifier."""
        occurred_micros = int(occurred_at.timestamp() * 1_000_000)
        digest = hashlib.sha256()
        digest.update(symbol.encode("utf-8"))
        digest.update(b"|")
        digest.update(str(occurred_micros).encode("utf-8"))
        digest.update(b"|")
        digest.update(source_system.encode("utf-8"))
        return digest.hexdigest()

