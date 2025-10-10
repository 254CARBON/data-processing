"""
Data models used by the normalize-ticks service.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

MICROSECONDS_IN_SECOND = 1_000_000


def _ensure_datetime(value: Any) -> datetime:
    """Coerce supported timestamp representations into UTC datetimes."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        integer_value = int(value)
        # Heuristic: values < 1e12 are considered seconds, otherwise microseconds.
        if integer_value < 1_000_000_000_000:
            return datetime.fromtimestamp(integer_value, tz=timezone.utc)
        seconds, micros = divmod(integer_value, MICROSECONDS_IN_SECOND)
        return datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(
            microseconds=micros
        )

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported timestamp format: {value}") from exc
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raise TypeError(f"Unsupported timestamp type: {type(value)!r}")


def datetime_to_micros(value: datetime) -> int:
    """Convert aware datetime to microseconds since epoch."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    seconds = int(value.timestamp())
    return seconds * MICROSECONDS_IN_SECOND + value.microsecond


class MarketTickRawPayload(BaseModel):
    """Payload portion of a raw market tick event."""

    model_config = ConfigDict(extra="allow")

    source_system: str
    market: str
    symbol: str
    sequence: Optional[int] = None
    received_at: datetime
    raw_payload: str
    encoding: str = "json"
    metadata: Dict[str, str] = Field(default_factory=dict)
    retry_count: int = 0
    checksum: Optional[str] = None

    @field_validator("received_at", mode="before")
    @classmethod
    def _coerce_received_at(cls, value: Any) -> datetime:
        return _ensure_datetime(value)


class RawMarketTick(BaseModel):
    """Top-level raw tick event after schema validation."""

    model_config = ConfigDict(extra="allow")

    event_id: str
    trace_id: str
    schema_version: str = "1.0.0"
    tenant_id: str = "default"
    producer: str
    occurred_at: datetime
    ingested_at: datetime
    payload: MarketTickRawPayload

    @field_validator("occurred_at", "ingested_at", mode="before")
    @classmethod
    def _coerce_datetimes(cls, value: Any) -> datetime:
        return _ensure_datetime(value)


class TickNormalizationQuality(BaseModel):
    """Quality metadata for a normalized tick."""

    model_config = ConfigDict(extra="allow")

    status: str = "canonical"
    issues: List[str] = Field(default_factory=list)


class MarketTickNormalizedPayload(BaseModel):
    """Payload portion of a normalized market tick event."""

    model_config = ConfigDict(extra="allow")

    tick_id: str
    source_event_id: str
    symbol: str
    market: str
    instrument_id: str
    price: float
    volume: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    currency: str = "USD"
    unit: str = "ton_CO2e"
    normalized_at: datetime
    processing_lag_ms: int
    source_system: str
    quality: TickNormalizationQuality = Field(default_factory=TickNormalizationQuality)
    metadata: Dict[str, str] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)

    @field_validator("normalized_at", mode="before")
    @classmethod
    def _coerce_normalized_at(cls, value: Any) -> datetime:
        return _ensure_datetime(value)


class MarketTickNormalizedEnvelope(BaseModel):
    """Envelope for normalized tick event."""

    model_config = ConfigDict(extra="allow")

    event_id: str
    trace_id: str
    schema_version: str = "1.0.0"
    tenant_id: str = "default"
    producer: str
    occurred_at: datetime
    ingested_at: datetime
    payload: MarketTickNormalizedPayload

    @field_validator("occurred_at", "ingested_at", mode="before")
    @classmethod
    def _coerce_datetimes(cls, value: Any) -> datetime:
        return _ensure_datetime(value)


class NormalizedMarketTick(BaseModel):
    """Complete normalized tick event ready for emission."""

    model_config = ConfigDict(extra="allow")

    envelope: MarketTickNormalizedEnvelope

    def to_message(self) -> Dict[str, Any]:
        """Serialize the normalized tick into a Kafka-friendly dictionary."""
        envelope = self.envelope
        payload = envelope.payload
        quality = payload.quality

        return {
            "event_id": envelope.event_id,
            "trace_id": envelope.trace_id,
            "schema_version": envelope.schema_version,
            "tenant_id": envelope.tenant_id,
            "producer": envelope.producer,
            "occurred_at": datetime_to_micros(envelope.occurred_at),
            "ingested_at": datetime_to_micros(envelope.ingested_at),
            "payload": {
                "tick_id": payload.tick_id,
                "source_event_id": payload.source_event_id,
                "symbol": payload.symbol,
                "market": payload.market,
                "instrument_id": payload.instrument_id,
                "price": payload.price,
                "volume": payload.volume,
                "bid_price": payload.bid_price,
                "ask_price": payload.ask_price,
                "currency": payload.currency,
                "unit": payload.unit,
                "normalized_at": datetime_to_micros(payload.normalized_at),
                "processing_lag_ms": payload.processing_lag_ms,
                "source_system": payload.source_system,
                "quality": {
                    "status": quality.status,
                    "issues": list(quality.issues),
                },
                "metadata": dict(payload.metadata),
                "tags": list(payload.tags),
            },
        }


def build_normalized_envelope(
    *,
    trace_id: str,
    tenant_id: str,
    producer: str,
    occurred_at: datetime,
    ingested_at: datetime,
    payload: Dict[str, Any],
) -> NormalizedMarketTick:
    """Helper factory for creating normalized tick envelopes."""
    normalized_payload = MarketTickNormalizedPayload(**payload)

    envelope = MarketTickNormalizedEnvelope(
        event_id=str(uuid4()),
        trace_id=trace_id,
        schema_version="1.0.0",
        tenant_id=tenant_id,
        producer=producer,
        occurred_at=occurred_at,
        ingested_at=ingested_at,
        payload=normalized_payload,
    )

    return NormalizedMarketTick(envelope=envelope)

