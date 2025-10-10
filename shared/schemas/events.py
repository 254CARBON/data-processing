"""
Event schema definitions for the data processing pipeline.

Defines the structure of events flowing through the system
with validation and serialization support.
"""

from enum import Enum
from typing import Dict, Any, Optional, List, Union, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import json


class EventType(Enum):
    """Event type enumeration."""
    # Raw ingestion events
    RAW_MARKET_DATA = "ingestion.market.raw.v1"
    
    # Normalized events
    NORMALIZED_TICK = "normalized.market.ticks.v1"
    
    # Enriched events
    ENRICHED_TICK = "enriched.market.ticks.v1"
    
    # Aggregated events
    AGGREGATED_BAR_5M = "aggregated.market.bars.5m.v1"
    AGGREGATED_BAR_1H = "aggregated.market.bars.1h.v1"
    CURVE_UPDATE = "pricing.curve.updates.v1"
    
    # Served events
    SERVED_LATEST_PRICE = "served.market.latest_prices.v1"
    
    # Control events
    INVALIDATION = "projection.invalidate.instrument.v1"
    BACKFILL_REQUEST = "processing.backfill.request.v1"
    JOB_STATUS = "processing.job.status.v1"


@dataclass
class EventEnvelope:
    """Event envelope with metadata."""
    event_type: EventType
    event_id: str
    timestamp: datetime
    tenant_id: Optional[str] = None
    source: str = "data-processing"
    version: str = "1.0"
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EventSchema:
    """Base event schema."""
    envelope: EventEnvelope
    payload: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "envelope": {
                "event_type": self.envelope.event_type.value,
                "event_id": self.envelope.event_id,
                "timestamp": self.envelope.timestamp.isoformat(),
                "tenant_id": self.envelope.tenant_id,
                "source": self.envelope.source,
                "version": self.envelope.version,
                "correlation_id": self.envelope.correlation_id,
                "causation_id": self.envelope.causation_id,
                "metadata": self.envelope.metadata,
            },
            "payload": self.payload,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EventSchema":
        """Create from dictionary."""
        envelope_data = data["envelope"]
        envelope = EventEnvelope(
            event_type=EventType(envelope_data["event_type"]),
            event_id=envelope_data["event_id"],
            timestamp=datetime.fromisoformat(envelope_data["timestamp"]),
            tenant_id=envelope_data["tenant_id"],
            source=envelope_data["source"],
            version=envelope_data["version"],
            correlation_id=envelope_data.get("correlation_id"),
            causation_id=envelope_data.get("causation_id"),
            metadata=envelope_data.get("metadata", {}),
        )
        
        return cls(envelope=envelope, payload=data["payload"])
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "EventSchema":
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


# Raw Market Data Event
@dataclass
class RawMarketDataEvent(EventSchema):
    """Raw market data event from ingestion."""
    
    @classmethod
    def create(
        cls,
        market: str,
        instrument_id: str,
        raw_data: Dict[str, Any],
        tenant_id: str = "default"
    ) -> "RawMarketDataEvent":
        """Create raw market data event."""
        envelope = EventEnvelope(
            event_type=EventType.RAW_MARKET_DATA,
            event_id=f"raw_{market}_{instrument_id}_{int(datetime.now().timestamp())}",
            timestamp=datetime.now(),
            tenant_id=tenant_id,
            source=f"ingestion.{market}",
        )
        
        payload = {
            "market": market,
            "instrument_id": instrument_id,
            "raw_data": raw_data,
        }
        
        return cls(envelope=envelope, payload=payload)


# Normalized Tick Event
@dataclass
class NormalizedTickEvent(EventSchema):
    """Normalized tick event."""
    
    @classmethod
    def create(
        cls,
        instrument_id: str,
        timestamp: datetime,
        price: float,
        volume: float,
        quality_flags: List[str] = None,
        tenant_id: str = "default"
    ) -> "NormalizedTickEvent":
        """Create normalized tick event."""
        envelope = EventEnvelope(
            event_type=EventType.NORMALIZED_TICK,
            event_id=f"norm_{instrument_id}_{int(timestamp.timestamp())}",
            timestamp=datetime.now(),
            tenant_id=tenant_id,
            source="normalization-service",
        )
        
        payload = {
            "instrument_id": instrument_id,
            "timestamp": timestamp.isoformat(),
            "price": price,
            "volume": volume,
            "quality_flags": quality_flags or [],
        }
        
        return cls(envelope=envelope, payload=payload)


# Enriched Tick Event
@dataclass
class EnrichedTickEvent(EventSchema):
    """Enriched tick event with metadata."""
    
    @classmethod
    def create(
        cls,
        instrument_id: str,
        timestamp: Union[datetime, int, float, str],
        price: float,
        volume: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        quality_flags: Optional[List[Union[str, Any]]] = None,
        tenant_id: str = "default",
        tick_id: Optional[str] = None,
        source_event_id: Optional[str] = None,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
        currency: Optional[str] = None,
        unit: Optional[str] = None,
        source_system: Optional[str] = None,
        source_id: Optional[str] = None,
        normalized_at: Optional[Union[datetime, int, float, str]] = None,
        taxonomy: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> "EnrichedTickEvent":
        """Create enriched tick event."""
        event_timestamp = _coerce_event_datetime(timestamp)
        normalized_at_dt = _coerce_event_datetime(normalized_at) if normalized_at is not None else None

        envelope = EventEnvelope(
            event_type=EventType.ENRICHED_TICK,
            event_id=tick_id or f"enrich_{instrument_id}_{int(event_timestamp.timestamp())}",
            timestamp=datetime.now(timezone.utc),
            tenant_id=tenant_id,
            source="enrichment-service",
        )
        
        metadata_payload = dict(metadata or {})
        flags = []
        for flag in quality_flags or []:
            flags.append(flag.value if hasattr(flag, "value") else str(flag))

        payload: Dict[str, Any] = {
            "tick_id": tick_id or f"{instrument_id}_{int(event_timestamp.timestamp())}",
            "source_event_id": source_event_id or tick_id or f"{instrument_id}_{int(event_timestamp.timestamp())}",
            "instrument_id": instrument_id,
            "symbol": symbol or instrument_id,
            "market": market or metadata_payload.get("market", "unknown"),
            "tenant_id": tenant_id,
            "timestamp": event_timestamp.isoformat(),
            "price": price,
            "volume": volume,
            "currency": currency or metadata_payload.get("currency", "USD"),
            "unit": unit or metadata_payload.get("unit", "ton_CO2e"),
            "source_system": source_system or metadata_payload.get("source_system"),
            "source_id": source_id,
            "metadata": metadata_payload,
            "quality_flags": flags,
            "tags": list(tags or []),
        }

        if normalized_at_dt:
            payload["normalized_at"] = normalized_at_dt.isoformat()
        elif normalized_at is not None and isinstance(normalized_at, str):
            payload["normalized_at"] = normalized_at

        if taxonomy:
            payload["taxonomy"] = taxonomy
        else:
            payload["taxonomy"] = None
        
        payload = {
            **payload
        }
        
        return cls(envelope=envelope, payload=payload)


# Aggregated Bar Event
@dataclass
class AggregatedBarEvent(EventSchema):
    """Aggregated OHLC bar event."""
    
    @classmethod
    def create(
        cls,
        instrument_id: str,
        interval: str,
        interval_start: datetime,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: float,
        trade_count: int,
        tenant_id: str = "default"
    ) -> "AggregatedBarEvent":
        """Create aggregated bar event."""
        event_type = EventType.AGGREGATED_BAR_5M if interval == "5m" else EventType.AGGREGATED_BAR_1H
        
        envelope = EventEnvelope(
            event_type=event_type,
            event_id=f"bar_{instrument_id}_{interval}_{int(interval_start.timestamp())}",
            timestamp=datetime.now(),
            tenant_id=tenant_id,
            source="aggregation-service",
        )
        
        payload = {
            "instrument_id": instrument_id,
            "interval": interval,
            "interval_start": interval_start.isoformat(),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "trade_count": trade_count,
        }
        
        return cls(envelope=envelope, payload=payload)


# Curve Update Event
@dataclass
class CurveUpdateEvent(EventSchema):
    """Forward curve update event."""
    
    @classmethod
    def create(
        cls,
        curve_id: str,
        as_of_date: datetime,
        curve_points: List[Dict[str, Any]],
        tenant_id: str = "default"
    ) -> "CurveUpdateEvent":
        """Create curve update event."""
        envelope = EventEnvelope(
            event_type=EventType.CURVE_UPDATE,
            event_id=f"curve_{curve_id}_{int(as_of_date.timestamp())}",
            timestamp=datetime.now(),
            tenant_id=tenant_id,
            source="aggregation-service",
        )
        
        payload = {
            "curve_id": curve_id,
            "as_of_date": as_of_date.isoformat(),
            "curve_points": curve_points,
        }
        
        return cls(envelope=envelope, payload=payload)


# Invalidation Event
@dataclass
class InvalidationEvent(EventSchema):
    """Projection invalidation event."""
    
    @classmethod
    def create(
        cls,
        projection_type: str,
        instrument_id: str,
        reason: str,
        tenant_id: str = "default"
    ) -> "InvalidationEvent":
        """Create invalidation event."""
        envelope = EventEnvelope(
            event_type=EventType.INVALIDATION,
            event_id=f"inv_{projection_type}_{instrument_id}_{int(datetime.now().timestamp())}",
            timestamp=datetime.now(),
            tenant_id=tenant_id,
            source="projection-service",
        )
        
        payload = {
            "projection_type": projection_type,
            "instrument_id": instrument_id,
            "reason": reason,
        }
        
        return cls(envelope=envelope, payload=payload)

MICROSECONDS_IN_SECOND = 1_000_000


def _coerce_event_datetime(value: Union[datetime, int, float, str, None]) -> datetime:
    """Coerce supported timestamp representations into timezone-aware datetimes."""
    if value is None:
        return datetime.now(timezone.utc)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        integer_value = int(value)
        if integer_value > 1_000_000_000_000:
            seconds, micros = divmod(integer_value, MICROSECONDS_IN_SECOND)
            return datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=micros)
        return datetime.fromtimestamp(integer_value, tz=timezone.utc)

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    raise ValueError(f"Unsupported timestamp value: {value!r}")


class _QualityFlagMixin:
    """Mixin providing helpers for quality flag handling."""

    quality_flags: Union[int, Iterable[str], None]

    def _init_quality_flags(self) -> None:
        raw_flags = getattr(self, "quality_flags", None)
        if raw_flags is None:
            self._quality_flags_mask = 0
            self._quality_flag_labels: List[str] = []
            self.quality_flags = 0
            return

        if isinstance(raw_flags, int):
            self._quality_flags_mask = raw_flags
            self._quality_flag_labels = []
            self.quality_flags = raw_flags
        elif isinstance(raw_flags, (list, tuple, set)):
            self._quality_flag_labels = [str(flag) for flag in raw_flags]
            self._quality_flags_mask = 0
            self.quality_flags = list(self._quality_flag_labels)
        else:
            raise ValueError("quality_flags must be an int or iterable of strings")

    @property
    def quality_flags_mask(self) -> int:
        """Integer representation of quality flags (bitmask style)."""
        return getattr(self, "_quality_flags_mask", 0)

    @property
    def quality_flag_labels(self) -> List[str]:
        """List representation of quality flag labels."""
        labels = getattr(self, "_quality_flag_labels", [])
        return list(labels)


@dataclass
class _TickBase:
    """Base class for tick-style events."""

    instrument_id: Optional[str] = None
    ts: Optional[Union[datetime, int, float, str]] = None
    price: Optional[float] = None
    volume: Optional[float] = None
    tenant_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    key: Optional[str] = None
    message_timestamp: Optional[Any] = None

    def __post_init__(self) -> None:
        event_payload: Dict[str, Any] = {}

        envelope: Dict[str, Any] = {}

        if self.payload:
            if isinstance(self.payload, dict):
                # Handle envelope-wrapped payloads
                if "payload" in self.payload and isinstance(self.payload["payload"], dict):
                    event_payload = self.payload["payload"]
                else:
                    event_payload = self.payload
                if "envelope" in self.payload and isinstance(self.payload["envelope"], dict):
                    envelope = self.payload["envelope"]

        # Prefer explicitly provided values, fall back to payload
        self.instrument_id = self.instrument_id or event_payload.get("instrument_id")
        raw_timestamp = self.ts if self.ts is not None else (
            event_payload.get("timestamp") or event_payload.get("ts")
        )
        self.ts = _coerce_event_datetime(raw_timestamp)

        if self.price is None:
            self.price = event_payload.get("price")
        if self.volume is None:
            self.volume = event_payload.get("volume", 0.0)
        if self.tenant_id is None:
            self.tenant_id = event_payload.get("tenant_id") or envelope.get("tenant_id")

        if not self.instrument_id:
            raise ValueError("instrument_id is required")
        if self.tenant_id is None:
            raise ValueError("tenant_id is required")
        if self.price is None:
            raise ValueError("price is required")
        if self.volume is None:
            raise ValueError("volume is required")

        payload_metadata = event_payload.get("metadata") if event_payload else None
        combined_metadata = self.metadata if self.metadata is not None else payload_metadata
        self.metadata = dict(combined_metadata) if combined_metadata else {}

    @property
    def timestamp(self) -> datetime:
        """Alias for the primary timestamp field."""
        return self.ts

    def to_dict(self) -> Dict[str, Any]:
        """Serialise tick to a dictionary."""
        return {
            "instrument_id": self.instrument_id,
            "ts": self.ts.isoformat(),
            "price": self.price,
            "volume": self.volume,
            "tenant_id": self.tenant_id,
            "metadata": self.metadata,
        }


@dataclass
class RawMarketTick(_TickBase):
    """Raw market tick structure used in tests and tooling."""

    source: Optional[str] = None
    raw_data: Optional[Any] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.source is None:
            raise ValueError("source is required for RawMarketTick")
        if self.raw_data is None:
            raise ValueError("raw_data is required for RawMarketTick")

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "source": self.source,
            "raw_data": self.raw_data,
        })
        return base


@dataclass
class NormalizedMarketTick(_QualityFlagMixin, _TickBase):
    """Normalized market tick structure."""

    quality_flags: Union[int, Iterable[str], None] = 0

    def __post_init__(self) -> None:
        super().__post_init__()
        self._init_quality_flags()

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "quality_flags": self.quality_flags,
        })
        return base


@dataclass
class EnrichedMarketTick(_QualityFlagMixin, _TickBase):
    """Enriched market tick structure with taxonomy."""

    quality_flags: Union[int, Iterable[str], None] = 0
    enrichment_data: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    taxonomy: Optional[Dict[str, Any]] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None
    normalized_at: Optional[Union[datetime, int, float, str]] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        payload = self.payload or {}
        event_payload = payload.get("payload", {}) if isinstance(payload, dict) else {}

        enrichment_payload = event_payload.get("enrichment_data")
        if self.enrichment_data is None:
            if enrichment_payload and isinstance(enrichment_payload, dict):
                self.enrichment_data = dict(enrichment_payload)
            else:
                self.enrichment_data = dict(self.metadata)

        if self.tags is None:
            tags_value = event_payload.get("tags")
            self.tags = list(tags_value) if isinstance(tags_value, (list, tuple, set)) else []

        taxonomy_value = self.taxonomy or event_payload.get("taxonomy")
        if taxonomy_value and isinstance(taxonomy_value, dict):
            self.taxonomy = dict(taxonomy_value)
        else:
            self.taxonomy = None

        if self.source_system is None:
            self.source_system = event_payload.get("source_system")
        if self.source_id is None:
            self.source_id = event_payload.get("source_id")

        norm_at_value = (
            self.normalized_at
            if self.normalized_at is not None
            else event_payload.get("normalized_at")
        )
        self.normalized_at = (
            _coerce_event_datetime(norm_at_value)
            if norm_at_value is not None
            else None
        )

        self._init_quality_flags()

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "quality_flags": self.quality_flags,
            "enrichment_data": self.enrichment_data,
            "tags": self.tags,
            "taxonomy": self.taxonomy,
            "source_system": self.source_system,
            "source_id": self.source_id,
            "normalized_at": self.normalized_at.isoformat() if self.normalized_at else None,
        })
        return base
