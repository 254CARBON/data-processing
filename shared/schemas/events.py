"""
Event schema definitions for the data processing pipeline.

Defines the structure of events flowing through the system
with validation and serialization support.
"""

from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
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
    tenant_id: str = "default"
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
        timestamp: datetime,
        price: float,
        volume: float,
        metadata: Dict[str, Any],
        quality_flags: List[str] = None,
        tenant_id: str = "default"
    ) -> "EnrichedTickEvent":
        """Create enriched tick event."""
        envelope = EventEnvelope(
            event_type=EventType.ENRICHED_TICK,
            event_id=f"enrich_{instrument_id}_{int(timestamp.timestamp())}",
            timestamp=datetime.now(),
            tenant_id=tenant_id,
            source="enrichment-service",
        )
        
        payload = {
            "instrument_id": instrument_id,
            "timestamp": timestamp.isoformat(),
            "price": price,
            "volume": volume,
            "metadata": metadata,
            "quality_flags": quality_flags or [],
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

