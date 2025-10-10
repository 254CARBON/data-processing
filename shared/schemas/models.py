"""
Data models for the data processing pipeline.

Defines the core data structures used throughout the system
with validation and serialization support.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum


class QualityFlag(Enum):
    """Data quality flags."""
    VALID = "valid"
    PRICE_NEGATIVE = "price_negative"
    VOLUME_SPIKE = "volume_spike"
    MISSING_METADATA = "missing_metadata"
    LATE_ARRIVAL = "late_arrival"
    DUPLICATE = "duplicate"
    OUT_OF_RANGE = "out_of_range"


@dataclass
class TickData:
    """Normalized tick data."""
    instrument_id: str
    timestamp: datetime
    price: float
    volume: float
    quality_flags: List[QualityFlag] = field(default_factory=list)
    tenant_id: str = "default"
    source_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    tick_id: Optional[str] = None
    source_event_id: Optional[str] = None
    symbol: Optional[str] = None
    market: Optional[str] = None
    currency: str = "USD"
    unit: str = "ton_CO2e"
    normalized_at: Optional[datetime] = None
    source_system: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    taxonomy: Optional[Dict[str, Any]] = None
    
    def add_quality_flag(self, flag: QualityFlag) -> None:
        """Add a quality flag."""
        if flag not in self.quality_flags:
            self.quality_flags.append(flag)
    
    def has_quality_flag(self, flag: QualityFlag) -> bool:
        """Check if quality flag is present."""
        return flag in self.quality_flags
    
    def is_valid(self) -> bool:
        """Check if tick is valid."""
        return QualityFlag.VALID in self.quality_flags or not self.quality_flags
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "instrument_id": self.instrument_id,
            "timestamp": self.timestamp.isoformat(),
            "price": self.price,
            "volume": self.volume,
            "quality_flags": [flag.value for flag in self.quality_flags],
            "tenant_id": self.tenant_id,
            "source_id": self.source_id,
            "metadata": self.metadata,
            "tick_id": self.tick_id,
            "source_event_id": self.source_event_id,
            "symbol": self.symbol,
            "market": self.market,
            "currency": self.currency,
            "unit": self.unit,
            "normalized_at": self.normalized_at.isoformat() if self.normalized_at else None,
            "source_system": self.source_system,
            "tags": [tag for tag in self.tags],
            "taxonomy": self.taxonomy,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TickData":
        """Create from dictionary."""
        return cls(
            instrument_id=data["instrument_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            price=data["price"],
            volume=data["volume"],
            quality_flags=[QualityFlag(flag) for flag in data.get("quality_flags", [])],
            tenant_id=data.get("tenant_id", "default"),
            source_id=data.get("source_id"),
            metadata=data.get("metadata", {}),
            tick_id=data.get("tick_id"),
            source_event_id=data.get("source_event_id"),
            symbol=data.get("symbol"),
            market=data.get("market"),
            currency=data.get("currency", "USD"),
            unit=data.get("unit", "ton_CO2e"),
            normalized_at=datetime.fromisoformat(data["normalized_at"]) if data.get("normalized_at") else None,
            source_system=data.get("source_system"),
            tags=data.get("tags", []),
            taxonomy=data.get("taxonomy"),
        )


@dataclass
class BarData:
    """OHLC bar data."""
    instrument_id: str
    interval: str
    interval_start: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    trade_count: int
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "instrument_id": self.instrument_id,
            "interval": self.interval,
            "interval_start": self.interval_start.isoformat(),
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "tenant_id": self.tenant_id,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BarData":
        """Create from dictionary."""
        return cls(
            instrument_id=data["instrument_id"],
            interval=data["interval"],
            interval_start=datetime.fromisoformat(data["interval_start"]),
            open_price=data["open"],
            high_price=data["high"],
            low_price=data["low"],
            close_price=data["close"],
            volume=data["volume"],
            trade_count=data["trade_count"],
            tenant_id=data.get("tenant_id", "default"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class CurvePoint:
    """Forward curve point."""
    contract_month: str
    price: float
    volume: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "contract_month": self.contract_month,
            "price": self.price,
            "volume": self.volume,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CurvePoint":
        """Create from dictionary."""
        return cls(
            contract_month=data["contract_month"],
            price=data["price"],
            volume=data["volume"],
            metadata=data.get("metadata", {}),
        )


@dataclass
class CurveData:
    """Forward curve data."""
    curve_id: str
    as_of_date: datetime
    curve_points: List[CurvePoint]
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "curve_id": self.curve_id,
            "as_of_date": self.as_of_date.isoformat(),
            "curve_points": [point.to_dict() for point in self.curve_points],
            "tenant_id": self.tenant_id,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CurveData":
        """Create from dictionary."""
        return cls(
            curve_id=data["curve_id"],
            as_of_date=datetime.fromisoformat(data["as_of_date"]),
            curve_points=[CurvePoint.from_dict(point) for point in data["curve_points"]],
            tenant_id=data.get("tenant_id", "default"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class InstrumentMetadata:
    """Instrument metadata."""
    instrument_id: str
    commodity: str
    region: str
    product_tier: str
    unit: str
    contract_size: float
    tick_size: float
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "instrument_id": self.instrument_id,
            "commodity": self.commodity,
            "region": self.region,
            "product_tier": self.product_tier,
            "unit": self.unit,
            "contract_size": self.contract_size,
            "tick_size": self.tick_size,
            "tenant_id": self.tenant_id,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InstrumentMetadata":
        """Create from dictionary."""
        return cls(
            instrument_id=data["instrument_id"],
            commodity=data["commodity"],
            region=data["region"],
            product_tier=data["product_tier"],
            unit=data["unit"],
            contract_size=data["contract_size"],
            tick_size=data["tick_size"],
            tenant_id=data.get("tenant_id", "default"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Instrument:
    """Core instrument definition for schema validation and tests."""

    instrument_id: str
    name: str
    symbol: str
    exchange: str
    currency: str
    asset_class: str
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        required = {
            "instrument_id": self.instrument_id,
            "name": self.name,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "currency": self.currency,
            "asset_class": self.asset_class,
            "tenant_id": self.tenant_id,
        }
        missing = [key for key, value in required.items() if value in (None, "")]
        if missing:
            raise ValueError(f"Instrument missing required fields: {', '.join(missing)}")

        if not isinstance(self.metadata, dict):
            raise ValueError("metadata must be a dictionary")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "name": self.name,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "currency": self.currency,
            "asset_class": self.asset_class,
            "tenant_id": self.tenant_id,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Instrument":
        return cls(
            instrument_id=data["instrument_id"],
            name=data["name"],
            symbol=data["symbol"],
            exchange=data["exchange"],
            currency=data["currency"],
            asset_class=data["asset_class"],
            tenant_id=data.get("tenant_id", "default"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Taxonomy:
    """Taxonomy definition used by enrichment services."""

    taxonomy_id: str
    name: str
    description: str
    rules: Dict[str, Any]
    tenant_id: str = "default"

    def __post_init__(self) -> None:
        required = {
            "taxonomy_id": self.taxonomy_id,
            "name": self.name,
            "description": self.description,
            "tenant_id": self.tenant_id,
        }
        missing = [key for key, value in required.items() if value in (None, "")]
        if missing:
            raise ValueError(f"Taxonomy missing required fields: {', '.join(missing)}")

        if not isinstance(self.rules, dict):
            raise ValueError("rules must be a dictionary")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "taxonomy_id": self.taxonomy_id,
            "name": self.name,
            "description": self.description,
            "rules": self.rules,
            "tenant_id": self.tenant_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Taxonomy":
        return cls(
            taxonomy_id=data["taxonomy_id"],
            name=data["name"],
            description=data.get("description", ""),
            rules=data.get("rules", {}),
            tenant_id=data.get("tenant_id", "default"),
        )


@dataclass
class ProjectionData:
    """Projection data."""
    projection_type: str
    instrument_id: str
    data: Dict[str, Any]
    last_updated: datetime
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "projection_type": self.projection_type,
            "instrument_id": self.instrument_id,
            "data": self.data,
            "last_updated": self.last_updated.isoformat(),
            "tenant_id": self.tenant_id,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProjectionData":
        """Create from dictionary."""
        return cls(
            projection_type=data["projection_type"],
            instrument_id=data["instrument_id"],
            data=data["data"],
            last_updated=datetime.fromisoformat(data["last_updated"]),
            tenant_id=data.get("tenant_id", "default"),
            metadata=data.get("metadata", {}),
        )
