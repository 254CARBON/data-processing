"""
Schema registry client for event validation.

Provides schema validation and versioning support
for events flowing through the system.
"""

import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
import structlog

from .events import EventSchema, EventType


logger = structlog.get_logger()


@dataclass
class SchemaVersion:
    """Schema version information."""
    version: str
    schema: Dict[str, Any]
    created_at: datetime
    is_active: bool = True


class SchemaRegistry:
    """
    Schema registry for event validation.
    
    Manages schema versions and provides validation
    for events flowing through the system.
    """
    
    def __init__(self):
        self.logger = structlog.get_logger("schema-registry")
        self.schemas: Dict[str, List[SchemaVersion]] = {}
        self._load_default_schemas()
    
    def _load_default_schemas(self) -> None:
        """Load default schemas for all event types."""
        for event_type in EventType:
            self._register_default_schema(event_type)
    
    def _register_default_schema(self, event_type: EventType) -> None:
        """Register default schema for event type."""
        schema = self._get_default_schema(event_type)
        self.register_schema(event_type.value, schema, "1.0")
    
    def _get_default_schema(self, event_type: EventType) -> Dict[str, Any]:
        """Get default schema for event type."""
        base_schema = {
            "type": "object",
            "properties": {
                "envelope": {
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string"},
                        "event_id": {"type": "string"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "tenant_id": {"type": "string"},
                        "source": {"type": "string"},
                        "version": {"type": "string"},
                        "correlation_id": {"type": ["string", "null"]},
                        "causation_id": {"type": ["string", "null"]},
                        "metadata": {"type": "object"},
                    },
                    "required": ["event_type", "event_id", "timestamp", "tenant_id", "source", "version"],
                },
                "payload": {"type": "object"},
            },
            "required": ["envelope", "payload"],
        }
        
        # Add event-specific payload schemas
        if event_type == EventType.RAW_MARKET_DATA:
            base_schema["properties"]["payload"] = {
                "type": "object",
                "properties": {
                    "market": {"type": "string"},
                    "instrument_id": {"type": "string"},
                    "raw_data": {"type": "object"},
                },
                "required": ["market", "instrument_id", "raw_data"],
            }
        
        elif event_type == EventType.NORMALIZED_TICK:
            base_schema["properties"]["payload"] = {
                "type": "object",
                "properties": {
                    "instrument_id": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"},
                    "price": {"type": "number"},
                    "volume": {"type": "number"},
                    "quality_flags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["instrument_id", "timestamp", "price", "volume"],
            }
        
        elif event_type == EventType.ENRICHED_TICK:
            base_schema["properties"]["payload"] = {
                "type": "object",
                "properties": {
                    "instrument_id": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"},
                    "price": {"type": "number"},
                    "volume": {"type": "number"},
                    "metadata": {"type": "object"},
                    "quality_flags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["instrument_id", "timestamp", "price", "volume", "metadata"],
            }
        
        elif event_type in [EventType.AGGREGATED_BAR_5M, EventType.AGGREGATED_BAR_1H]:
            base_schema["properties"]["payload"] = {
                "type": "object",
                "properties": {
                    "instrument_id": {"type": "string"},
                    "interval": {"type": "string"},
                    "interval_start": {"type": "string", "format": "date-time"},
                    "open": {"type": "number"},
                    "high": {"type": "number"},
                    "low": {"type": "number"},
                    "close": {"type": "number"},
                    "volume": {"type": "number"},
                    "trade_count": {"type": "integer"},
                },
                "required": ["instrument_id", "interval", "interval_start", "open", "high", "low", "close", "volume", "trade_count"],
            }
        
        elif event_type == EventType.CURVE_UPDATE:
            base_schema["properties"]["payload"] = {
                "type": "object",
                "properties": {
                    "curve_id": {"type": "string"},
                    "as_of_date": {"type": "string", "format": "date-time"},
                    "curve_points": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "contract_month": {"type": "string"},
                                "price": {"type": "number"},
                                "volume": {"type": "number"},
                                "metadata": {"type": "object"},
                            },
                            "required": ["contract_month", "price", "volume"],
                        },
                    },
                },
                "required": ["curve_id", "as_of_date", "curve_points"],
            }
        
        elif event_type == EventType.INVALIDATION:
            base_schema["properties"]["payload"] = {
                "type": "object",
                "properties": {
                    "projection_type": {"type": "string"},
                    "instrument_id": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["projection_type", "instrument_id", "reason"],
            }
        
        return base_schema
    
    def register_schema(self, event_type: str, schema: Dict[str, Any], version: str = "1.0") -> None:
        """Register a new schema version."""
        if event_type not in self.schemas:
            self.schemas[event_type] = []
        
        schema_version = SchemaVersion(
            version=version,
            schema=schema,
            created_at=datetime.now(),
        )
        
        self.schemas[event_type].append(schema_version)
        
        self.logger.info(
            "Schema registered",
            event_type=event_type,
            version=version
        )
    
    def get_schema(self, event_type: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get schema for event type and version."""
        if event_type not in self.schemas:
            return None
        
        versions = self.schemas[event_type]
        
        if version:
            # Get specific version
            for schema_version in versions:
                if schema_version.version == version and schema_version.is_active:
                    return schema_version.schema
        else:
            # Get latest active version
            active_versions = [v for v in versions if v.is_active]
            if active_versions:
                latest = max(active_versions, key=lambda v: v.created_at)
                return latest.schema

        return None

    def validate(self, schema_name: str, data: Dict[str, Any], version: Optional[str] = None) -> bool:
        """Validate arbitrary data against a registered schema.

        This lightweight helper checks required keys defined on the stored
        JSON schema representation. It is intentionally simple because the
        repository's production services rely on dedicated Avro/JSON schema
        toolchains. The helper exists to keep unit tests exercising simple
        validation logic without heavy dependencies.
        """
        schema = self.get_schema(schema_name, version)
        if schema is None:
            raise ValueError(f"Schema not found for {schema_name}")

        required_keys = schema.get("required", [])
        for key in required_keys:
            if key not in data or data[key] is None:
                return False

        type_mapping = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "object": dict,
            "array": list,
            "boolean": bool,
        }

        properties = schema.get("properties", {})
        for field, definition in properties.items():
            if field not in data:
                continue

            expected_type = definition.get("type")
            if expected_type in type_mapping and not isinstance(data[field], type_mapping[expected_type]):
                return False

        return True
    
    def validate_event(self, event: EventSchema) -> bool:
        """Validate event against schema."""
        try:
            schema = self.get_schema(event.envelope.event_type.value)
            if not schema:
                self.logger.warning(
                    "No schema found for event type",
                    event_type=event.envelope.event_type.value
                )
                return False
            
            # Basic validation - in production, use jsonschema library
            event_dict = event.to_dict()
            
            # Check required fields
            if "envelope" not in event_dict or "payload" not in event_dict:
                return False
            
            envelope = event_dict["envelope"]
            required_envelope_fields = ["event_type", "event_id", "timestamp", "tenant_id", "source", "version"]
            for field in required_envelope_fields:
                if field not in envelope:
                    return False
            
            # Check event type matches
            if envelope["event_type"] != event.envelope.event_type.value:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(
                "Event validation error",
                error=str(e),
                event_type=event.envelope.event_type.value
            )
            return False
    
    def get_supported_event_types(self) -> List[str]:
        """Get list of supported event types."""
        return list(self.schemas.keys())
    
    def get_schema_versions(self, event_type: str) -> List[str]:
        """Get available schema versions for event type."""
        if event_type not in self.schemas:
            return []
        
        return [v.version for v in self.schemas[event_type] if v.is_active]
    
    def deprecate_schema(self, event_type: str, version: str) -> None:
        """Deprecate a schema version."""
        if event_type not in self.schemas:
            return
        
        for schema_version in self.schemas[event_type]:
            if schema_version.version == version:
                schema_version.is_active = False
                self.logger.info(
                    "Schema deprecated",
                    event_type=event_type,
                    version=version
                )
                break
