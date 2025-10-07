"""Unit tests for schema validation."""

import pytest
from datetime import datetime
from shared.schemas.events import RawMarketTick, NormalizedMarketTick, EnrichedMarketTick
from shared.schemas.models import Instrument, Taxonomy
from shared.schemas.registry import SchemaRegistry


class TestEventSchemas:
    """Test event schema validation."""
    
    def test_raw_market_tick_schema(self):
        """Test RawMarketTick schema validation."""
        # Valid tick
        tick = RawMarketTick(
            instrument_id="TEST_INSTRUMENT",
            ts=datetime.now(),
            price=100.0,
            volume=1000,
            source="test",
            raw_data="test_data",
            tenant_id="test_tenant"
        )
        assert tick.instrument_id == "TEST_INSTRUMENT"
        assert tick.price == 100.0
        
        # Invalid tick (missing required field)
        with pytest.raises(ValueError):
            RawMarketTick(
                instrument_id="TEST_INSTRUMENT",
                ts=datetime.now(),
                price=100.0,
                volume=1000,
                source="test",
                raw_data="test_data"
                # Missing tenant_id
            )
            
    def test_normalized_market_tick_schema(self):
        """Test NormalizedMarketTick schema validation."""
        # Valid tick
        tick = NormalizedMarketTick(
            instrument_id="TEST_INSTRUMENT",
            ts=datetime.now(),
            price=100.0,
            volume=1000,
            quality_flags=0,
            tenant_id="test_tenant"
        )
        assert tick.instrument_id == "TEST_INSTRUMENT"
        assert tick.quality_flags == 0
        
    def test_enriched_market_tick_schema(self):
        """Test EnrichedMarketTick schema validation."""
        # Valid tick
        tick = EnrichedMarketTick(
            instrument_id="TEST_INSTRUMENT",
            ts=datetime.now(),
            price=100.0,
            volume=1000,
            quality_flags=0,
            tenant_id="test_tenant",
            enrichment_data={
                "taxonomy": "commodity",
                "region": "us",
                "metadata": "test_metadata"
            }
        )
        assert tick.instrument_id == "TEST_INSTRUMENT"
        assert tick.enrichment_data["taxonomy"] == "commodity"


class TestModelSchemas:
    """Test model schema validation."""
    
    def test_instrument_schema(self):
        """Test Instrument schema validation."""
        # Valid instrument
        instrument = Instrument(
            instrument_id="TEST_INSTRUMENT",
            name="Test Instrument",
            symbol="TEST",
            exchange="TEST_EXCHANGE",
            currency="USD",
            asset_class="commodity",
            tenant_id="test_tenant"
        )
        assert instrument.instrument_id == "TEST_INSTRUMENT"
        assert instrument.asset_class == "commodity"
        
    def test_taxonomy_schema(self):
        """Test Taxonomy schema validation."""
        # Valid taxonomy
        taxonomy = Taxonomy(
            taxonomy_id="TEST_TAXONOMY",
            name="Test Taxonomy",
            description="Test taxonomy description",
            rules={"test": "rule"},
            tenant_id="test_tenant"
        )
        assert taxonomy.taxonomy_id == "TEST_TAXONOMY"
        assert taxonomy.rules["test"] == "rule"


class TestSchemaRegistry:
    """Test SchemaRegistry functionality."""
    
    def test_schema_registration(self):
        """Test schema registration."""
        registry = SchemaRegistry()
        
        # Register a schema
        schema = {
            "type": "object",
            "properties": {
                "test": {"type": "string"}
            }
        }
        
        registry.register_schema("test_schema", schema)
        assert "test_schema" in registry.schemas
        
    def test_schema_validation(self):
        """Test schema validation."""
        registry = SchemaRegistry()
        
        # Register a schema
        schema = {
            "type": "object",
            "properties": {
                "test": {"type": "string"}
            },
            "required": ["test"]
        }
        
        registry.register_schema("test_schema", schema)
        
        # Valid data
        valid_data = {"test": "value"}
        assert registry.validate("test_schema", valid_data)
        
        # Invalid data
        invalid_data = {"test": 123}
        assert not registry.validate("test_schema", invalid_data)
        
    def test_schema_not_found(self):
        """Test handling of unknown schema."""
        registry = SchemaRegistry()
        
        # Try to validate against unknown schema
        with pytest.raises(ValueError):
            registry.validate("unknown_schema", {"test": "value"})

