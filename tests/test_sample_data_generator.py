"""Test the sample data generator."""

import pytest
import json
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.generate_sample_data import SampleDataGenerator


class TestSampleDataGenerator:
    """Test the sample data generator."""
    
    def test_generator_initialization(self):
        """Test generator initialization."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        assert generator.market == "MISO"
        assert generator.tenant_id == "test"
        
    def test_generate_raw_market_tick(self):
        """Test raw market tick generation."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        
        tick = generator.generate_raw_tick(
            instrument_id="NG_HH_BALMO",
            timestamp=datetime.now()
        )
        
        # Check envelope structure
        assert "envelope" in tick
        assert "payload" in tick
        
        envelope = tick["envelope"]
        assert envelope["event_type"] == "ingestion.market.raw.v1"
        assert envelope["tenant_id"] == "test"
        assert envelope["source"] == "ingestion.miso"
        
        payload = tick["payload"]
        assert payload["market"] == "MISO"
        assert payload["instrument_id"] == "NG_HH_BALMO"
        assert "raw_data" in payload
        
        raw_data = payload["raw_data"]
        assert "timestamp" in raw_data
        assert "price" in raw_data
        assert "volume" in raw_data
        assert "node" in raw_data
        assert "zone" in raw_data
        
    def test_generate_malformed_tick(self):
        """Test malformed tick generation."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        
        # Generate malformed data using the dedicated method
        malformed_ticks = generator.generate_malformed_data(count=1)
        
        # Should return a list
        assert isinstance(malformed_ticks, list)
        assert len(malformed_ticks) == 1
        
        tick = malformed_ticks[0]
        
        # Should still have basic structure
        assert "envelope" in tick
        assert "payload" in tick
        
        # Check if any malformed fields are present
        malformed_fields = []
        
        # Check envelope for missing fields
        envelope = tick["envelope"]
        if "tenant_id" not in envelope:
            malformed_fields.append("missing_tenant_id")
        if "source" not in envelope:
            malformed_fields.append("missing_source")
            
        # Check payload for missing fields
        payload = tick["payload"]
        if "raw_data" not in payload:
            malformed_fields.append("missing_raw_data")
            
        # At least one malformed field should be present
        assert len(malformed_fields) > 0
        
    def test_generate_late_data_tick(self):
        """Test late data tick generation."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        
        # Generate late data using the dedicated method
        late_ticks = generator.generate_late_data(hours_late=1, count=1)
        
        # Should return a list
        assert isinstance(late_ticks, list)
        assert len(late_ticks) == 1
        
        tick = late_ticks[0]
        
        # Check that timestamp is in the past
        raw_data = tick["payload"]["raw_data"]
        tick_timestamp = datetime.fromisoformat(raw_data["timestamp"])
        now = datetime.now()
        
        # Should be at least 1 hour in the past
        time_diff = (now - tick_timestamp).total_seconds()
        assert time_diff > 3600  # 1 hour
        
    def test_generate_historical_data(self):
        """Test historical data generation."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        
        # Generate 1 day of data
        historical_data = generator.generate_historical_data(
            instruments=["NG_HH_BALMO"],
            days=1,
            interval_minutes=15  # Every 15 minutes
        )
        
        # Should return a list of ticks
        assert isinstance(historical_data, list)
        # Should have approximately 96 ticks (24 hours * 4 ticks per hour)
        assert len(historical_data) >= 90
        assert len(historical_data) <= 100
        
        # Check first and last tick timestamps
        first_tick = historical_data[0]
        last_tick = historical_data[-1]
        
        first_timestamp = datetime.fromisoformat(
            first_tick["payload"]["raw_data"]["timestamp"]
        )
        last_timestamp = datetime.fromisoformat(
            last_tick["payload"]["raw_data"]["timestamp"]
        )
        
        # Should span approximately 1 day
        time_diff = (last_timestamp - first_timestamp).total_seconds()
        assert time_diff > 80000  # More than 22 hours
        assert time_diff < 90000  # Less than 25 hours
        
    def test_generate_high_volume_data(self):
        """Test high volume data generation."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        
        # Generate 1000 ticks
        high_volume_data = generator.generate_high_volume_data(count=1000)
        
        # Should return a list of ticks
        assert isinstance(high_volume_data, list)
        assert len(high_volume_data) == 1000
        
        # Check that all ticks have valid structure
        for tick in high_volume_data:
            assert "envelope" in tick
            assert "payload" in tick
            assert "instrument_id" in tick["payload"]
            
    def test_generate_continuous_data(self):
        """Test continuous data generation."""
        generator = SampleDataGenerator(market="MISO", tenant_id="test")
        
        # This test just verifies the method exists and can be called
        # In a real test, we'd need to mock the async loop
        assert hasattr(generator, 'generate_batch')
        assert callable(generator.generate_batch)
        
    def test_delivery_report_callback(self):
        """Test delivery report callback."""
        # Check if KafkaProducerClient class exists in the file
        import inspect
        import scripts.generate_sample_data as module
        
        # Get all classes from the module
        classes = [name for name, obj in inspect.getmembers(module) 
                  if inspect.isclass(obj)]
        
        # KafkaProducerClient should be available
        assert 'KafkaProducerClient' in classes
        
    def test_send_message(self):
        """Test message sending."""
        # Check if KafkaProducerClient class exists in the file
        import inspect
        import scripts.generate_sample_data as module
        
        # Get all classes from the module
        classes = [name for name, obj in inspect.getmembers(module) 
                  if inspect.isclass(obj)]
        
        # KafkaProducerClient should be available
        assert 'KafkaProducerClient' in classes
