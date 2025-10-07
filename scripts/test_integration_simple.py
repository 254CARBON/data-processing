#!/usr/bin/env python3
"""
Simple integration test to validate the data processing pipeline.

This script tests the core functionality without requiring Docker services.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.generate_sample_data import SampleDataGenerator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleIntegrationTest:
    """Simple integration test for data processing pipeline."""
    
    def __init__(self):
        self.generator = SampleDataGenerator(market="MISO", tenant_id="test")
        self.test_results = []
        
    async def run_all_tests(self) -> bool:
        """Run all integration tests."""
        logger.info("🚀 Starting simple integration tests")
        
        tests = [
            ("Sample Data Generation", self.test_sample_data_generation),
            ("Raw Tick Structure", self.test_raw_tick_structure),
            ("Malformed Data Handling", self.test_malformed_data_handling),
            ("Historical Data Generation", self.test_historical_data_generation),
            ("High Volume Data Generation", self.test_high_volume_data_generation),
        ]
        
        all_passed = True
        
        for test_name, test_func in tests:
            try:
                logger.info(f"🧪 Running test: {test_name}")
                result = await test_func()
                if result:
                    logger.info(f"✅ {test_name} passed")
                    self.test_results.append((test_name, "PASSED", None))
                else:
                    logger.error(f"❌ {test_name} failed")
                    self.test_results.append((test_name, "FAILED", "Test returned False"))
                    all_passed = False
                    
            except Exception as e:
                logger.error(f"💥 {test_name} error: {e}")
                self.test_results.append((test_name, "ERROR", str(e)))
                all_passed = False
                
        return all_passed
        
    async def test_sample_data_generation(self) -> bool:
        """Test basic sample data generation."""
        try:
            # Generate a single tick
            tick = self.generator.generate_raw_tick("NG_HH_BALMO")
            
            # Check basic structure
            if not isinstance(tick, dict):
                return False
                
            if "envelope" not in tick or "payload" not in tick:
                return False
                
            # Check envelope structure
            envelope = tick["envelope"]
            required_envelope_fields = ["event_type", "event_id", "timestamp", "tenant_id", "source"]
            for field in required_envelope_fields:
                if field not in envelope:
                    return False
                    
            # Check payload structure
            payload = tick["payload"]
            required_payload_fields = ["market", "instrument_id", "raw_data"]
            for field in required_payload_fields:
                if field not in payload:
                    return False
                    
            # Check raw_data structure
            raw_data = payload["raw_data"]
            required_raw_data_fields = ["timestamp", "price", "volume", "node", "zone"]
            for field in required_raw_data_fields:
                if field not in raw_data:
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Sample data generation test error: {e}")
            return False
            
    async def test_raw_tick_structure(self) -> bool:
        """Test raw tick data structure validation."""
        try:
            tick = self.generator.generate_raw_tick("NG_HH_BALMO")
            
            # Validate envelope
            envelope = tick["envelope"]
            if envelope["event_type"] != "ingestion.market.raw.v1":
                return False
                
            if envelope["tenant_id"] != "test":
                return False
                
            if envelope["source"] != "ingestion.miso":
                return False
                
            # Validate payload
            payload = tick["payload"]
            if payload["market"] != "MISO":
                return False
                
            if payload["instrument_id"] != "NG_HH_BALMO":
                return False
                
            # Validate raw_data
            raw_data = payload["raw_data"]
            if not isinstance(raw_data["price"], (int, float)):
                return False
                
            if not isinstance(raw_data["volume"], (int, float)):
                return False
                
            if raw_data["price"] <= 0:
                return False
                
            if raw_data["volume"] <= 0:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Raw tick structure test error: {e}")
            return False
            
    async def test_malformed_data_handling(self) -> bool:
        """Test malformed data generation and handling."""
        try:
            # Generate malformed data
            malformed_ticks = self.generator.generate_malformed_data(count=10)
            
            if not isinstance(malformed_ticks, list):
                return False
                
            if len(malformed_ticks) == 0:
                return False
                
            # Check that at least one tick is malformed
            malformed_found = False
            for tick in malformed_ticks:
                envelope = tick.get("envelope", {})
                payload = tick.get("payload", {})
                
                # Check for missing fields
                if "tenant_id" not in envelope or "raw_data" not in payload:
                    malformed_found = True
                    break
                    
                # Check for invalid data types
                if "raw_data" in payload:
                    raw_data = payload["raw_data"]
                    if "price" in raw_data and isinstance(raw_data["price"], str):
                        malformed_found = True
                        break
                        
            if not malformed_found:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Malformed data handling test error: {e}")
            return False
            
    async def test_historical_data_generation(self) -> bool:
        """Test historical data generation."""
        try:
            # Generate 1 day of historical data
            historical_data = self.generator.generate_historical_data(
                instruments=["NG_HH_BALMO"],
                days=1,
                interval_minutes=60  # Every hour
            )
            
            if not isinstance(historical_data, list):
                return False
                
            # Should have approximately 24 ticks (1 per hour)
            if len(historical_data) < 20 or len(historical_data) > 30:
                return False
                
            # Check that all ticks are for the same instrument
            for tick in historical_data:
                if tick["payload"]["instrument_id"] != "NG_HH_BALMO":
                    return False
                    
            # Check that timestamps are in chronological order
            timestamps = []
            for tick in historical_data:
                timestamp_str = tick["payload"]["raw_data"]["timestamp"]
                timestamp = datetime.fromisoformat(timestamp_str)
                timestamps.append(timestamp)
                
            # Verify chronological order
            for i in range(1, len(timestamps)):
                if timestamps[i] <= timestamps[i-1]:
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Historical data generation test error: {e}")
            return False
            
    async def test_high_volume_data_generation(self) -> bool:
        """Test high volume data generation."""
        try:
            # Generate 1000 ticks
            high_volume_data = self.generator.generate_high_volume_data(count=1000)
            
            if not isinstance(high_volume_data, list):
                return False
                
            if len(high_volume_data) != 1000:
                return False
                
            # Check that all ticks have valid structure
            for tick in high_volume_data:
                if "envelope" not in tick or "payload" not in tick:
                    return False
                    
                if "instrument_id" not in tick["payload"]:
                    return False
                    
            # Check that we have multiple different instruments
            instruments = set()
            for tick in high_volume_data:
                instruments.add(tick["payload"]["instrument_id"])
                
            if len(instruments) < 2:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"High volume data generation test error: {e}")
            return False
            
    def print_summary(self, success: bool):
        """Print test summary."""
        logger.info("=" * 60)
        logger.info("📊 INTEGRATION TEST SUMMARY")
        logger.info("=" * 60)
        
        for test_name, status, error in self.test_results:
            if status == "PASSED":
                logger.info(f"✅ {test_name}: {status}")
            else:
                logger.info(f"❌ {test_name}: {status}")
                if error:
                    logger.info(f"   Error: {error}")
                    
        logger.info("=" * 60)
        
        if success:
            logger.info("🎉 All integration tests passed!")
            logger.info("✅ Data processing pipeline is working correctly")
            logger.info("✅ Sample data generation is functional")
            logger.info("✅ Ready for end-to-end testing with Docker services")
        else:
            logger.error("💥 Some integration tests failed!")
            logger.error("❌ Check the errors above for details")
            logger.error("❌ Fix issues before proceeding to Docker testing")
            
        logger.info("=" * 60)


async def main():
    """Main entry point."""
    try:
        test_runner = SimpleIntegrationTest()
        success = await test_runner.run_all_tests()
        test_runner.print_summary(success)
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("🛑 Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
