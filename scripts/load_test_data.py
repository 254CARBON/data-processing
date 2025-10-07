#!/usr/bin/env python3
"""
Load test data script for data processing services.

Generates and loads test data for development and testing.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any, List
import argparse
import structlog
import json
from datetime import datetime, timedelta
import uuid
import random


logger = structlog.get_logger()


class TestDataLoader:
    """Test data loader for data processing services."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("test-data-loader")
        
        # Test data configuration
        self.instruments = [
            "NG_HH_BALMO",
            "NG_HH_PROMO", 
            "NG_HH_STD",
            "ELEC_MISO_BALMO",
            "ELEC_CAISO_BALMO",
            "ELEC_ERCOT_BALMO",
            "CRUDE_WTI_BALMO",
            "CRUDE_BRENT_BALMO"
        ]
        
        self.markets = ["miso", "caiso", "ercot"]
        
        # Metrics
        self.records_generated = 0
        self.start_time = None
    
    async def start(self) -> None:
        """Start the test data loader."""
        self.logger.info("Starting test data loader")
        self.start_time = datetime.now()
    
    def generate_raw_market_data(self, count: int = 100) -> List[Dict[str, Any]]:
        """Generate raw market data records."""
        records = []
        
        for i in range(count):
            # Select random instrument and market
            instrument = random.choice(self.instruments)
            market = random.choice(self.markets)
            
            # Generate timestamp (last 24 hours)
            timestamp = datetime.now() - timedelta(hours=random.randint(0, 24))
            
            # Generate price data
            base_price = random.uniform(20.0, 100.0)
            price = base_price + random.uniform(-5.0, 5.0)
            volume = random.uniform(100.0, 10000.0)
            
            # Create record
            record = {
                "envelope": {
                    "event_type": "ingestion.market.raw.v1",
                    "event_id": str(uuid.uuid4()),
                    "timestamp": timestamp.isoformat(),
                    "tenant_id": "default",
                    "source": f"ingestion.{market}",
                    "version": "1.0",
                    "correlation_id": str(uuid.uuid4()),
                    "causation_id": None,
                    "metadata": {}
                },
                "payload": {
                    "market": market,
                    "instrument_id": instrument,
                    "raw_data": {
                        "timestamp": timestamp.isoformat(),
                        "price": price,
                        "volume": volume,
                        "node": f"{market.upper()}_NODE_{random.randint(1, 10):03d}",
                        "zone": f"{market.upper()}_ZONE_{random.choice(['A', 'B', 'C'])}",
                        "hub": f"{market.upper()}_HUB_{random.randint(1, 5)}"
                    }
                }
            }
            
            records.append(record)
            self.records_generated += 1
        
        return records
    
    def generate_normalized_ticks(self, count: int = 100) -> List[Dict[str, Any]]:
        """Generate normalized tick records."""
        records = []
        
        for i in range(count):
            # Select random instrument
            instrument = random.choice(self.instruments)
            
            # Generate timestamp (last 24 hours)
            timestamp = datetime.now() - timedelta(hours=random.randint(0, 24))
            
            # Generate price data
            base_price = random.uniform(20.0, 100.0)
            price = base_price + random.uniform(-5.0, 5.0)
            volume = random.uniform(100.0, 10000.0)
            
            # Generate quality flags
            quality_flags = []
            if random.random() < 0.1:  # 10% chance of quality issues
                quality_flags.append("LATE_ARRIVAL")
            if random.random() < 0.05:  # 5% chance of volume spike
                quality_flags.append("VOLUME_SPIKE")
            if not quality_flags:
                quality_flags.append("VALID")
            
            # Create record
            record = {
                "envelope": {
                    "event_type": "normalized.market.ticks.v1",
                    "event_id": str(uuid.uuid4()),
                    "timestamp": timestamp.isoformat(),
                    "tenant_id": "default",
                    "source": "normalization-service",
                    "version": "1.0",
                    "correlation_id": str(uuid.uuid4()),
                    "causation_id": None,
                    "metadata": {}
                },
                "payload": {
                    "instrument_id": instrument,
                    "timestamp": timestamp.isoformat(),
                    "price": price,
                    "volume": volume,
                    "quality_flags": quality_flags
                }
            }
            
            records.append(record)
            self.records_generated += 1
        
        return records
    
    def generate_enriched_ticks(self, count: int = 100) -> List[Dict[str, Any]]:
        """Generate enriched tick records."""
        records = []
        
        for i in range(count):
            # Select random instrument
            instrument = random.choice(self.instruments)
            
            # Generate timestamp (last 24 hours)
            timestamp = datetime.now() - timedelta(hours=random.randint(0, 24))
            
            # Generate price data
            base_price = random.uniform(20.0, 100.0)
            price = base_price + random.uniform(-5.0, 5.0)
            volume = random.uniform(100.0, 10000.0)
            
            # Generate quality flags
            quality_flags = []
            if random.random() < 0.1:  # 10% chance of quality issues
                quality_flags.append("LATE_ARRIVAL")
            if random.random() < 0.05:  # 5% chance of volume spike
                quality_flags.append("VOLUME_SPIKE")
            if not quality_flags:
                quality_flags.append("VALID")
            
            # Generate metadata
            metadata = {
                "commodity": "natural_gas" if "NG" in instrument else "electricity" if "ELEC" in instrument else "crude_oil",
                "region": "northeast" if "MISO" in instrument else "west" if "CAISO" in instrument else "texas",
                "product_tier": "premium" if "PROMO" in instrument else "standard",
                "classification_confidence": random.uniform(0.7, 1.0),
                "classification_method": "rule_based",
                "unit": "MMBtu" if "NG" in instrument else "MWh" if "ELEC" in instrument else "bbl",
                "contract_size": 1000.0,
                "tick_size": 0.01
            }
            
            # Create record
            record = {
                "envelope": {
                    "event_type": "enriched.market.ticks.v1",
                    "event_id": str(uuid.uuid4()),
                    "timestamp": timestamp.isoformat(),
                    "tenant_id": "default",
                    "source": "enrichment-service",
                    "version": "1.0",
                    "correlation_id": str(uuid.uuid4()),
                    "causation_id": None,
                    "metadata": {}
                },
                "payload": {
                    "instrument_id": instrument,
                    "timestamp": timestamp.isoformat(),
                    "price": price,
                    "volume": volume,
                    "quality_flags": quality_flags,
                    "metadata": metadata
                }
            }
            
            records.append(record)
            self.records_generated += 1
        
        return records
    
    def save_test_data(self, data: List[Dict[str, Any]], output_path: Path) -> None:
        """Save test data to file."""
        try:
            # Create output directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info("Test data saved", output_path=str(output_path), count=len(data))
            
        except Exception as e:
            self.logger.error(
                "Failed to save test data",
                error=str(e),
                output_path=str(output_path)
            )
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get loader metrics."""
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "records_generated": self.records_generated,
            "runtime_seconds": runtime,
        }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Load test data for data processing services")
    parser.add_argument("--type", choices=["raw", "normalized", "enriched", "all"], default="all", help="Type of test data to generate")
    parser.add_argument("--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--output-dir", default="test_data", help="Output directory")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    try:
        # Configuration
        config = {}
        
        # Create loader
        loader = TestDataLoader(config)
        await loader.start()
        
        # Generate test data
        output_dir = Path(args.output_dir)
        
        if args.type in ["raw", "all"]:
            raw_data = loader.generate_raw_market_data(args.count)
            loader.save_test_data(raw_data, output_dir / "raw_market_data.json")
        
        if args.type in ["normalized", "all"]:
            normalized_data = loader.generate_normalized_ticks(args.count)
            loader.save_test_data(normalized_data, output_dir / "normalized_ticks.json")
        
        if args.type in ["enriched", "all"]:
            enriched_data = loader.generate_enriched_ticks(args.count)
            loader.save_test_data(enriched_data, output_dir / "enriched_ticks.json")
        
        # Print summary
        print(f"\n--- Test Data Generation Summary ---")
        print(f"Data type: {args.type}")
        print(f"Records generated: {loader.records_generated}")
        print(f"Output directory: {output_dir}")
        
        # List generated files
        print(f"\nGenerated files:")
        for file_path in output_dir.glob("*.json"):
            print(f"  - {file_path}")
        
        print("--- End Summary ---\n")
        
        # Print metrics
        metrics = loader.get_metrics()
        print(f"--- Loader Metrics ---")
        print(f"Records generated: {metrics['records_generated']}")
        print(f"Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print("--- End Metrics ---\n")
        
    except Exception as e:
        logger.error("Test data generation failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

