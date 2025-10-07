#!/usr/bin/env python3
"""
Generate comprehensive sample data for testing all service flows.

This script generates realistic market data for various scenarios:
- Real-time tick generation
- Historical backfill data
- High-volume stress testing
- Error scenarios (malformed data)
"""

import asyncio
import json
import argparse
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from confluent_kafka import Producer
import uuid


class SampleDataGenerator:
    """Generator for comprehensive sample market data."""
    
    MARKETS = ["MISO", "CAISO", "PJM", "ERCOT", "NYISO", "ISO-NE"]
    INSTRUMENTS = [
        "NG_HH_BALMO", "NG_HH_PROMPT", "NG_HH_M1", "NG_HH_M2",
        "POWER_PEAK_DAY_AHEAD", "POWER_OFF_PEAK_DAY_AHEAD",
        "POWER_PEAK_REAL_TIME", "POWER_OFF_PEAK_REAL_TIME",
        "CRUDE_WTI", "BRENT_ICE", "RBOB_GASOLINE", "HEATING_OIL"
    ]
    
    REGIONS = ["US_MIDWEST", "US_NORTHEAST", "US_SOUTH", "US_WEST"]
    COMMODITIES = ["natural_gas", "power", "crude_oil", "refined_products"]
    
    def __init__(self, market: str = "MISO", tenant_id: str = "default"):
        self.market = market
        self.tenant_id = tenant_id
        self.base_prices = {inst: random.uniform(50.0, 150.0) for inst in self.INSTRUMENTS}
        
    def generate_raw_tick(self, instrument_id: str, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single raw market tick with envelope."""
        if timestamp is None:
            timestamp = datetime.now()
            
        # Generate realistic price movement
        base_price = self.base_prices.get(instrument_id, 100.0)
        price_change = random.gauss(0, 0.5)  # Normal distribution for price changes
        new_price = max(base_price + price_change, 1.0)  # Ensure positive price
        self.base_prices[instrument_id] = new_price
        
        # Generate bid-ask spread
        spread = new_price * random.uniform(0.001, 0.005)  # 0.1-0.5% spread
        bid = new_price - (spread / 2)
        ask = new_price + (spread / 2)
        
        return {
            "envelope": {
                "event_type": "ingestion.market.raw.v1",
                "event_id": str(uuid.uuid4()),
                "timestamp": timestamp.isoformat(),
                "tenant_id": self.tenant_id,
                "source": f"ingestion.{self.market.lower()}",
                "version": "1.0",
                "correlation_id": str(uuid.uuid4()),
                "causation_id": None,
                "metadata": {
                    "market": self.market,
                    "data_quality": "good"
                }
            },
            "payload": {
                "market": self.market,
                "instrument_id": instrument_id,
                "raw_data": {
                    "timestamp": timestamp.isoformat(),
                    "price": round(new_price, 2),
                    "volume": random.randint(100, 10000),
                    "bid": round(bid, 2),
                    "ask": round(ask, 2),
                    "last_trade_time": timestamp.isoformat(),
                    "node": f"{self.market.upper()}_NODE_{random.randint(1, 100):03d}",
                    "zone": f"{self.market.upper()}_ZONE_{random.choice(['A', 'B', 'C', 'D'])}",
                    "market_status": "open"
                }
            }
        }
        
    def generate_batch(self, count: int, instrument_id: str = None) -> List[Dict[str, Any]]:
        """Generate a batch of raw ticks."""
        if instrument_id is None:
            instrument_id = random.choice(self.INSTRUMENTS)
            
        ticks = []
        base_time = datetime.now()
        
        for i in range(count):
            tick_time = base_time - timedelta(seconds=i)
            tick = self.generate_raw_tick(instrument_id, tick_time)
            ticks.append(tick)
            
        return ticks
        
    def generate_historical_data(
        self, 
        days: int = 7, 
        instruments: List[str] = None,
        interval_minutes: int = 5
    ) -> List[Dict[str, Any]]:
        """Generate historical data for backfill testing."""
        if instruments is None:
            instruments = self.INSTRUMENTS[:4]  # Use first 4 instruments
            
        ticks = []
        start_time = datetime.now() - timedelta(days=days)
        
        for instrument in instruments:
            # Reset base price for each instrument
            self.base_prices[instrument] = random.uniform(50.0, 150.0)
            
            current_time = start_time
            end_time = datetime.now()
            
            while current_time < end_time:
                tick = self.generate_raw_tick(instrument, current_time)
                ticks.append(tick)
                current_time += timedelta(minutes=interval_minutes)
                
        return ticks
        
    def generate_high_volume_data(self, count: int = 10000) -> List[Dict[str, Any]]:
        """Generate high-volume data for stress testing."""
        ticks = []
        base_time = datetime.now()
        
        for i in range(count):
            instrument = random.choice(self.INSTRUMENTS)
            tick_time = base_time - timedelta(milliseconds=i * 100)  # 10 ticks per second
            tick = self.generate_raw_tick(instrument, tick_time)
            ticks.append(tick)
            
        return ticks
        
    def generate_malformed_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate malformed data for error handling testing."""
        malformed_ticks = []
        
        # Missing required fields
        malformed_ticks.append({
            "envelope": {
                "event_type": "ingestion.market.raw.v1",
                "event_id": str(uuid.uuid4()),
                "timestamp": datetime.now().isoformat()
                # Missing tenant_id, source
            },
            "payload": {
                "instrument_id": "TEST_INSTRUMENT"
                # Missing raw_data
            }
        })
        
        # Invalid data types
        malformed_ticks.append({
            "envelope": {
                "event_type": "ingestion.market.raw.v1",
                "event_id": str(uuid.uuid4()),
                "timestamp": "invalid_timestamp",
                "tenant_id": self.tenant_id,
                "source": f"ingestion.{self.market.lower()}"
            },
            "payload": {
                "market": self.market,
                "instrument_id": 12345,  # Should be string
                "raw_data": {
                    "price": "not_a_number",  # Should be float
                    "volume": "not_a_number"  # Should be int
                }
            }
        })
        
        # Negative prices
        malformed_ticks.append({
            "envelope": {
                "event_type": "ingestion.market.raw.v1",
                "event_id": str(uuid.uuid4()),
                "timestamp": datetime.now().isoformat(),
                "tenant_id": self.tenant_id,
                "source": f"ingestion.{self.market.lower()}"
            },
            "payload": {
                "market": self.market,
                "instrument_id": "TEST_INSTRUMENT",
                "raw_data": {
                    "timestamp": datetime.now().isoformat(),
                    "price": -100.0,  # Negative price
                    "volume": -1000  # Negative volume
                }
            }
        })
        
        return malformed_ticks[:count]
        
    def generate_late_data(self, hours_late: int = 2, count: int = 100) -> List[Dict[str, Any]]:
        """Generate late-arriving data for watermark testing."""
        ticks = []
        late_time = datetime.now() - timedelta(hours=hours_late)
        
        for i in range(count):
            instrument = random.choice(self.INSTRUMENTS)
            tick_time = late_time - timedelta(seconds=i)
            tick = self.generate_raw_tick(instrument, tick_time)
            ticks.append(tick)
            
        return ticks


class KafkaProducerClient:
    """Kafka producer for sending sample data."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'sample-data-producer',
            'compression.type': 'snappy',
            'linger.ms': 10,
            'batch.size': 16384
        })
        self.sent_count = 0
        self.failed_count = 0
        
    def send_tick(self, topic: str, tick: Dict[str, Any]):
        """Send a single tick to Kafka."""
        try:
            self.producer.produce(
                topic,
                key=tick.get("payload", {}).get("instrument_id", "unknown").encode('utf-8'),
                value=json.dumps(tick).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"Error sending tick: {e}")
            self.failed_count += 1
        
    def send_batch(self, topic: str, ticks: List[Dict[str, Any]], batch_size: int = 100):
        """Send a batch of ticks to Kafka."""
        print(f"Sending {len(ticks)} ticks to {topic}...")
        
        for i, tick in enumerate(ticks):
            self.send_tick(topic, tick)
            
            # Flush periodically
            if (i + 1) % batch_size == 0:
                self.producer.flush()
                print(f"Sent {i + 1}/{len(ticks)} ticks...")
                
        self.producer.flush()
        print(f"Completed sending {len(ticks)} ticks (sent: {self.sent_count}, failed: {self.failed_count})")
        
    def _delivery_callback(self, err, msg):
        """Callback for message delivery."""
        if err:
            print(f"Message delivery failed: {err}")
            self.failed_count += 1
        else:
            self.sent_count += 1
            if self.sent_count % 100 == 0:
                print(f"Delivered {self.sent_count} messages")
            
    def close(self):
        """Close the producer."""
        self.producer.flush()


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate comprehensive sample market data")
    parser.add_argument("--topic", default="ingestion.miso.raw.v1", help="Kafka topic to send data to")
    parser.add_argument("--market", default="MISO", choices=SampleDataGenerator.MARKETS, help="Market to simulate")
    parser.add_argument("--tenant", default="default", help="Tenant ID")
    parser.add_argument("--mode", choices=["batch", "historical", "high-volume", "malformed", "late", "continuous"], 
                       default="batch", help="Data generation mode")
    parser.add_argument("--count", type=int, default=100, help="Number of ticks to generate")
    parser.add_argument("--days", type=int, default=7, help="Days of historical data")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between ticks in seconds (continuous mode)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--instrument", help="Specific instrument ID")
    
    args = parser.parse_args()
    
    # Initialize generator and producer
    generator = SampleDataGenerator(market=args.market, tenant_id=args.tenant)
    producer = KafkaProducerClient(bootstrap_servers=args.bootstrap_servers)
    
    try:
        if args.mode == "batch":
            print(f"Generating {args.count} batch ticks...")
            ticks = generator.generate_batch(count=args.count, instrument_id=args.instrument)
            producer.send_batch(args.topic, ticks)
            
        elif args.mode == "historical":
            print(f"Generating {args.days} days of historical data...")
            ticks = generator.generate_historical_data(days=args.days)
            print(f"Generated {len(ticks)} historical ticks")
            producer.send_batch(args.topic, ticks)
            
        elif args.mode == "high-volume":
            print(f"Generating {args.count} high-volume ticks...")
            ticks = generator.generate_high_volume_data(count=args.count)
            producer.send_batch(args.topic, ticks, batch_size=500)
            
        elif args.mode == "malformed":
            print(f"Generating {args.count} malformed ticks...")
            ticks = generator.generate_malformed_data(count=args.count)
            producer.send_batch(args.topic, ticks)
            
        elif args.mode == "late":
            print(f"Generating {args.count} late-arriving ticks...")
            ticks = generator.generate_late_data(hours_late=2, count=args.count)
            producer.send_batch(args.topic, ticks)
            
        elif args.mode == "continuous":
            print(f"Starting continuous data generation (interval: {args.interval}s)")
            print("Press Ctrl+C to stop")
            
            while True:
                instrument = args.instrument or random.choice(generator.INSTRUMENTS)
                tick = generator.generate_raw_tick(instrument)
                producer.send_tick(args.topic, tick)
                await asyncio.sleep(args.interval)
                
    except KeyboardInterrupt:
        print("\nStopping data generation...")
    finally:
        producer.close()
        print("Producer closed")


if __name__ == "__main__":
    asyncio.run(main())

