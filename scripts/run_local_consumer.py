#!/usr/bin/env python3
"""
Local consumer script for testing data processing services.

Simulates raw event ingestion and monitors the processing pipeline.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Any, List
import argparse
import structlog

from confluent_kafka import Producer, Consumer, KafkaError
from datetime import datetime
import uuid


logger = structlog.get_logger()


class LocalConsumer:
    """Local consumer for testing data processing pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("local-consumer")
        
        # Create Kafka producer
        producer_config = {
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'client.id': 'local-consumer',
        }
        self.producer = Producer(producer_config)
        
        # Create Kafka consumer
        consumer_config = {
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': 'local-consumer-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }
        self.consumer = Consumer(consumer_config)
        
        # Metrics
        self.messages_sent = 0
        self.messages_received = 0
        self.start_time = None
    
    async def start(self) -> None:
        """Start the local consumer."""
        self.logger.info("Starting local consumer")
        self.start_time = datetime.now()
        
        # Subscribe to output topics
        output_topics = [
            "normalized.market.ticks.v1",
            "enriched.market.ticks.v1",
            "aggregated.market.bars.5m.v1",
            "aggregated.market.bars.1h.v1",
            "pricing.curve.updates.v1",
            "served.market.latest_prices.v1",
        ]
        
        self.consumer.subscribe(output_topics)
        
        # Start consuming
        await self._consume_loop()
    
    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        self.logger.info("Starting consumption loop")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.logger.error("Consumer error", error=str(msg.error()))
                        continue
                
                # Process message
                await self._process_message(msg)
                
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer")
        finally:
            self.consumer.close()
    
    async def _process_message(self, msg) -> None:
        """Process received message."""
        try:
            # Parse message
            message_data = json.loads(msg.value().decode('utf-8'))
            
            # Update metrics
            self.messages_received += 1
            
            # Log message
            self.logger.info(
                "Message received",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                event_type=message_data.get('envelope', {}).get('event_type'),
                instrument_id=message_data.get('payload', {}).get('instrument_id'),
            )
            
            # Print message details
            print(f"\n--- Message from {msg.topic()} ---")
            print(f"Event Type: {message_data.get('envelope', {}).get('event_type')}")
            print(f"Instrument ID: {message_data.get('payload', {}).get('instrument_id')}")
            print(f"Timestamp: {message_data.get('envelope', {}).get('timestamp')}")
            print(f"Payload: {json.dumps(message_data.get('payload', {}), indent=2)}")
            print("--- End Message ---\n")
            
        except Exception as e:
            self.logger.error(
                "Message processing error",
                error=str(e),
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )
    
    def send_test_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Send test message to topic."""
        try:
            # Serialize message
            message_json = json.dumps(message)
            
            # Send message
            self.producer.produce(
                topic=topic,
                value=message_json,
                key=message.get('payload', {}).get('instrument_id', 'unknown')
            )
            
            # Flush producer
            self.producer.flush()
            
            # Update metrics
            self.messages_sent += 1
            
            self.logger.info(
                "Test message sent",
                topic=topic,
                instrument_id=message.get('payload', {}).get('instrument_id')
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to send test message",
                error=str(e),
                topic=topic
            )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "runtime_seconds": runtime,
            "messages_per_second": self.messages_received / runtime if runtime > 0 else 0,
        }


def create_test_message(market: str, instrument_id: str) -> Dict[str, Any]:
    """Create test message for raw market data."""
    return {
        "envelope": {
            "event_type": "ingestion.market.raw.v1",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "tenant_id": "default",
            "source": f"ingestion.{market}",
            "version": "1.0",
            "correlation_id": str(uuid.uuid4()),
            "causation_id": None,
            "metadata": {}
        },
        "payload": {
            "market": market,
            "instrument_id": instrument_id,
            "raw_data": {
                "timestamp": datetime.now().isoformat(),
                "price": 45.67,
                "volume": 1000.0,
                "node": f"{market.upper()}_NODE_001",
                "zone": f"{market.upper()}_ZONE_A"
            }
        }
    }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Local consumer for testing data processing pipeline")
    parser.add_argument("--topic", help="Topic to send test messages to")
    parser.add_argument("--market", choices=["miso", "caiso", "ercot"], default="miso", help="Market for test data")
    parser.add_argument("--instrument", default="NG_HH_BALMO", help="Instrument ID for test data")
    parser.add_argument("--count", type=int, default=1, help="Number of test messages to send")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between messages (seconds)")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    
    args = parser.parse_args()
    
    # Setup logging
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
    
    # Configuration
    config = {
        'kafka': {
            'bootstrap_servers': args.kafka_bootstrap
        }
    }
    
    # Create consumer
    consumer = LocalConsumer(config)
    
    try:
        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.start())
        
        # Send test messages if topic specified
        if args.topic:
            for i in range(args.count):
                message = create_test_message(args.market, args.instrument)
                consumer.send_test_message(args.topic, message)
                
                if i < args.count - 1:
                    await asyncio.sleep(args.interval)
        
        # Wait for consumer
        await consumer_task
        
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        # Print metrics
        metrics = consumer.get_metrics()
        print(f"\n--- Metrics ---")
        print(f"Messages sent: {metrics['messages_sent']}")
        print(f"Messages received: {metrics['messages_received']}")
        print(f"Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print(f"Messages per second: {metrics['messages_per_second']:.2f}")
        print("--- End Metrics ---\n")


if __name__ == "__main__":
    asyncio.run(main())

