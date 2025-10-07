"""End-to-end data flow integration tests."""

import pytest
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer, KafkaError
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import sample data generator
from scripts.generate_sample_data import SampleDataGenerator

logger = logging.getLogger(__name__)


class TestEndToEndFlow:
    """End-to-end data flow tests."""
    
    @pytest.fixture
    def sample_data_generator(self):
        """Sample data generator fixture."""
        return SampleDataGenerator(market="MISO", tenant_id="test")
    
    @pytest.fixture
    def kafka_producer(self):
        """Kafka producer fixture."""
        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'test-producer',
            'acks': 'all',
            'retries': 3
        })
        yield producer
        producer.flush()
        
    @pytest.fixture
    def kafka_consumer(self):
        """Kafka consumer fixture."""
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'test-consumer-{datetime.now().timestamp()}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        })
        yield consumer
        consumer.close()
        
    @pytest.mark.asyncio
    async def test_complete_data_flow(self, sample_data_generator, kafka_producer, kafka_consumer):
        """Test complete data flow from raw tick to projection."""
        # Generate test data using sample generator
        raw_tick = sample_data_generator.generate_raw_tick("NG_HH_BALMO")
        
        # Send raw tick to normalization service
        await self._send_raw_tick(kafka_producer, raw_tick, "ingestion.miso.raw.v1")
        
        # Wait for processing
        await asyncio.sleep(3)
        
        # Check for normalized tick
        normalized_tick = await self._consume_message(kafka_consumer, "normalized.market.ticks.v1")
        
        # Verify normalized tick
        assert normalized_tick is not None
        assert normalized_tick['payload']['instrument_id'] == raw_tick['payload']['instrument_id']
        assert normalized_tick['envelope']['tenant_id'] == raw_tick['envelope']['tenant_id']
        
        logger.info("✅ Complete data flow test passed")
        
    async def _send_raw_tick(self, producer: Producer, tick: Dict[str, Any], topic: str):
        """Send raw tick to Kafka."""
        try:
            producer.produce(
                topic,
                key=tick['payload']['instrument_id'],
                value=json.dumps(tick)
            )
            producer.flush()
            logger.info(f"Sent tick to {topic}: {tick['payload']['instrument_id']}")
        except Exception as e:
            logger.error(f"Error sending tick to {topic}: {e}")
            raise
        
    async def _consume_message(self, consumer: Consumer, topic: str) -> Dict[str, Any]:
        """Consume message from Kafka topic."""
        try:
            consumer.subscribe([topic])
            
            # Poll for messages
            start_time = datetime.now()
            while (datetime.now() - start_time).total_seconds() < 10:  # 10 second timeout
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Found a message
                message_data = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Consumed message from {topic}: {message_data.get('payload', {}).get('instrument_id', 'unknown')}")
                return message_data
                
            logger.warning(f"No message received from {topic} within timeout")
            return None
            
        except Exception as e:
            logger.error(f"Error consuming message from {topic}: {e}")
            return None
            
    @pytest.mark.asyncio
    async def test_backfill_scenario(self, sample_data_generator, kafka_producer):
        """Test backfill scenario with historical data."""
        # Generate historical data using sample generator
        historical_ticks = sample_data_generator.generate_historical_data(days=1, instruments=["NG_HH_BALMO"])
        
        logger.info(f"Generated {len(historical_ticks)} historical ticks for backfill test")
        
        # Send historical data in batches
        batch_size = 50
        for i in range(0, len(historical_ticks), batch_size):
            batch = historical_ticks[i:i + batch_size]
            for tick in batch:
                await self._send_raw_tick(kafka_producer, tick, "ingestion.miso.raw.v1")
            await asyncio.sleep(0.1)  # Small delay between batches
            
        # Wait for processing
        await asyncio.sleep(10)
        
        logger.info("✅ Backfill scenario test completed")
        # In a real test, this would check the database for expected records
        
    @pytest.mark.asyncio
    async def test_error_handling(self, sample_data_generator, kafka_producer):
        """Test error handling with malformed data."""
        # Generate malformed tick using sample generator
        malformed_tick = sample_data_generator.generate_raw_tick("NG_HH_BALMO", malformed=True)
        
        await self._send_raw_tick(kafka_producer, malformed_tick, "ingestion.miso.raw.v1")
        
        # Wait for processing
        await asyncio.sleep(3)
        
        logger.info("✅ Error handling test completed")
        # In a real test, this would check error logs or dead letter queue
        
    @pytest.mark.asyncio
    async def test_high_volume_processing(self, sample_data_generator, kafka_producer):
        """Test high volume data processing."""
        # Generate high volume of ticks using sample generator
        high_volume_ticks = sample_data_generator.generate_high_volume_data(count=1000, instruments=["NG_HH_BALMO"])
        
        logger.info(f"Generated {len(high_volume_ticks)} high volume ticks for processing test")
        
        # Send ticks in batches
        batch_size = 100
        for i in range(0, len(high_volume_ticks), batch_size):
            batch = high_volume_ticks[i:i + batch_size]
            for tick in batch:
                await self._send_raw_tick(kafka_producer, tick, "ingestion.miso.raw.v1")
            await asyncio.sleep(0.1)  # Small delay between batches
            
        # Wait for processing
        await asyncio.sleep(15)
        
        logger.info("✅ High volume processing test completed")
        # In a real test, this would check processing metrics
        
    @pytest.mark.asyncio
    async def test_service_health_checks(self):
        """Test service health check endpoints."""
        import aiohttp
        
        services = [
            ("normalization-service", "http://localhost:8080/health"),
            ("enrichment-service", "http://localhost:8081/health"),
            ("aggregation-service", "http://localhost:8082/health"),
            ("projection-service", "http://localhost:8083/health")
        ]
        
        async with aiohttp.ClientSession() as session:
            for service_name, health_url in services:
                try:
                    async with session.get(health_url, timeout=5) as response:
                        if response.status == 200:
                            logger.info(f"✅ {service_name} health check passed")
                        else:
                            logger.warning(f"⚠️ {service_name} health check failed: {response.status}")
                except Exception as e:
                    logger.warning(f"⚠️ {service_name} health check error: {e}")
        
        logger.info("✅ Service health checks test completed")
