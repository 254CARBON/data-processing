"""Performance benchmarks for normalization service."""

import pytest
import asyncio
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer


class TestNormalizationPerformance:
    """Normalization service performance tests."""
    
    @pytest.fixture
    async def kafka_producer(self):
        """Kafka producer fixture."""
        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'perf-test-producer'
        })
        yield producer
        producer.flush()
        
    @pytest.mark.asyncio
    async def test_normalization_throughput(self, kafka_producer):
        """Test normalization service throughput."""
        # Generate test data
        test_data = self._generate_test_data(count=10000)
        
        # Measure throughput
        start_time = time.time()
        
        # Send data in batches
        batch_size = 100
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            for tick in batch:
                await self._send_tick(kafka_producer, tick)
            await asyncio.sleep(0.01)  # Small delay between batches
            
        end_time = time.time()
        
        # Calculate throughput
        duration = end_time - start_time
        throughput = len(test_data) / duration
        
        print(f"Normalization throughput: {throughput:.2f} ticks/second")
        
        # Assert minimum throughput
        assert throughput > 1000, f"Throughput too low: {throughput}"
        
    def _generate_test_data(self, count: int) -> List[Dict[str, Any]]:
        """Generate test data for performance testing."""
        data = []
        base_time = datetime.now()
        
        for i in range(count):
            tick = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 100}',  # 100 different instruments
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'price': 100.0 + (i * 0.01),
                'volume': 1000 + i,
                'source': 'perf_test',
                'raw_data': f'perf_test_data_{i}',
                'tenant_id': 'test_tenant'
            }
            data.append(tick)
            
        return data
        
    async def _send_tick(self, producer: Producer, tick: Dict[str, Any]):
        """Send tick to Kafka."""
        producer.produce(
            'raw-market-ticks',
            key=tick['instrument_id'],
            value=json.dumps(tick)
        )
        producer.flush()
        
    @pytest.mark.asyncio
    async def test_normalization_latency(self, kafka_producer):
        """Test normalization service latency."""
        # Generate test data
        test_data = self._generate_test_data(count=1000)
        
        # Measure latency
        latencies = []
        
        for tick in test_data:
            start_time = time.time()
            await self._send_tick(kafka_producer, tick)
            end_time = time.time()
            
            latency = end_time - start_time
            latencies.append(latency)
            
        # Calculate statistics
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        min_latency = min(latencies)
        
        print(f"Normalization latency - Avg: {avg_latency:.4f}s, Max: {max_latency:.4f}s, Min: {min_latency:.4f}s")
        
        # Assert maximum latency
        assert max_latency < 0.1, f"Latency too high: {max_latency}"
        
    @pytest.mark.asyncio
    async def test_normalization_memory_usage(self, kafka_producer):
        """Test normalization service memory usage."""
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Generate and send large amount of data
        test_data = self._generate_test_data(count=50000)
        
        for tick in test_data:
            await self._send_tick(kafka_producer, tick)
            
        # Wait for processing
        await asyncio.sleep(5)
        
        # Get final memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"Memory usage increase: {memory_increase:.2f} MB")
        
        # Assert memory usage is reasonable
        assert memory_increase < 500, f"Memory usage too high: {memory_increase} MB"
        
    @pytest.mark.asyncio
    async def test_normalization_concurrent_load(self, kafka_producer):
        """Test normalization service under concurrent load."""
        # Generate test data
        test_data = self._generate_test_data(count=5000)
        
        # Send data concurrently
        start_time = time.time()
        
        tasks = []
        for tick in test_data:
            task = asyncio.create_task(self._send_tick(kafka_producer, tick))
            tasks.append(task)
            
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        
        # Calculate throughput
        duration = end_time - start_time
        throughput = len(test_data) / duration
        
        print(f"Concurrent normalization throughput: {throughput:.2f} ticks/second")
        
        # Assert minimum throughput
        assert throughput > 500, f"Concurrent throughput too low: {throughput}"

