"""Performance benchmarks for aggregation service."""

import pytest
import asyncio
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer


class TestAggregationPerformance:
    """Aggregation service performance tests."""
    
    @pytest.fixture
    async def kafka_producer(self):
        """Kafka producer fixture."""
        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'agg-perf-test-producer'
        })
        yield producer
        producer.flush()
        
    @pytest.mark.asyncio
    async def test_aggregation_throughput(self, kafka_producer):
        """Test aggregation service throughput."""
        # Generate enriched test data
        test_data = self._generate_enriched_data(count=10000)
        
        # Measure throughput
        start_time = time.time()
        
        # Send data in batches
        batch_size = 100
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            for tick in batch:
                await self._send_enriched_tick(kafka_producer, tick)
            await asyncio.sleep(0.01)  # Small delay between batches
            
        end_time = time.time()
        
        # Calculate throughput
        duration = end_time - start_time
        throughput = len(test_data) / duration
        
        print(f"Aggregation throughput: {throughput:.2f} ticks/second")
        
        # Assert minimum throughput
        assert throughput > 500, f"Throughput too low: {throughput}"
        
    def _generate_enriched_data(self, count: int) -> List[Dict[str, Any]]:
        """Generate enriched test data for performance testing."""
        data = []
        base_time = datetime.now()
        
        for i in range(count):
            tick = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 100}',  # 100 different instruments
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'price': 100.0 + (i * 0.01),
                'volume': 1000 + i,
                'quality_flags': 0,
                'tenant_id': 'test_tenant',
                'enrichment_data': {
                    'taxonomy': 'commodity',
                    'region': 'us',
                    'metadata': f'metadata_{i}'
                }
            }
            data.append(tick)
            
        return data
        
    async def _send_enriched_tick(self, producer: Producer, tick: Dict[str, Any]):
        """Send enriched tick to Kafka."""
        producer.produce(
            'enriched-market-ticks',
            key=tick['instrument_id'],
            value=json.dumps(tick)
        )
        producer.flush()
        
    @pytest.mark.asyncio
    async def test_bar_generation_performance(self, kafka_producer):
        """Test OHLC bar generation performance."""
        # Generate data for bar generation
        test_data = self._generate_enriched_data(count=5000)
        
        # Measure bar generation time
        start_time = time.time()
        
        for tick in test_data:
            await self._send_enriched_tick(kafka_producer, tick)
            
        # Wait for bar generation
        await asyncio.sleep(5)
        
        end_time = time.time()
        
        # Calculate performance
        duration = end_time - start_time
        bars_per_second = len(test_data) / duration
        
        print(f"Bar generation performance: {bars_per_second:.2f} bars/second")
        
        # Assert minimum performance
        assert bars_per_second > 100, f"Bar generation too slow: {bars_per_second}"
        
    @pytest.mark.asyncio
    async def test_curve_calculation_performance(self, kafka_producer):
        """Test forward curve calculation performance."""
        # Generate data for curve calculation
        test_data = self._generate_enriched_data(count=2000)
        
        # Measure curve calculation time
        start_time = time.time()
        
        for tick in test_data:
            await self._send_enriched_tick(kafka_producer, tick)
            
        # Wait for curve calculation
        await asyncio.sleep(3)
        
        end_time = time.time()
        
        # Calculate performance
        duration = end_time - start_time
        curves_per_second = len(test_data) / duration
        
        print(f"Curve calculation performance: {curves_per_second:.2f} curves/second")
        
        # Assert minimum performance
        assert curves_per_second > 50, f"Curve calculation too slow: {curves_per_second}"
        
    @pytest.mark.asyncio
    async def test_aggregation_memory_usage(self, kafka_producer):
        """Test aggregation service memory usage."""
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Generate and send large amount of data
        test_data = self._generate_enriched_data(count=30000)
        
        for tick in test_data:
            await self._send_enriched_tick(kafka_producer, tick)
            
        # Wait for processing
        await asyncio.sleep(10)
        
        # Get final memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"Aggregation memory usage increase: {memory_increase:.2f} MB")
        
        # Assert memory usage is reasonable
        assert memory_increase < 1000, f"Memory usage too high: {memory_increase} MB"
        
    @pytest.mark.asyncio
    async def test_aggregation_scalability(self, kafka_producer):
        """Test aggregation service scalability."""
        # Test with increasing load
        load_levels = [1000, 5000, 10000, 20000]
        results = []
        
        for load in load_levels:
            test_data = self._generate_enriched_data(count=load)
            
            start_time = time.time()
            
            for tick in test_data:
                await self._send_enriched_tick(kafka_producer, tick)
                
            # Wait for processing
            await asyncio.sleep(2)
            
            end_time = time.time()
            
            duration = end_time - start_time
            throughput = load / duration
            
            results.append({
                'load': load,
                'throughput': throughput,
                'duration': duration
            })
            
            print(f"Load {load}: {throughput:.2f} ticks/second")
            
        # Verify scalability
        for i in range(1, len(results)):
            prev_throughput = results[i-1]['throughput']
            curr_throughput = results[i]['throughput']
            
            # Throughput should not degrade significantly
            assert curr_throughput > prev_throughput * 0.8, f"Throughput degraded: {curr_throughput} vs {prev_throughput}"

