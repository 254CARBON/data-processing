"""Backfill scenario integration tests."""

import pytest
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer


class TestBackfillScenarios:
    """Backfill scenario tests."""
    
    @pytest.fixture
    async def kafka_producer(self):
        """Kafka producer fixture."""
        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'backfill-test-producer'
        })
        yield producer
        producer.flush()
        
    @pytest.mark.asyncio
    async def test_historical_data_backfill(self, kafka_producer):
        """Test backfill with historical data."""
        # Create historical data spanning multiple days
        historical_data = self._create_historical_data(days=7)
        
        # Send historical data in chronological order
        for tick in historical_data:
            await self._send_tick(kafka_producer, tick)
            await asyncio.sleep(0.01)  # Small delay to simulate real-time
        
        # Wait for processing
        await asyncio.sleep(10)
        
        # Verify backfill completed
        # In a real test, this would check database for expected records
        
    def _create_historical_data(self, days: int) -> List[Dict[str, Any]]:
        """Create historical data for backfill testing."""
        data = []
        start_time = datetime.now() - timedelta(days=days)
        
        for day in range(days):
            for hour in range(24):
                for minute in range(0, 60, 5):  # Every 5 minutes
                    tick_time = start_time + timedelta(
                        days=day, hours=hour, minutes=minute
                    )
                    
                    tick = {
                        'instrument_id': 'TEST_INSTRUMENT',
                        'ts': tick_time.isoformat(),
                        'price': 100.0 + (day * 0.1) + (hour * 0.01) + (minute * 0.001),
                        'volume': 1000 + (day * 100) + (hour * 10) + minute,
                        'source': 'historical',
                        'raw_data': f'historical_data_{day}_{hour}_{minute}',
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
    async def test_partial_backfill(self, kafka_producer):
        """Test partial backfill with missing data."""
        # Create data with gaps
        data_with_gaps = self._create_data_with_gaps()
        
        # Send data with gaps
        for tick in data_with_gaps:
            await self._send_tick(kafka_producer, tick)
            await asyncio.sleep(0.01)
            
        # Wait for processing
        await asyncio.sleep(5)
        
        # Verify gaps were handled
        # In a real test, this would check for missing data handling
        
    def _create_data_with_gaps(self) -> List[Dict[str, Any]]:
        """Create data with intentional gaps."""
        data = []
        base_time = datetime.now() - timedelta(hours=2)
        
        # Create data with gaps every 30 minutes
        for i in range(0, 120, 5):  # Every 5 minutes
            if i % 30 == 0:  # Skip every 30 minutes
                continue
                
            tick_time = base_time + timedelta(minutes=i)
            tick = {
                'instrument_id': 'TEST_INSTRUMENT',
                'ts': tick_time.isoformat(),
                'price': 100.0 + (i * 0.1),
                'volume': 1000 + i,
                'source': 'test',
                'raw_data': f'test_data_{i}',
                'tenant_id': 'test_tenant'
            }
            data.append(tick)
            
        return data
        
    @pytest.mark.asyncio
    async def test_backfill_with_corrections(self, kafka_producer):
        """Test backfill with data corrections."""
        # Create initial data
        initial_data = self._create_initial_data()
        
        # Send initial data
        for tick in initial_data:
            await self._send_tick(kafka_producer, tick)
            await asyncio.sleep(0.01)
            
        # Wait for initial processing
        await asyncio.sleep(2)
        
        # Create corrected data
        corrected_data = self._create_corrected_data()
        
        # Send corrected data
        for tick in corrected_data:
            await self._send_tick(kafka_producer, tick)
            await asyncio.sleep(0.01)
            
        # Wait for correction processing
        await asyncio.sleep(3)
        
        # Verify corrections were applied
        # In a real test, this would check for updated records
        
    def _create_initial_data(self) -> List[Dict[str, Any]]:
        """Create initial data for correction testing."""
        data = []
        base_time = datetime.now() - timedelta(hours=1)
        
        for i in range(10):
            tick_time = base_time + timedelta(minutes=i * 5)
            tick = {
                'instrument_id': 'TEST_INSTRUMENT',
                'ts': tick_time.isoformat(),
                'price': 100.0 + (i * 0.1),
                'volume': 1000 + i,
                'source': 'test',
                'raw_data': f'initial_data_{i}',
                'tenant_id': 'test_tenant'
            }
            data.append(tick)
            
        return data
        
    def _create_corrected_data(self) -> List[Dict[str, Any]]:
        """Create corrected data for testing."""
        data = []
        base_time = datetime.now() - timedelta(hours=1)
        
        # Correct some of the initial data
        for i in range(5):  # Correct first 5 records
            tick_time = base_time + timedelta(minutes=i * 5)
            tick = {
                'instrument_id': 'TEST_INSTRUMENT',
                'ts': tick_time.isoformat(),
                'price': 100.0 + (i * 0.1) + 0.5,  # Add correction
                'volume': 1000 + i + 100,  # Add correction
                'source': 'test_corrected',
                'raw_data': f'corrected_data_{i}',
                'tenant_id': 'test_tenant'
            }
            data.append(tick)
            
        return data

