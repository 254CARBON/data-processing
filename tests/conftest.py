"""Pytest configuration and fixtures."""

import pytest
import asyncio
import os
from typing import Dict, Any
from tests.fixtures.mock_services import (
    MockKafkaProducer, MockKafkaConsumer, MockClickHouseClient,
    MockRedisClient, MockPostgresClient, MockService
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_kafka_producer():
    """Mock Kafka producer fixture."""
    producer = MockKafkaProducer()
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture
async def mock_kafka_consumer():
    """Mock Kafka consumer fixture."""
    consumer = MockKafkaConsumer(['test-topic'])
    await consumer.start()
    yield consumer
    await consumer.stop()


@pytest.fixture
async def mock_clickhouse_client():
    """Mock ClickHouse client fixture."""
    client = MockClickHouseClient()
    await client.connect()
    yield client
    await client.close()


@pytest.fixture
async def mock_redis_client():
    """Mock Redis client fixture."""
    client = MockRedisClient()
    await client.connect()
    yield client
    await client.close()


@pytest.fixture
async def mock_postgres_client():
    """Mock PostgreSQL client fixture."""
    client = MockPostgresClient()
    await client.connect()
    yield client
    await client.close()


@pytest.fixture
async def mock_service():
    """Mock service fixture."""
    service = MockService()
    await service.start()
    yield service
    await service.stop()


@pytest.fixture
def test_config():
    """Test configuration fixture."""
    return {
        'kafka_bootstrap_servers': 'localhost:9092',
        'clickhouse_host': 'localhost',
        'clickhouse_port': 9000,
        'redis_host': 'localhost',
        'redis_port': 6379,
        'postgres_host': 'localhost',
        'postgres_port': 5432,
        'service_name': 'test-service',
        'port': 8080
    }


@pytest.fixture
def sample_raw_tick():
    """Sample raw market tick fixture."""
    return {
        'instrument_id': 'TEST_INSTRUMENT',
        'ts': '2024-01-01T00:00:00Z',
        'price': 100.0,
        'volume': 1000,
        'source': 'test',
        'raw_data': 'test_data',
        'tenant_id': 'test_tenant'
    }


@pytest.fixture
def sample_normalized_tick():
    """Sample normalized market tick fixture."""
    return {
        'instrument_id': 'TEST_INSTRUMENT',
        'ts': '2024-01-01T00:00:00Z',
        'price': 100.0,
        'volume': 1000,
        'quality_flags': 0,
        'tenant_id': 'test_tenant'
    }


@pytest.fixture
def sample_enriched_tick():
    """Sample enriched market tick fixture."""
    return {
        'instrument_id': 'TEST_INSTRUMENT',
        'ts': '2024-01-01T00:00:00Z',
        'price': 100.0,
        'volume': 1000,
        'quality_flags': 0,
        'tenant_id': 'test_tenant',
        'enrichment_data': {
            'taxonomy': 'commodity',
            'region': 'us',
            'metadata': 'test_metadata'
        }
    }

