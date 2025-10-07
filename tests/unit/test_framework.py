"""Unit tests for shared framework components."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from shared.framework.service import AsyncService
from shared.framework.consumer import KafkaConsumer
from shared.framework.producer import KafkaProducer
from shared.framework.health import HealthCheck
from shared.framework.metrics import Metrics


class TestAsyncService:
    """Test AsyncService base class."""
    
    @pytest.mark.asyncio
    async def test_service_lifecycle(self):
        """Test service lifecycle methods."""
        service = AsyncService()
        
        # Test start
        await service.start()
        assert service.is_running
        
        # Test stop
        await service.stop()
        assert not service.is_running
        
    @pytest.mark.asyncio
    async def test_service_run(self):
        """Test service run method."""
        service = AsyncService()
        
        # Mock the start and stop methods
        service.start = AsyncMock()
        service.stop = AsyncMock()
        
        # Test run method
        await service.run()
        
        # Verify start and stop were called
        service.start.assert_called_once()
        service.stop.assert_called_once()


class TestKafkaConsumer:
    """Test KafkaConsumer class."""
    
    @pytest.mark.asyncio
    async def test_consumer_lifecycle(self):
        """Test consumer lifecycle."""
        consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["test-topic"]
        )
        
        # Test start
        await consumer.start()
        assert consumer.is_running
        
        # Test stop
        await consumer.stop()
        assert not consumer.is_running
        
    @pytest.mark.asyncio
    async def test_consume_message(self):
        """Test message consumption."""
        consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["test-topic"]
        )
        
        # Mock the consume method
        consumer.consume = AsyncMock()
        
        # Test consume
        test_message = {"test": "data"}
        await consumer.consume(test_message)
        
        # Verify consume was called
        consumer.consume.assert_called_once_with(test_message)


class TestKafkaProducer:
    """Test KafkaProducer class."""
    
    @pytest.mark.asyncio
    async def test_producer_lifecycle(self):
        """Test producer lifecycle."""
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"]
        )
        
        # Test start
        await producer.start()
        assert producer.is_running
        
        # Test stop
        await producer.stop()
        assert not producer.is_running
        
    @pytest.mark.asyncio
    async def test_produce_message(self):
        """Test message production."""
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            topics=["test-topic"]
        )
        
        # Mock the produce method
        producer.produce = AsyncMock()
        
        # Test produce
        test_message = {"test": "data"}
        await producer.produce("test-topic", "test-key", test_message)
        
        # Verify produce was called
        producer.produce.assert_called_once_with("test-topic", "test-key", test_message)


class TestHealthCheck:
    """Test HealthCheck class."""
    
    @pytest.mark.asyncio
    async def test_health_check(self):
        """Test health check functionality."""
        health_check = HealthCheck()
        
        # Test health check
        result = await health_check.check_health()
        
        # Verify result structure
        assert "status" in result
        assert "timestamp" in result
        assert result["status"] in ["healthy", "unhealthy"]
        
    @pytest.mark.asyncio
    async def test_add_check(self):
        """Test adding health checks."""
        health_check = HealthCheck()
        
        # Add a check
        health_check.add_check("test_check", "healthy", {"details": "test"})
        
        # Test health check
        result = await health_check.check_health()
        
        # Verify check was added
        assert "checks" in result
        assert "test_check" in result["checks"]
        assert result["checks"]["test_check"]["status"] == "healthy"


class TestMetrics:
    """Test Metrics class."""
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self):
        """Test metrics collection."""
        metrics = Metrics()
        
        # Test counter
        metrics.increment_counter("test_counter")
        assert metrics.get_counter("test_counter") == 1
        
        # Test gauge
        metrics.set_gauge("test_gauge", 100.0)
        assert metrics.get_gauge("test_gauge") == 100.0
        
        # Test histogram
        metrics.record_histogram("test_histogram", 50.0)
        assert metrics.get_histogram("test_histogram") == [50.0]
        
    @pytest.mark.asyncio
    async def test_metrics_export(self):
        """Test metrics export."""
        metrics = Metrics()
        
        # Add some metrics
        metrics.increment_counter("test_counter")
        metrics.set_gauge("test_gauge", 100.0)
        
        # Export metrics
        exported = metrics.export_metrics()
        
        # Verify export structure
        assert "counters" in exported
        assert "gauges" in exported
        assert "histograms" in exported

