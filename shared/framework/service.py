"""
Base AsyncService class for microservices.

Provides lifecycle management, HTTP server, health checks,
and graceful shutdown capabilities.
"""

import asyncio
import signal
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import web
import structlog
import psutil
import time

from .config import ServiceConfig
from shared.utils.audit import AuditLogger, NullAuditLogger, build_audit_logger
from .health import HealthChecker
from .metrics import MetricsCollector
from .consumer import KafkaConsumer
from .producer import KafkaProducer


logger = structlog.get_logger(__name__)


class AsyncService(ABC):
    """
    Base class for async microservices.
    
    Provides common functionality:
    - HTTP API server
    - Health checks
    - Metrics collection
    - Graceful shutdown
    - Kafka integration
    """
    
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.logger = structlog.get_logger(self.config.service_name).bind(service=self.config.service_name)
        
        # Core components
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Framework components
        self.health_checker = HealthChecker(self.config)
        self.metrics = MetricsCollector(self.config.service_name)
        
        # Kafka components (optional)
        self.consumers: List[KafkaConsumer] = []
        self.producers: List[KafkaProducer] = []
        
        # Shutdown event
        self.shutdown_event = asyncio.Event()
        
        # Metrics update task
        self.metrics_task: Optional[asyncio.Task] = None

        # Audit logger
        self.audit: AuditLogger | NullAuditLogger = build_audit_logger(
            service_name=self.config.service_name,
            service_environment=self.config.environment,
            service_audit_config=self.config.audit,
            clickhouse_url=self.config.database.clickhouse_url,
            kafka_config=self.config.kafka,
        )
        
        # Setup signal handlers
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal", signal=signum)
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    async def startup(self) -> None:
        """Initialize service components."""
        self.logger.info("Starting service")
        
        # Initialize HTTP session
        self.session = aiohttp.ClientSession()
        
        # Create web application
        self.app = web.Application()
        self._setup_routes()
        
        # Initialize service-specific components
        await self.audit.startup()
        await self.audit.log_service_event("service_start", status="starting")
        await self._startup_hook()
        
        # Start metrics update task
        self.metrics_task = asyncio.create_task(self._update_metrics_periodically())
        
        # Start HTTP server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        self.site = web.TCPSite(
            self.runner,
            host="0.0.0.0",
            port=self.config.observability.health_port
        )
        await self.site.start()
        
        self.logger.info(
            "Service started",
            port=self.config.observability.health_port
        )
    
    async def shutdown(self) -> None:
        """Gracefully shutdown service."""
        self.logger.info("Shutting down service")
        
        # Stop accepting new requests
        if self.site:
            await self.site.stop()
        
        # Shutdown service-specific components
        await self._shutdown_hook()
        
        # Stop metrics update task
        if self.metrics_task:
            self.metrics_task.cancel()
            try:
                await self.metrics_task
            except asyncio.CancelledError:
                pass
        
        # Stop Kafka consumers and producers
        for consumer in self.consumers:
            await consumer.stop()
        
        for producer in self.producers:
            await producer.stop()
        
        # Cleanup HTTP components
        if self.runner:
            await self.runner.cleanup()
        
        if self.session:
            await self.session.close()
        
        await self.audit.log_service_event("service_stop", status="stopped")
        await self.audit.shutdown()

        # Signal shutdown complete
        self.shutdown_event.set()
        
        self.logger.info("Service shutdown complete")
    
    def _setup_routes(self) -> None:
        """Setup HTTP routes."""
        if not self.app:
            return
        
        # Health check endpoints
        self.app.router.add_get("/health", self._health_handler)
        self.app.router.add_get("/health/ready", self._readiness_handler)
        self.app.router.add_get("/health/live", self._liveness_handler)
        
        # Metrics endpoint
        self.app.router.add_get("/metrics", self._metrics_handler)
        
        # Service-specific routes
        self._setup_service_routes()
    
    def _setup_service_routes(self) -> None:
        """Setup service-specific HTTP routes. Override in subclasses."""
        pass
    
    async def _health_handler(self, request: web.Request) -> web.Response:
        """Health check handler."""
        health_status = await self.health_checker.check_health()
        status_code = 200 if health_status["healthy"] else 503
        
        return web.json_response(health_status, status=status_code)
    
    async def _readiness_handler(self, request: web.Request) -> web.Response:
        """Readiness check handler."""
        ready_status = await self.health_checker.check_readiness()
        status_code = 200 if ready_status["ready"] else 503
        
        return web.json_response(ready_status, status=status_code)
    
    async def _liveness_handler(self, request: web.Request) -> web.Response:
        """Liveness check handler."""
        return web.json_response({"alive": True})
    
    async def _metrics_handler(self, request: web.Request) -> web.Response:
        """Metrics handler."""
        metrics_data = self.metrics.get_metrics()
        return web.Response(
            body=metrics_data,
            content_type=self.metrics.get_content_type()
        )
    
    @abstractmethod
    async def _startup_hook(self) -> None:
        """Service-specific startup logic. Override in subclasses."""
        pass
    
    @abstractmethod
    async def _shutdown_hook(self) -> None:
        """Service-specific shutdown logic. Override in subclasses."""
        pass
    
    async def _update_metrics_periodically(self) -> None:
        """Update metrics periodically."""
        while not self.shutdown_event.is_set():
            try:
                # Update service info
                self.metrics.update_service_info(
                    version=getattr(self.config, 'version', '1.0.0'),
                    environment=getattr(self.config, 'environment', 'development')
                )
                
                # Update health status
                health_status = await self.health_checker.check_health()
                self.metrics.set_health_status(health_status["healthy"])
                
                # Update memory usage
                try:
                    process = psutil.Process()
                    memory_info = process.memory_info()
                    self.metrics.set_memory_usage(memory_info.rss)
                except Exception as e:
                    logger.warning(f"Failed to update memory metrics: {e}")
                
                # Update active connections
                try:
                    # Count active connections (simplified)
                    self.metrics.set_active_connections("http", 1)  # At least the HTTP server
                except Exception as e:
                    logger.warning(f"Failed to update connection metrics: {e}")
                
                # Wait before next update
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating metrics: {e}")
                await asyncio.sleep(30)
    
    async def run(self) -> None:
        """Run the service."""
        try:
            await self.startup()
            await self.shutdown_event.wait()
        except Exception as e:
            self.logger.error("Service error", error=str(e), exc_info=True)
            raise
        finally:
            await self.shutdown()
    
    def add_consumer(self, consumer: KafkaConsumer) -> None:
        """Add a Kafka consumer to the service."""
        self.consumers.append(consumer)
    
    def add_producer(self, producer: KafkaProducer) -> None:
        """Add a Kafka producer to the service."""
        self.producers.append(producer)
    
    @asynccontextmanager
    async def get_session(self):
        """Get HTTP session context manager."""
        if not self.session:
            raise RuntimeError("Service not started")
        yield self.session
