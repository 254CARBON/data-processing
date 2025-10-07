"""Graceful shutdown handlers for services."""

import asyncio
import signal
import logging
from typing import List, Callable, Optional, Dict, Any
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class ShutdownReason(str, Enum):
    """Shutdown reason enumeration."""
    SIGNAL = "signal"
    MANUAL = "manual"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class ShutdownHandler:
    """Shutdown handler configuration."""
    name: str
    handler: Callable
    timeout: float = 30.0
    priority: int = 0  # Lower numbers run first
    critical: bool = False  # If True, shutdown fails if this handler fails


class GracefulShutdownManager:
    """Manages graceful shutdown of services."""
    
    def __init__(self, shutdown_timeout: float = 60.0):
        self.shutdown_timeout = shutdown_timeout
        self.handlers: List[ShutdownHandler] = []
        self.is_shutting_down = False
        self.shutdown_reason: Optional[ShutdownReason] = None
        self.logger = structlog.get_logger("graceful-shutdown")
        self._shutdown_event = asyncio.Event()
        self._shutdown_task: Optional[asyncio.Task] = None
    
    def add_handler(self, name: str, handler: Callable, timeout: float = 30.0, 
                   priority: int = 0, critical: bool = False) -> None:
        """Add a shutdown handler."""
        shutdown_handler = ShutdownHandler(
            name=name,
            handler=handler,
            timeout=timeout,
            priority=priority,
            critical=critical
        )
        
        self.handlers.append(shutdown_handler)
        # Sort by priority (lower numbers first)
        self.handlers.sort(key=lambda h: h.priority)
        
        self.logger.info("Shutdown handler added", 
                        name=name, 
                        timeout=timeout, 
                        priority=priority, 
                        critical=critical)
    
    def remove_handler(self, name: str) -> None:
        """Remove a shutdown handler."""
        self.handlers = [h for h in self.handlers if h.name != name]
        self.logger.info("Shutdown handler removed", name=name)
    
    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        try:
            # Setup signal handlers for Unix systems
            if hasattr(signal, 'SIGTERM'):
                signal.signal(signal.SIGTERM, self._signal_handler)
            if hasattr(signal, 'SIGINT'):
                signal.signal(signal.SIGINT, self._signal_handler)
            
            self.logger.info("Signal handlers setup completed")
            
        except Exception as e:
            self.logger.error("Failed to setup signal handlers", error=str(e))
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name
        self.logger.info("Received shutdown signal", signal=signal_name)
        
        if not self.is_shutting_down:
            self.shutdown_reason = ShutdownReason.SIGNAL
            asyncio.create_task(self.shutdown())
    
    async def shutdown(self, reason: ShutdownReason = ShutdownReason.MANUAL) -> None:
        """Initiate graceful shutdown."""
        if self.is_shutting_down:
            self.logger.warning("Shutdown already in progress")
            return
        
        self.is_shutting_down = True
        self.shutdown_reason = reason
        
        self.logger.info("Starting graceful shutdown", reason=reason.value)
        
        # Set shutdown event
        self._shutdown_event.set()
        
        # Execute shutdown handlers
        success = await self._execute_shutdown_handlers()
        
        if success:
            self.logger.info("Graceful shutdown completed successfully")
        else:
            self.logger.error("Graceful shutdown completed with errors")
    
    async def _execute_shutdown_handlers(self) -> bool:
        """Execute all shutdown handlers."""
        success = True
        
        for handler in self.handlers:
            try:
                self.logger.info("Executing shutdown handler", name=handler.name)
                
                # Execute handler with timeout
                await asyncio.wait_for(
                    self._execute_handler(handler),
                    timeout=handler.timeout
                )
                
                self.logger.info("Shutdown handler completed", name=handler.name)
                
            except asyncio.TimeoutError:
                self.logger.error("Shutdown handler timeout", 
                                name=handler.name, 
                                timeout=handler.timeout)
                if handler.critical:
                    success = False
                    
            except Exception as e:
                self.logger.error("Shutdown handler failed", 
                                name=handler.name, 
                                error=str(e))
                if handler.critical:
                    success = False
        
        return success
    
    async def _execute_handler(self, handler: ShutdownHandler) -> None:
        """Execute a single shutdown handler."""
        try:
            if asyncio.iscoroutinefunction(handler.handler):
                await handler.handler()
            else:
                handler.handler()
                
        except Exception as e:
            self.logger.error("Handler execution failed", 
                            name=handler.name, 
                            error=str(e))
            raise
    
    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()
    
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self.is_shutting_down
    
    def get_shutdown_reason(self) -> Optional[ShutdownReason]:
        """Get the reason for shutdown."""
        return self.shutdown_reason


class ServiceShutdownHandler:
    """Base class for service shutdown handlers."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = structlog.get_logger(f"shutdown-handler-{service_name}")
    
    async def shutdown(self) -> None:
        """Override this method to implement shutdown logic."""
        self.logger.info("Service shutdown handler called", service=self.service_name)
    
    def __call__(self) -> None:
        """Make the handler callable."""
        asyncio.create_task(self.shutdown())


class DatabaseShutdownHandler(ServiceShutdownHandler):
    """Shutdown handler for database connections."""
    
    def __init__(self, service_name: str, db_client):
        super().__init__(service_name)
        self.db_client = db_client
    
    async def shutdown(self) -> None:
        """Close database connections."""
        try:
            if hasattr(self.db_client, 'disconnect'):
                await self.db_client.disconnect()
                self.logger.info("Database connections closed", service=self.service_name)
            elif hasattr(self.db_client, 'close'):
                await self.db_client.close()
                self.logger.info("Database connections closed", service=self.service_name)
        except Exception as e:
            self.logger.error("Failed to close database connections", 
                            service=self.service_name, 
                            error=str(e))
            raise


class KafkaShutdownHandler(ServiceShutdownHandler):
    """Shutdown handler for Kafka consumers/producers."""
    
    def __init__(self, service_name: str, kafka_client):
        super().__init__(service_name)
        self.kafka_client = kafka_client
    
    async def shutdown(self) -> None:
        """Close Kafka connections."""
        try:
            if hasattr(self.kafka_client, 'stop'):
                await self.kafka_client.stop()
                self.logger.info("Kafka client stopped", service=self.service_name)
            elif hasattr(self.kafka_client, 'close'):
                await self.kafka_client.close()
                self.logger.info("Kafka client closed", service=self.service_name)
        except Exception as e:
            self.logger.error("Failed to close Kafka client", 
                            service=self.service_name, 
                            error=str(e))
            raise


class CacheShutdownHandler(ServiceShutdownHandler):
    """Shutdown handler for cache connections."""
    
    def __init__(self, service_name: str, cache_client):
        super().__init__(service_name)
        self.cache_client = cache_client
    
    async def shutdown(self) -> None:
        """Close cache connections."""
        try:
            if hasattr(self.cache_client, 'disconnect'):
                await self.cache_client.disconnect()
                self.logger.info("Cache connections closed", service=self.service_name)
            elif hasattr(self.cache_client, 'close'):
                await self.cache_client.close()
                self.logger.info("Cache connections closed", service=self.service_name)
        except Exception as e:
            self.logger.error("Failed to close cache connections", 
                            service=self.service_name, 
                            error=str(e))
            raise


class HTTPShutdownHandler(ServiceShutdownHandler):
    """Shutdown handler for HTTP servers."""
    
    def __init__(self, service_name: str, http_server):
        super().__init__(service_name)
        self.http_server = http_server
    
    async def shutdown(self) -> None:
        """Close HTTP server."""
        try:
            if hasattr(self.http_server, 'shutdown'):
                await self.http_server.shutdown()
                self.logger.info("HTTP server shutdown", service=self.service_name)
            elif hasattr(self.http_server, 'close'):
                await self.http_server.close()
                self.logger.info("HTTP server closed", service=self.service_name)
        except Exception as e:
            self.logger.error("Failed to shutdown HTTP server", 
                            service=self.service_name, 
                            error=str(e))
            raise


@asynccontextmanager
async def graceful_shutdown_context(shutdown_manager: GracefulShutdownManager):
    """Context manager for graceful shutdown."""
    try:
        # Setup signal handlers
        shutdown_manager.setup_signal_handlers()
        
        # Yield control
        yield shutdown_manager
        
    finally:
        # Ensure shutdown is called
        if not shutdown_manager.is_shutdown_requested():
            await shutdown_manager.shutdown(ShutdownReason.MANUAL)


class ShutdownMetrics:
    """Metrics for shutdown monitoring."""
    
    def __init__(self):
        self.logger = structlog.get_logger("shutdown-metrics")
        self.shutdown_count = 0
        self.shutdown_duration = 0.0
        self.handler_durations: Dict[str, float] = {}
        self.handler_failures: Dict[str, int] = {}
    
    def record_shutdown(self, duration: float) -> None:
        """Record shutdown metrics."""
        self.shutdown_count += 1
        self.shutdown_duration = duration
        self.logger.info("Shutdown metrics recorded", 
                        duration=duration, 
                        total_shutdowns=self.shutdown_count)
    
    def record_handler_duration(self, handler_name: str, duration: float) -> None:
        """Record handler execution duration."""
        self.handler_durations[handler_name] = duration
        self.logger.debug("Handler duration recorded", 
                         handler=handler_name, 
                         duration=duration)
    
    def record_handler_failure(self, handler_name: str) -> None:
        """Record handler failure."""
        self.handler_failures[handler_name] = self.handler_failures.get(handler_name, 0) + 1
        self.logger.warning("Handler failure recorded", handler=handler_name)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get shutdown metrics."""
        return {
            "shutdown_count": self.shutdown_count,
            "shutdown_duration": self.shutdown_duration,
            "handler_durations": self.handler_durations,
            "handler_failures": self.handler_failures,
            "avg_handler_duration": sum(self.handler_durations.values()) / len(self.handler_durations) if self.handler_durations else 0
        }
