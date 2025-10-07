"""Prometheus metrics collection for data processing services."""

import time
import logging
from typing import Dict, Any, Optional, Callable
from functools import wraps
from dataclasses import dataclass
from enum import Enum

try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Summary, Info,
        CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Mock classes for when prometheus_client is not available
    class Counter:
        def __init__(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    class Histogram:
        def __init__(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    class Gauge:
        def __init__(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def dec(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    class Summary:
        def __init__(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    class Info:
        def __init__(self, *args, **kwargs): pass
        def info(self, *args, **kwargs): pass
    class CollectorRegistry:
        def register(self, *args, **kwargs): pass
    def generate_latest(): return b""
    CONTENT_TYPE_LATEST = "text/plain"

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics supported."""
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    SUMMARY = "summary"
    INFO = "info"


@dataclass
class MetricConfig:
    """Configuration for a metric."""
    name: str
    description: str
    metric_type: MetricType
    labels: Optional[list] = None
    buckets: Optional[list] = None  # For histograms


class MetricsCollector:
    """Centralized metrics collection for data processing services."""
    
    def __init__(self, service_name: str, registry: Optional[CollectorRegistry] = None):
        self.service_name = service_name
        self.registry = registry or CollectorRegistry()
        self.metrics: Dict[str, Any] = {}
        
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus client not available. Metrics will be no-ops.")
            
        # Initialize common metrics
        self._init_common_metrics()
        
    def _init_common_metrics(self):
        """Initialize common metrics for all services."""
        # Service info
        self.info = Info(
            f"{self.service_name}_info",
            f"Information about {self.service_name}",
            registry=self.registry
        )
        
        # Request metrics
        self.request_count = Counter(
            f"{self.service_name}_requests_total",
            f"Total number of requests processed by {self.service_name}",
            ["method", "endpoint", "status"],
            registry=self.registry
        )
        
        self.request_duration = Histogram(
            f"{self.service_name}_request_duration_seconds",
            f"Request duration in seconds for {self.service_name}",
            ["method", "endpoint"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        # Processing metrics
        self.messages_processed = Counter(
            f"{self.service_name}_messages_processed_total",
            f"Total number of messages processed by {self.service_name}",
            ["topic", "status"],
            registry=self.registry
        )
        
        self.processing_duration = Histogram(
            f"{self.service_name}_processing_duration_seconds",
            f"Message processing duration in seconds",
            ["topic", "message_type"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
            registry=self.registry
        )
        
        # Error metrics
        self.errors_total = Counter(
            f"{self.service_name}_errors_total",
            f"Total number of errors in {self.service_name}",
            ["error_type", "component"],
            registry=self.registry
        )
        
        # Health metrics
        self.health_status = Gauge(
            f"{self.service_name}_health_status",
            f"Health status of {self.service_name} (1=healthy, 0=unhealthy)",
            registry=self.registry
        )
        
        # Resource metrics
        self.active_connections = Gauge(
            f"{self.service_name}_active_connections",
            f"Number of active connections for {self.service_name}",
            ["connection_type"],
            registry=self.registry
        )
        
        self.memory_usage = Gauge(
            f"{self.service_name}_memory_usage_bytes",
            f"Memory usage in bytes for {self.service_name}",
            registry=self.registry
        )
        
    def create_counter(self, name: str, description: str, labels: Optional[list] = None) -> Counter:
        """Create a custom counter metric."""
        full_name = f"{self.service_name}_{name}"
        counter = Counter(full_name, description, labels or [], registry=self.registry)
        self.metrics[name] = counter
        return counter
        
    def create_histogram(self, name: str, description: str, labels: Optional[list] = None, 
                        buckets: Optional[list] = None) -> Histogram:
        """Create a custom histogram metric."""
        full_name = f"{self.service_name}_{name}"
        histogram = Histogram(
            full_name, description, labels or [], 
            buckets=buckets or [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
            registry=self.registry
        )
        self.metrics[name] = histogram
        return histogram
        
    def create_gauge(self, name: str, description: str, labels: Optional[list] = None) -> Gauge:
        """Create a custom gauge metric."""
        full_name = f"{self.service_name}_{name}"
        gauge = Gauge(full_name, description, labels or [], registry=self.registry)
        self.metrics[name] = gauge
        return gauge
        
    def create_summary(self, name: str, description: str, labels: Optional[list] = None) -> Summary:
        """Create a custom summary metric."""
        full_name = f"{self.service_name}_{name}"
        summary = Summary(full_name, description, labels or [], registry=self.registry)
        self.metrics[name] = summary
        return summary
        
    def record_request(self, method: str, endpoint: str, status: str, duration: float):
        """Record a request metric."""
        self.request_count.labels(method=method, endpoint=endpoint, status=status).inc()
        self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)
        
    def record_message_processed(self, topic: str, status: str, duration: Optional[float] = None, 
                                message_type: Optional[str] = None):
        """Record a message processing metric."""
        self.messages_processed.labels(topic=topic, status=status).inc()
        if duration is not None and message_type is not None:
            self.processing_duration.labels(topic=topic, message_type=message_type).observe(duration)
            
    def record_error(self, error_type: str, component: str):
        """Record an error metric."""
        self.errors_total.labels(error_type=error_type, component=component).inc()
        
    def set_health_status(self, healthy: bool):
        """Set the health status metric."""
        self.health_status.set(1 if healthy else 0)
        
    def set_active_connections(self, connection_type: str, count: int):
        """Set the active connections metric."""
        self.active_connections.labels(connection_type=connection_type).set(count)
        
    def set_memory_usage(self, bytes_used: int):
        """Set the memory usage metric."""
        self.memory_usage.set(bytes_used)
        
    def update_service_info(self, version: str, environment: str, **kwargs):
        """Update service information."""
        info_dict = {
            "version": version,
            "environment": environment,
            **kwargs
        }
        self.info.info(info_dict)
        
    def get_metrics(self) -> bytes:
        """Get all metrics in Prometheus format."""
        return generate_latest(self.registry)
        
    def get_content_type(self) -> str:
        """Get the content type for metrics."""
        return CONTENT_TYPE_LATEST


def track_processing_time(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """Decorator to track processing time for functions."""
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Try to get metrics collector from the service instance
                if args and hasattr(args[0], 'metrics'):
                    metrics = args[0].metrics
                    if hasattr(metrics, metric_name):
                        metric = getattr(metrics, metric_name)
                        if labels:
                            metric.labels(**labels).observe(duration)
                        else:
                            metric.observe(duration)
                
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Error in {func.__name__}: {e}")
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Try to get metrics collector from the service instance
                if args and hasattr(args[0], 'metrics'):
                    metrics = args[0].metrics
                    if hasattr(metrics, metric_name):
                        metric = getattr(metrics, metric_name)
                        if labels:
                            metric.labels(**labels).observe(duration)
                        else:
                            metric.observe(duration)
                
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Error in {func.__name__}: {e}")
                raise
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def track_message_count(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """Decorator to track message count for functions."""
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                
                # Try to get metrics collector from the service instance
                if args and hasattr(args[0], 'metrics'):
                    metrics = args[0].metrics
                    if hasattr(metrics, metric_name):
                        metric = getattr(metrics, metric_name)
                        if labels:
                            metric.labels(**labels).inc()
                        else:
                            metric.inc()
                
                return result
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {e}")
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                
                # Try to get metrics collector from the service instance
                if args and hasattr(args[0], 'metrics'):
                    metrics = args[0].metrics
                    if hasattr(metrics, metric_name):
                        metric = getattr(metrics, metric_name)
                        if labels:
                            metric.labels(**labels).inc()
                        else:
                            metric.inc()
                
                return result
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {e}")
                raise
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


# Import asyncio for the decorator
import asyncio