"""Connection pool metrics for monitoring."""

import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry


@dataclass
class PoolMetrics:
    """Connection pool metrics."""
    
    # Connection metrics
    connections_active: Gauge
    connections_idle: Gauge
    connections_total: Gauge
    
    # Request metrics
    requests_total: Counter
    request_duration: Histogram
    request_errors: Counter
    
    # Pool metrics
    pool_size: Gauge
    pool_max_size: Gauge
    pool_wait_time: Histogram


class PoolMetricsCollector:
    """Collects metrics from connection pools."""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self.metrics: Dict[str, PoolMetrics] = {}
    
    def register_pool(self, pool_name: str, pool_obj: Any) -> PoolMetrics:
        """Register a connection pool for metrics collection."""
        
        # Create metrics for this pool
        metrics = PoolMetrics(
            connections_active=Gauge(
                f'{pool_name}_connections_active',
                f'Active connections for {pool_name}',
                registry=self.registry
            ),
            connections_idle=Gauge(
                f'{pool_name}_connections_idle', 
                f'Idle connections for {pool_name}',
                registry=self.registry
            ),
            connections_total=Gauge(
                f'{pool_name}_connections_total',
                f'Total connections for {pool_name}',
                registry=self.registry
            ),
            requests_total=Counter(
                f'{pool_name}_requests_total',
                f'Total requests for {pool_name}',
                registry=self.registry
            ),
            request_duration=Histogram(
                f'{pool_name}_request_duration_seconds',
                f'Request duration for {pool_name}',
                registry=self.registry
            ),
            request_errors=Counter(
                f'{pool_name}_request_errors_total',
                f'Request errors for {pool_name}',
                registry=self.registry
            ),
            pool_size=Gauge(
                f'{pool_name}_pool_size',
                f'Current pool size for {pool_name}',
                registry=self.registry
            ),
            pool_max_size=Gauge(
                f'{pool_name}_pool_max_size',
                f'Maximum pool size for {pool_name}',
                registry=self.registry
            ),
            pool_wait_time=Histogram(
                f'{pool_name}_pool_wait_seconds',
                f'Pool wait time for {pool_name}',
                registry=self.registry
            )
        )
        
        self.metrics[pool_name] = metrics
        return metrics
    
    def update_pool_metrics(self, pool_name: str, pool_obj: Any) -> None:
        """Update metrics for a specific pool."""
        if pool_name not in self.metrics:
            return
        
        metrics = self.metrics[pool_name]
        
        try:
            # Update connection metrics
            if hasattr(pool_obj, 'size'):
                metrics.pool_size.set(pool_obj.size)
            if hasattr(pool_obj, 'maxsize'):
                metrics.pool_max_size.set(pool_obj.maxsize)
            
            # Update connection counts
            if hasattr(pool_obj, '_pool'):
                # For asyncpg pools
                metrics.connections_active.set(len(pool_obj._pool._holders))
                metrics.connections_idle.set(len(pool_obj._pool._queue))
            elif hasattr(pool_obj, 'connections'):
                # For other pool types
                metrics.connections_active.set(len(pool_obj.connections))
                metrics.connections_idle.set(len(pool_obj.idle_connections))
                
        except Exception as e:
            # Log error but don't fail
            pass
    
    def record_request(self, pool_name: str, duration: float, success: bool = True) -> None:
        """Record a request metric."""
        if pool_name not in self.metrics:
            return
        
        metrics = self.metrics[pool_name]
        metrics.requests_total.inc()
        metrics.request_duration.observe(duration)
        
        if not success:
            metrics.request_errors.inc()
    
    def record_pool_wait(self, pool_name: str, wait_time: float) -> None:
        """Record pool wait time."""
        if pool_name not in self.metrics:
            return
        
        metrics = self.metrics[pool_name]
        metrics.pool_wait_time.observe(wait_time)


# Global metrics collector instance
pool_metrics_collector = PoolMetricsCollector()
