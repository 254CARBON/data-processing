"""Cache metrics for Prometheus monitoring."""

import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
import structlog

logger = structlog.get_logger()


class CacheMetrics:
    """Cache metrics for monitoring."""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self.logger = structlog.get_logger("cache-metrics")
        
        # Cache operation metrics
        self.cache_operations_total = Counter(
            'cache_operations_total',
            'Total cache operations',
            ['operation', 'cache_type', 'result'],
            registry=self.registry
        )
        
        self.cache_operation_duration = Histogram(
            'cache_operation_duration_seconds',
            'Cache operation duration',
            ['operation', 'cache_type'],
            registry=self.registry
        )
        
        # Cache hit/miss metrics
        self.cache_hits_total = Counter(
            'cache_hits_total',
            'Total cache hits',
            ['cache_type'],
            registry=self.registry
        )
        
        self.cache_misses_total = Counter(
            'cache_misses_total',
            'Total cache misses',
            ['cache_type'],
            registry=self.registry
        )
        
        # Cache size metrics
        self.cache_size = Gauge(
            'cache_size_bytes',
            'Cache size in bytes',
            ['cache_type'],
            registry=self.registry
        )
        
        self.cache_entries = Gauge(
            'cache_entries_total',
            'Total cache entries',
            ['cache_type'],
            registry=self.registry
        )
        
        # Cache performance metrics
        self.cache_hit_rate = Gauge(
            'cache_hit_rate',
            'Cache hit rate percentage',
            ['cache_type'],
            registry=self.registry
        )
        
        self.cache_evictions_total = Counter(
            'cache_evictions_total',
            'Total cache evictions',
            ['cache_type', 'reason'],
            registry=self.registry
        )
        
        # Redis-specific metrics
        self.redis_connections = Gauge(
            'redis_connections_total',
            'Redis connections',
            registry=self.registry
        )
        
        self.redis_memory_usage = Gauge(
            'redis_memory_usage_bytes',
            'Redis memory usage',
            registry=self.registry
        )
        
        self.redis_memory_usage_percent = Gauge(
            'redis_memory_usage_percent',
            'Redis memory usage percentage',
            registry=self.registry
        )
        
        # Cache invalidation metrics
        self.cache_invalidations_total = Counter(
            'cache_invalidations_total',
            'Total cache invalidations',
            ['cache_type', 'invalidation_type'],
            registry=self.registry
        )
        
        self.cache_invalidation_duration = Histogram(
            'cache_invalidation_duration_seconds',
            'Cache invalidation duration',
            ['cache_type', 'invalidation_type'],
            registry=self.registry
        )
        
        # Cache warming metrics
        self.cache_warming_operations_total = Counter(
            'cache_warming_operations_total',
            'Total cache warming operations',
            ['cache_type', 'result'],
            registry=self.registry
        )
        
        self.cache_warming_duration = Histogram(
            'cache_warming_duration_seconds',
            'Cache warming duration',
            ['cache_type'],
            registry=self.registry
        )
    
    def record_cache_operation(self, operation: str, cache_type: str, 
                             result: str, duration: float) -> None:
        """Record cache operation metrics."""
        self.cache_operations_total.labels(
            operation=operation,
            cache_type=cache_type,
            result=result
        ).inc()
        
        self.cache_operation_duration.labels(
            operation=operation,
            cache_type=cache_type
        ).observe(duration)
    
    def record_cache_hit(self, cache_type: str) -> None:
        """Record cache hit."""
        self.cache_hits_total.labels(cache_type=cache_type).inc()
        self._update_hit_rate(cache_type)
    
    def record_cache_miss(self, cache_type: str) -> None:
        """Record cache miss."""
        self.cache_misses_total.labels(cache_type=cache_type).inc()
        self._update_hit_rate(cache_type)
    
    def update_cache_size(self, cache_type: str, size_bytes: int) -> None:
        """Update cache size metric."""
        self.cache_size.labels(cache_type=cache_type).set(size_bytes)
    
    def update_cache_entries(self, cache_type: str, count: int) -> None:
        """Update cache entries metric."""
        self.cache_entries.labels(cache_type=cache_type).set(count)
    
    def record_cache_eviction(self, cache_type: str, reason: str) -> None:
        """Record cache eviction."""
        self.cache_evictions_total.labels(
            cache_type=cache_type,
            reason=reason
        ).inc()
    
    def update_redis_metrics(self, redis_info: Dict[str, Any]) -> None:
        """Update Redis-specific metrics."""
        self.redis_connections.set(redis_info.get("connected_clients", 0))
        self.redis_memory_usage.set(redis_info.get("used_memory", 0))
        self.redis_memory_usage_percent.set(redis_info.get("used_memory_percent", 0))
    
    def record_cache_invalidation(self, cache_type: str, invalidation_type: str, 
                                 duration: float) -> None:
        """Record cache invalidation metrics."""
        self.cache_invalidations_total.labels(
            cache_type=cache_type,
            invalidation_type=invalidation_type
        ).inc()
        
        self.cache_invalidation_duration.labels(
            cache_type=cache_type,
            invalidation_type=invalidation_type
        ).observe(duration)
    
    def record_cache_warming(self, cache_type: str, result: str, duration: float) -> None:
        """Record cache warming metrics."""
        self.cache_warming_operations_total.labels(
            cache_type=cache_type,
            result=result
        ).inc()
        
        self.cache_warming_duration.labels(cache_type=cache_type).observe(duration)
    
    def _update_hit_rate(self, cache_type: str) -> None:
        """Update cache hit rate."""
        hits = self.cache_hits_total.labels(cache_type=cache_type)._value.get()
        misses = self.cache_misses_total.labels(cache_type=cache_type)._value.get()
        
        total = hits + misses
        if total > 0:
            hit_rate = (hits / total) * 100
            self.cache_hit_rate.labels(cache_type=cache_type).set(hit_rate)


class CacheMetricsCollector:
    """Collects cache metrics from various sources."""
    
    def __init__(self, cache_metrics: CacheMetrics):
        self.cache_metrics = cache_metrics
        self.logger = structlog.get_logger("cache-metrics-collector")
    
    def collect_from_cache_manager(self, cache_manager, cache_type: str) -> None:
        """Collect metrics from cache manager."""
        try:
            stats = cache_manager.get_stats()
            
            # Update hit/miss metrics
            self.cache_metrics.cache_hits_total.labels(cache_type=cache_type)._value.set(stats["hits"])
            self.cache_metrics.cache_misses_total.labels(cache_type=cache_type)._value.set(stats["misses"])
            
            # Update hit rate
            total = stats["hits"] + stats["misses"]
            if total > 0:
                hit_rate = (stats["hits"] / total) * 100
                self.cache_metrics.cache_hit_rate.labels(cache_type=cache_type).set(hit_rate)
            
            # Update cache entries
            self.cache_metrics.cache_entries.labels(cache_type=cache_type).set(stats["local_cache_size"])
            
        except Exception as e:
            self.logger.error("Failed to collect cache manager metrics", 
                            cache_type=cache_type, 
                            error=str(e))
    
    def collect_from_redis(self, redis_client, cache_type: str) -> None:
        """Collect metrics from Redis."""
        try:
            redis_info = redis_client.client.info()
            
            # Update Redis metrics
            self.cache_metrics.update_redis_metrics(redis_info)
            
            # Update cache size
            self.cache_metrics.update_cache_size(cache_type, redis_info.get("used_memory", 0))
            
        except Exception as e:
            self.logger.error("Failed to collect Redis metrics", 
                            cache_type=cache_type, 
                            error=str(e))
    
    def collect_from_invalidation_manager(self, invalidation_manager) -> None:
        """Collect metrics from invalidation manager."""
        try:
            stats = invalidation_manager.get_invalidation_stats()
            
            # Update invalidation metrics
            self.cache_metrics.cache_invalidations_total.labels(
                cache_type="all",
                invalidation_type="pattern"
            )._value.set(stats["pattern_invalidations"])
            
            self.cache_metrics.cache_invalidations_total.labels(
                cache_type="all",
                invalidation_type="dependency"
            )._value.set(stats["dependency_invalidations"])
            
            self.cache_metrics.cache_invalidations_total.labels(
                cache_type="all",
                invalidation_type="manual"
            )._value.set(stats["manual_invalidations"])
            
        except Exception as e:
            self.logger.error("Failed to collect invalidation manager metrics", error=str(e))
    
    def collect_from_warming_strategy(self, warming_strategy) -> None:
        """Collect metrics from warming strategy."""
        try:
            stats = warming_strategy.get_warming_stats()
            
            # Update warming metrics
            self.cache_metrics.cache_warming_operations_total.labels(
                cache_type="all",
                result="success"
            )._value.set(stats["successful_warming_operations"])
            
            self.cache_metrics.cache_warming_operations_total.labels(
                cache_type="all",
                result="failure"
            )._value.set(stats["failed_warming_operations"])
            
        except Exception as e:
            self.logger.error("Failed to collect warming strategy metrics", error=str(e))


class CachePerformanceMonitor:
    """Monitors cache performance and provides insights."""
    
    def __init__(self, cache_metrics: CacheMetrics):
        self.cache_metrics = cache_metrics
        self.logger = structlog.get_logger("cache-performance-monitor")
        self.performance_history: Dict[str, List[float]] = {}
    
    def analyze_performance(self, cache_type: str) -> Dict[str, Any]:
        """Analyze cache performance."""
        try:
            # Get current metrics
            hits = self.cache_metrics.cache_hits_total.labels(cache_type=cache_type)._value.get()
            misses = self.cache_metrics.cache_misses_total.labels(cache_type=cache_type)._value.get()
            hit_rate = self.cache_metrics.cache_hit_rate.labels(cache_type=cache_type)._value.get()
            
            # Calculate performance metrics
            total_requests = hits + misses
            miss_rate = 100 - hit_rate if hit_rate else 0
            
            # Performance recommendations
            recommendations = []
            if hit_rate < 70:
                recommendations.append("Consider increasing cache TTL")
            if hit_rate < 50:
                recommendations.append("Review cache key strategy")
            if miss_rate > 30:
                recommendations.append("Implement cache warming")
            
            return {
                "cache_type": cache_type,
                "hit_rate": hit_rate,
                "miss_rate": miss_rate,
                "total_requests": total_requests,
                "hits": hits,
                "misses": misses,
                "recommendations": recommendations,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error("Failed to analyze cache performance", 
                            cache_type=cache_type, 
                            error=str(e))
            return {}
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for all cache types."""
        try:
            summary = {
                "overall_hit_rate": 0,
                "total_requests": 0,
                "cache_types": {},
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Analyze each cache type
            cache_types = ["metadata", "price", "taxonomy", "projection"]
            total_hits = 0
            total_misses = 0
            
            for cache_type in cache_types:
                analysis = self.analyze_performance(cache_type)
                if analysis:
                    summary["cache_types"][cache_type] = analysis
                    total_hits += analysis.get("hits", 0)
                    total_misses += analysis.get("misses", 0)
            
            # Calculate overall metrics
            total_requests = total_hits + total_misses
            if total_requests > 0:
                summary["overall_hit_rate"] = (total_hits / total_requests) * 100
                summary["total_requests"] = total_requests
            
            return summary
            
        except Exception as e:
            self.logger.error("Failed to get performance summary", error=str(e))
            return {}
    
    def detect_performance_issues(self) -> List[Dict[str, Any]]:
        """Detect cache performance issues."""
        try:
            issues = []
            summary = self.get_performance_summary()
            
            # Check overall hit rate
            if summary.get("overall_hit_rate", 0) < 70:
                issues.append({
                    "type": "low_hit_rate",
                    "severity": "warning",
                    "message": f"Overall cache hit rate is {summary.get('overall_hit_rate', 0):.1f}%",
                    "recommendation": "Review cache configuration and TTL settings"
                })
            
            # Check individual cache types
            for cache_type, analysis in summary.get("cache_types", {}).items():
                hit_rate = analysis.get("hit_rate", 0)
                if hit_rate < 50:
                    issues.append({
                        "type": "critical_hit_rate",
                        "severity": "critical",
                        "cache_type": cache_type,
                        "message": f"Cache hit rate for {cache_type} is {hit_rate:.1f}%",
                        "recommendation": "Immediate cache optimization required"
                    })
            
            return issues
            
        except Exception as e:
            self.logger.error("Failed to detect performance issues", error=str(e))
            return []
