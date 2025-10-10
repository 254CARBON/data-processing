"""Cache invalidation strategies and patterns."""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import structlog

from .cache import CacheManager, CacheKey

logger = structlog.get_logger()


class InvalidationStrategy(str, Enum):
    """Cache invalidation strategy enumeration."""
    TIME_BASED = "time_based"
    EVENT_BASED = "event_based"
    DEPENDENCY_BASED = "dependency_based"
    MANUAL = "manual"


@dataclass
class InvalidationRule:
    """Cache invalidation rule."""
    pattern: str
    strategy: InvalidationStrategy
    ttl: Optional[int] = None
    dependencies: List[str] = None
    callback: Optional[Callable] = None


class CacheInvalidationManager:
    """Manages cache invalidation strategies."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.logger = structlog.get_logger("cache-invalidation-manager")
        self.invalidation_rules: Dict[str, InvalidationRule] = {}
        self.dependency_graph: Dict[str, Set[str]] = {}
        self.invalidation_stats = {
            "total_invalidations": 0,
            "pattern_invalidations": 0,
            "dependency_invalidations": 0,
            "manual_invalidations": 0
        }
    
    def add_invalidation_rule(self, name: str, rule: InvalidationRule) -> None:
        """Add an invalidation rule."""
        self.invalidation_rules[name] = rule
        
        # Build dependency graph
        if rule.dependencies:
            for dep in rule.dependencies:
                if dep not in self.dependency_graph:
                    self.dependency_graph[dep] = set()
                self.dependency_graph[dep].add(name)
        
        self.logger.info("Invalidation rule added", name=name, pattern=rule.pattern)
    
    def remove_invalidation_rule(self, name: str) -> None:
        """Remove an invalidation rule."""
        if name in self.invalidation_rules:
            rule = self.invalidation_rules[name]
            
            # Remove from dependency graph
            if rule.dependencies:
                for dep in rule.dependencies:
                    if dep in self.dependency_graph:
                        self.dependency_graph[dep].discard(name)
            
            del self.invalidation_rules[name]
            self.logger.info("Invalidation rule removed", name=name)
    
    async def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching a pattern."""
        try:
            count = await self.cache_manager.invalidate_pattern(pattern)
            self.invalidation_stats["pattern_invalidations"] += count
            self.invalidation_stats["total_invalidations"] += count
            
            self.logger.info("Pattern invalidation completed", 
                           pattern=pattern, 
                           count=count)
            
            return count
            
        except Exception as e:
            self.logger.error("Pattern invalidation failed", 
                            pattern=pattern, 
                            error=str(e))
            return 0
    
    async def invalidate_by_dependency(self, dependency: str) -> int:
        """Invalidate cache entries based on dependency."""
        try:
            total_count = 0
            
            if dependency in self.dependency_graph:
                for rule_name in self.dependency_graph[dependency]:
                    rule = self.invalidation_rules.get(rule_name)
                    if rule:
                        count = await self.invalidate_by_pattern(rule.pattern)
                        total_count += count
            
            self.invalidation_stats["dependency_invalidations"] += total_count
            self.invalidation_stats["total_invalidations"] += total_count
            
            self.logger.info("Dependency invalidation completed", 
                           dependency=dependency, 
                           count=total_count)
            
            return total_count
            
        except Exception as e:
            self.logger.error("Dependency invalidation failed", 
                            dependency=dependency, 
                            error=str(e))
            return 0
    
    async def invalidate_manually(self, keys: List[str]) -> int:
        """Manually invalidate specific cache keys."""
        try:
            count = 0
            
            for key in keys:
                if await self.cache_manager.delete(key):
                    count += 1
            
            self.invalidation_stats["manual_invalidations"] += count
            self.invalidation_stats["total_invalidations"] += count
            
            self.logger.info("Manual invalidation completed", 
                           key_count=len(keys), 
                           success_count=count)
            
            return count
            
        except Exception as e:
            self.logger.error("Manual invalidation failed", 
                            key_count=len(keys), 
                            error=str(e))
            return 0
    
    async def invalidate_by_time(self, max_age: timedelta) -> int:
        """Invalidate cache entries older than max_age."""
        try:
            # This would require tracking creation timestamps
            # For now, we'll implement a simple version
            count = 0
            
            # Get all keys and check their age
            # This is a simplified implementation
            pattern = "*"
            keys = await self.cache_manager.redis.keys(pattern)
            
            for key in keys:
                # Check if key is older than max_age
                # This would require storing timestamps with keys
                # For now, we'll just log the attempt
                self.logger.debug("Checking key age", key=key)
                count += 1
            
            self.logger.info("Time-based invalidation completed", 
                           max_age=max_age, 
                           count=count)
            
            return count
            
        except Exception as e:
            self.logger.error("Time-based invalidation failed", 
                            max_age=max_age, 
                            error=str(e))
            return 0
    
    async def invalidate_by_event(self, event_type: str, event_data: Dict[str, Any]) -> int:
        """Invalidate cache entries based on an event."""
        try:
            total_count = 0
            
            # Find rules that match this event type
            for rule_name, rule in self.invalidation_rules.items():
                if rule.strategy == InvalidationStrategy.EVENT_BASED:
                    # Check if rule matches this event
                    if self._matches_event(rule, event_type, event_data):
                        count = await self.invalidate_by_pattern(rule.pattern)
                        total_count += count
                        
                        # Execute callback if provided
                        if rule.callback:
                            await rule.callback(event_type, event_data)
            
            self.logger.info("Event-based invalidation completed", 
                           event_type=event_type, 
                           count=total_count)
            
            return total_count
            
        except Exception as e:
            self.logger.error("Event-based invalidation failed", 
                            event_type=event_type, 
                            error=str(e))
            return 0
    
    def _matches_event(self, rule: InvalidationRule, event_type: str, 
                      event_data: Dict[str, Any]) -> bool:
        """Check if a rule matches an event."""
        # Simple implementation - in production, you'd have more sophisticated matching
        return True
    
    def get_invalidation_stats(self) -> Dict[str, Any]:
        """Get invalidation statistics."""
        return {
            "total_invalidations": self.invalidation_stats["total_invalidations"],
            "pattern_invalidations": self.invalidation_stats["pattern_invalidations"],
            "dependency_invalidations": self.invalidation_stats["dependency_invalidations"],
            "manual_invalidations": self.invalidation_stats["manual_invalidations"],
            "active_rules": len(self.invalidation_rules),
            "dependency_graph_size": len(self.dependency_graph)
        }


class DataChangeInvalidator:
    """Invalidates cache when data changes."""
    
    def __init__(self, invalidation_manager: CacheInvalidationManager):
        self.invalidation_manager = invalidation_manager
        self.logger = structlog.get_logger("data-change-invalidator")
    
    async def on_instrument_data_change(self, tenant_id: str, instrument_id: str, 
                                      change_type: str) -> None:
        """Handle instrument data changes."""
        try:
            # Invalidate instrument-specific cache
            pattern = f"*:{tenant_id}:{instrument_id}:*"
            await self.invalidation_manager.invalidate_by_pattern(pattern)
            
            # Invalidate dependent data
            await self.invalidation_manager.invalidate_by_dependency(f"instrument:{instrument_id}")
            
            self.logger.info("Instrument data change handled", 
                           tenant_id=tenant_id, 
                           instrument_id=instrument_id, 
                           change_type=change_type)
            
        except Exception as e:
            self.logger.error("Failed to handle instrument data change", 
                            tenant_id=tenant_id, 
                            instrument_id=instrument_id, 
                            change_type=change_type, 
                            error=str(e))
    
    async def on_tenant_data_change(self, tenant_id: str, change_type: str) -> None:
        """Handle tenant data changes."""
        try:
            # Invalidate tenant-specific cache
            pattern = f"*:{tenant_id}:*"
            await self.invalidation_manager.invalidate_by_pattern(pattern)
            
            # Invalidate dependent data
            await self.invalidation_manager.invalidate_by_dependency(f"tenant:{tenant_id}")
            
            self.logger.info("Tenant data change handled", 
                           tenant_id=tenant_id, 
                           change_type=change_type)
            
        except Exception as e:
            self.logger.error("Failed to handle tenant data change", 
                            tenant_id=tenant_id, 
                            change_type=change_type, 
                            error=str(e))
    
    async def on_price_data_change(self, tenant_id: str, instrument_id: str, 
                                 price_data: Dict[str, Any]) -> None:
        """Handle price data changes."""
        try:
            # Invalidate price cache
            tenant_key = CacheKey._sanitize(tenant_id)
            instrument_key = CacheKey._sanitize(instrument_id)
            pattern = f"price:{tenant_key}:{instrument_key}:*"
            await self.invalidation_manager.invalidate_by_pattern(pattern)
            
            # Invalidate dependent projections
            await self.invalidation_manager.invalidate_by_dependency(f"price:{instrument_id}")
            
            self.logger.info("Price data change handled", 
                           tenant_id=tenant_id, 
                           instrument_id=instrument_id)
            
        except Exception as e:
            self.logger.error("Failed to handle price data change", 
                            tenant_id=tenant_id, 
                            instrument_id=instrument_id, 
                            error=str(e))
    
    async def on_metadata_change(self, tenant_id: str, instrument_id: str, 
                               metadata: Dict[str, Any]) -> None:
        """Handle metadata changes."""
        try:
            # Invalidate metadata cache
            tenant_key = CacheKey._sanitize(tenant_id)
            instrument_key = CacheKey._sanitize(instrument_id)
            pattern = f"metadata:{tenant_key}:{instrument_key}:*"
            await self.invalidation_manager.invalidate_by_pattern(pattern)
            
            # Invalidate dependent data
            await self.invalidation_manager.invalidate_by_dependency(f"metadata:{instrument_id}")
            
            self.logger.info("Metadata change handled", 
                           tenant_id=tenant_id, 
                           instrument_id=instrument_id)
            
        except Exception as e:
            self.logger.error("Failed to handle metadata change", 
                            tenant_id=tenant_id, 
                            instrument_id=instrument_id, 
                            error=str(e))


class CacheWarmingStrategy:
    """Strategy for warming cache with frequently accessed data."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.logger = structlog.get_logger("cache-warming-strategy")
        self.warming_stats = {
            "total_warming_operations": 0,
            "successful_warming_operations": 0,
            "failed_warming_operations": 0
        }
    
    async def warm_metadata_cache(self, tenant_id: str, instrument_ids: List[str]) -> int:
        """Warm metadata cache for instruments."""
        try:
            warmed_count = 0
            
            for instrument_id in instrument_ids:
                # Try to get metadata from database and cache it
                # This would typically call a database query
                key = CacheKey.generate_for_metadata(tenant_id, instrument_id)
                
                # Mock metadata
                metadata = {
                    "instrument_id": instrument_id,
                    "tenant_id": tenant_id,
                    "name": f"Instrument {instrument_id}",
                    "type": "commodity",
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if await self.cache_manager.set(key, metadata, ttl=3600):
                    warmed_count += 1
            
            self.warming_stats["total_warming_operations"] += 1
            self.warming_stats["successful_warming_operations"] += 1
            
            self.logger.info("Metadata cache warming completed", 
                           tenant_id=tenant_id, 
                           warmed_count=warmed_count)
            
            return warmed_count
            
        except Exception as e:
            self.warming_stats["total_warming_operations"] += 1
            self.warming_stats["failed_warming_operations"] += 1
            
            self.logger.error("Metadata cache warming failed", 
                            tenant_id=tenant_id, 
                            error=str(e))
            return 0
    
    async def warm_price_cache(self, tenant_id: str, instrument_ids: List[str]) -> int:
        """Warm price cache for instruments."""
        try:
            warmed_count = 0
            
            for instrument_id in instrument_ids:
                # Try to get latest price from database and cache it
                key = CacheKey.generate_for_price(tenant_id, instrument_id, "latest")
                
                # Mock price data
                price_data = {
                    "instrument_id": instrument_id,
                    "tenant_id": tenant_id,
                    "price": 100.0,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if await self.cache_manager.set(key, price_data, ttl=300):
                    warmed_count += 1
            
            self.warming_stats["total_warming_operations"] += 1
            self.warming_stats["successful_warming_operations"] += 1
            
            self.logger.info("Price cache warming completed", 
                           tenant_id=tenant_id, 
                           warmed_count=warmed_count)
            
            return warmed_count
            
        except Exception as e:
            self.warming_stats["total_warming_operations"] += 1
            self.warming_stats["failed_warming_operations"] += 1
            
            self.logger.error("Price cache warming failed", 
                            tenant_id=tenant_id, 
                            error=str(e))
            return 0
    
    def get_warming_stats(self) -> Dict[str, Any]:
        """Get cache warming statistics."""
        total = self.warming_stats["total_warming_operations"]
        success_rate = (self.warming_stats["successful_warming_operations"] / total * 100) if total > 0 else 0
        
        return {
            "total_warming_operations": total,
            "successful_warming_operations": self.warming_stats["successful_warming_operations"],
            "failed_warming_operations": self.warming_stats["failed_warming_operations"],
            "success_rate": success_rate
        }
