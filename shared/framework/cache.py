"""Caching framework for high-performance data access."""

import json
import logging
import hashlib
from typing import Dict, Any, Optional, List, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import structlog

from ..storage.redis import RedisClient, RedisConfig

logger = structlog.get_logger()


class CacheStrategy(str, Enum):
    """Cache strategy enumeration."""
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    WRITE_AROUND = "write_around"
    CACHE_ASIDE = "cache_aside"


@dataclass
class CacheConfig:
    """Cache configuration."""
    default_ttl: int = 3600  # 1 hour
    max_size: int = 10000
    strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
    enable_compression: bool = True
    enable_serialization: bool = True


class CacheKey:
    """Cache key generator."""
    
    @staticmethod
    def generate(key_parts: List[str], namespace: str = "default") -> str:
        """Generate a cache key from parts."""
        key_string = f"{namespace}:{'|'.join(key_parts)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    @staticmethod
    def generate_for_metadata(tenant_id: str, instrument_id: str) -> str:
        """Generate cache key for metadata lookup."""
        return CacheKey.generate([tenant_id, instrument_id], "metadata")
    
    @staticmethod
    def generate_for_price(tenant_id: str, instrument_id: str, timestamp: str) -> str:
        """Generate cache key for price lookup."""
        return CacheKey.generate([tenant_id, instrument_id, timestamp], "price")
    
    @staticmethod
    def generate_for_taxonomy(tenant_id: str, instrument_id: str) -> str:
        """Generate cache key for taxonomy lookup."""
        return CacheKey.generate([tenant_id, instrument_id], "taxonomy")


class CacheManager:
    """High-level cache manager."""
    
    def __init__(self, redis_client: RedisClient, config: CacheConfig = None):
        self.redis = redis_client
        self.config = config or CacheConfig()
        self.logger = structlog.get_logger("cache-manager")
        self.local_cache: Dict[str, Any] = {}
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0
        }
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            # Try local cache first
            if key in self.local_cache:
                self.cache_stats["hits"] += 1
                return self.local_cache[key]
            
            # Try Redis cache
            value = await self.redis.get(key)
            if value:
                # Deserialize if needed
                if self.config.enable_serialization:
                    value = json.loads(value)
                
                # Store in local cache
                self.local_cache[key] = value
                self.cache_stats["hits"] += 1
                return value
            
            self.cache_stats["misses"] += 1
            return None
            
        except Exception as e:
            self.logger.error("Cache get error", key=key, error=str(e))
            self.cache_stats["misses"] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache."""
        try:
            ttl = ttl or self.config.default_ttl
            
            # Serialize if needed
            if self.config.enable_serialization:
                value = json.dumps(value)
            
            # Set in Redis
            await self.redis.set(key, value, ttl)
            
            # Set in local cache
            self.local_cache[key] = value
            
            self.cache_stats["sets"] += 1
            return True
            
        except Exception as e:
            self.logger.error("Cache set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        try:
            # Delete from Redis
            await self.redis.delete(key)
            
            # Delete from local cache
            if key in self.local_cache:
                del self.local_cache[key]
            
            self.cache_stats["deletes"] += 1
            return True
            
        except Exception as e:
            self.logger.error("Cache delete error", key=key, error=str(e))
            return False
    
    async def get_or_set(self, key: str, factory: Callable, ttl: Optional[int] = None) -> Any:
        """Get value from cache or set using factory function."""
        value = await self.get(key)
        if value is not None:
            return value
        
        # Generate value using factory
        value = await factory()
        await self.set(key, value, ttl)
        return value
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern."""
        try:
            keys = await self.redis.keys(pattern)
            count = 0
            
            for key in keys:
                if await self.delete(key):
                    count += 1
            
            return count
            
        except Exception as e:
            self.logger.error("Cache pattern invalidation error", pattern=pattern, error=str(e))
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (self.cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "hits": self.cache_stats["hits"],
            "misses": self.cache_stats["misses"],
            "sets": self.cache_stats["sets"],
            "deletes": self.cache_stats["deletes"],
            "hit_rate": hit_rate,
            "local_cache_size": len(self.local_cache)
        }
    
    def clear_local_cache(self) -> None:
        """Clear local cache."""
        self.local_cache.clear()
        self.logger.info("Local cache cleared")


class MetadataCache:
    """Specialized cache for metadata lookups."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.logger = structlog.get_logger("metadata-cache")
    
    async def get_instrument_metadata(self, tenant_id: str, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Get instrument metadata from cache."""
        key = CacheKey.generate_for_metadata(tenant_id, instrument_id)
        return await self.cache_manager.get(key)
    
    async def set_instrument_metadata(self, tenant_id: str, instrument_id: str, 
                                    metadata: Dict[str, Any], ttl: int = 3600) -> bool:
        """Set instrument metadata in cache."""
        key = CacheKey.generate_for_metadata(tenant_id, instrument_id)
        return await self.cache_manager.set(key, metadata, ttl)
    
    async def invalidate_instrument_metadata(self, tenant_id: str, instrument_id: str) -> bool:
        """Invalidate instrument metadata cache."""
        key = CacheKey.generate_for_metadata(tenant_id, instrument_id)
        return await self.cache_manager.delete(key)
    
    async def invalidate_tenant_metadata(self, tenant_id: str) -> int:
        """Invalidate all metadata for a tenant."""
        pattern = f"metadata:{tenant_id}:*"
        return await self.cache_manager.invalidate_pattern(pattern)


class PriceCache:
    """Specialized cache for price data."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.logger = structlog.get_logger("price-cache")
    
    async def get_latest_price(self, tenant_id: str, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Get latest price from cache."""
        key = CacheKey.generate_for_price(tenant_id, instrument_id, "latest")
        return await self.cache_manager.get(key)
    
    async def set_latest_price(self, tenant_id: str, instrument_id: str, 
                             price_data: Dict[str, Any], ttl: int = 300) -> bool:
        """Set latest price in cache."""
        key = CacheKey.generate_for_price(tenant_id, instrument_id, "latest")
        return await self.cache_manager.set(key, price_data, ttl)
    
    async def get_price_history(self, tenant_id: str, instrument_id: str, 
                               start_time: str, end_time: str) -> Optional[List[Dict[str, Any]]]:
        """Get price history from cache."""
        key = CacheKey.generate_for_price(tenant_id, instrument_id, f"{start_time}_{end_time}")
        return await self.cache_manager.get(key)
    
    async def set_price_history(self, tenant_id: str, instrument_id: str, 
                              start_time: str, end_time: str, price_data: List[Dict[str, Any]], 
                              ttl: int = 1800) -> bool:
        """Set price history in cache."""
        key = CacheKey.generate_for_price(tenant_id, instrument_id, f"{start_time}_{end_time}")
        return await self.cache_manager.set(key, price_data, ttl)
    
    async def invalidate_instrument_prices(self, tenant_id: str, instrument_id: str) -> int:
        """Invalidate all price data for an instrument."""
        pattern = f"price:{tenant_id}:{instrument_id}:*"
        return await self.cache_manager.invalidate_pattern(pattern)
    
    async def invalidate_tenant_prices(self, tenant_id: str) -> int:
        """Invalidate all price data for a tenant."""
        pattern = f"price:{tenant_id}:*"
        return await self.cache_manager.invalidate_pattern(pattern)


class TaxonomyCache:
    """Specialized cache for taxonomy data."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.logger = structlog.get_logger("taxonomy-cache")
    
    async def get_instrument_taxonomy(self, tenant_id: str, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Get instrument taxonomy from cache."""
        key = CacheKey.generate_for_taxonomy(tenant_id, instrument_id)
        return await self.cache_manager.get(key)
    
    async def set_instrument_taxonomy(self, tenant_id: str, instrument_id: str, 
                                    taxonomy: Dict[str, Any], ttl: int = 7200) -> bool:
        """Set instrument taxonomy in cache."""
        key = CacheKey.generate_for_taxonomy(tenant_id, instrument_id)
        return await self.cache_manager.set(key, taxonomy, ttl)
    
    async def invalidate_instrument_taxonomy(self, tenant_id: str, instrument_id: str) -> bool:
        """Invalidate instrument taxonomy cache."""
        key = CacheKey.generate_for_taxonomy(tenant_id, instrument_id)
        return await self.cache_manager.delete(key)
    
    async def invalidate_tenant_taxonomy(self, tenant_id: str) -> int:
        """Invalidate all taxonomy data for a tenant."""
        pattern = f"taxonomy:{tenant_id}:*"
        return await self.cache_manager.invalidate_pattern(pattern)


class CacheMetrics:
    """Cache metrics for monitoring."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.logger = structlog.get_logger("cache-metrics")
    
    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get comprehensive cache metrics."""
        stats = self.cache_manager.get_stats()
        
        return {
            "cache_hits_total": stats["hits"],
            "cache_misses_total": stats["misses"],
            "cache_sets_total": stats["sets"],
            "cache_deletes_total": stats["deletes"],
            "cache_hit_rate": stats["hit_rate"],
            "local_cache_size": stats["local_cache_size"],
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def get_redis_metrics(self) -> Dict[str, Any]:
        """Get Redis-specific metrics."""
        try:
            # Get Redis info
            info = await self.redis.client.info()
            
            return {
                "redis_used_memory": info.get("used_memory", 0),
                "redis_used_memory_percent": info.get("used_memory_percent", 0),
                "redis_connected_clients": info.get("connected_clients", 0),
                "redis_total_commands_processed": info.get("total_commands_processed", 0),
                "redis_keyspace_hits": info.get("keyspace_hits", 0),
                "redis_keyspace_misses": info.get("keyspace_misses", 0),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error("Failed to get Redis metrics", error=str(e))
            return {}
