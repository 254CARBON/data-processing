"""Query result caching for ClickHouse."""

import hashlib
import json
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import structlog

from .cache import CacheManager, CacheConfig
from ..storage.redis import RedisClient

logger = structlog.get_logger()


class QueryCacheStrategy(str, Enum):
    """Query cache strategy enumeration."""
    CACHE_ALL = "cache_all"
    CACHE_SELECT = "cache_select"
    CACHE_AGGREGATE = "cache_aggregate"
    CACHE_NONE = "cache_none"


@dataclass
class QueryCacheConfig:
    """Query cache configuration."""
    default_ttl: int = 300  # 5 minutes
    max_cache_size: int = 1000
    strategy: QueryCacheStrategy = QueryCacheStrategy.CACHE_SELECT
    enable_compression: bool = True
    enable_serialization: bool = True
    cache_select_queries: bool = True
    cache_aggregate_queries: bool = True
    cache_join_queries: bool = False
    min_result_size: int = 100  # Minimum result size to cache
    max_result_size: int = 10000  # Maximum result size to cache


class QueryCache:
    """Query result cache for ClickHouse."""
    
    def __init__(self, redis_client: RedisClient, config: QueryCacheConfig = None):
        self.redis = redis_client
        self.config = config or QueryCacheConfig()
        self.cache_config = CacheConfig(
            default_ttl=self.config.default_ttl,
            max_size=self.config.max_cache_size,
            enable_compression=self.config.enable_compression,
            enable_serialization=self.config.enable_serialization
        )
        self.cache_manager = CacheManager(redis_client, self.cache_config)
        self.logger = structlog.get_logger("query-cache")
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "evictions": 0
        }
    
    def generate_cache_key(self, query: str, params: Dict[str, Any] = None) -> str:
        """Generate cache key for a query."""
        try:
            # Normalize query
            normalized_query = self._normalize_query(query)
            
            # Add parameters if provided
            if params:
                param_string = json.dumps(params, sort_keys=True)
                normalized_query += f"|params:{param_string}"
            
            # Generate hash
            cache_key = hashlib.md5(normalized_query.encode()).hexdigest()
            return f"query_cache:{cache_key}"
            
        except Exception as e:
            self.logger.error("Failed to generate cache key", error=str(e))
            return ""
    
    def should_cache_query(self, query: str) -> bool:
        """Determine if a query should be cached."""
        try:
            query_upper = query.upper().strip()
            
            # Check strategy
            if self.config.strategy == QueryCacheStrategy.CACHE_NONE:
                return False
            
            # Check query type
            if query_upper.startswith("SELECT"):
                if self.config.strategy in [QueryCacheStrategy.CACHE_ALL, QueryCacheStrategy.CACHE_SELECT]:
                    return True
            elif query_upper.startswith("WITH") and "SELECT" in query_upper:
                if self.config.strategy in [QueryCacheStrategy.CACHE_ALL, QueryCacheStrategy.CACHE_SELECT]:
                    return True
            elif any(keyword in query_upper for keyword in ["GROUP BY", "ORDER BY", "HAVING"]):
                if self.config.strategy in [QueryCacheStrategy.CACHE_ALL, QueryCacheStrategy.CACHE_AGGREGATE]:
                    return True
            
            # Check for specific patterns
            if "JOIN" in query_upper and not self.config.cache_join_queries:
                return False
            
            if "INSERT" in query_upper or "UPDATE" in query_upper or "DELETE" in query_upper:
                return False
            
            if "CREATE" in query_upper or "DROP" in query_upper or "ALTER" in query_upper:
                return False
            
            return False
            
        except Exception as e:
            self.logger.error("Failed to determine if query should be cached", error=str(e))
            return False
    
    async def get_cached_result(self, query: str, params: Dict[str, Any] = None) -> Optional[List[Dict[str, Any]]]:
        """Get cached query result."""
        try:
            if not self.should_cache_query(query):
                return None
            
            cache_key = self.generate_cache_key(query, params)
            if not cache_key:
                return None
            
            result = await self.cache_manager.get(cache_key)
            if result:
                self.cache_stats["hits"] += 1
                self.logger.debug("Query result retrieved from cache", cache_key=cache_key)
                return result
            
            self.cache_stats["misses"] += 1
            return None
            
        except Exception as e:
            self.logger.error("Failed to get cached result", error=str(e))
            self.cache_stats["misses"] += 1
            return None
    
    async def cache_result(self, query: str, result: List[Dict[str, Any]], 
                          params: Dict[str, Any] = None, ttl: Optional[int] = None) -> bool:
        """Cache query result."""
        try:
            if not self.should_cache_query(query):
                return False
            
            # Check result size
            result_size = len(result)
            if result_size < self.config.min_result_size or result_size > self.config.max_result_size:
                self.logger.debug("Query result size not suitable for caching", 
                                result_size=result_size,
                                min_size=self.config.min_result_size,
                                max_size=self.config.max_result_size)
                return False
            
            cache_key = self.generate_cache_key(query, params)
            if not cache_key:
                return False
            
            # Determine TTL based on query type
            if ttl is None:
                ttl = self._determine_ttl(query)
            
            success = await self.cache_manager.set(cache_key, result, ttl)
            if success:
                self.cache_stats["sets"] += 1
                self.logger.debug("Query result cached", 
                               cache_key=cache_key, 
                               result_size=result_size, 
                               ttl=ttl)
            
            return success
            
        except Exception as e:
            self.logger.error("Failed to cache result", error=str(e))
            return False
    
    async def invalidate_query_cache(self, pattern: str = None) -> int:
        """Invalidate query cache entries."""
        try:
            if pattern:
                cache_pattern = f"query_cache:{pattern}"
            else:
                cache_pattern = "query_cache:*"
            
            count = await self.cache_manager.invalidate_pattern(cache_pattern)
            self.logger.info("Query cache invalidated", pattern=cache_pattern, count=count)
            return count
            
        except Exception as e:
            self.logger.error("Failed to invalidate query cache", error=str(e))
            return 0
    
    async def invalidate_table_cache(self, table_name: str) -> int:
        """Invalidate cache for queries involving a specific table."""
        try:
            # This is a simplified implementation
            # In production, you'd track which queries involve which tables
            count = await self.invalidate_query_cache(f"*{table_name}*")
            self.logger.info("Table cache invalidated", table_name=table_name, count=count)
            return count
            
        except Exception as e:
            self.logger.error("Failed to invalidate table cache", error=str(e))
            return 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (self.cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "hits": self.cache_stats["hits"],
            "misses": self.cache_stats["misses"],
            "sets": self.cache_stats["sets"],
            "evictions": self.cache_stats["evictions"],
            "hit_rate": hit_rate,
            "total_requests": total_requests
        }
    
    def _normalize_query(self, query: str) -> str:
        """Normalize query for consistent cache keys."""
        try:
            # Remove extra whitespace
            normalized = " ".join(query.split())
            
            # Convert to uppercase for consistency
            normalized = normalized.upper()
            
            # Remove comments
            lines = normalized.split("\n")
            cleaned_lines = []
            for line in lines:
                if "--" in line:
                    line = line.split("--")[0]
                cleaned_lines.append(line.strip())
            
            normalized = " ".join(cleaned_lines)
            
            return normalized
            
        except Exception as e:
            self.logger.error("Failed to normalize query", error=str(e))
            return query
    
    def _determine_ttl(self, query: str) -> int:
        """Determine TTL based on query characteristics."""
        try:
            query_upper = query.upper()
            
            # Short TTL for frequently changing data
            if any(keyword in query_upper for keyword in ["LATEST", "CURRENT", "NOW()", "TODAY"]):
                return 60  # 1 minute
            
            # Medium TTL for aggregated data
            if any(keyword in query_upper for keyword in ["GROUP BY", "SUM", "AVG", "COUNT", "MAX", "MIN"]):
                return 300  # 5 minutes
            
            # Longer TTL for historical data
            if any(keyword in query_upper for keyword in ["HISTORICAL", "ARCHIVE", "BACKUP"]):
                return 3600  # 1 hour
            
            # Default TTL
            return self.config.default_ttl
            
        except Exception as e:
            self.logger.error("Failed to determine TTL", error=str(e))
            return self.config.default_ttl


class ClickHouseQueryCache:
    """ClickHouse-specific query cache wrapper."""
    
    def __init__(self, clickhouse_client, query_cache: QueryCache):
        self.clickhouse_client = clickhouse_client
        self.query_cache = query_cache
        self.logger = structlog.get_logger("clickhouse-query-cache")
    
    async def execute_cached(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute query with caching."""
        try:
            # Try to get from cache first
            cached_result = await self.query_cache.get_cached_result(query, params)
            if cached_result is not None:
                self.logger.debug("Query executed from cache")
                return cached_result
            
            # Execute query
            result = await self.clickhouse_client.execute(query, params)
            
            # Cache result
            await self.query_cache.cache_result(query, result, params)
            
            self.logger.debug("Query executed and cached", result_size=len(result))
            return result
            
        except Exception as e:
            self.logger.error("Failed to execute cached query", error=str(e))
            # Fallback to direct execution
            return await self.clickhouse_client.execute(query, params)
    
    async def execute_uncached(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute query without caching."""
        try:
            result = await self.clickhouse_client.execute(query, params)
            self.logger.debug("Query executed without caching", result_size=len(result))
            return result
            
        except Exception as e:
            self.logger.error("Failed to execute uncached query", error=str(e))
            raise
    
    async def invalidate_cache(self, pattern: str = None) -> int:
        """Invalidate query cache."""
        return await self.query_cache.invalidate_query_cache(pattern)
    
    async def invalidate_table_cache(self, table_name: str) -> int:
        """Invalidate cache for a specific table."""
        return await self.query_cache.invalidate_table_cache(table_name)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self.query_cache.get_cache_stats()


class QueryCacheManager:
    """Manages multiple query caches."""
    
    def __init__(self):
        self.caches: Dict[str, QueryCache] = {}
        self.logger = structlog.get_logger("query-cache-manager")
    
    def add_cache(self, name: str, cache: QueryCache) -> None:
        """Add a query cache."""
        self.caches[name] = cache
        self.logger.info("Query cache added", name=name)
    
    def get_cache(self, name: str) -> Optional[QueryCache]:
        """Get a query cache by name."""
        return self.caches.get(name)
    
    def remove_cache(self, name: str) -> None:
        """Remove a query cache."""
        if name in self.caches:
            del self.caches[name]
            self.logger.info("Query cache removed", name=name)
    
    async def invalidate_all_caches(self) -> int:
        """Invalidate all query caches."""
        total_count = 0
        
        for name, cache in self.caches.items():
            try:
                count = await cache.invalidate_query_cache()
                total_count += count
                self.logger.info("Cache invalidated", name=name, count=count)
            except Exception as e:
                self.logger.error("Failed to invalidate cache", name=name, error=str(e))
        
        return total_count
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all caches."""
        stats = {}
        
        for name, cache in self.caches.items():
            try:
                stats[name] = cache.get_cache_stats()
            except Exception as e:
                self.logger.error("Failed to get cache stats", name=name, error=str(e))
                stats[name] = {}
        
        return stats
