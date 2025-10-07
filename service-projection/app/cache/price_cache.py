"""Price cache implementation for projection service."""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

import structlog

from shared.framework.cache import PriceCache, CacheManager, CacheConfig
from shared.storage.redis import RedisClient, RedisConfig

logger = structlog.get_logger()


class ProjectionPriceCache:
    """Price cache for projection service."""
    
    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        self.cache_config = CacheConfig(
            default_ttl=300,  # 5 minutes for price data
            max_size=50000,
            strategy="cache_aside"
        )
        self.cache_manager = CacheManager(redis_client, self.cache_config)
        self.price_cache = PriceCache(self.cache_manager)
        self.logger = structlog.get_logger("projection-price-cache")
    
    async def start(self) -> None:
        """Start the price cache."""
        await self.redis.connect()
        self.logger.info("Price cache started")
    
    async def stop(self) -> None:
        """Stop the price cache."""
        await self.redis.disconnect()
        self.logger.info("Price cache stopped")
    
    async def cache_latest_price(self, tenant_id: str, instrument_id: str, 
                               price_data: Dict[str, Any]) -> bool:
        """Cache latest price for an instrument."""
        try:
            # Add timestamp if not present
            if "timestamp" not in price_data:
                price_data["timestamp"] = datetime.utcnow().isoformat()
            
            # Cache the price
            success = await self.price_cache.set_latest_price(
                tenant_id, instrument_id, price_data, ttl=300
            )
            
            if success:
                self.logger.debug("Latest price cached", 
                                tenant_id=tenant_id, 
                                instrument_id=instrument_id)
            
            return success
            
        except Exception as e:
            self.logger.error("Failed to cache latest price", 
                            tenant_id=tenant_id, 
                            instrument_id=instrument_id, 
                            error=str(e))
            return False
    
    async def get_latest_price(self, tenant_id: str, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Get latest price from cache."""
        try:
            price_data = await self.price_cache.get_latest_price(tenant_id, instrument_id)
            
            if price_data:
                self.logger.debug("Latest price retrieved from cache", 
                                tenant_id=tenant_id, 
                                instrument_id=instrument_id)
            
            return price_data
            
        except Exception as e:
            self.logger.error("Failed to get latest price from cache", 
                            tenant_id=tenant_id, 
                            instrument_id=instrument_id, 
                            error=str(e))
            return None
    
    async def cache_price_snapshot(self, tenant_id: str, snapshot_data: Dict[str, Any]) -> bool:
        """Cache a price snapshot for multiple instruments."""
        try:
            timestamp = datetime.utcnow().isoformat()
            snapshot_key = f"snapshot:{tenant_id}:{timestamp}"
            
            # Add timestamp to snapshot
            snapshot_data["timestamp"] = timestamp
            snapshot_data["tenant_id"] = tenant_id
            
            # Cache the snapshot
            success = await self.cache_manager.set(snapshot_key, snapshot_data, ttl=600)
            
            if success:
                self.logger.debug("Price snapshot cached", 
                                tenant_id=tenant_id, 
                                instrument_count=len(snapshot_data.get("prices", [])))
            
            return success
            
        except Exception as e:
            self.logger.error("Failed to cache price snapshot", 
                            tenant_id=tenant_id, 
                            error=str(e))
            return False
    
    async def get_price_snapshot(self, tenant_id: str, timestamp: str) -> Optional[Dict[str, Any]]:
        """Get price snapshot from cache."""
        try:
            snapshot_key = f"snapshot:{tenant_id}:{timestamp}"
            snapshot_data = await self.cache_manager.get(snapshot_key)
            
            if snapshot_data:
                self.logger.debug("Price snapshot retrieved from cache", 
                                tenant_id=tenant_id, 
                                timestamp=timestamp)
            
            return snapshot_data
            
        except Exception as e:
            self.logger.error("Failed to get price snapshot from cache", 
                            tenant_id=tenant_id, 
                            timestamp=timestamp, 
                            error=str(e))
            return None
    
    async def cache_curve_snapshot(self, tenant_id: str, curve_data: Dict[str, Any]) -> bool:
        """Cache a curve snapshot."""
        try:
            timestamp = datetime.utcnow().isoformat()
            curve_key = f"curve:{tenant_id}:{curve_data.get('curve_type', 'default')}:{timestamp}"
            
            # Add metadata
            curve_data["timestamp"] = timestamp
            curve_data["tenant_id"] = tenant_id
            
            # Cache the curve
            success = await self.cache_manager.set(curve_key, curve_data, ttl=1800)
            
            if success:
                self.logger.debug("Curve snapshot cached", 
                                tenant_id=tenant_id, 
                                curve_type=curve_data.get('curve_type'))
            
            return success
            
        except Exception as e:
            self.logger.error("Failed to cache curve snapshot", 
                            tenant_id=tenant_id, 
                            error=str(e))
            return False
    
    async def get_curve_snapshot(self, tenant_id: str, curve_type: str, 
                               timestamp: str) -> Optional[Dict[str, Any]]:
        """Get curve snapshot from cache."""
        try:
            curve_key = f"curve:{tenant_id}:{curve_type}:{timestamp}"
            curve_data = await self.cache_manager.get(curve_key)
            
            if curve_data:
                self.logger.debug("Curve snapshot retrieved from cache", 
                                tenant_id=tenant_id, 
                                curve_type=curve_type, 
                                timestamp=timestamp)
            
            return curve_data
            
        except Exception as e:
            self.logger.error("Failed to get curve snapshot from cache", 
                            tenant_id=tenant_id, 
                            curve_type=curve_type, 
                            timestamp=timestamp, 
                            error=str(e))
            return None
    
    async def invalidate_instrument_cache(self, tenant_id: str, instrument_id: str) -> int:
        """Invalidate all cache entries for an instrument."""
        try:
            count = await self.price_cache.invalidate_instrument_prices(tenant_id, instrument_id)
            
            self.logger.info("Instrument cache invalidated", 
                           tenant_id=tenant_id, 
                           instrument_id=instrument_id, 
                           count=count)
            
            return count
            
        except Exception as e:
            self.logger.error("Failed to invalidate instrument cache", 
                            tenant_id=tenant_id, 
                            instrument_id=instrument_id, 
                            error=str(e))
            return 0
    
    async def invalidate_tenant_cache(self, tenant_id: str) -> int:
        """Invalidate all cache entries for a tenant."""
        try:
            count = await self.price_cache.invalidate_tenant_prices(tenant_id)
            
            self.logger.info("Tenant cache invalidated", 
                           tenant_id=tenant_id, 
                           count=count)
            
            return count
            
        except Exception as e:
            self.logger.error("Failed to invalidate tenant cache", 
                            tenant_id=tenant_id, 
                            error=str(e))
            return 0
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            stats = self.cache_manager.get_stats()
            
            # Add Redis-specific metrics
            redis_info = await self.redis.client.info()
            
            return {
                "cache_hits": stats["hits"],
                "cache_misses": stats["misses"],
                "cache_hit_rate": stats["hit_rate"],
                "local_cache_size": stats["local_cache_size"],
                "redis_used_memory": redis_info.get("used_memory", 0),
                "redis_used_memory_percent": redis_info.get("used_memory_percent", 0),
                "redis_connected_clients": redis_info.get("connected_clients", 0),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error("Failed to get cache stats", error=str(e))
            return {}
    
    async def warm_cache(self, tenant_id: str, instrument_ids: List[str]) -> int:
        """Warm the cache with recent price data."""
        try:
            warmed_count = 0
            
            for instrument_id in instrument_ids:
                # Try to get latest price from database
                # This would typically call a database query
                # For now, we'll just log the attempt
                self.logger.debug("Warming cache for instrument", 
                                tenant_id=tenant_id, 
                                instrument_id=instrument_id)
                warmed_count += 1
            
            self.logger.info("Cache warming completed", 
                           tenant_id=tenant_id, 
                           warmed_count=warmed_count)
            
            return warmed_count
            
        except Exception as e:
            self.logger.error("Failed to warm cache", 
                            tenant_id=tenant_id, 
                            error=str(e))
            return 0
