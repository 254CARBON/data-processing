"""Cache writer for Redis updates."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.storage.redis import RedisClient
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class CacheWriter:
    """Writer for Redis cache updates."""
    
    def __init__(self, config):
        self.config = config
        self.client = RedisClient(
            host=config.redis_host,
            port=config.redis_port,
            database=config.redis_database,
            password=config.redis_password
        )
        
    async def start(self):
        """Start the cache writer."""
        await self.client.connect()
        logger.info("Cache writer started")
        
    async def stop(self):
        """Stop the cache writer."""
        await self.client.close()
        logger.info("Cache writer stopped")
        
    async def write_latest_price(self, projection: Dict[str, Any]):
        """Write latest price projection to cache."""
        try:
            key = f"latest_price:{projection['instrument_id']}:{projection['tenant_id']}"
            await self.client.set(key, projection, ttl=self.config.cache_ttl_seconds)
            logger.debug(f"Cached latest price for {projection['instrument_id']}")
        except Exception as e:
            logger.error(f"Error writing latest price to cache: {e}")
            raise DataProcessingError(f"Failed to write latest price to cache: {e}")
            
    async def write_curve_snapshot(self, projection: Dict[str, Any]):
        """Write curve snapshot projection to cache."""
        try:
            key = f"curve_snapshot:{projection['instrument_id']}:{projection['tenant_id']}:{projection['horizon']}"
            await self.client.set(key, projection, ttl=self.config.cache_ttl_seconds)
            logger.debug(f"Cached curve snapshot for {projection['instrument_id']}")
        except Exception as e:
            logger.error(f"Error writing curve snapshot to cache: {e}")
            raise DataProcessingError(f"Failed to write curve snapshot to cache: {e}")
            
    async def write_custom_projection(self, projection: Dict[str, Any]):
        """Write custom projection to cache."""
        try:
            key = f"custom:{projection['instrument_id']}:{projection['tenant_id']}:{projection['projection_type']}"
            await self.client.set(key, projection, ttl=self.config.cache_ttl_seconds)
            logger.debug(f"Cached custom projection for {projection['instrument_id']}")
        except Exception as e:
            logger.error(f"Error writing custom projection to cache: {e}")
            raise DataProcessingError(f"Failed to write custom projection to cache: {e}")

