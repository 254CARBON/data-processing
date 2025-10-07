"""
Metadata lookup service for enrichment.

Provides instrument metadata lookup with caching
and fallback mechanisms.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from shared.storage.postgres import PostgresClient
from shared.storage.redis import RedisClient
from shared.schemas.models import InstrumentMetadata
from shared.utils.errors import StorageError, create_error_context


logger = logging.getLogger(__name__)


class MetadataLookup:
    """
    Metadata lookup service.
    
    Provides instrument metadata lookup with caching
    and fallback mechanisms.
    """
    
    def __init__(self, config):
        self.config = config
        
        # Create database clients
        self.postgres = PostgresClient(config.postgres_dsn)
        self.redis = RedisClient(config.redis_url)
        
        # Metrics
        self.lookup_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.db_lookups = 0
        self.last_lookup_time = None
        
        # Cache management
        self.cache_ttl = getattr(config, 'cache_ttl', 3600)  # 1 hour default
        self.cache_enabled = getattr(config, 'cache_enabled', True)
        self.refresh_interval = getattr(config, 'metadata_refresh_interval', 300)  # 5 minutes
        self.last_refresh = None
    
    async def startup(self) -> None:
        """Initialize metadata lookup."""
        logger.info("Starting metadata lookup service")
        
        # Connect to databases
        await self.postgres.connect()
        await self.redis.connect()
        
        # Load initial metadata cache
        await self._refresh_cache()
    
    async def shutdown(self) -> None:
        """Shutdown metadata lookup."""
        logger.info("Shutting down metadata lookup service")
        
        # Disconnect from databases
        await self.postgres.disconnect()
        await self.redis.disconnect()
    
    async def lookup_metadata(self, instrument_id: str) -> Optional[InstrumentMetadata]:
        """
        Lookup instrument metadata.
        
        Args:
            instrument_id: Instrument identifier
            
        Returns:
            Instrument metadata or None if not found
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Check cache first
            if self.cache_enabled:
                cached_metadata = await self._get_from_cache(instrument_id)
                if cached_metadata:
                    self.cache_hits += 1
                    logger.debug(f"Metadata cache hit: {instrument_id}")
                    return cached_metadata
                
                self.cache_misses += 1
            
            # Lookup in database
            metadata = await self._lookup_in_database(instrument_id)
            
            if metadata:
                # Cache the result
                if self.cache_enabled:
                    await self._cache_metadata(instrument_id, metadata)
                
                logger.debug(
                    f"Metadata lookup successful: {instrument_id} -> {metadata.commodity}/{metadata.region}"
                )
            else:
                logger.warning(f"Metadata not found: {instrument_id}")
            
            # Update metrics
            self.lookup_count += 1
            self.last_lookup_time = asyncio.get_event_loop().time() - start_time
            
            return metadata
            
        except Exception as e:
            logger.error(f"Metadata lookup error: {e}", exc_info=True)
            return None
    
    async def _get_from_cache(self, instrument_id: str) -> Optional[InstrumentMetadata]:
        """Get metadata from cache."""
        try:
            cache_key = f"metadata:{instrument_id}"
            cached_data = await self.redis.get_json(cache_key)
            
            if cached_data:
                return InstrumentMetadata.from_dict(cached_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Cache lookup error: {e}")
            return None
    
    async def _cache_metadata(self, instrument_id: str, metadata: InstrumentMetadata) -> None:
        """Cache metadata."""
        try:
            cache_key = f"metadata:{instrument_id}"
            await self.redis.set_json(cache_key, metadata.to_dict(), ttl=self.cache_ttl)
            
            logger.debug(f"Metadata cached: {instrument_id}")
            
        except Exception as e:
            logger.error(f"Cache store error: {e}")
    
    async def _lookup_in_database(self, instrument_id: str) -> Optional[InstrumentMetadata]:
        """Lookup metadata in database."""
        try:
            query = """
                SELECT 
                    instrument_id,
                    commodity,
                    region,
                    product_tier,
                    unit,
                    contract_size,
                    tick_size,
                    tenant_id,
                    metadata
                FROM taxonomy_instruments 
                WHERE instrument_id = $1
            """
            
            row = await self.postgres.execute_one(query, instrument_id)
            
            if row:
                self.db_lookups += 1
                return InstrumentMetadata.from_dict(row)
            
            return None
            
        except Exception as e:
            logger.error(f"Database lookup error: {e}", exc_info=True)
            return None
    
    async def _refresh_cache(self) -> None:
        """Refresh metadata cache."""
        try:
            logger.info("Refreshing metadata cache")
            
            # Get all instruments from database
            query = """
                SELECT 
                    instrument_id,
                    commodity,
                    region,
                    product_tier,
                    unit,
                    contract_size,
                    tick_size,
                    tenant_id,
                    metadata
                FROM taxonomy_instruments
                ORDER BY instrument_id
            """
            
            rows = await self.postgres.execute(query)
            
            # Cache all instruments
            for row in rows:
                metadata = InstrumentMetadata.from_dict(row)
                await self._cache_metadata(metadata.instrument_id, metadata)
            
            self.last_refresh = datetime.now()
            
            logger.info(f"Metadata cache refreshed: {len(rows)} instruments")
            
        except Exception as e:
            logger.error(f"Cache refresh error: {e}", exc_info=True)
    
    async def get_status(self) -> Dict[str, Any]:
        """Get metadata lookup status."""
        return {
            "lookup_count": self.lookup_count,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "db_lookups": self.db_lookups,
            "last_lookup_time": self.last_lookup_time,
            "cache_hit_ratio": self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0,
            "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
            "cache_enabled": self.cache_enabled,
            "cache_ttl": self.cache_ttl,
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get metadata lookup metrics."""
        return {
            "lookup_count": self.lookup_count,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "db_lookups": self.db_lookups,
            "last_lookup_time": self.last_lookup_time,
            "cache_hit_ratio": self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0,
        }
