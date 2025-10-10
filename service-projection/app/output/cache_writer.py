"""Cache writer for Redis updates."""

from typing import Optional

import structlog

from shared.schemas.models import ProjectionData
from shared.storage.redis import RedisClient, RedisConfig
from shared.utils.errors import DataProcessingError
from services.cache_updater import MarketLatestCache

from .served_cache import ServedProjectionCache


logger = structlog.get_logger(__name__)


class CacheWriter:
    """Writer for Redis cache updates."""
    
    def __init__(self, config):
        self.config = config
        self.client = RedisClient(
            RedisConfig(
                url=self._build_redis_url(),
                max_connections=config.redis_pool_max,
                timeout=config.processing_timeout,
            )
        )
        self.served_cache = ServedProjectionCache(
            self.client,
            default_ttl=getattr(self.config, "cache_ttl_seconds", 3600),
        )
        ttl_min = getattr(self.config, "market_cache_ttl_min", 15)
        ttl_max = max(ttl_min, getattr(self.config, "market_cache_ttl_max", 60))
        refresh_margin = getattr(self.config, "market_cache_refresh_margin", 5)
        lock_timeout = getattr(self.config, "market_cache_lock_timeout", 5)
        self.market_latest_cache = MarketLatestCache(
            self.client,
            ttl_range_seconds=(ttl_min, ttl_max),
            refresh_margin_seconds=refresh_margin,
            lock_timeout_seconds=lock_timeout,
        )
        
    async def start(self) -> None:
        """Start the cache writer."""
        await self.served_cache.ensure_connected()
        logger.info("Cache writer started")
        
    async def stop(self) -> None:
        """Stop the cache writer."""
        await self.served_cache.close()
        logger.info("Cache writer stopped")
        
    async def write_latest_price(self, projection: Optional[ProjectionData]) -> None:
        """Write latest price projection to cache."""
        if projection is None:
            return
        
        try:
            await self.served_cache.set_latest_price(projection, ttl=self.config.cache_ttl_seconds)
            market = (
                projection.metadata.get("market")
                or projection.data.get("market")
                or projection.data.get("source_market")
                or "unknown"
            )
            symbol = (
                projection.metadata.get("symbol")
                or projection.metadata.get("instrument_symbol")
                or projection.instrument_id
            )
            payload = {
                "instrument_id": projection.instrument_id,
                "price": projection.data.get("price"),
                "volume": projection.data.get("volume", 0),
                "timestamp": projection.data.get("timestamp", projection.last_updated.isoformat()),
                "source": projection.data.get("source", "unknown"),
                "quality_flags": projection.data.get("quality_flags", []),
                "market": market,
                "symbol": symbol,
            }
            await self.market_latest_cache.write_through(
                symbol=symbol,
                market=market,
                tenant_id=projection.tenant_id,
                payload=payload,
            )
            logger.info(
                "Cached latest price",
                instrument_id=projection.instrument_id,
                tenant_id=projection.tenant_id,
                cache_key=f"market:latest:{symbol}",
            )
        except Exception as exc:
            logger.error("Error writing latest price to cache", error=str(exc))
            raise DataProcessingError(f"Failed to write latest price to cache: {exc}") from exc
            
    async def write_curve_snapshot(self, projection: Optional[ProjectionData]) -> None:
        """Write curve snapshot projection to cache."""
        if projection is None:
            return
        
        try:
            await self.served_cache.set_curve_snapshot(projection, ttl=self.config.cache_ttl_seconds)
            logger.info(
                "Cached curve snapshot",
                instrument_id=projection.instrument_id,
                horizon=projection.data.get("horizon", "unknown"),
            )
        except Exception as exc:
            logger.error("Error writing curve snapshot to cache", error=str(exc))
            raise DataProcessingError(f"Failed to write curve snapshot to cache: {exc}") from exc
            
    async def write_custom_projection(self, projection: Optional[ProjectionData]) -> None:
        """Write custom projection to cache."""
        if projection is None:
            return
        
        try:
            await self.served_cache.set_custom_projection(
                projection,
                ttl=self.config.cache_ttl_seconds,
            )
            logger.info(
                "Cached custom projection",
                instrument_id=projection.instrument_id,
                projection_type=projection.projection_type,
            )
        except Exception as exc:
            logger.error("Error writing custom projection to cache", error=str(exc))
            raise DataProcessingError(f"Failed to write custom projection to cache: {exc}") from exc
    
    def _build_redis_url(self) -> str:
        """Construct Redis connection URL from configuration."""
        credentials = ""
        if getattr(self.config, "redis_password", ""):
            credentials = f":{self.config.redis_password}@"
        return f"redis://{credentials}{self.config.redis_host}:{self.config.redis_port}/{self.config.redis_database}"
