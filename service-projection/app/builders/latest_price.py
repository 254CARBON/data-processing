"""Latest price projection builder."""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from shared.schemas.models import ProjectionData
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


# Use ProjectionData from shared.schemas.models


class LatestPriceBuilder:
    """Builder for latest price projections.

    Maintains a per-instrument, per-tenant cache of the most recent price
    snapshot suitable for fast lookups in downstream APIs.
    """
    
    def __init__(self, config):
        self.config = config
        self.price_cache: Dict[str, ProjectionData] = {}
        
    async def build_projection(self, event: Dict[str, Any]) -> Optional[ProjectionData]:
        """Build latest price projection from event.

        Args:
            event: Dictionary with at least `instrument_id`, a timestamp
                (`timestamp` or `ts`), and a `price` (or `close_price`).

        Returns:
            ProjectionData with the latest price snapshot, or None if
            required fields are missing.
        """
        try:
            # Extract event data
            instrument_id = event.get("instrument_id")
            tenant_id = event.get("tenant_id", "default")
            timestamp = event.get("timestamp") or event.get("ts")
            price = event.get("close_price") or event.get("price")
            volume = event.get("volume", 0)
            
            if not all([instrument_id, timestamp, price is not None]):
                logger.warning("Missing required fields for latest price projection")
                return None
                
            # Parse timestamp if it's a string
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            if isinstance(timestamp, datetime):
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                else:
                    timestamp = timestamp.astimezone(timezone.utc)
            
            # Create projection data
            projection_data = {
                "price": price,
                "volume": volume,
                "timestamp": timestamp.isoformat(),
                "source": event.get("source", "unknown"),
                "quality_flags": event.get("quality_flags", []),
            }
                
            # Create projection
            projection = ProjectionData(
                projection_type="latest_price",
                instrument_id=instrument_id,
                data=projection_data,
                last_updated=datetime.utcnow(),
                tenant_id=tenant_id
            )
            
            # Update cache
            cache_key = f"{instrument_id}_{tenant_id}"
            self.price_cache[cache_key] = projection
            
            logger.debug(f"Built latest price projection for {instrument_id}")
            return projection
            
        except Exception as e:
            logger.error(f"Error building latest price projection: {e}")
            raise DataProcessingError(f"Failed to build latest price projection: {e}")
            
    async def get_latest_price(self, instrument_id: str, tenant_id: str) -> Optional[ProjectionData]:
        """Get latest price projection from cache.

        Returns None if no projection has been computed yet for the key.
        """
        cache_key = f"{instrument_id}_{tenant_id}"
        return self.price_cache.get(cache_key)
        
    async def invalidate_price(self, instrument_id: str, tenant_id: str):
        """Invalidate price projection.

        Removes the latest price snapshot from the in-memory cache. Useful
        when upstream invalidation events indicate the snapshot is stale.
        """
        cache_key = f"{instrument_id}_{tenant_id}"
        if cache_key in self.price_cache:
            del self.price_cache[cache_key]
            logger.info(f"Invalidated latest price for {instrument_id}")
