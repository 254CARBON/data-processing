"""Custom projection builder."""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


@dataclass
class CustomProjection:
    """Custom projection data structure."""
    instrument_id: str
    ts: datetime
    projection_type: str
    data: Dict[str, Any]
    quality_flags: int
    tenant_id: str


class CustomProjectionBuilder:
    """Builder for custom projections."""
    
    def __init__(self, config):
        self.config = config
        self.custom_cache: Dict[str, CustomProjection] = {}
        
    async def build_projection(self, event: Dict[str, Any], projection_type: str) -> Optional[CustomProjection]:
        """Build custom projection from event."""
        try:
            # Extract event data
            instrument_id = event.get("instrument_id")
            tenant_id = event.get("tenant_id")
            ts = event.get("ts")
            quality_flags = event.get("quality_flags", 0)
            
            if not all([instrument_id, tenant_id, ts]):
                logger.warning("Missing required fields for custom projection")
                return None
                
            # Prepare projection data based on type
            projection_data = await self._prepare_projection_data(event, projection_type)
            
            # Create projection
            projection = CustomProjection(
                instrument_id=instrument_id,
                ts=ts,
                projection_type=projection_type,
                data=projection_data,
                quality_flags=quality_flags,
                tenant_id=tenant_id
            )
            
            # Update cache
            cache_key = f"{instrument_id}_{tenant_id}_{projection_type}"
            self.custom_cache[cache_key] = projection
            
            logger.debug(f"Built custom projection {projection_type} for {instrument_id}")
            return projection
            
        except Exception as e:
            logger.error(f"Error building custom projection: {e}")
            raise DataProcessingError(f"Failed to build custom projection: {e}")
            
    async def _prepare_projection_data(self, event: Dict[str, Any], projection_type: str) -> Dict[str, Any]:
        """Prepare projection data based on type."""
        if projection_type == "spread":
            return {
                "spread_type": event.get("spread_type"),
                "spread_value": event.get("spread_value"),
                "price": event.get("price", 0)
            }
        elif projection_type == "basis":
            return {
                "basis_type": event.get("basis_type"),
                "basis_value": event.get("basis_value"),
                "price": event.get("price", 0)
            }
        elif projection_type == "rolling_stat":
            return {
                "stat_type": event.get("stat_type"),
                "stat_value": event.get("stat_value"),
                "window_size": event.get("window_size"),
                "price": event.get("price", 0)
            }
        else:
            # Generic projection data
            return {
                "raw_data": event,
                "processed_at": datetime.now().isoformat()
            }
            
    async def get_custom_projection(self, instrument_id: str, tenant_id: str, projection_type: str) -> Optional[CustomProjection]:
        """Get custom projection from cache."""
        cache_key = f"{instrument_id}_{tenant_id}_{projection_type}"
        return self.custom_cache.get(cache_key)
        
    async def invalidate_custom_projection(self, instrument_id: str, tenant_id: str, projection_type: str = None):
        """Invalidate custom projection."""
        if projection_type:
            cache_key = f"{instrument_id}_{tenant_id}_{projection_type}"
            if cache_key in self.custom_cache:
                del self.custom_cache[cache_key]
                logger.info(f"Invalidated custom projection {projection_type} for {instrument_id}")
        else:
            # Invalidate all custom projections for this instrument
            keys_to_remove = [
                key for key in self.custom_cache.keys()
                if key.startswith(f"{instrument_id}_{tenant_id}_")
            ]
            for key in keys_to_remove:
                del self.custom_cache[key]
            logger.info(f"Invalidated all custom projections for {instrument_id}")

