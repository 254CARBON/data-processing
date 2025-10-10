"""Custom projection builder."""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from shared.schemas.models import ProjectionData
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class CustomProjectionBuilder:
    """Builder for custom projections."""
    
    def __init__(self, config):
        self.config = config
        self.custom_cache: Dict[str, ProjectionData] = {}
        
    async def build_projection(self, event: Dict[str, Any], projection_type: str) -> Optional[ProjectionData]:
        """Build custom projection from event."""
        try:
            instrument_id = event.get("instrument_id")
            tenant_id = event.get("tenant_id", "default")
            timestamp = event.get("timestamp") or event.get("ts")
            quality_flags = event.get("quality_flags", [])
            
            if not all([instrument_id, tenant_id, timestamp]):
                logger.warning("Missing required fields for custom projection")
                return None
            
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            if isinstance(timestamp, datetime):
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                else:
                    timestamp = timestamp.astimezone(timezone.utc)
            
            projection_data = await self._prepare_projection_data(event, projection_type)
            projection_data.update({
                "timestamp": timestamp.isoformat(),
                "quality_flags": quality_flags,
            })
            
            projection = ProjectionData(
                projection_type=projection_type,
                instrument_id=instrument_id,
                data=projection_data,
                last_updated=datetime.utcnow(),
                tenant_id=tenant_id,
            )
            
            cache_key = self._cache_key(instrument_id, tenant_id, projection_type)
            self.custom_cache[cache_key] = projection
            
            logger.debug("Built custom projection", instrument_id=instrument_id, projection_type=projection_type)
            return projection
            
        except Exception as e:
            logger.error("Error building custom projection", error=str(e))
            raise DataProcessingError(f"Failed to build custom projection: {e}")
            
    async def _prepare_projection_data(self, event: Dict[str, Any], projection_type: str) -> Dict[str, Any]:
        """Prepare projection data based on type."""
        if projection_type == "spread":
            return {
                "spread_type": event.get("spread_type"),
                "spread_value": event.get("spread_value"),
                "price": event.get("price", 0),
            }
        if projection_type == "basis":
            return {
                "basis_type": event.get("basis_type"),
                "basis_value": event.get("basis_value"),
                "price": event.get("price", 0),
            }
        if projection_type == "rolling_stat":
            return {
                "stat_type": event.get("stat_type"),
                "stat_value": event.get("stat_value"),
                "window_size": event.get("window_size"),
                "price": event.get("price", 0),
            }
        
        return {
            "raw_data": event,
            "processed_at": datetime.utcnow().isoformat(),
        }
            
    async def get_custom_projection(self, instrument_id: str, tenant_id: str, projection_type: str) -> Optional[ProjectionData]:
        """Get custom projection from cache."""
        return self.custom_cache.get(self._cache_key(instrument_id, tenant_id, projection_type))
        
    async def invalidate_custom_projection(self, instrument_id: str, tenant_id: str, projection_type: Optional[str] = None) -> None:
        """Invalidate custom projection."""
        if projection_type:
            cache_key = self._cache_key(instrument_id, tenant_id, projection_type)
            if cache_key in self.custom_cache:
                del self.custom_cache[cache_key]
                logger.info("Invalidated custom projection", instrument_id=instrument_id, projection_type=projection_type)
        else:
            keys_to_remove = [
                key for key in list(self.custom_cache.keys())
                if key.startswith(f"{instrument_id}_{tenant_id}_")
            ]
            for key in keys_to_remove:
                del self.custom_cache[key]
            if keys_to_remove:
                logger.info("Invalidated all custom projections", instrument_id=instrument_id)
    
    def _cache_key(self, instrument_id: str, tenant_id: str, projection_type: str) -> str:
        """Generate cache key for custom projections."""
        return f"{instrument_id}_{tenant_id}_{projection_type}"
