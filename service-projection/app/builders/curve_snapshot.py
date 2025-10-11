"""Curve snapshot projection builder."""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from shared.schemas.models import ProjectionData
from shared.utils.errors import DataProcessingError
from shared.utils.identifiers import PROJECTION_NAMESPACE, deterministic_uuid


logger = logging.getLogger(__name__)


class CurveSnapshotBuilder:
    """Builder for curve snapshot projections."""
    
    def __init__(self, config):
        self.config = config
        self.curve_cache: Dict[str, ProjectionData] = {}
        
    async def build_projection(self, event: Dict[str, Any]) -> Optional[ProjectionData]:
        """Build curve snapshot projection from event."""
        try:
            instrument_id = event.get("instrument_id")
            tenant_id = event.get("tenant_id", "default")
            timestamp = event.get("timestamp") or event.get("ts")
            horizon = event.get("horizon")
            curve_points = event.get("curve_points", [])
            interpolation_method = event.get("interpolation_method", "linear")
            quality_flags = event.get("quality_flags", [])
            
            if not all([instrument_id, tenant_id, timestamp, horizon, curve_points]):
                logger.warning("Missing required fields for curve snapshot projection")
                return None
            
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            if isinstance(timestamp, datetime):
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                else:
                    timestamp = timestamp.astimezone(timezone.utc)
            
            projection_data = {
                "timestamp": timestamp.isoformat(),
                "horizon": horizon,
                "curve_points": curve_points,
                "interpolation_method": interpolation_method,
                "quality_flags": quality_flags,
            }

            metadata = {
                "projection_id": deterministic_uuid(
                    PROJECTION_NAMESPACE,
                    tenant_id,
                    instrument_id,
                    "curve_snapshot",
                    horizon,
                )
            }
            source_event_id = event.get("event_id")
            if source_event_id:
                metadata["source_event_id"] = source_event_id
            
            projection = ProjectionData(
                projection_type="curve_snapshot",
                instrument_id=instrument_id,
                data=projection_data,
                last_updated=datetime.utcnow(),
                tenant_id=tenant_id,
                metadata=metadata,
            )
            
            cache_key = self._cache_key(instrument_id, tenant_id, horizon)
            self.curve_cache[cache_key] = projection
            
            logger.debug("Built curve snapshot projection", instrument_id=instrument_id, horizon=horizon)
            return projection
            
        except Exception as e:
            logger.error("Error building curve snapshot projection", error=str(e))
            raise DataProcessingError(f"Failed to build curve snapshot projection: {e}")
    
    async def get_curve_snapshot(self, instrument_id: str, tenant_id: str, horizon: str) -> Optional[ProjectionData]:
        """Get curve snapshot projection from cache."""
        return self.curve_cache.get(self._cache_key(instrument_id, tenant_id, horizon))
        
    async def invalidate_curve(self, instrument_id: str, tenant_id: str, horizon: Optional[str] = None) -> None:
        """Invalidate curve projection."""
        if horizon:
            cache_key = self._cache_key(instrument_id, tenant_id, horizon)
            if cache_key in self.curve_cache:
                del self.curve_cache[cache_key]
                logger.info("Invalidated curve snapshot", instrument_id=instrument_id, horizon=horizon)
        else:
            keys_to_remove = [
                key for key in list(self.curve_cache.keys())
                if key.startswith(f"{instrument_id}_{tenant_id}_")
            ]
            for key in keys_to_remove:
                del self.curve_cache[key]
            if keys_to_remove:
                logger.info("Invalidated all curve snapshots", instrument_id=instrument_id)
    
    def _cache_key(self, instrument_id: str, tenant_id: str, horizon: str) -> str:
        """Generate cache key for curve snapshots."""
        return f"{instrument_id}_{tenant_id}_{horizon}"
