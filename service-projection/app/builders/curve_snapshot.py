"""Curve snapshot projection builder."""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


@dataclass
class CurveSnapshotProjection:
    """Curve snapshot projection data structure."""
    instrument_id: str
    ts: datetime
    horizon: str
    curve_points: List[Dict[str, Any]]
    interpolation_method: str
    quality_flags: int
    tenant_id: str
    projection_type: str = "curve_snapshot"


class CurveSnapshotBuilder:
    """Builder for curve snapshot projections."""
    
    def __init__(self, config):
        self.config = config
        self.curve_cache: Dict[str, CurveSnapshotProjection] = {}
        
    async def build_projection(self, event: Dict[str, Any]) -> Optional[CurveSnapshotProjection]:
        """Build curve snapshot projection from event."""
        try:
            # Extract event data
            instrument_id = event.get("instrument_id")
            tenant_id = event.get("tenant_id")
            ts = event.get("ts")
            horizon = event.get("horizon")
            curve_points = event.get("curve_points", [])
            interpolation_method = event.get("interpolation_method", "linear")
            quality_flags = event.get("quality_flags", 0)
            
            if not all([instrument_id, tenant_id, ts, horizon, curve_points]):
                logger.warning("Missing required fields for curve snapshot projection")
                return None
                
            # Create projection
            projection = CurveSnapshotProjection(
                instrument_id=instrument_id,
                ts=ts,
                horizon=horizon,
                curve_points=curve_points,
                interpolation_method=interpolation_method,
                quality_flags=quality_flags,
                tenant_id=tenant_id
            )
            
            # Update cache
            cache_key = f"{instrument_id}_{tenant_id}_{horizon}"
            self.curve_cache[cache_key] = projection
            
            logger.debug(f"Built curve snapshot projection for {instrument_id} {horizon}")
            return projection
            
        except Exception as e:
            logger.error(f"Error building curve snapshot projection: {e}")
            raise DataProcessingError(f"Failed to build curve snapshot projection: {e}")
            
    async def get_curve_snapshot(self, instrument_id: str, tenant_id: str, horizon: str) -> Optional[CurveSnapshotProjection]:
        """Get curve snapshot projection from cache."""
        cache_key = f"{instrument_id}_{tenant_id}_{horizon}"
        return self.curve_cache.get(cache_key)
        
    async def invalidate_curve(self, instrument_id: str, tenant_id: str, horizon: str = None):
        """Invalidate curve projection."""
        if horizon:
            cache_key = f"{instrument_id}_{tenant_id}_{horizon}"
            if cache_key in self.curve_cache:
                del self.curve_cache[cache_key]
                logger.info(f"Invalidated curve snapshot for {instrument_id} {horizon}")
        else:
            # Invalidate all horizons for this instrument
            keys_to_remove = [
                key for key in self.curve_cache.keys()
                if key.startswith(f"{instrument_id}_{tenant_id}_")
            ]
            for key in keys_to_remove:
                del self.curve_cache[key]
            logger.info(f"Invalidated all curve snapshots for {instrument_id}")

