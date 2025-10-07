"""Forward curve calculator for market data."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from shared.schemas.events import EnrichedMarketTick
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


@dataclass
class ForwardCurve:
    """Forward curve data structure."""
    instrument_id: str
    ts: datetime
    horizon: str
    curve_points: List[Dict[str, Any]]
    interpolation_method: str
    quality_flags: int
    tenant_id: str


class ForwardCurveCalculator:
    """Calculator for forward curves from market data."""
    
    def __init__(self, config):
        self.config = config
        self.horizons = config.curve_horizons
        self.curve_cache: Dict[str, Dict[str, ForwardCurve]] = {}
        
    async def calculate_curves(self, ticks: List[EnrichedMarketTick]) -> List[ForwardCurve]:
        """Calculate forward curves from market ticks."""
        if not ticks:
            return []
            
        curves = []
        
        # Group ticks by instrument and tenant
        grouped_ticks = self._group_ticks(ticks)
        
        for (instrument_id, tenant_id), instrument_ticks in grouped_ticks.items():
            try:
                # Calculate curves for each horizon
                for horizon in self.horizons:
                    curve = await self._calculate_curve(
                        instrument_id, tenant_id, horizon, instrument_ticks
                    )
                    if curve:
                        curves.append(curve)
                        
            except Exception as e:
                logger.error(f"Error calculating curves for {instrument_id}: {e}")
                continue
                
        return curves
        
    def _group_ticks(self, ticks: List[EnrichedMarketTick]) -> Dict[tuple, List[EnrichedMarketTick]]:
        """Group ticks by instrument and tenant."""
        grouped = {}
        for tick in ticks:
            key = (tick.instrument_id, tick.tenant_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tick)
        return grouped
        
    async def _calculate_curve(
        self, 
        instrument_id: str, 
        tenant_id: str, 
        horizon: str, 
        ticks: List[EnrichedMarketTick]
    ) -> Optional[ForwardCurve]:
        """Calculate a forward curve for a specific horizon."""
        if not ticks:
            return None
            
        # Sort ticks by timestamp
        ticks.sort(key=lambda t: t.ts)
        
        # Get the latest timestamp
        latest_ts = ticks[-1].ts
        
        # Calculate curve points
        curve_points = await self._calculate_curve_points(horizon, ticks, latest_ts)
        
        if not curve_points:
            return None
            
        # Calculate quality flags
        quality_flags = 0
        for tick in ticks:
            quality_flags |= tick.quality_flags
            
        return ForwardCurve(
            instrument_id=instrument_id,
            ts=latest_ts,
            horizon=horizon,
            curve_points=curve_points,
            interpolation_method="linear",
            quality_flags=quality_flags,
            tenant_id=tenant_id
        )
        
    async def _calculate_curve_points(
        self, 
        horizon: str, 
        ticks: List[EnrichedMarketTick], 
        base_time: datetime
    ) -> List[Dict[str, Any]]:
        """Calculate curve points for a given horizon."""
        points = []
        
        # Parse horizon
        horizon_delta = self._parse_horizon(horizon)
        
        # Calculate forward points
        for i in range(1, 13):  # 12 forward points
            forward_time = base_time + (horizon_delta * i)
            
            # Find the closest tick to this forward time
            closest_tick = self._find_closest_tick(ticks, forward_time)
            
            if closest_tick:
                # Apply forward adjustment (simplified)
                forward_price = self._calculate_forward_price(closest_tick.price, horizon_delta * i)
                
                points.append({
                    "time": forward_time.isoformat(),
                    "price": forward_price,
                    "confidence": self._calculate_confidence(closest_tick, forward_time)
                })
                
        return points
        
    def _parse_horizon(self, horizon: str) -> timedelta:
        """Parse horizon string to timedelta."""
        if horizon.endswith('m'):
            months = int(horizon[:-1])
            return timedelta(days=months * 30)  # Approximate
        elif horizon.endswith('y'):
            years = int(horizon[:-1])
            return timedelta(days=years * 365)  # Approximate
        else:
            raise ValueError(f"Invalid horizon format: {horizon}")
            
    def _find_closest_tick(self, ticks: List[EnrichedMarketTick], target_time: datetime) -> Optional[EnrichedMarketTick]:
        """Find the closest tick to a target time."""
        if not ticks:
            return None
            
        closest_tick = None
        min_diff = None
        
        for tick in ticks:
            diff = abs((tick.ts - target_time).total_seconds())
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest_tick = tick
                
        return closest_tick
        
    def _calculate_forward_price(self, spot_price: float, time_to_expiry: timedelta) -> float:
        """Calculate forward price from spot price."""
        # Simplified forward price calculation
        # In reality, this would involve interest rates, storage costs, etc.
        days_to_expiry = time_to_expiry.total_seconds() / 86400
        forward_rate = 0.02  # 2% annual rate (simplified)
        
        return spot_price * (1 + forward_rate * days_to_expiry / 365)
        
    def _calculate_confidence(self, tick: EnrichedMarketTick, target_time: datetime) -> float:
        """Calculate confidence score for a curve point."""
        # Simplified confidence calculation based on time difference
        time_diff = abs((tick.ts - target_time).total_seconds())
        max_diff = 86400  # 1 day
        
        return max(0.0, 1.0 - (time_diff / max_diff))

