"""Rolling statistics calculations."""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from shared.schemas.events import EnrichedMarketTick


logger = logging.getLogger(__name__)


@dataclass
class RollingStatData:
    """Rolling statistics result."""
    instrument_id: str
    ts: datetime
    stat_type: str
    stat_value: float
    window_size: int
    quality_flags: int
    tenant_id: str


def _mask_from_tick(tick: EnrichedMarketTick) -> int:
    """Convert mixed quality flag representations into an integer mask."""
    if isinstance(tick.quality_flags, int):
        return tick.quality_flags
    return getattr(tick, "quality_flags_mask", 0)


class RollingStatisticsCalculator:
    """Calculator for rolling statistics."""
    
    def __init__(self, config):
        self.config = config
        self.window_sizes = [10, 50, 100]  # Rolling window sizes
        
    async def calculate_stats(self, ticks: List[EnrichedMarketTick]) -> List[RollingStatData]:
        """Calculate rolling statistics from market ticks."""
        if not ticks:
            return []
            
        stats = []
        
        # Group ticks by instrument and tenant
        grouped_ticks = self._group_ticks(ticks)
        
        for (instrument_id, tenant_id), instrument_ticks in grouped_ticks.items():
            try:
                # Calculate rolling statistics for each window size
                for window_size in self.window_sizes:
                    rolling_stats = await self._calculate_rolling_stats(
                        instrument_ticks, window_size
                    )
                    stats.extend(rolling_stats)
                    
            except Exception as e:
                logger.error(f"Error calculating rolling stats for {instrument_id}: {e}")
                continue
                
        return stats
        
    def _group_ticks(self, ticks: List[EnrichedMarketTick]) -> Dict[tuple, List[EnrichedMarketTick]]:
        """Group ticks by instrument and tenant."""
        grouped = {}
        for tick in ticks:
            key = (tick.instrument_id, tick.tenant_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tick)
        return grouped
        
    async def _calculate_rolling_stats(
        self, 
        ticks: List[EnrichedMarketTick], 
        window_size: int
    ) -> List[RollingStatData]:
        """Calculate rolling statistics for a specific window size."""
        if len(ticks) < window_size:
            return []
            
        # Sort by timestamp
        ticks.sort(key=lambda t: t.ts)
        
        stats = []
        
        # Calculate rolling statistics
        for i in range(window_size - 1, len(ticks)):
            window_ticks = ticks[i - window_size + 1:i + 1]
            prices = [tick.price for tick in window_ticks]
            
            # Calculate various statistics
            mean_price = sum(prices) / len(prices)
            max_price = max(prices)
            min_price = min(prices)
            price_range = max_price - min_price
            
            # Add statistics
            stats.extend([
                RollingStatData(
                    instrument_id=ticks[i].instrument_id,
                    ts=ticks[i].ts,
                    stat_type="mean",
                    stat_value=mean_price,
                    window_size=window_size,
                    quality_flags=_mask_from_tick(ticks[i]),
                    tenant_id=ticks[i].tenant_id
                ),
                RollingStatData(
                    instrument_id=ticks[i].instrument_id,
                    ts=ticks[i].ts,
                    stat_type="max",
                    stat_value=max_price,
                    window_size=window_size,
                    quality_flags=_mask_from_tick(ticks[i]),
                    tenant_id=ticks[i].tenant_id
                ),
                RollingStatData(
                    instrument_id=ticks[i].instrument_id,
                    ts=ticks[i].ts,
                    stat_type="min",
                    stat_value=min_price,
                    window_size=window_size,
                    quality_flags=_mask_from_tick(ticks[i]),
                    tenant_id=ticks[i].tenant_id
                ),
                RollingStatData(
                    instrument_id=ticks[i].instrument_id,
                    ts=ticks[i].ts,
                    stat_type="range",
                    stat_value=price_range,
                    window_size=window_size,
                    quality_flags=_mask_from_tick(ticks[i]),
                    tenant_id=ticks[i].tenant_id
                )
            ])
            
        return stats
