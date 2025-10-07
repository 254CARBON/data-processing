"""Spread calculations for market data."""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from shared.schemas.events import EnrichedMarketTick


logger = logging.getLogger(__name__)


@dataclass
class SpreadData:
    """Spread calculation result."""
    instrument_id: str
    ts: datetime
    spread_type: str
    spread_value: float
    quality_flags: int
    tenant_id: str


class SpreadCalculator:
    """Calculator for various spread types.

    Currently supports:
    - Bid/ask spread (approximate using a fixed ratio of price if book data
      is unavailable)
    - Calendar spread (difference between earliest and latest prices in a
      sample window)
    """
    
    def __init__(self, config):
        self.config = config
        
    async def calculate_spreads(self, ticks: List[EnrichedMarketTick]) -> List[SpreadData]:
        """Calculate spreads from market ticks.

        Args:
            ticks: Enriched ticks for one or more instruments/tenants.

        Returns:
            A list of calculated `SpreadData` metrics across supported
            spread types.
        """
        if not ticks:
            return []
            
        spreads = []
        
        # Group ticks by instrument and tenant
        grouped_ticks = self._group_ticks(ticks)
        
        for (instrument_id, tenant_id), instrument_ticks in grouped_ticks.items():
            try:
                # Calculate bid-ask spread
                bid_ask_spread = await self._calculate_bid_ask_spread(instrument_ticks)
                if bid_ask_spread:
                    spreads.append(bid_ask_spread)
                    
                # Calculate calendar spread
                calendar_spread = await self._calculate_calendar_spread(instrument_ticks)
                if calendar_spread:
                    spreads.append(calendar_spread)
                    
            except Exception as e:
                logger.error(f"Error calculating spreads for {instrument_id}: {e}")
                continue
                
        return spreads
        
    def _group_ticks(self, ticks: List[EnrichedMarketTick]) -> Dict[tuple, List[EnrichedMarketTick]]:
        """Group ticks by instrument and tenant."""
        grouped = {}
        for tick in ticks:
            key = (tick.instrument_id, tick.tenant_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tick)
        return grouped
        
    async def _calculate_bid_ask_spread(self, ticks: List[EnrichedMarketTick]) -> Optional[SpreadData]:
        """Calculate bid-ask spread.

        Uses a simplified heuristic (0.1% of price) where explicit bid/ask
        quotes are not available. Replace with book-based calculation when
        such data is present in the enrichment payloads.
        """
        if not ticks:
            return None
            
        # Sort by timestamp
        ticks.sort(key=lambda t: t.ts)
        latest_tick = ticks[-1]
        
        # Simplified bid-ask spread calculation
        # In reality, this would use actual bid/ask data
        spread_value = latest_tick.price * 0.001  # 0.1% spread
        
        return SpreadData(
            instrument_id=latest_tick.instrument_id,
            ts=latest_tick.ts,
            spread_type="bid_ask",
            spread_value=spread_value,
            quality_flags=latest_tick.quality_flags,
            tenant_id=latest_tick.tenant_id
        )
        
    async def _calculate_calendar_spread(self, ticks: List[EnrichedMarketTick]) -> Optional[SpreadData]:
        """Calculate calendar spread.

        Computes the price difference between the oldest and newest ticks
        in the input window for the same instrument/tenant.
        """
        if len(ticks) < 2:
            return None
            
        # Sort by timestamp
        ticks.sort(key=lambda t: t.ts)
        
        # Calculate price difference between first and last tick
        price_diff = ticks[-1].price - ticks[0].price
        
        return SpreadData(
            instrument_id=ticks[0].instrument_id,
            ts=ticks[-1].ts,
            spread_type="calendar",
            spread_value=price_diff,
            quality_flags=ticks[-1].quality_flags,
            tenant_id=ticks[0].tenant_id
        )
