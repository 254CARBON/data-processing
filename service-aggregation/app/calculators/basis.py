"""Basis calculations for market data."""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from shared.schemas.events import EnrichedMarketTick


logger = logging.getLogger(__name__)


@dataclass
class BasisData:
    """Basis calculation result."""
    instrument_id: str
    ts: datetime
    basis_type: str
    basis_value: float
    quality_flags: int
    tenant_id: str


class BasisCalculator:
    """Calculator for basis differentials."""
    
    def __init__(self, config):
        self.config = config
        
    async def calculate_basis(self, ticks: List[EnrichedMarketTick]) -> List[BasisData]:
        """Calculate basis from market ticks."""
        if not ticks:
            return []
            
        basis_data = []
        
        # Group ticks by instrument and tenant
        grouped_ticks = self._group_ticks(ticks)
        
        for (instrument_id, tenant_id), instrument_ticks in grouped_ticks.items():
            try:
                # Calculate spot-future basis
                spot_future_basis = await self._calculate_spot_future_basis(instrument_ticks)
                if spot_future_basis:
                    basis_data.append(spot_future_basis)
                    
            except Exception as e:
                logger.error(f"Error calculating basis for {instrument_id}: {e}")
                continue
                
        return basis_data
        
    def _group_ticks(self, ticks: List[EnrichedMarketTick]) -> Dict[tuple, List[EnrichedMarketTick]]:
        """Group ticks by instrument and tenant."""
        grouped = {}
        for tick in ticks:
            key = (tick.instrument_id, tick.tenant_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tick)
        return grouped
        
    async def _calculate_spot_future_basis(self, ticks: List[EnrichedMarketTick]) -> Optional[BasisData]:
        """Calculate spot-future basis."""
        if not ticks:
            return None
            
        # Sort by timestamp
        ticks.sort(key=lambda t: t.ts)
        latest_tick = ticks[-1]
        
        # Simplified basis calculation
        # In reality, this would compare spot vs future prices
        basis_value = latest_tick.price * 0.005  # 0.5% basis
        
        return BasisData(
            instrument_id=latest_tick.instrument_id,
            ts=latest_tick.ts,
            basis_type="spot_future",
            basis_value=basis_value,
            quality_flags=latest_tick.quality_flags,
            tenant_id=latest_tick.tenant_id
        )
