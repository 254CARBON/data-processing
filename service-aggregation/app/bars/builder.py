"""OHLC bar builder for market data aggregation."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from shared.schemas.models import TickData, BarData
from shared.utils.errors import DataProcessingError
from shared.utils.identifiers import AGGREGATION_BAR_NAMESPACE, deterministic_uuid


logger = logging.getLogger(__name__)


# Use the existing BarData from shared.schemas.models


class OHLCBarBuilder:
    """Builder for OHLC bars from market ticks.

    Converts a sequence of normalized `TickData` records into OHLC bar
    aggregates across configured time intervals. Batching/grouping is
    performed by instrument and tenant to preserve multi-tenant isolation.

    The builder is stateless across calls except for an optional in-memory
    cache that can be used by callers for incremental computations.
    """
    
    def __init__(self, config):
        self.config = config
        self.intervals = getattr(config, 'bar_intervals', ['5m', '1h'])
        self.bar_cache: Dict[str, Dict[str, BarData]] = {}
        
    async def build_bars(self, ticks: List[TickData]) -> List[BarData]:
        """Build OHLC bars from market ticks.

        Args:
            ticks: A list of normalized `TickData` for one or more
                instruments and tenants. All ticks should be in UTC
                and comparable on the `timestamp` field.

        Returns:
            A list of `BarData` objects across all configured intervals.
            The result may be empty if the input list is empty.
        """
        if not ticks:
            return []
            
        bars = []
        
        # Group ticks by instrument and tenant
        grouped_ticks = self._group_ticks(ticks)
        
        for (instrument_id, tenant_id), instrument_ticks in grouped_ticks.items():
            try:
                # Build bars for each interval
                for interval in self.intervals:
                    interval_bars = await self._build_interval_bars(
                        instrument_id, tenant_id, interval, instrument_ticks
                    )
                    bars.extend(interval_bars)
                    
            except Exception as e:
                logger.error(f"Error building bars for {instrument_id}: {e}")
                continue
                
        return bars
        
    def _group_ticks(self, ticks: List[TickData]) -> Dict[tuple, List[TickData]]:
        """Group ticks by instrument and tenant.

        Returns a mapping keyed by `(instrument_id, tenant_id)` with the
        list of ticks for each key. This ensures multi-tenant isolation and
        per-instrument aggregation.
        """
        grouped = {}
        for tick in ticks:
            key = (tick.instrument_id, tick.tenant_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tick)
        return grouped
        
    async def _build_interval_bars(
        self, 
        instrument_id: str, 
        tenant_id: str, 
        interval: str, 
        ticks: List[TickData]
    ) -> List[BarData]:
        """Build bars for a specific interval.

        Args:
            instrument_id: The instrument identifier being aggregated.
            tenant_id: Tenant to which all ticks belong.
            interval: Interval label (e.g. "5m", "1h", "1d").
            ticks: Sorted or unsorted list of `TickData` for the instrument.

        Returns:
            A list of `BarData` for the interval within the time windows
            derived from the ticks.
        """
        bars = []
        
        # Sort ticks by timestamp
        ticks.sort(key=lambda t: t.timestamp)
        
        # Group ticks by time window
        time_windows = self._create_time_windows(interval, ticks)
        
        for window_start, window_ticks in time_windows.items():
            if not window_ticks:
                continue
                
            try:
                bar = await self._create_bar(
                    instrument_id, tenant_id, interval, window_start, window_ticks
                )
                if bar:
                    bars.append(bar)
                    
            except Exception as e:
                logger.error(f"Error creating bar for {instrument_id} {interval}: {e}")
                continue
                
        return bars
        
    def _create_time_windows(
        self, interval: str, ticks: List[TickData]
    ) -> Dict[datetime, List[TickData]]:
        """Create time windows for bar aggregation.

        Groups ticks by the start time of the interval window. For example,
        with a 5-minute interval, all ticks with timestamps from 10:00:00
        to 10:04:59 are grouped under the 10:00:00 window start.
        """
        windows = {}
        
        # Parse interval
        interval_delta = self._parse_interval(interval)
        
        for tick in ticks:
            # Calculate window start
            window_start = self._get_window_start(tick.timestamp, interval_delta)
            
            if window_start not in windows:
                windows[window_start] = []
            windows[window_start].append(tick)
            
        return windows
        
    def _parse_interval(self, interval: str) -> timedelta:
        """Parse interval string to timedelta.

        Supported suffixes:
        - m: minutes
        - h: hours
        - d: days

        Raises:
            ValueError: If the interval string uses an unsupported format.
        """
        if interval.endswith('m'):
            minutes = int(interval[:-1])
            return timedelta(minutes=minutes)
        elif interval.endswith('h'):
            hours = int(interval[:-1])
            return timedelta(hours=hours)
        elif interval.endswith('d'):
            days = int(interval[:-1])
            return timedelta(days=days)
        else:
            raise ValueError(f"Invalid interval format: {interval}")
            
    def _get_window_start(self, timestamp: datetime, interval_delta: timedelta) -> datetime:
        """Get the start of the time window for a timestamp.

        Aligns timestamps to a coarse boundary to build consistent windows.
        For sub-hour intervals, aligns to minute; for sub-day, aligns to
        hour; otherwise aligns to day.
        """
        # For simplicity, align to minute boundaries
        if interval_delta.total_seconds() < 3600:  # Less than 1 hour
            return timestamp.replace(second=0, microsecond=0)
        elif interval_delta.total_seconds() < 86400:  # Less than 1 day
            return timestamp.replace(minute=0, second=0, microsecond=0)
        else:  # Daily or longer
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            
    async def _create_bar(
        self, 
        instrument_id: str, 
        tenant_id: str, 
        interval: str, 
        window_start: datetime, 
        ticks: List[TickData]
    ) -> Optional[BarData]:
        """Create a single OHLC bar from ticks.

        Computes open, high, low, close, volume and trade count for a
        contiguous time window of ticks.

        Returns:
            A `BarData` object if ticks are present; otherwise None.
        """
        if not ticks:
            return None
            
        # Sort ticks by timestamp
        ticks.sort(key=lambda t: t.timestamp)
        
        # Calculate OHLC
        open_price = ticks[0].price
        close_price = ticks[-1].price
        high_price = max(tick.price for tick in ticks)
        low_price = min(tick.price for tick in ticks)
        
        # Calculate volume and trade count
        volume = sum(tick.volume for tick in ticks)
        trade_count = len(ticks)
        
        # Create metadata with quality flags summary
        metadata = {
            "quality_flags_count": len({
                flag
                for tick in ticks
                for flag in tick.quality_flag_labels
            }),
            "source_ticks": len(ticks),
            "aggregation_id": deterministic_uuid(
                AGGREGATION_BAR_NAMESPACE,
                instrument_id,
                tenant_id,
                interval,
                window_start.isoformat(),
            ),
        }
            
        return BarData(
            instrument_id=instrument_id,
            interval=interval,
            interval_start=window_start,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
            trade_count=trade_count,
            tenant_id=tenant_id,
            metadata=metadata
        )
