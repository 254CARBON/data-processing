"""Time window management for bar aggregation."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field

from shared.schemas.models import TickData


logger = logging.getLogger(__name__)


@dataclass
class TimeWindow:
    """Time window for bar aggregation."""
    start: datetime
    end: datetime
    interval: str
    is_complete: bool = False
    ticks: List[TickData] = field(default_factory=list)


class TimeWindowManager:
    """Manages time windows for bar aggregation."""
    
    def __init__(self, config):
        self.config = config
        self.windows: Dict[str, Dict[str, TimeWindow]] = {}
        
    def get_or_create_window(
        self, 
        instrument_id: str, 
        tenant_id: str, 
        interval: str, 
        timestamp: datetime
    ) -> TimeWindow:
        """Get or create a time window for the given parameters."""
        key = f"{instrument_id}_{tenant_id}"
        
        if key not in self.windows:
            self.windows[key] = {}
            
        if interval not in self.windows[key]:
            self.windows[key][interval] = {}
            
        # Calculate window start
        window_start = self._get_window_start(timestamp, interval)
        window_key = window_start.isoformat()
        
        if window_key not in self.windows[key][interval]:
            window_end = window_start + self._parse_interval(interval)
            self.windows[key][interval][window_key] = TimeWindow(
                start=window_start,
                end=window_end,
                interval=interval
            )
            
        return self.windows[key][interval][window_key]
        
    def add_tick(self, window: TimeWindow, tick: TickData):
        """Add a tick to a time window."""
        window.ticks.append(tick)
        
    def is_window_complete(self, window: TimeWindow, current_time: datetime) -> bool:
        """Check if a time window is complete."""
        return current_time >= window.end
        
    def get_complete_windows(self, current_time: datetime) -> List[TimeWindow]:
        """Get all complete windows."""
        complete_windows = []
        
        for instrument_windows in self.windows.values():
            for interval_windows in instrument_windows.values():
                for window in interval_windows.values():
                    if self.is_window_complete(window, current_time) and not window.is_complete:
                        window.is_complete = True
                        complete_windows.append(window)
                        
        return complete_windows
        
    def cleanup_old_windows(self, current_time: datetime, max_age_hours: int = 24):
        """Clean up old windows to prevent memory leaks."""
        cutoff_time = current_time - timedelta(hours=max_age_hours)
        
        for instrument_windows in self.windows.values():
            for interval_windows in instrument_windows.values():
                # Get keys to remove
                keys_to_remove = [
                    key for key, window in interval_windows.items()
                    if window.start < cutoff_time
                ]
                
                # Remove old windows
                for key in keys_to_remove:
                    del interval_windows[key]
                    
    def _get_window_start(self, timestamp: datetime, interval: str) -> datetime:
        """Get the start of the time window for a timestamp."""
        interval_delta = self._parse_interval(interval)
        
        # For simplicity, align to minute boundaries
        if interval_delta.total_seconds() < 3600:  # Less than 1 hour
            return timestamp.replace(second=0, microsecond=0)
        elif interval_delta.total_seconds() < 86400:  # Less than 1 day
            return timestamp.replace(minute=0, second=0, microsecond=0)
        else:  # Daily or longer
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            
    def _parse_interval(self, interval: str) -> timedelta:
        """Parse interval string to timedelta."""
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
