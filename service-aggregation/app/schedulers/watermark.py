"""Watermark management for late data handling."""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class WatermarkManager:
    """Manages watermarks for late data handling."""
    
    def __init__(self, config):
        self.config = config
        self.watermarks: Dict[str, datetime] = {}
        self.is_running = False
        
    async def start(self):
        """Start the watermark manager."""
        self.is_running = True
        logger.info("Watermark manager started")
        
    async def stop(self):
        """Stop the watermark manager."""
        self.is_running = False
        logger.info("Watermark manager stopped")
        
    async def update_watermark(self, timestamp: datetime, key: str = "default"):
        """Update watermark for a given key."""
        if key not in self.watermarks or timestamp > self.watermarks[key]:
            self.watermarks[key] = timestamp
            logger.debug(f"Updated watermark for {key}: {timestamp}")
            
    def get_watermark(self, key: str = "default") -> Optional[datetime]:
        """Get current watermark for a key."""
        return self.watermarks.get(key)
        
    def get_effective_watermark(self, key: str = "default") -> Optional[datetime]:
        """Get effective watermark (watermark - delay)."""
        watermark = self.get_watermark(key)
        if watermark is None:
            return None
            
        delay = timedelta(milliseconds=self.config.watermark_delay_ms)
        return watermark - delay
        
    def is_late_data(self, timestamp: datetime, key: str = "default") -> bool:
        """Check if data is late based on watermark."""
        effective_watermark = self.get_effective_watermark(key)
        if effective_watermark is None:
            return False
            
        return timestamp < effective_watermark
        
    def is_too_late(self, timestamp: datetime, key: str = "default") -> bool:
        """Check if data is too late to process."""
        effective_watermark = self.get_effective_watermark(key)
        if effective_watermark is None:
            return False
            
        max_late = timedelta(milliseconds=self.config.max_late_data_ms)
        return timestamp < (effective_watermark - max_late)

