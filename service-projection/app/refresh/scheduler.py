"""Refresh scheduling for projections."""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class RefreshScheduler:
    """Scheduler for projection refreshes."""
    
    def __init__(self, config):
        self.config = config
        self.scheduled_refreshes: Dict[str, asyncio.Task] = {}
        self.refresh_intervals: Dict[str, int] = {}
        self.is_running = False
        
    async def start(self):
        """Start the refresh scheduler."""
        self.is_running = True
        
        # Schedule periodic refreshes
        await self.schedule_periodic_refresh("latest_price", 300)  # 5 minutes
        await self.schedule_periodic_refresh("curve_snapshot", 600)  # 10 minutes
        await self.schedule_periodic_refresh("custom", 900)  # 15 minutes
        
        logger.info("Refresh scheduler started")
        
    async def stop(self):
        """Stop the refresh scheduler."""
        self.is_running = False
        
        # Cancel all scheduled refreshes
        for refresh_id, task in self.scheduled_refreshes.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        self.scheduled_refreshes.clear()
        logger.info("Refresh scheduler stopped")
        
    async def schedule_periodic_refresh(self, projection_type: str, interval_seconds: int):
        """Schedule periodic refresh for a projection type."""
        refresh_id = f"periodic_{projection_type}"
        
        if refresh_id in self.scheduled_refreshes:
            logger.warning(f"Periodic refresh for {projection_type} already scheduled")
            return
            
        self.refresh_intervals[projection_type] = interval_seconds
        
        async def run_periodic_refresh():
            while self.is_running:
                try:
                    await self._execute_refresh(projection_type)
                except Exception as e:
                    logger.error(f"Error in periodic refresh for {projection_type}: {e}")
                    
                await asyncio.sleep(interval_seconds)
                
        task = asyncio.create_task(run_periodic_refresh())
        self.scheduled_refreshes[refresh_id] = task
        
        logger.info(f"Scheduled periodic refresh for {projection_type} every {interval_seconds}s")
        
    async def schedule_one_time_refresh(self, projection_type: str, delay_seconds: int = 0):
        """Schedule a one-time refresh."""
        refresh_id = f"onetime_{projection_type}_{datetime.now().timestamp()}"
        
        async def run_one_time_refresh():
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
                
            try:
                await self._execute_refresh(projection_type)
            except Exception as e:
                logger.error(f"Error in one-time refresh for {projection_type}: {e}")
            finally:
                if refresh_id in self.scheduled_refreshes:
                    del self.scheduled_refreshes[refresh_id]
                    
        task = asyncio.create_task(run_one_time_refresh())
        self.scheduled_refreshes[refresh_id] = task
        
        logger.info(f"Scheduled one-time refresh for {projection_type} in {delay_seconds}s")
        
    async def _execute_refresh(self, projection_type: str):
        """Execute refresh for a projection type."""
        try:
            logger.info(f"Executing refresh for {projection_type}")
            
            # In a real implementation, this would trigger the actual refresh
            # For now, just log the action
            await asyncio.sleep(0.1)  # Simulate work
            
            logger.info(f"Refresh completed for {projection_type}")
            
        except Exception as e:
            logger.error(f"Error executing refresh for {projection_type}: {e}")
            raise
            
    def get_scheduled_refreshes(self) -> List[str]:
        """Get list of scheduled refresh IDs."""
        return list(self.scheduled_refreshes.keys())
        
    def cancel_refresh(self, refresh_id: str):
        """Cancel a scheduled refresh."""
        if refresh_id in self.scheduled_refreshes:
            task = self.scheduled_refreshes[refresh_id]
            if not task.done():
                task.cancel()
            del self.scheduled_refreshes[refresh_id]
            logger.info(f"Cancelled refresh: {refresh_id}")
        else:
            logger.warning(f"Refresh not found: {refresh_id}")
            
    def get_refresh_interval(self, projection_type: str) -> int:
        """Get refresh interval for a projection type."""
        return self.refresh_intervals.get(projection_type, 0)

