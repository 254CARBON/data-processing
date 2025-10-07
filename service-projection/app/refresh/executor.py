"""Refresh execution for projections."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class RefreshExecutor:
    """Executor for projection refreshes."""
    
    def __init__(self, config):
        self.config = config
        self.running_refreshes: Dict[str, asyncio.Task] = {}
        self.is_running = False
        
    async def start(self):
        """Start the refresh executor."""
        self.is_running = True
        logger.info("Refresh executor started")
        
    async def stop(self):
        """Stop the refresh executor."""
        self.is_running = False
        
        # Wait for all refreshes to complete
        if self.running_refreshes:
            logger.info(f"Waiting for {len(self.running_refreshes)} refreshes to complete")
            await asyncio.gather(*self.running_refreshes.values(), return_exceptions=True)
            
        self.running_refreshes.clear()
        logger.info("Refresh executor stopped")
        
    async def refresh_projection(self, projection_type: str):
        """Refresh a specific projection type."""
        if projection_type in self.running_refreshes:
            logger.warning(f"Refresh for {projection_type} already running")
            return
            
        async def run_refresh():
            try:
                logger.info(f"Starting refresh for {projection_type}")
                await self._execute_refresh(projection_type)
                logger.info(f"Refresh completed for {projection_type}")
            except Exception as e:
                logger.error(f"Refresh failed for {projection_type}: {e}")
            finally:
                if projection_type in self.running_refreshes:
                    del self.running_refreshes[projection_type]
                    
        task = asyncio.create_task(run_refresh())
        self.running_refreshes[projection_type] = task
        
    async def _execute_refresh(self, projection_type: str):
        """Execute refresh for a projection type."""
        # Simulate refresh work
        await asyncio.sleep(0.1)
        logger.debug(f"Executed refresh for {projection_type}")

