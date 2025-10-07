"""Batch job scheduling for aggregation."""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class BatchScheduler:
    """Scheduler for batch aggregation jobs."""
    
    def __init__(self, config):
        self.config = config
        self.scheduled_jobs: Dict[str, asyncio.Task] = {}
        self.is_running = False
        
    async def start(self):
        """Start the batch scheduler."""
        self.is_running = True
        logger.info("Batch scheduler started")
        
    async def stop(self):
        """Stop the batch scheduler."""
        self.is_running = False
        
        # Cancel all scheduled jobs
        for job_id, task in self.scheduled_jobs.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        self.scheduled_jobs.clear()
        logger.info("Batch scheduler stopped")
        
    async def schedule_job(self, job_id: str, job_func, delay_seconds: int = 0):
        """Schedule a batch job."""
        if job_id in self.scheduled_jobs:
            logger.warning(f"Job {job_id} already scheduled")
            return
            
        async def run_job():
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
                
            try:
                await job_func()
            except Exception as e:
                logger.error(f"Error in scheduled job {job_id}: {e}")
            finally:
                if job_id in self.scheduled_jobs:
                    del self.scheduled_jobs[job_id]
                    
        task = asyncio.create_task(run_job())
        self.scheduled_jobs[job_id] = task
        
    async def schedule_periodic_job(self, job_id: str, job_func, interval_seconds: int):
        """Schedule a periodic job."""
        if job_id in self.scheduled_jobs:
            logger.warning(f"Periodic job {job_id} already scheduled")
            return
            
        async def run_periodic_job():
            while self.is_running:
                try:
                    await job_func()
                except Exception as e:
                    logger.error(f"Error in periodic job {job_id}: {e}")
                    
                await asyncio.sleep(interval_seconds)
                
        task = asyncio.create_task(run_periodic_job())
        self.scheduled_jobs[job_id] = task
        
    def cancel_job(self, job_id: str):
        """Cancel a scheduled job."""
        if job_id in self.scheduled_jobs:
            task = self.scheduled_jobs[job_id]
            if not task.done():
                task.cancel()
            del self.scheduled_jobs[job_id]

