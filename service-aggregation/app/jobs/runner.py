"""Job execution engine for aggregation."""

import asyncio
import logging
from typing import Dict, Any, List, Callable
from datetime import datetime
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class JobRunner:
    """Job execution engine."""
    
    def __init__(self, config):
        self.config = config
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self.is_running = False
        
    async def start(self):
        """Start the job runner."""
        self.is_running = True
        logger.info("Job runner started")
        
    async def stop(self):
        """Stop the job runner."""
        self.is_running = False
        
        # Wait for all jobs to complete
        if self.running_jobs:
            logger.info(f"Waiting for {len(self.running_jobs)} jobs to complete")
            await asyncio.gather(*self.running_jobs.values(), return_exceptions=True)
            
        self.running_jobs.clear()
        logger.info("Job runner stopped")
        
    async def execute_job(self, job_id: str, job_func: Callable, *args, **kwargs):
        """Execute a job."""
        if job_id in self.running_jobs:
            logger.warning(f"Job {job_id} already running")
            return
            
        async def run_job():
            try:
                logger.info(f"Starting job {job_id}")
                result = await job_func(*args, **kwargs)
                logger.info(f"Job {job_id} completed successfully")
                return result
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                raise
            finally:
                if job_id in self.running_jobs:
                    del self.running_jobs[job_id]
                    
        task = asyncio.create_task(run_job())
        self.running_jobs[job_id] = task
        
        return task
        
    async def wait_for_job(self, job_id: str):
        """Wait for a specific job to complete."""
        if job_id in self.running_jobs:
            task = self.running_jobs[job_id]
            await task
            return task.result()
        else:
            raise ValueError(f"Job {job_id} not found")
            
    def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        if job_id not in self.running_jobs:
            return "not_found"
            
        task = self.running_jobs[job_id]
        if task.done():
            if task.exception():
                return "failed"
            else:
                return "completed"
        else:
            return "running"

