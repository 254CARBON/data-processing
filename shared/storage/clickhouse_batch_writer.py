"""
Batch writer for ClickHouse with optimized bulk inserts.
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import structlog

logger = structlog.get_logger()


@dataclass
class BatchConfig:
    """Configuration for batch writing."""
    batch_size: int = 1000
    flush_interval: float = 5.0  # seconds
    max_retries: int = 3
    retry_delay: float = 1.0


class ClickHouseBatchWriter:
    """Optimized batch writer for ClickHouse."""
    
    def __init__(self, clickhouse_client, config: Optional[BatchConfig] = None):
        """
        Initialize batch writer.
        
        Args:
            clickhouse_client: ClickHouse client instance
            config: Batch configuration
        """
        self.client = clickhouse_client
        self.config = config or BatchConfig()
        self.logger = logger.bind(component="clickhouse_batch_writer")
        
        # Buffers for each table
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.buffer_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.last_flush: Dict[str, float] = defaultdict(float)
        
        # Statistics
        self.stats = {
            "batches_written": 0,
            "rows_written": 0,
            "flush_count": 0,
            "error_count": 0,
        }
        
        # Background flush task
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self) -> None:
        """Start the batch writer background task."""
        if self._running:
            return
        
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        self.logger.info("Batch writer started", config=self.config)
    
    async def stop(self) -> None:
        """Stop the batch writer and flush remaining data."""
        if not self._running:
            return
        
        self._running = False
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush all remaining data
        await self.flush_all()
        self.logger.info("Batch writer stopped", stats=self.stats)
    
    async def write(self, table: str, data: Dict[str, Any]) -> None:
        """
        Add data to batch buffer.
        
        Args:
            table: Table name
            data: Data dictionary
        """
        async with self.buffer_locks[table]:
            self.buffers[table].append(data)
            
            # Check if we need to flush
            if len(self.buffers[table]) >= self.config.batch_size:
                await self._flush_table(table)
    
    async def write_batch(self, table: str, data: List[Dict[str, Any]]) -> None:
        """
        Add multiple rows to batch buffer.
        
        Args:
            table: Table name
            data: List of data dictionaries
        """
        async with self.buffer_locks[table]:
            self.buffers[table].extend(data)
            
            # Check if we need to flush
            if len(self.buffers[table]) >= self.config.batch_size:
                await self._flush_table(table)
    
    async def flush(self, table: str) -> None:
        """
        Manually flush a specific table's buffer.
        
        Args:
            table: Table name
        """
        await self._flush_table(table)
    
    async def flush_all(self) -> None:
        """Flush all table buffers."""
        tables = list(self.buffers.keys())
        await asyncio.gather(*[self._flush_table(table) for table in tables])
    
    async def _flush_table(self, table: str) -> None:
        """
        Flush a table's buffer to ClickHouse.
        
        Args:
            table: Table name
        """
        async with self.buffer_locks[table]:
            if not self.buffers[table]:
                return
            
            batch = self.buffers[table].copy()
            self.buffers[table].clear()
        
        # Write batch with retries
        for attempt in range(self.config.max_retries):
            try:
                start_time = time.time()
                
                # Execute batch insert
                await self._execute_batch(table, batch)
                
                # Update statistics
                self.stats["batches_written"] += 1
                self.stats["rows_written"] += len(batch)
                self.stats["flush_count"] += 1
                self.last_flush[table] = time.time()
                
                duration = time.time() - start_time
                self.logger.info(
                    "Batch written",
                    table=table,
                    rows=len(batch),
                    duration_ms=int(duration * 1000),
                    attempt=attempt + 1,
                )
                return
                
            except Exception as e:
                self.stats["error_count"] += 1
                self.logger.error(
                    "Batch write failed",
                    table=table,
                    rows=len(batch),
                    attempt=attempt + 1,
                    error=str(e),
                )
                
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                else:
                    # Re-add to buffer on final failure
                    async with self.buffer_locks[table]:
                        self.buffers[table].extend(batch)
                    raise
    
    async def _execute_batch(self, table: str, batch: List[Dict[str, Any]]) -> None:
        """
        Execute batch insert to ClickHouse.
        
        Args:
            table: Table name
            batch: List of data dictionaries
        """
        # Use ClickHouse's native insert format for best performance
        await self.client.insert(table, batch)
    
    async def _periodic_flush(self) -> None:
        """Background task to periodically flush buffers."""
        while self._running:
            try:
                await asyncio.sleep(1)  # Check every second
                
                current_time = time.time()
                tables_to_flush = []
                
                # Check which tables need flushing
                for table in list(self.buffers.keys()):
                    if not self.buffers[table]:
                        continue
                    
                    time_since_flush = current_time - self.last_flush.get(table, 0)
                    if time_since_flush >= self.config.flush_interval:
                        tables_to_flush.append(table)
                
                # Flush tables that need it
                if tables_to_flush:
                    await asyncio.gather(
                        *[self._flush_table(table) for table in tables_to_flush],
                        return_exceptions=True
                    )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Periodic flush error", error=str(e))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch writer statistics."""
        return {
            **self.stats,
            "buffer_sizes": {
                table: len(buffer) for table, buffer in self.buffers.items()
            },
        }
