"""
ClickHouse writer for normalized data.

Writes normalized tick data to ClickHouse with proper
error handling and metrics collection.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import structlog

from shared.storage.clickhouse import ClickHouseClient, ClickHouseConfig
from shared.schemas.models import TickData
from shared.utils.errors import StorageError, create_error_context
from shared.utils.audit import AuditActorType, AuditEventType


logger = structlog.get_logger()


class ClickHouseWriter:
    """
    ClickHouse writer for normalized data.
    
    Writes normalized tick data to ClickHouse with proper
    error handling and metrics collection.
    """
    
    def __init__(self, config, audit_logger):
        self.config = config
        self.logger = structlog.get_logger("clickhouse-writer")
        self.audit = audit_logger
        
        # Create ClickHouse client
        ch_config = ClickHouseConfig(
            url=config.database.clickhouse_url,
            database="data_processing",
            timeout=30,
            max_connections=10
        )
        
        self.client = ClickHouseClient(ch_config)
        
        # Metrics
        self.write_count = 0
        self.write_errors = 0
        self.last_write_time = None
        
        # Batch writing
        self.batch_size = 100
        self.batch_buffer: List[TickData] = []
        self.batch_lock = asyncio.Lock()
    
    async def startup(self) -> None:
        """Initialize writer."""
        self.logger.info("Starting ClickHouse writer")
        await self.client.connect()
        
        # Create table if it doesn't exist
        await self._create_table()
    
    async def shutdown(self) -> None:
        """Shutdown writer."""
        self.logger.info("Shutting down ClickHouse writer")
        
        # Flush remaining batch
        await self._flush_batch()
        
        await self.client.disconnect()
    
    async def write_tick(self, tick: TickData) -> None:
        """
        Write tick data to ClickHouse.
        
        Args:
            tick: Normalized tick data
        """
        try:
            async with self.batch_lock:
                self.batch_buffer.append(tick)
                
                # Flush batch if it's full
                if len(self.batch_buffer) >= self.batch_size:
                    await self._flush_batch()
            
            self.logger.debug(
                "Tick added to batch",
                instrument_id=tick.instrument_id,
                batch_size=len(self.batch_buffer)
            )
            
        except Exception as e:
            self.write_errors += 1
            self.logger.error(
                "Failed to write tick",
                error=str(e),
                instrument_id=tick.instrument_id,
                exc_info=True
            )
            raise
    
    async def _flush_batch(self) -> None:
        """Flush batch buffer to ClickHouse."""
        if not self.batch_buffer:
            return
        
        try:
            # Convert ticks to dictionaries
            tick_dicts = [tick.to_dict() for tick in self.batch_buffer]
            
            # Write to ClickHouse
            await self.client.insert("silver_ticks", tick_dicts)
            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="silver_ticks_batch_write",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-writer",
                tenant_id=self.config.tenant_id,
                resource_type="table",
                resource_id="silver_ticks",
                details={"count": len(self.batch_buffer)},
            )
            
            # Update metrics
            self.write_count += len(self.batch_buffer)
            self.last_write_time = asyncio.get_event_loop().time()
            
            self.logger.info(
                "Batch written to ClickHouse",
                count=len(self.batch_buffer),
                table="silver_ticks"
            )
            
            # Clear buffer
            self.batch_buffer.clear()
            
        except Exception as e:
            self.write_errors += 1
            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="silver_ticks_batch_fail",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-writer",
                tenant_id=self.config.tenant_id,
                resource_type="table",
                resource_id="silver_ticks",
                details={"error": str(e), "batch_size": len(self.batch_buffer)},
            )
            raise
    
    async def _create_table(self) -> None:
        """Create ClickHouse table if it doesn't exist."""
        try:
            schema = """
                instrument_id String,
                timestamp DateTime64(3),
                price Float64,
                volume Float64,
                quality_flags Array(String),
                tenant_id String,
                source_id Nullable(String),
                metadata Map(String, String)
            """
            
            await self.client.create_table("silver_ticks", schema)
            
            self.logger.info("ClickHouse table created", table="silver_ticks")
            
        except Exception as e:
            self.logger.error(
                "Failed to create ClickHouse table",
                error=str(e),
                table="silver_ticks",
                exc_info=True
            )
            raise
    
    def get_write_count(self) -> int:
        """Get write count."""
        return self.write_count
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get writer metrics."""
        return {
            "write_count": self.write_count,
            "write_errors": self.write_errors,
            "last_write_time": self.last_write_time,
            "batch_size": len(self.batch_buffer),
            "success_rate": self.write_count / (self.write_count + self.write_errors) if (self.write_count + self.write_errors) > 0 else 0,
        }
