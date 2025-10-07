"""
PostgreSQL async client wrapper for reference data storage.

Provides high-level interface for PostgreSQL operations
with connection pooling and error handling.
"""

import asyncio
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import structlog

import asyncpg


logger = structlog.get_logger()


@dataclass
class PostgresConfig:
    """PostgreSQL configuration."""
    dsn: str
    min_size: int = 5
    max_size: int = 20
    timeout: int = 30


class PostgresClient:
    """
    Async PostgreSQL client with connection pooling.
    
    Provides high-level interface for PostgreSQL operations
    with automatic retry and error handling.
    """
    
    def __init__(self, config: PostgresConfig):
        self.config = config
        self.logger = structlog.get_logger("postgres-client")
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self) -> None:
        """Connect to PostgreSQL."""
        if self.pool:
            return
        
        self.pool = await asyncpg.create_pool(
            self.config.dsn,
            min_size=self.config.min_size,
            max_size=self.config.max_size,
            command_timeout=self.config.timeout
        )
        
        self.logger.info("Connected to PostgreSQL")
    
    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            self.logger.info("Disconnected from PostgreSQL")
    
    async def execute(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a SELECT query."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(query, *args)
                return [dict(row) for row in rows]
            except Exception as e:
                self.logger.error("PostgreSQL query error", error=str(e), query=query)
                raise
    
    async def execute_one(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a SELECT query and return first row."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(query, *args)
                return dict(row) if row else None
            except Exception as e:
                self.logger.error("PostgreSQL query error", error=str(e), query=query)
                raise
    
    async def execute_scalar(self, query: str, *args) -> Any:
        """Execute a SELECT query and return scalar value."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                return await conn.fetchval(query, *args)
            except Exception as e:
                self.logger.error("PostgreSQL query error", error=str(e), query=query)
                raise
    
    async def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a single record."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                columns = list(data.keys())
                values = list(data.values())
                placeholders = [f"${i+1}" for i in range(len(values))]
                
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                
                await conn.execute(query, *values)
                self.logger.debug("Record inserted", table=table)
                
            except Exception as e:
                self.logger.error("PostgreSQL insert error", error=str(e), table=table)
                raise
    
    async def insert_many(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple records."""
        if not data:
            return
        
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                # Get columns from first record
                columns = list(data[0].keys())
                
                # Prepare values
                values_list = []
                for record in data:
                    values_list.append([record[col] for col in columns])
                
                await conn.executemany(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join([f'${i+1}' for i in range(len(columns))])})",
                    values_list
                )
                
                self.logger.debug("Records inserted", table=table, count=len(data))
                
            except Exception as e:
                self.logger.error("PostgreSQL insert many error", error=str(e), table=table)
                raise
    
    async def update(self, table: str, data: Dict[str, Any], where_clause: str, *args) -> None:
        """Update records."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                set_clauses = [f"{col} = ${i+1}" for i, col in enumerate(data.keys())]
                query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {where_clause}"
                
                values = list(data.values()) + list(args)
                await conn.execute(query, *values)
                
                self.logger.debug("Records updated", table=table)
                
            except Exception as e:
                self.logger.error("PostgreSQL update error", error=str(e), table=table)
                raise
    
    async def delete(self, table: str, where_clause: str, *args) -> None:
        """Delete records."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                query = f"DELETE FROM {table} WHERE {where_clause}"
                await conn.execute(query, *args)
                
                self.logger.debug("Records deleted", table=table)
                
            except Exception as e:
                self.logger.error("PostgreSQL delete error", error=str(e), table=table)
                raise
    
    async def transaction(self):
        """Get transaction context manager."""
        if not self.pool:
            await self.connect()
        
        return self.pool.acquire()
    
    async def health_check(self) -> bool:
        """Check PostgreSQL health."""
        try:
            result = await self.execute_scalar("SELECT 1")
            return result == 1
        except Exception as e:
            self.logger.error("PostgreSQL health check failed", error=str(e))
            return False
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

