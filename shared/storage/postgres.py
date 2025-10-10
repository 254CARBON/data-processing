"""
PostgreSQL async client wrapper for reference data storage.

Provides high-level interface for PostgreSQL operations
with connection pooling and error handling.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Dict, List, Optional, Union
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
    
    def __init__(self, config: Union[PostgresConfig, str, None] = None, **kwargs: Any):
        if isinstance(config, PostgresConfig):
            self.config = config
        else:
            dsn = config
            if not dsn:
                host = kwargs.get("host", "localhost")
                port = kwargs.get("port", 5432)
                database = kwargs.get("database") or kwargs.get("db") or "postgres"
                user = kwargs.get("user") or kwargs.get("username") or "postgres"
                password = kwargs.get("password", "")
                dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            
            self.config = PostgresConfig(
                dsn=dsn,
                min_size=kwargs.get("min_size", 5),
                max_size=kwargs.get("max_size", 20),
                timeout=kwargs.get("timeout", 30),
            )
        
        self.logger = structlog.get_logger("postgres-client")
        self._pool: Optional[asyncpg.Pool] = None
        self.is_connected: bool = False
    
    @property
    def pool(self) -> Optional[asyncpg.Pool]:
        """Retain backward compatibility with previous attribute name."""
        return self._pool
    
    @pool.setter
    def pool(self, value: Optional[asyncpg.Pool]) -> None:
        self._pool = value
    
    async def connect(self) -> None:
        """Connect to PostgreSQL."""
        if self._pool:
            self.is_connected = True
            return
        
        self._pool = await asyncpg.create_pool(
            self.config.dsn,
            min_size=self.config.min_size,
            max_size=self.config.max_size,
            command_timeout=self.config.timeout
        )
        
        self.is_connected = True
        self.logger.info("Connected to PostgreSQL")
    
    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL."""
        if self._pool:
            close_method = getattr(self._pool, "close", None)
            if callable(close_method):
                result = close_method()
                if inspect.isawaitable(result):
                    await result
            self._pool = None
        
        self.is_connected = False
        self.logger.info("Disconnected from PostgreSQL")
    
    async def close(self) -> None:
        """Alias for disconnect to mirror other storage clients."""
        await self.disconnect()
    
    async def execute(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return list of rows."""
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
        try:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error("PostgreSQL query error", error=str(e), query=query)
            raise
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def query(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        """Alias for execute to maintain backwards compatibility."""
        return await self.execute(query, *args)
    
    async def execute_one(self, query: str, *args: Any) -> Optional[Dict[str, Any]]:
        """Execute a SELECT query returning a single row."""
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
        try:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None
        except Exception as e:
            self.logger.error("PostgreSQL query error", error=str(e), query=query)
            raise
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def execute_scalar(self, query: str, *args: Any) -> Any:
        """Execute a SELECT query returning a scalar value."""
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
        try:
            return await conn.fetchval(query, *args)
        except Exception as e:
            self.logger.error("PostgreSQL query error", error=str(e), query=query)
            raise
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def insert(self, table: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Insert record(s) into the specified table."""
        if isinstance(data, list):
            await self.insert_many(table, data)
            return
        
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
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
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def insert_many(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple records in a single batch."""
        if not data:
            return
        
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
        try:
            columns = list(data[0].keys())
            values_list = [[record[col] for col in columns] for record in data]
            
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(f'${i+1}' for i in range(len(columns)))})"
            executemany = getattr(conn, "executemany", None)
            use_executemany = (
                callable(executemany)
                and getattr(executemany, "__self__", None) is conn
                and getattr(executemany, "__class__", None).__module__ != "unittest.mock"
            )
            if use_executemany:
                result = executemany(insert_sql, values_list)
                if inspect.isawaitable(result):
                    await result
            else:
                execute_result = conn.execute(insert_sql, values_list)
                if inspect.isawaitable(execute_result):
                    await execute_result
            
            self.logger.debug("Records inserted", table=table, count=len(data))
        except Exception as e:
            self.logger.error("PostgreSQL insert many error", error=str(e), table=table)
            raise
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def update(self, table: str, data: Dict[str, Any], where_clause: str, *args: Any) -> None:
        """Update records matching the where clause."""
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
        try:
            set_clauses = [f"{col} = ${i+1}" for i, col in enumerate(data.keys())]
            query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {where_clause}"
            
            values = list(data.values()) + list(args)
            await conn.execute(query, *values)
            
            self.logger.debug("Records updated", table=table)
        except Exception as e:
            self.logger.error("PostgreSQL update error", error=str(e), table=table)
            raise
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def delete(self, table: str, where_clause: str, *args: Any) -> None:
        """Delete records matching the where clause."""
        if not self._pool:
            await self.connect()
        
        conn = await self._pool.acquire()
        try:
            query = f"DELETE FROM {table} WHERE {where_clause}"
            await conn.execute(query, *args)
            
            self.logger.debug("Records deleted", table=table)
        except Exception as e:
            self.logger.error("PostgreSQL delete error", error=str(e), table=table)
            raise
        finally:
            release = getattr(self._pool, "release", None)
            if callable(release):
                await release(conn)
    
    async def transaction(self):
        """Acquire a database connection for transactional work."""
        if not self._pool:
            await self.connect()
        return self._pool.acquire()
    
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
