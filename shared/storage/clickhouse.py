"""
ClickHouse async client wrapper for analytical data storage.

Provides high-level interface for ClickHouse operations
with connection pooling and error handling.
"""

import asyncio
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import json
import structlog

import aiohttp


logger = structlog.get_logger()


@dataclass
class ClickHouseConfig:
    """ClickHouse configuration."""
    url: str
    database: str = "default"
    username: Optional[str] = None
    password: Optional[str] = None
    timeout: int = 30
    max_connections: int = 10


class ClickHouseClient:
    """
    Async ClickHouse client with connection pooling.
    
    Provides high-level interface for ClickHouse operations
    with automatic retry and error handling.
    """
    
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.logger = structlog.get_logger("clickhouse-client")
        self.session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_connections)
    
    async def connect(self) -> None:
        """Connect to ClickHouse."""
        if self.session:
            return
        
        connector = aiohttp.TCPConnector(limit=self.config.max_connections)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            auth=aiohttp.BasicAuth(self.config.username, self.config.password) if self.config.username else None
        )
        
        self.logger.info("Connected to ClickHouse", url=self.config.url)
    
    async def disconnect(self) -> None:
        """Disconnect from ClickHouse."""
        if self.session:
            await self.session.close()
            self.session = None
            self.logger.info("Disconnected from ClickHouse")
    
    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query."""
        async with self._semaphore:
            if not self.session:
                await self.connect()
            
            try:
                # Prepare request
                data = {"query": query}
                if params:
                    data["params"] = json.dumps(params)
                
                async with self.session.post(
                    f"{self.config.url}/",
                    data=data,
                    params={"database": self.config.database}
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"ClickHouse error {response.status}: {error_text}")
                    
                    result = await response.json()
                    return result.get("data", [])
                    
            except Exception as e:
                self.logger.error("ClickHouse query error", error=str(e), query=query)
                raise
    
    async def insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert data into table."""
        if not data:
            return
        
        async with self._semaphore:
            if not self.session:
                await self.connect()
            
            try:
                # Convert data to TSV format
                tsv_data = self._dicts_to_tsv(data)
                
                query = f"INSERT INTO {table} FORMAT TSV"
                
                async with self.session.post(
                    f"{self.config.url}/",
                    data=tsv_data,
                    params={
                        "query": query,
                        "database": self.config.database
                    }
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"ClickHouse insert error {response.status}: {error_text}")
                    
                    self.logger.debug("Data inserted", table=table, count=len(data))
                    
            except Exception as e:
                self.logger.error("ClickHouse insert error", error=str(e), table=table)
                raise
    
    async def create_table(self, table: str, schema: str) -> None:
        """Create a table with the given schema."""
        query = f"CREATE TABLE IF NOT EXISTS {table} ({schema})"
        
        async with self._semaphore:
            if not self.session:
                await self.connect()
            
            try:
                async with self.session.post(
                    f"{self.config.url}/",
                    data={"query": query},
                    params={"database": self.config.database}
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"ClickHouse create table error {response.status}: {error_text}")
                    
                    self.logger.info("Table created", table=table)
                    
            except Exception as e:
                self.logger.error("ClickHouse create table error", error=str(e), table=table)
                raise
    
    async def drop_table(self, table: str) -> None:
        """Drop a table."""
        query = f"DROP TABLE IF EXISTS {table}"
        
        async with self._semaphore:
            if not self.session:
                await self.connect()
            
            try:
                async with self.session.post(
                    f"{self.config.url}/",
                    data={"query": query},
                    params={"database": self.config.database}
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"ClickHouse drop table error {response.status}: {error_text}")
                    
                    self.logger.info("Table dropped", table=table)
                    
            except Exception as e:
                self.logger.error("ClickHouse drop table error", error=str(e), table=table)
                raise
    
    async def health_check(self) -> bool:
        """Check ClickHouse health."""
        try:
            result = await self.execute("SELECT 1")
            return len(result) > 0 and result[0].get("1") == 1
        except Exception as e:
            self.logger.error("ClickHouse health check failed", error=str(e))
            return False
    
    def _dicts_to_tsv(self, data: List[Dict[str, Any]]) -> str:
        """Convert list of dictionaries to TSV format."""
        if not data:
            return ""
        
        # Get all keys from first record
        keys = list(data[0].keys())
        
        # Convert to TSV
        lines = []
        for record in data:
            values = []
            for key in keys:
                value = record.get(key)
                if value is None:
                    values.append("\\N")
                elif isinstance(value, str):
                    # Escape special characters
                    escaped = value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                    values.append(escaped)
                else:
                    values.append(str(value))
            lines.append("\t".join(values))
        
        return "\n".join(lines)
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
