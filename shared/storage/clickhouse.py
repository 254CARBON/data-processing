"""
ClickHouse async client wrapper for analytical data storage.

Provides high-level interface for ClickHouse operations
with connection pooling and error handling.
"""

import asyncio
import inspect
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import aiohttp
import structlog


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
    
    def __init__(self, config: Union[ClickHouseConfig, str, None] = None, **kwargs: Any):
        if isinstance(config, ClickHouseConfig):
            self.config = config
        else:
            params: Dict[str, Any] = {}
            if isinstance(config, str):
                # If a URL is provided use it, otherwise treat as host
                if config.startswith("http://") or config.startswith("https://"):
                    params["url"] = config
                else:
                    params["host"] = config
            params.update(kwargs)
            
            url = params.get("url")
            host = params.get("host", "localhost")
            port = params.get("port", 8123)
            scheme = params.get("scheme", "http")
            database = params.get("database", "default")
            username = params.get("user") or params.get("username")
            password = params.get("password")
            timeout = params.get("timeout", 30)
            max_connections = params.get("max_connections", 10)
            
            if not url:
                url = f"{scheme}://{host}:{port}"
            
            self.config = ClickHouseConfig(
                url=url,
                database=database,
                username=username,
                password=password,
                timeout=timeout,
                max_connections=max_connections,
            )
        
        self.logger = structlog.get_logger("clickhouse-client")
        self.session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(self.config.max_connections)
        self._client: Optional[Any] = kwargs.get("client")
        self.is_connected: bool = False
    
    async def connect(self) -> None:
        """Connect to ClickHouse."""
        if self.is_connected:
            return
        
        if self._client is not None:
            # Support injected client (used in tests or alternative drivers)
            ping = getattr(self._client, "ping", None)
            if callable(ping):
                result = ping()
                if inspect.isawaitable(result):
                    await result
            self.is_connected = True
            self.logger.info("Using injected ClickHouse client")
            return
        
        connector = aiohttp.TCPConnector(limit=self.config.max_connections)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        auth = None
        if self.config.username:
            auth = aiohttp.BasicAuth(self.config.username, self.config.password or "")
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            auth=auth
        )
        
        self.is_connected = True
        self.logger.info("Connected to ClickHouse", url=self.config.url)
    
    async def disconnect(self) -> None:
        """Disconnect from ClickHouse."""
        if self._client is not None:
            close_method = getattr(self._client, "close", None) or getattr(self._client, "disconnect", None)
            if callable(close_method):
                result = close_method()
                if inspect.isawaitable(result):
                    await result
            self.is_connected = False
            return
        
        if self.session:
            await self.session.close()
            self.session = None
        
        self.is_connected = False
        self.logger.info("Disconnected from ClickHouse")
    
    async def close(self) -> None:
        """Alias for disconnect."""
        await self.disconnect()
    
    async def _call_injected_client(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Invoke a method on the injected client, handling async/sync transparently."""
        if self._client is None:
            return None
        
        target = getattr(self._client, method, None)
        if not callable(target):
            return None
        
        result = target(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result
    
    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query."""
        if self._client is not None:
            result = await self._call_injected_client("execute", query, params)
            return result or []
        
        async with self._semaphore:
            if not self.session:
                await self.connect()
            
            try:
                payload: Dict[str, Any] = {"query": query}
                if params:
                    payload["params"] = json.dumps(params)
                
                async with self.session.post(
                    f"{self.config.url}/",
                    data=payload,
                    params={"database": self.config.database}
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"ClickHouse error {response.status}: {error_text}")
                    
                    if response.content_type == "application/json":
                        result = await response.json()
                        return result.get("data", [])
                    
                    text_result = await response.text()
                    return json.loads(text_result).get("data", []) if text_result else []
                    
            except Exception as e:
                self.logger.error("ClickHouse query error", error=str(e), query=query)
                raise
    
    async def query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Alias for execute to improve readability."""
        return await self.execute(query, params)
    
    async def insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert data into table."""
        if not data:
            return
        
        if self._client is not None:
            query = f"INSERT INTO {table} FORMAT JSONEachRow"
            await self._call_injected_client("execute", query, data)
            return

        async with self._semaphore:
            if not self.session:
                await self.connect()
            
            try:
                payload = self._dicts_to_jsonl(data)
                query = f"INSERT INTO {table} FORMAT JSONEachRow"

                async with self.session.post(
                    f"{self.config.url}/",
                    data=payload,
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
        
        if self._client is not None:
            await self._call_injected_client("execute", query)
            return
        
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
        
        if self._client is not None:
            await self._call_injected_client("execute", query)
            return
        
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
            if not result:
                return False
            first_row = result[0]
            return 1 in first_row.values()
        except Exception as e:
            self.logger.error("ClickHouse health check failed", error=str(e))
            return False
    
    def _dicts_to_jsonl(self, data: List[Dict[str, Any]]) -> str:
        """Convert list of dictionaries to JSON Lines for ClickHouse ingestion."""
        def _default(value: Any) -> Any:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc).isoformat()
            if isinstance(value, (set, tuple)):
                return list(value)
            if isinstance(value, Enum):
                return value.value
            return str(value)

        return "\n".join(json.dumps(record, default=_default, separators=(",", ":")) for record in data)
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
