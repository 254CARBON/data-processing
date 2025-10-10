"""Redis async client wrapper for caching and shared state.

Provides high-level interface for Redis operations
with connection pooling and error handling.
"""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import json
import structlog

import redis.asyncio as redis


logger = structlog.get_logger()


@dataclass
class RedisConfig:
    """Redis configuration."""
    url: str
    max_connections: int = 20
    timeout: int = 30
    retry_on_timeout: bool = True


class RedisClient:
    """
    Async Redis client with connection pooling.
    
    Provides high-level interface for Redis operations
    with automatic retry and error handling.
    """
    
    def __init__(self, config: RedisConfig | str):
        if isinstance(config, str):
            config = RedisConfig(url=config)
        self.config = config
        self.logger = structlog.get_logger("redis-client")
        self.client: Optional[redis.Redis] = None
        self.is_connected: bool = False
    
    async def connect(self) -> None:
        """Connect to Redis."""
        if self.client:
            return
        
        self.client = redis.from_url(
            self.config.url,
            decode_responses=True,
            max_connections=self.config.max_connections,
            socket_timeout=self.config.timeout,
            retry_on_timeout=self.config.retry_on_timeout
        )
        
        # Validate connection
        await self.client.ping()
        self.is_connected = True
        self.logger.info("Connected to Redis")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.client:
            await self.client.close()
            self.client = None
            self.is_connected = False
            self.logger.info("Disconnected from Redis")
    
    # Alias for consistency with other storage clients
    async def close(self) -> None:
        """Close Redis connection."""
        await self.disconnect()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value by key."""
        if not self.client:
            await self.connect()
        
        try:
            value = await self.client.get(key)
            if value is None:
                return None
            
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            
            return value
        except Exception as e:
            self.logger.error("Redis get error", error=str(e), key=key)
            raise
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value with optional TTL."""
        if not self.client:
            await self.connect()
        
        stored_value = value
        if not isinstance(value, (str, bytes)):
            stored_value = json.dumps(value)
        
        try:
            await self.client.set(key, stored_value, ex=ttl)
            self.logger.debug("Value set", key=key, ttl=ttl)
        except Exception as e:
            self.logger.error("Redis set error", error=str(e), key=key)
            raise
    
    async def delete(self, key: str) -> int:
        """Delete key."""
        if not self.client:
            await self.connect()
        
        try:
            deleted = await self.client.delete(key)
            self.logger.debug("Key deleted", key=key, deleted=deleted)
            return deleted
        except Exception as e:
            self.logger.error("Redis delete error", error=str(e), key=key)
            raise
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.exists(key) > 0
        except Exception as e:
            self.logger.error("Redis exists error", error=str(e), key=key)
            raise
    
    async def expire(self, key: str, ttl: int) -> None:
        """Set TTL for key."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.expire(key, ttl)
            self.logger.debug("TTL set", key=key, ttl=ttl)
        except Exception as e:
            self.logger.error("Redis expire error", error=str(e), key=key)
            raise
    
    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Get JSON value by key."""
        value = await self.get(key)
        if isinstance(value, dict):
            return value
        if value is None:
            return None
        
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error("Redis JSON decode error", error=str(e), key=key)
            return None
    
    async def set_json(self, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """Set JSON value with optional TTL."""
        try:
            json_value = json.dumps(value)
            await self.set(key, json_value, ttl)
        except Exception as e:
            self.logger.error("Redis JSON set error", error=str(e), key=key)
            raise
    
    async def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.hget(key, field)
        except Exception as e:
            self.logger.error("Redis hget error", error=str(e), key=key, field=field)
            raise
    
    async def hset(self, key: str, field: str, value: Union[str, bytes, int, float]) -> None:
        """Set hash field value."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.hset(key, field, value)
            self.logger.debug("Hash field set", key=key, field=field)
        except Exception as e:
            self.logger.error("Redis hset error", error=str(e), key=key, field=field)
            raise
    
    async def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.hgetall(key)
        except Exception as e:
            self.logger.error("Redis hgetall error", error=str(e), key=key)
            raise
    
    async def hdel(self, key: str, field: str) -> None:
        """Delete hash field."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.hdel(key, field)
            self.logger.debug("Hash field deleted", key=key, field=field)
        except Exception as e:
            self.logger.error("Redis hdel error", error=str(e), key=key, field=field)
            raise
    
    async def lpush(self, key: str, value: Union[str, bytes]) -> None:
        """Push value to list."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.lpush(key, value)
            self.logger.debug("Value pushed to list", key=key)
        except Exception as e:
            self.logger.error("Redis lpush error", error=str(e), key=key)
            raise
    
    async def rpop(self, key: str) -> Optional[str]:
        """Pop value from list."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.rpop(key)
        except Exception as e:
            self.logger.error("Redis rpop error", error=str(e), key=key)
            raise
    
    async def llen(self, key: str) -> int:
        """Get list length."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.llen(key)
        except Exception as e:
            self.logger.error("Redis llen error", error=str(e), key=key)
            raise
    
    async def sadd(self, key: str, value: Union[str, bytes]) -> None:
        """Add value to set."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.sadd(key, value)
            self.logger.debug("Value added to set", key=key)
        except Exception as e:
            self.logger.error("Redis sadd error", error=str(e), key=key)
            raise
    
    async def srem(self, key: str, value: Union[str, bytes]) -> None:
        """Remove value from set."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.srem(key, value)
            self.logger.debug("Value removed from set", key=key)
        except Exception as e:
            self.logger.error("Redis srem error", error=str(e), key=key)
            raise
    
    async def smembers(self, key: str) -> set:
        """Get all set members."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.smembers(key)
        except Exception as e:
            self.logger.error("Redis smembers error", error=str(e), key=key)
            raise
    
    async def keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern."""
        if not self.client:
            await self.connect()
        
        try:
            return await self.client.keys(pattern)
        except Exception as e:
            self.logger.error("Redis keys error", error=str(e), pattern=pattern)
            raise
    
    async def flushdb(self) -> None:
        """Flush current database."""
        if not self.client:
            await self.connect()
        
        try:
            await self.client.flushdb()
            self.logger.info("Database flushed")
        except Exception as e:
            self.logger.error("Redis flushdb error", error=str(e))
            raise
    
    async def health_check(self) -> bool:
        """Check Redis health."""
        if not self.client:
            await self.connect()
        
        try:
            result = await self.client.ping()
            return result is True
        except Exception as e:
            self.logger.error("Redis health check failed", error=str(e))
            return False
    
    async def get_connection_info(self) -> Dict[str, Any]:
        """Get Redis connection statistics."""
        if not self.client:
            await self.connect()
        
        try:
            info = await self.client.info()
            return {
                "status": "connected" if self.is_connected else "disconnected",
                "used_memory_human": info.get("used_memory_human"),
                "used_memory": info.get("used_memory"),
                "connected_clients": info.get("connected_clients"),
                "keyspace_hits": info.get("keyspace_hits"),
                "keyspace_misses": info.get("keyspace_misses"),
            }
        except Exception as e:
            self.logger.error("Redis info error", error=str(e))
            return {"status": "error", "error": str(e)}
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
