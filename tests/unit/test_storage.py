"""Unit tests for storage components."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from shared.storage.clickhouse import ClickHouseClient
from shared.storage.postgres import PostgresClient
from shared.storage.redis import RedisClient


class TestClickHouseClient:
    """Test ClickHouseClient class."""
    
    @pytest.mark.asyncio
    async def test_client_lifecycle(self):
        """Test client lifecycle."""
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Mock the connection
        client._client = MagicMock()
        client._client.ping = AsyncMock(return_value=True)
        
        # Test connect
        await client.connect()
        assert client.is_connected
        
        # Test close
        await client.close()
        assert not client.is_connected
        
    @pytest.mark.asyncio
    async def test_insert_data(self):
        """Test data insertion."""
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Mock the client
        client._client = MagicMock()
        client._client.execute = AsyncMock()
        client.is_connected = True
        
        # Test insert
        test_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        await client.insert("test_table", test_data)
        client._client.execute.assert_called_once()
        
    @pytest.mark.asyncio
    async def test_query_execution(self):
        """Test query execution."""
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Mock the client
        client._client = MagicMock()
        client._client.execute = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        client.is_connected = True
        
        # Test query
        result = await client.query("SELECT * FROM test_table")
        assert len(result) == 1
        assert result[0]["id"] == 1


class TestPostgresClient:
    """Test PostgresClient class."""
    
    @pytest.mark.asyncio
    async def test_client_lifecycle(self):
        """Test client lifecycle."""
        client = PostgresClient(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Mock the connection
        client._pool = MagicMock()
        client._pool.acquire = AsyncMock()
        client._pool.release = AsyncMock()
        
        # Test connect
        await client.connect()
        assert client.is_connected
        
        # Test close
        await client.close()
        assert not client.is_connected
        
    @pytest.mark.asyncio
    async def test_insert_data(self):
        """Test data insertion."""
        client = PostgresClient(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Mock the connection
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        client._pool = MagicMock()
        client._pool.acquire = AsyncMock(return_value=mock_conn)
        client._pool.release = AsyncMock()
        client.is_connected = True
        
        # Test insert
        test_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        await client.insert("test_table", test_data)
        mock_conn.execute.assert_called_once()
        
    @pytest.mark.asyncio
    async def test_query_execution(self):
        """Test query execution."""
        client = PostgresClient(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Mock the connection
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        client._pool = MagicMock()
        client._pool.acquire = AsyncMock(return_value=mock_conn)
        client._pool.release = AsyncMock()
        client.is_connected = True
        
        # Test query
        result = await client.query("SELECT * FROM test_table")
        assert len(result) == 1
        assert result[0]["id"] == 1


class TestRedisClient:
    """Test RedisClient class."""
    
    @pytest.mark.asyncio
    async def test_client_lifecycle(self):
        """Test client lifecycle."""
        client = RedisClient(
            host="localhost",
            port=6379,
            database=0,
            password="test_password"
        )
        
        # Mock the connection
        client._redis = MagicMock()
        client._redis.ping = AsyncMock(return_value=True)
        
        # Test connect
        await client.connect()
        assert client.is_connected
        
        # Test close
        await client.close()
        assert not client.is_connected
        
    @pytest.mark.asyncio
    async def test_set_get_operations(self):
        """Test set and get operations."""
        client = RedisClient(
            host="localhost",
            port=6379,
            database=0,
            password="test_password"
        )
        
        # Mock the client
        client._redis = MagicMock()
        client._redis.set = AsyncMock()
        client._redis.get = AsyncMock(return_value=b'{"test": "value"}')
        client.is_connected = True
        
        # Test set
        await client.set("test_key", {"test": "value"}, ttl=3600)
        client._redis.set.assert_called_once()
        
        # Test get
        result = await client.get("test_key")
        assert result == {"test": "value"}
        
    @pytest.mark.asyncio
    async def test_delete_operation(self):
        """Test delete operation."""
        client = RedisClient(
            host="localhost",
            port=6379,
            database=0,
            password="test_password"
        )
        
        # Mock the client
        client._redis = MagicMock()
        client._redis.delete = AsyncMock(return_value=1)
        client.is_connected = True
        
        # Test delete
        result = await client.delete("test_key")
        assert result == 1
        client._redis.delete.assert_called_once_with("test_key")

