"""Mock services for testing."""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from unittest.mock import AsyncMock, MagicMock
from shared.framework.service import AsyncService


logger = logging.getLogger(__name__)


class MockKafkaProducer:
    """Mock Kafka producer for testing."""
    
    def __init__(self):
        self.messages = []
        self.is_connected = False
        
    async def start(self):
        """Start the mock producer."""
        self.is_connected = True
        logger.info("Mock Kafka producer started")
        
    async def stop(self):
        """Stop the mock producer."""
        self.is_connected = False
        logger.info("Mock Kafka producer stopped")
        
    async def produce(self, topic: str, key: str, value: Dict[str, Any]):
        """Produce a message."""
        message = {
            'topic': topic,
            'key': key,
            'value': value,
            'timestamp': asyncio.get_event_loop().time()
        }
        self.messages.append(message)
        logger.debug(f"Mock producer sent message to {topic}")
        
    def get_messages(self, topic: str = None) -> List[Dict[str, Any]]:
        """Get messages sent to a specific topic."""
        if topic:
            return [msg for msg in self.messages if msg['topic'] == topic]
        return self.messages
        
    def clear_messages(self):
        """Clear all messages."""
        self.messages.clear()


class MockKafkaConsumer:
    """Mock Kafka consumer for testing."""
    
    def __init__(self, topics: List[str]):
        self.topics = topics
        self.messages = []
        self.is_connected = False
        self.message_index = 0
        
    async def start(self):
        """Start the mock consumer."""
        self.is_connected = True
        logger.info("Mock Kafka consumer started")
        
    async def stop(self):
        """Stop the mock consumer."""
        self.is_connected = False
        logger.info("Mock Kafka consumer stopped")
        
    async def consume(self, message: Dict[str, Any]) -> None:
        """Consume a message."""
        logger.debug(f"Mock consumer received message: {message}")
        
    def add_message(self, topic: str, key: str, value: Dict[str, Any]):
        """Add a message to the consumer."""
        message = {
            'topic': topic,
            'key': key,
            'value': value,
            'timestamp': asyncio.get_event_loop().time()
        }
        self.messages.append(message)
        
    async def poll(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Poll for messages."""
        if self.message_index < len(self.messages):
            message = self.messages[self.message_index]
            self.message_index += 1
            return message
        return None


class MockClickHouseClient:
    """Mock ClickHouse client for testing."""
    
    def __init__(self):
        self.is_connected = False
        self.inserted_data = {}
        
    async def connect(self):
        """Connect to mock ClickHouse."""
        self.is_connected = True
        logger.info("Mock ClickHouse client connected")
        
    async def close(self):
        """Close mock ClickHouse connection."""
        self.is_connected = False
        logger.info("Mock ClickHouse client disconnected")
        
    async def insert(self, table: str, data: List[Dict[str, Any]]):
        """Insert data into mock table."""
        if table not in self.inserted_data:
            self.inserted_data[table] = []
        self.inserted_data[table].extend(data)
        logger.debug(f"Mock ClickHouse inserted {len(data)} records into {table}")
        
    async def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute mock query."""
        logger.debug(f"Mock ClickHouse query: {query}")
        return []
        
    def get_inserted_data(self, table: str) -> List[Dict[str, Any]]:
        """Get inserted data for a table."""
        return self.inserted_data.get(table, [])
        
    def clear_data(self):
        """Clear all inserted data."""
        self.inserted_data.clear()


class MockRedisClient:
    """Mock Redis client for testing."""
    
    def __init__(self):
        self.is_connected = False
        self.cache = {}
        
    async def connect(self):
        """Connect to mock Redis."""
        self.is_connected = True
        logger.info("Mock Redis client connected")
        
    async def close(self):
        """Close mock Redis connection."""
        self.is_connected = False
        logger.info("Mock Redis client disconnected")
        
    async def set(self, key: str, value: Any, ttl: int = None):
        """Set a value in mock Redis."""
        self.cache[key] = {
            'value': value,
            'ttl': ttl,
            'timestamp': asyncio.get_event_loop().time()
        }
        logger.debug(f"Mock Redis set key: {key}")
        
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from mock Redis."""
        if key in self.cache:
            return self.cache[key]['value']
        return None
        
    async def delete(self, key: str):
        """Delete a key from mock Redis."""
        if key in self.cache:
            del self.cache[key]
            logger.debug(f"Mock Redis deleted key: {key}")
            
    def get_cache(self) -> Dict[str, Any]:
        """Get all cached data."""
        return self.cache
        
    def clear_cache(self):
        """Clear all cached data."""
        self.cache.clear()


class MockPostgresClient:
    """Mock PostgreSQL client for testing."""
    
    def __init__(self):
        self.is_connected = False
        self.inserted_data = {}
        
    async def connect(self):
        """Connect to mock PostgreSQL."""
        self.is_connected = True
        logger.info("Mock PostgreSQL client connected")
        
    async def close(self):
        """Close mock PostgreSQL connection."""
        self.is_connected = False
        logger.info("Mock PostgreSQL client disconnected")
        
    async def insert(self, table: str, data: List[Dict[str, Any]]):
        """Insert data into mock table."""
        if table not in self.inserted_data:
            self.inserted_data[table] = []
        self.inserted_data[table].extend(data)
        logger.debug(f"Mock PostgreSQL inserted {len(data)} records into {table}")
        
    async def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute mock query."""
        logger.debug(f"Mock PostgreSQL query: {query}")
        return []
        
    def get_inserted_data(self, table: str) -> List[Dict[str, Any]]:
        """Get inserted data for a table."""
        return self.inserted_data.get(table, [])
        
    def clear_data(self):
        """Clear all inserted data."""
        self.inserted_data.clear()


class MockService(AsyncService):
    """Mock service for testing."""
    
    def __init__(self, name: str = "mock-service"):
        super().__init__()
        self.name = name
        self.is_running = False
        self.processed_messages = []
        
    async def start(self):
        """Start the mock service."""
        self.is_running = True
        logger.info(f"Mock service {self.name} started")
        
    async def stop(self):
        """Stop the mock service."""
        self.is_running = False
        logger.info(f"Mock service {self.name} stopped")
        
    async def process_message(self, message: Dict[str, Any]):
        """Process a message."""
        self.processed_messages.append(message)
        logger.debug(f"Mock service {self.name} processed message: {message}")
        
    def get_processed_messages(self) -> List[Dict[str, Any]]:
        """Get all processed messages."""
        return self.processed_messages
        
    def clear_messages(self):
        """Clear all processed messages."""
        self.processed_messages.clear()


class MockHealthCheck:
    """Mock health check for testing."""
    
    def __init__(self):
        self.is_healthy = True
        self.checks = {}
        
    async def check_health(self) -> Dict[str, Any]:
        """Check health status."""
        return {
            'status': 'healthy' if self.is_healthy else 'unhealthy',
            'checks': self.checks,
            'timestamp': asyncio.get_event_loop().time()
        }
        
    def set_healthy(self, healthy: bool):
        """Set health status."""
        self.is_healthy = healthy
        
    def add_check(self, name: str, status: str, details: Dict[str, Any] = None):
        """Add a health check."""
        self.checks[name] = {
            'status': status,
            'details': details or {}
        }

