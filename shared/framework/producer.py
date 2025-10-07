"""
Kafka producer abstraction for async microservices.

Provides high-level Kafka producer with automatic error handling,
metrics collection, and message batching.
"""

import asyncio
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
import json
import time

from confluent_kafka import Producer, KafkaError
import structlog

from .config import KafkaConfig


logger = structlog.get_logger()


@dataclass
class ProducerConfig:
    """Producer configuration."""
    topic: str
    batch_size: int = 100
    flush_timeout: float = 1.0
    retry_backoff_ms: int = 100
    max_retries: int = 3


class KafkaProducer:
    """
    High-level Kafka producer with async processing.
    
    Features:
    - Automatic error handling and retry logic
    - Metrics collection
    - Message batching
    - Graceful shutdown
    """
    
    def __init__(
        self,
        config: ProducerConfig,
        kafka_config: KafkaConfig,
        error_handler: Optional[Callable[[Exception], None]] = None
    ):
        self.config = config
        self.kafka_config = kafka_config
        self.error_handler = error_handler or self._default_error_handler
        
        self.logger = structlog.get_logger("kafka-producer")
        self.producer: Optional[Producer] = None
        self.running = False
        self.task: Optional[asyncio.Task] = None
        
        # Message queue
        self.message_queue: asyncio.Queue = asyncio.Queue()
        
        # Metrics
        self.messages_sent = 0
        self.messages_failed = 0
        self.last_message_time = None
    
    def _create_producer(self) -> Producer:
        """Create Kafka producer instance."""
        producer_config = {
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'retries': self.config.max_retries,
            'retry.backoff.ms': self.config.retry_backoff_ms,
            'batch.size': self.config.batch_size,
            'linger.ms': 10,  # Wait up to 10ms to batch messages
            'compression.type': 'snappy',
        }
        
        return Producer(producer_config)
    
    async def start(self) -> None:
        """Start the producer."""
        if self.running:
            return
        
        self.logger.info(
            "Starting Kafka producer",
            topic=self.config.topic
        )
        
        self.producer = self._create_producer()
        self.running = True
        self.task = asyncio.create_task(self._produce_loop())
    
    async def stop(self) -> None:
        """Stop the producer."""
        if not self.running:
            return
        
        self.logger.info("Stopping Kafka producer")
        
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        if self.producer:
            # Flush remaining messages
            self.producer.flush(timeout=self.config.flush_timeout)
            self.producer.close()
        
        self.logger.info("Kafka producer stopped")
    
    async def send_message(
        self,
        payload: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        """Send a message asynchronously."""
        message_data = {
            "payload": payload,
            "key": key,
            "headers": headers or {},
            "timestamp": time.time(),
        }
        
        await self.message_queue.put(message_data)
    
    async def _produce_loop(self) -> None:
        """Main production loop."""
        while self.running:
            try:
                # Get message from queue with timeout
                try:
                    message_data = await asyncio.wait_for(
                        self.message_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Send message
                await self._send_message_sync(message_data)
                
            except Exception as e:
                self.logger.error("Producer loop error", error=str(e), exc_info=True)
                await self.error_handler(e)
                await asyncio.sleep(1)  # Brief pause before retry
    
    async def _send_message_sync(self, message_data: Dict[str, Any]) -> None:
        """Send message synchronously."""
        if not self.producer:
            return
        
        try:
            # Serialize payload
            payload_bytes = json.dumps(message_data["payload"]).encode('utf-8')
            key_bytes = message_data["key"].encode('utf-8') if message_data["key"] else None
            
            # Send message
            self.producer.produce(
                topic=self.config.topic,
                value=payload_bytes,
                key=key_bytes,
                headers=message_data["headers"],
                callback=self._delivery_callback
            )
            
            self.messages_sent += 1
            self.last_message_time = asyncio.get_event_loop().time()
            
        except Exception as e:
            self.logger.error(
                "Message send error",
                error=str(e),
                topic=self.config.topic
            )
            self.messages_failed += 1
            raise
    
    def _delivery_callback(self, err: Optional[KafkaError], msg: Any) -> None:
        """Delivery callback for Kafka messages."""
        if err:
            self.logger.error(
                "Message delivery failed",
                error=str(err),
                topic=msg.topic() if msg else None,
                partition=msg.partition() if msg else None,
                offset=msg.offset() if msg else None
            )
            self.messages_failed += 1
        else:
            self.logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )
    
    def _default_error_handler(self, error: Exception) -> None:
        """Default error handler."""
        self.logger.error("Producer error", error=str(error), exc_info=True)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "last_message_time": self.last_message_time,
            "queue_size": self.message_queue.qsize(),
            "running": self.running,
        }

