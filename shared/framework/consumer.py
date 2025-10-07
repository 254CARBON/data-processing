"""
Kafka consumer abstraction for async microservices.

Provides high-level Kafka consumer with automatic error handling,
metrics collection, and graceful shutdown.
"""

import asyncio
from typing import Optional, Callable, Any, Dict, List
from dataclasses import dataclass
import json

from confluent_kafka import Consumer, KafkaError, Message
import structlog

from .config import KafkaConfig


logger = structlog.get_logger()


@dataclass
class ConsumerConfig:
    """Consumer configuration."""
    topics: List[str]
    group_id: str
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000


class KafkaConsumer:
    """
    High-level Kafka consumer with async processing.
    
    Features:
    - Automatic error handling and retry logic
    - Metrics collection
    - Graceful shutdown
    - Message batching
    """
    
    def __init__(
        self,
        config: ConsumerConfig,
        kafka_config: KafkaConfig,
        message_handler: Callable[[List[Dict[str, Any]]], None],
        error_handler: Optional[Callable[[Exception], None]] = None
    ):
        self.config = config
        self.kafka_config = kafka_config
        self.message_handler = message_handler
        self.error_handler = error_handler or self._default_error_handler
        
        self.logger = structlog.get_logger("kafka-consumer")
        self.consumer: Optional[Consumer] = None
        self.running = False
        self.task: Optional[asyncio.Task] = None
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_message_time = None
    
    def _create_consumer(self) -> Consumer:
        """Create Kafka consumer instance."""
        consumer_config = {
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': self.config.enable_auto_commit,
            'max.poll.records': self.config.max_poll_records,
            'session.timeout.ms': self.config.session_timeout_ms,
            'heartbeat.interval.ms': self.config.heartbeat_interval_ms,
        }
        
        return Consumer(consumer_config)
    
    async def start(self) -> None:
        """Start the consumer."""
        if self.running:
            return
        
        self.logger.info(
            "Starting Kafka consumer",
            topics=self.config.topics,
            group_id=self.config.group_id
        )
        
        self.consumer = self._create_consumer()
        self.consumer.subscribe(self.config.topics)
        
        self.running = True
        self.task = asyncio.create_task(self._consume_loop())
    
    async def stop(self) -> None:
        """Stop the consumer."""
        if not self.running:
            return
        
        self.logger.info("Stopping Kafka consumer")
        
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            self.consumer.close()
        
        self.logger.info("Kafka consumer stopped")
    
    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        while self.running:
            try:
                # Poll for messages
                messages = self.consumer.consume(
                    num_messages=self.config.max_poll_records,
                    timeout=1.0
                )
                
                if not messages:
                    continue
                
                # Process messages
                await self._process_messages(messages)
                
            except Exception as e:
                self.logger.error("Consumer loop error", error=str(e), exc_info=True)
                await self.error_handler(e)
                await asyncio.sleep(1)  # Brief pause before retry
    
    async def _process_messages(self, messages: List[Message]) -> None:
        """Process a batch of messages."""
        processed_messages = []
        
        for message in messages:
            if message is None:
                continue
            
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition - normal condition
                    continue
                else:
                    self.logger.error(
                        "Message error",
                        error=str(message.error()),
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset()
                    )
                    self.messages_failed += 1
                    continue
            
            try:
                # Parse message
                message_data = self._parse_message(message)
                processed_messages.append(message_data)
                
            except Exception as e:
                self.logger.error(
                    "Message parsing error",
                    error=str(e),
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset()
                )
                self.messages_failed += 1
                continue
        
        # Process batch
        if processed_messages:
            try:
                await self.message_handler(processed_messages)
                self.messages_processed += len(processed_messages)
                self.last_message_time = asyncio.get_event_loop().time()
                
            except Exception as e:
                self.logger.error(
                    "Message handler error",
                    error=str(e),
                    batch_size=len(processed_messages)
                )
                self.messages_failed += len(processed_messages)
                await self.error_handler(e)
    
    def _parse_message(self, message: Message) -> Dict[str, Any]:
        """Parse Kafka message to dictionary."""
        try:
            # Try JSON parsing first
            payload = json.loads(message.value().decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Fallback to raw bytes
            payload = message.value().decode('utf-8', errors='replace')
        
        return {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "timestamp": message.timestamp(),
            "key": message.key().decode('utf-8') if message.key() else None,
            "payload": payload,
        }
    
    def _default_error_handler(self, error: Exception) -> None:
        """Default error handler."""
        self.logger.error("Consumer error", error=str(error), exc_info=True)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        return {
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "last_message_time": self.last_message_time,
            "running": self.running,
        }

