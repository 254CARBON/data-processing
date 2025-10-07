"""
Kafka producer for enriched events.

Produces enriched tick events to downstream consumers
with proper error handling and metrics collection.
"""

import asyncio
from typing import Dict, Any, Optional
import structlog

from shared.framework.producer import KafkaProducer, ProducerConfig
from shared.schemas.events import EnrichedTickEvent
from shared.utils.errors import KafkaError, create_error_context


logger = structlog.get_logger()


class EnrichedProducer:
    """
    Kafka producer for enriched events.
    
    Produces enriched tick events to downstream consumers
    with proper error handling and metrics collection.
    """
    
    def __init__(self, config, logger: structlog.BoundLogger):
        self.config = config
        self.logger = logger
        
        # Create Kafka producer
        producer_config = ProducerConfig(
            topic=config.output_topic,
            batch_size=100,
            flush_timeout=1.0,
            retry_backoff_ms=100,
            max_retries=3
        )
        
        self.kafka_producer = KafkaProducer(
            config=producer_config,
            kafka_config=config.kafka,
            error_handler=self._handle_error
        )
        
        # Metrics
        self.events_sent = 0
        self.events_failed = 0
        self.last_send_time = None
    
    async def start(self) -> None:
        """Start the producer."""
        self.logger.info("Starting enriched producer", topic=self.config.output_topic)
        await self.kafka_producer.start()
    
    async def stop(self) -> None:
        """Stop the producer."""
        self.logger.info("Stopping enriched producer")
        await self.kafka_producer.stop()
    
    async def send_enriched_event(self, tick_data: Dict[str, Any]) -> None:
        """
        Send enriched tick event.
        
        Args:
            tick_data: Enriched tick data dictionary
        """
        try:
            # Create enriched event
            event = EnrichedTickEvent.create(
                instrument_id=tick_data["instrument_id"],
                timestamp=tick_data["timestamp"],
                price=tick_data["price"],
                volume=tick_data["volume"],
                metadata=tick_data.get("metadata", {}),
                quality_flags=tick_data.get("quality_flags", []),
                tenant_id=tick_data.get("tenant_id", "default")
            )
            
            # Send to Kafka
            await self.kafka_producer.send_message(
                payload=event.to_dict(),
                key=tick_data["instrument_id"]
            )
            
            # Update metrics
            self.events_sent += 1
            self.last_send_time = asyncio.get_event_loop().time()
            
            self.logger.debug(
                "Enriched event sent",
                instrument_id=tick_data["instrument_id"],
                commodity=tick_data.get("metadata", {}).get("commodity"),
                region=tick_data.get("metadata", {}).get("region")
            )
            
        except Exception as e:
            self.events_failed += 1
            self.logger.error(
                "Failed to send enriched event",
                error=str(e),
                tick_data=tick_data,
                exc_info=True
            )
            raise
    
    async def _handle_error(self, error: Exception) -> None:
        """Handle producer errors."""
        self.logger.error(
            "Producer error",
            error=str(error),
            exc_info=True
        )
        
        # Increment error metrics
        self.events_failed += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            "events_sent": self.events_sent,
            "events_failed": self.events_failed,
            "last_send_time": self.last_send_time,
            "kafka_metrics": self.kafka_producer.get_metrics(),
        }

