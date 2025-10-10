"""
Normalized tick consumer for enrichment service.

Consumes normalized tick events and enriches them with
metadata and taxonomy information.
"""

import asyncio
from typing import List, Dict, Any
import structlog

from shared.framework.consumer import KafkaConsumer, ConsumerConfig
from shared.schemas.events import EventSchema, EventType
from shared.utils.errors import KafkaError, create_error_context


logger = structlog.get_logger()


class NormalizedConsumer:
    """
    Normalized tick consumer.
    
    Consumes normalized tick events and enriches them with
    metadata and taxonomy information.
    """
    
    def __init__(
        self,
        config,
        enricher,
        writer,
        logger: structlog.BoundLogger
    ):
        self.config = config
        self.enricher = enricher
        self.writer = writer
        self.logger = logger
        
        # Create Kafka consumer
        consumer_config = ConsumerConfig(
            topics=[config.input_topic],
            group_id=f"enrichment-{config.environment}",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            max_poll_records=config.max_batch_size,
            session_timeout_ms=30000
        )
        
        self.kafka_consumer = KafkaConsumer(
            config=consumer_config,
            kafka_config=config.kafka,
            message_handler=self._process_messages,
            error_handler=self._handle_error
        )
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_processing_time = None
    
    async def start(self) -> None:
        """Start the consumer."""
        self.logger.info("Starting normalized consumer", topic=self.config.input_topic)
        await self.kafka_consumer.start()
    
    async def stop(self) -> None:
        """Stop the consumer."""
        self.logger.info("Stopping normalized consumer")
        await self.kafka_consumer.stop()
    
    async def _process_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Process a batch of messages."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            self.logger.info(
                "Processing message batch",
                count=len(messages),
                topics=[msg["topic"] for msg in messages]
            )
            
            # Process each message
            for message in messages:
                try:
                    await self._process_single_message(message)
                    self.messages_processed += 1
                    
                except Exception as e:
                    self.logger.error(
                        "Message processing error",
                        error=str(e),
                        topic=message["topic"],
                        partition=message["partition"],
                        offset=message["offset"],
                        exc_info=True
                    )
                    self.messages_failed += 1
                    
                    # Send to dead letter queue
                    await self._send_to_dlq(message, str(e))
            
            # Update metrics
            processing_time = asyncio.get_event_loop().time() - start_time
            self.last_processing_time = processing_time
            
            self.logger.info(
                "Batch processing complete",
                processed=self.messages_processed,
                failed=self.messages_failed,
                duration_ms=processing_time * 1000
            )
            
        except Exception as e:
            self.logger.error("Batch processing error", error=str(e), exc_info=True)
            self.messages_failed += len(messages)
            raise
    
    async def _process_single_message(self, message: Dict[str, Any]) -> None:
        """Process a single message."""
        try:
            # Parse event
            event = EventSchema.from_dict(message["payload"])
            
            # Validate event type
            if event.envelope.event_type != EventType.NORMALIZED_TICK:
                raise ValueError(f"Unexpected event type: {event.envelope.event_type}")
            
            # Extract normalized tick data
            tick_data = event.payload
            
            # Enrich the tick
            enrichment_result = await self.enricher.enrich_tick(tick_data)
            
            if enrichment_result and enrichment_result.get("enriched"):
                enriched_tick = enrichment_result["enriched"]
                # Write to ClickHouse
                await self.writer.write_tick(enriched_tick)
                
                # Emit enriched event
                await self._emit_enriched_event(enriched_tick)
                
                self.logger.debug(
                    "Tick enriched successfully",
                    instrument_id=enriched_tick["instrument_id"],
                    metadata=enriched_tick.get("metadata", {})
                )
            else:
                self.logger.warning(
                    "Tick enrichment failed",
                    topic=message["topic"],
                    partition=message["partition"],
                    offset=message["offset"]
                )
                
        except Exception as e:
            self.logger.error(
                "Single message processing error",
                error=str(e),
                topic=message["topic"],
                partition=message["partition"],
                offset=message["offset"],
                exc_info=True
            )
            raise
    
    async def _emit_enriched_event(self, tick_data: Dict[str, Any]) -> None:
        """Emit enriched tick event."""
        try:
            # Create enriched event
            from shared.schemas.events import EnrichedTickEvent
            
            event = EnrichedTickEvent.create(
                instrument_id=tick_data["instrument_id"],
                timestamp=tick_data["timestamp"],
                price=tick_data["price"],
                volume=tick_data.get("volume"),
                metadata=tick_data.get("metadata", {}),
                quality_flags=tick_data.get("quality_flags", []),
                tenant_id=tick_data.get("tenant_id", "default"),
                tick_id=tick_data.get("tick_id"),
                source_event_id=tick_data.get("source_event_id"),
                symbol=tick_data.get("symbol"),
                market=tick_data.get("market"),
                currency=tick_data.get("currency"),
                unit=tick_data.get("unit"),
                source_system=tick_data.get("source_system"),
                source_id=tick_data.get("source_id"),
                normalized_at=tick_data.get("normalized_at"),
                taxonomy=tick_data.get("taxonomy"),
                tags=tick_data.get("tags")
            )
            
            # Send to Kafka
            await self.kafka_consumer.producers[0].send_message(
                payload=event.to_dict(),
                key=tick_data["instrument_id"]
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to emit enriched event",
                error=str(e),
                instrument_id=tick_data["instrument_id"],
                exc_info=True
            )
            raise
    
    async def _send_to_dlq(self, message: Dict[str, Any], error: str) -> None:
        """Send message to dead letter queue."""
        try:
            dlq_message = {
                "original_message": message,
                "error": error,
                "timestamp": asyncio.get_event_loop().time(),
                "service": "enrichment"
            }
            
            # Send to DLQ topic
            await self.kafka_consumer.producers[0].send_message(
                payload=dlq_message,
                key=message.get("key", "unknown")
            )
            
            self.logger.info(
                "Message sent to DLQ",
                topic=message["topic"],
                partition=message["partition"],
                offset=message["offset"],
                error=error
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to send message to DLQ",
                error=str(e),
                original_error=error,
                exc_info=True
            )
    
    async def _handle_error(self, error: Exception) -> None:
        """Handle consumer errors."""
        self.logger.error(
            "Consumer error",
            error=str(error),
            exc_info=True
        )
        
        # Increment error metrics
        self.messages_failed += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        return {
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "last_processing_time": self.last_processing_time,
            "kafka_metrics": self.kafka_consumer.get_metrics(),
        }
