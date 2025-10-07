"""
Raw market data consumer for normalization service.

Consumes raw events from ingestion topics and processes them
through the normalization pipeline.
"""

import asyncio
import logging
import json
from typing import Dict, Any
from datetime import datetime

from shared.framework.consumer import KafkaConsumer
from shared.utils.errors import DataProcessingError
from shared.utils.audit import AuditActorType, AuditEventType, _coerce_metadata


logger = logging.getLogger(__name__)


class RawConsumer:
    """
    Raw market data consumer.
    
    Consumes raw events and processes them through normalization
    pipeline with error handling and metrics collection.
    """
    
    def __init__(self, config, normalizer, writer, producer, audit_logger):
        self.config = config
        self.normalizer = normalizer
        self.writer = writer
        self.producer = producer
        self.audit = audit_logger
        
        # Create Kafka consumer
        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=f"normalization-{config.environment}",
            topics=config.input_topics,
            message_handler=self._process_message
        )
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_processing_time = None
    
    async def start(self) -> None:
        """Start the consumer."""
        logger.info(f"Starting raw consumer for topics: {self.config.input_topics}")
        await self.kafka_consumer.start()
    
    async def stop(self) -> None:
        """Stop the consumer."""
        logger.info("Stopping raw consumer")
        await self.kafka_consumer.stop()
    
    async def _process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            logger.debug(f"Processing message from topic: {message.get('topic')}")
            
            # Parse the message payload
            payload = json.loads(message["value"]) if isinstance(message["value"], bytes) else message["value"]
            
            # Normalize the data
            normalized_tick = await self.normalizer.normalize(payload)
            
            if normalized_tick:
                # Write to ClickHouse
                await self.writer.write_tick(normalized_tick)
                
                # Emit normalized event
                normalized_event = await self._emit_normalized_event(normalized_tick)

                self.messages_processed += 1
                await self.audit.log_event(
                    event_type=AuditEventType.DATA_PROCESSING,
                    action="normalization_success",
                    actor_type=AuditActorType.SERVICE,
                    actor_id="normalization-consumer",
                    tenant_id=normalized_tick.tenant_id,
                    resource_type="instrument",
                    resource_id=normalized_tick.instrument_id,
                    correlation_id=normalized_event["envelope"].get("correlation_id"),
                    metadata=_coerce_metadata({
                        "topic": message.get("topic"),
                        "partition": str(message.get("partition")),
                    }),
                    details={"price": normalized_tick.price, "volume": normalized_tick.volume},
                )
                
                logger.debug(
                    f"Message normalized successfully: {normalized_tick.instrument_id} @ {normalized_tick.price}"
                )
            else:
                self.messages_failed += 1
                logger.warning(f"Message normalization failed for topic: {message.get('topic')}")
                
        except Exception as e:
            self.messages_failed += 1
            logger.error(f"Message processing error: {e}", exc_info=True)
            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="normalization_failure",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-consumer",
                tenant_id=message.get("tenant_id", "unknown"),
                resource_type="instrument",
                resource_id=message.get("key"),
                metadata=_coerce_metadata({
                    "topic": message.get("topic"),
                    "partition": str(message.get("partition")),
                }),
                details={"error": str(e)},
            )
            
            # Send to dead letter queue
            await self._send_to_dlq(message, str(e))
            
        finally:
            # Update metrics
            processing_time = asyncio.get_event_loop().time() - start_time
            self.last_processing_time = processing_time
    
    
    async def _emit_normalized_event(self, tick) -> Dict[str, Any]:
        """Emit normalized tick event and return the event payload."""
        try:
            # Create normalized event envelope
            event = {
                "envelope": {
                    "event_type": "normalized.market.tick.v1",
                    "event_id": f"norm_{tick.instrument_id}_{int(tick.timestamp.timestamp())}",
                    "timestamp": datetime.now().isoformat(),
                    "tenant_id": tick.tenant_id,
                    "source": "normalization-service",
                    "version": "1.0",
                    "correlation_id": None,
                    "causation_id": None,
                    "metadata": {}
                },
                "payload": {
                    "instrument_id": tick.instrument_id,
                    "timestamp": tick.timestamp.isoformat(),
                    "price": tick.price,
                    "volume": tick.volume,
                    "quality_flags": [flag.value for flag in tick.quality_flags],
                    "metadata": tick.metadata
                }
            }
            
            # Send to Kafka
            await self.producer.send_message(
                topic=self.config.output_topic,
                payload=event,
                key=tick.instrument_id
            )
            
            logger.debug(f"Emitted normalized event for {tick.instrument_id}")

            return event
            
        except Exception as e:
            logger.error(f"Failed to emit normalized event: {e}", exc_info=True)
            raise
    
    async def _send_to_dlq(self, message: Dict[str, Any], error: str) -> None:
        """Send message to dead letter queue."""
        try:
            dlq_message = {
                "original_message": message,
                "error": error,
                "timestamp": datetime.now().isoformat(),
                "service": "normalization"
            }
            
            # Send to DLQ topic
            await self.producer.send_message(
                topic=self.config.dlq_topic,
                payload=dlq_message,
                key=message.get("key", "unknown")
            )
            
            logger.info(f"Message sent to DLQ: {error}")
            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="normalization_dlq",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-consumer",
                tenant_id=message.get("tenant_id", "unknown"),
                resource_type="instrument",
                resource_id=message.get("key"),
                metadata=_coerce_metadata({
                    "topic": message.get("topic"),
                    "partition": str(message.get("partition")),
                }),
                details={"error": error},
            )
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}", exc_info=True)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        return {
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "last_processing_time": self.last_processing_time,
            "success_rate": self.messages_processed / (self.messages_processed + self.messages_failed) if (self.messages_processed + self.messages_failed) > 0 else 0,
        }
