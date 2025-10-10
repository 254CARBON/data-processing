"""
Raw market data consumer for normalization service.

Consumes raw events from ingestion topics and processes them
through the normalization pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.framework.consumer import ConsumerConfig, KafkaConsumer
from shared.framework.metrics import MetricsCollector
from shared.framework.producer import KafkaProducer
from shared.utils.audit import AuditActorType, AuditEventType, _coerce_metadata


logger = logging.getLogger(__name__)


class RawConsumer:
    """
    Raw market data consumer.

    Consumes raw events and processes them through normalization
    pipeline with error handling and metrics collection.
    """

    def __init__(
        self,
        config,
        normalizer,
        writer,
        *,
        normalized_producer: KafkaProducer,
        dlq_producer: KafkaProducer,
        audit_logger,
        metrics: MetricsCollector,
    ) -> None:
        self.config = config
        self.normalizer = normalizer
        self.writer = writer
        self.normalized_producer = normalized_producer
        self.dlq_producer = dlq_producer
        self.audit = audit_logger
        self.metrics = metrics

        consumer_config = ConsumerConfig(
            topics=config.input_topics,
            group_id=f"normalization-{config.environment}",
            auto_offset_reset=config.kafka.auto_offset_reset,
            enable_auto_commit=config.kafka.enable_auto_commit,
            max_poll_records=config.kafka.max_poll_records,
            session_timeout_ms=config.kafka.session_timeout_ms,
            heartbeat_interval_ms=config.kafka.heartbeat_interval_ms,
        )
        self.kafka_consumer = KafkaConsumer(
            config=consumer_config,
            kafka_config=config.kafka,
            message_handler=self._process_messages,
            error_handler=self._consume_error_handler,
        )

        # Local counters for legacy metrics endpoints
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_processing_time: Optional[float] = None

        # Prometheus metrics
        self.metrics_processed = metrics.create_counter(
            "normalized_messages_total",
            "Number of normalized messages processed by status",
            labels=["status"],
        )
        self.metrics_errors = metrics.create_counter(
            "normalized_errors_total",
            "Number of normalization errors by type",
            labels=["type"],
        )
        self.metrics_emitted = metrics.create_counter(
            "normalized_events_emitted_total",
            "Number of normalized events emitted per topic",
            labels=["topic"],
        )
        self.metrics_processing = metrics.create_histogram(
            "normalized_processing_duration_seconds",
            "Time spent processing normalized messages (seconds)",
            labels=["topic"],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        )

    async def start(self) -> None:
        """Start the consumer."""
        logger.info("Starting raw consumer", topics=self.config.input_topics)
        await self.kafka_consumer.start()

    async def stop(self) -> None:
        """Stop the consumer."""
        logger.info("Stopping raw consumer")
        await self.kafka_consumer.stop()

    async def _process_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Process a batch of messages from Kafka."""
        for message in messages:
            await self._process_message(message)

    async def _process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message."""
        start_time = asyncio.get_event_loop().time()
        topic = message.get("topic") or "unknown"
        partition = message.get("partition")
        offset = message.get("offset")

        raw_payload = message.get("payload")
        payload = self._coerce_payload(raw_payload)
        tenant_id = (
            payload.get("envelope", {}).get("tenant_id")
            if isinstance(payload, dict)
            else self.config.tenant_id
        )

        try:
            logger.debug(
                "Processing normalization message",
                topic=topic,
                partition=partition,
                offset=offset,
            )

            normalized_tick = await self.normalizer.normalize(payload)
            if not normalized_tick:
                self.messages_failed += 1
                self.metrics_processed.labels(status="no_output").inc()
                self.metrics_errors.labels(type="normalizer_no_output").inc()
                self.metrics.record_message_processed(
                    topic=topic,
                    status="no_output",
                    duration=None,
                    message_type="market_tick",
                )
                logger.warning(
                    "Normalizer returned no result",
                    topic=topic,
                    partition=partition,
                    offset=offset,
                )
                return

            await self.writer.write_tick(normalized_tick)
            normalized_event = await self._emit_normalized_event(
                normalized_tick, payload, message
            )

            self.messages_processed += 1
            duration = asyncio.get_event_loop().time() - start_time
            self.last_processing_time = duration

            self.metrics_processed.labels(status="success").inc()
            self.metrics_emitted.labels(topic=self.config.output_topic).inc()
            self.metrics_processing.labels(topic=topic).observe(duration)
            self.metrics.record_message_processed(
                topic=topic,
                status="success",
                duration=duration,
                message_type="market_tick",
            )

            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="normalization_success",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-consumer",
                tenant_id=normalized_tick.tenant_id,
                resource_type="instrument",
                resource_id=normalized_tick.instrument_id,
                correlation_id=normalized_event["envelope"].get("correlation_id"),
                metadata=_coerce_metadata(
                    {
                        "topic": topic,
                        "partition": str(partition),
                        "offset": str(offset),
                    }
                ),
                details={
                    "price": normalized_tick.price,
                    "volume": normalized_tick.volume,
                },
            )

            logger.debug(
                "Message normalized successfully",
                instrument_id=normalized_tick.instrument_id,
                price=normalized_tick.price,
            )

        except Exception as exc:  # pragma: no cover - defensive logging
            self.messages_failed += 1
            duration = asyncio.get_event_loop().time() - start_time
            self.last_processing_time = duration

            error_type = exc.__class__.__name__
            self.metrics_processed.labels(status="error").inc()
            self.metrics_errors.labels(type=error_type).inc()
            self.metrics.record_error(error_type=error_type, component="normalization")

            logger.error(
                "Message processing error",
                error=str(exc),
                topic=topic,
                partition=partition,
                offset=offset,
                exc_info=True,
            )

            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="normalization_failure",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-consumer",
                tenant_id=tenant_id or "unknown",
                resource_type="instrument",
                resource_id=message.get("key"),
                metadata=_coerce_metadata(
                    {
                        "topic": topic,
                        "partition": str(partition),
                        "offset": str(offset),
                    }
                ),
                details={"error": str(exc)},
            )

            await self._send_to_dlq(message, str(exc))

    async def _emit_normalized_event(
        self,
        tick,
        raw_payload: Any,
        message: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Emit normalized tick event and return the event payload."""
        occurred_at = datetime.now(timezone.utc)
        raw_envelope = (
            raw_payload.get("envelope", {}) if isinstance(raw_payload, dict) else {}
        )
        raw_body = raw_payload.get("payload", {}) if isinstance(raw_payload, dict) else {}

        event_id = (
            tick.tick_id
            or f"norm_{tick.instrument_id}_{int(tick.timestamp.timestamp())}"
        )

        payload_metadata: Dict[str, Any] = dict(tick.metadata or {})
        if raw_body and isinstance(raw_body, dict):
            payload_metadata.setdefault("market", raw_body.get("market"))
            payload_metadata.setdefault("source_system", raw_body.get("source"))
        payload_metadata.setdefault("topic", message.get("topic"))

        normalized_payload: Dict[str, Any] = {
            "instrument_id": tick.instrument_id,
            "timestamp": tick.timestamp.isoformat(),
            "price": tick.price,
            "volume": tick.volume,
            "quality_flags": [flag.value for flag in tick.quality_flags],
            "metadata": payload_metadata,
        }

        if tick.tick_id:
            normalized_payload["tick_id"] = tick.tick_id
        if tick.source_event_id:
            normalized_payload["source_event_id"] = tick.source_event_id
        if tick.symbol:
            normalized_payload["symbol"] = tick.symbol
        if tick.market:
            normalized_payload["market"] = tick.market
        if tick.currency:
            normalized_payload["currency"] = tick.currency
        if tick.unit:
            normalized_payload["unit"] = tick.unit
        if tick.normalized_at:
            normalized_payload["normalized_at"] = tick.normalized_at.isoformat()
        else:
            normalized_payload["normalized_at"] = occurred_at.isoformat()
        if tick.source_system:
            normalized_payload["source_system"] = tick.source_system
        if tick.tags:
            normalized_payload["tags"] = list(tick.tags)
        if tick.taxonomy:
            normalized_payload["taxonomy"] = tick.taxonomy

        event = {
            "envelope": {
                "event_type": "normalized.market.ticks.v1",
                "event_id": event_id,
                "timestamp": occurred_at.isoformat(),
                "tenant_id": tick.tenant_id,
                "source": "normalization-service",
                "version": "1.0",
                "correlation_id": raw_envelope.get("correlation_id"),
                "causation_id": raw_envelope.get("event_id"),
                "metadata": {
                    "input_topic": message.get("topic"),
                    "input_partition": str(message.get("partition")),
                    "input_offset": str(message.get("offset")),
                },
            },
            "payload": normalized_payload,
        }

        await self.normalized_producer.send_message(
            payload=event,
            key=tick.instrument_id,
        )

        logger.debug(
            "Emitted normalized event",
            instrument_id=tick.instrument_id,
            event_id=event_id,
        )
        return event

    async def _send_to_dlq(self, message: Dict[str, Any], error: str) -> None:
        """Send message to dead letter queue."""
        try:
            dlq_message = {
                "original_message": message,
                "error": error,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": "normalization",
            }

            await self.dlq_producer.send_message(
                payload=dlq_message,
                key=message.get("key") or "unknown",
            )

            logger.info(
                "Message sent to DLQ",
                topic=message.get("topic"),
                partition=message.get("partition"),
                offset=message.get("offset"),
            )

            await self.audit.log_event(
                event_type=AuditEventType.DATA_PROCESSING,
                action="normalization_dlq",
                actor_type=AuditActorType.SERVICE,
                actor_id="normalization-consumer",
                tenant_id=message.get("tenant_id", "unknown"),
                resource_type="instrument",
                resource_id=message.get("key"),
                metadata=_coerce_metadata(
                    {
                        "topic": message.get("topic"),
                        "partition": str(message.get("partition")),
                        "offset": str(message.get("offset")),
                    }
                ),
                details={"error": error},
            )

        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "Failed to send message to DLQ",
                error=str(exc),
                exc_info=True,
            )
            self.metrics_errors.labels(type="dlq_error").inc()

    async def _consume_error_handler(self, error: Exception) -> None:
        """Handle consumer errors from the Kafka framework."""
        logger.error("Consumer error", error=str(error), exc_info=True)
        self.metrics_errors.labels(type="consumer_error").inc()
        self.metrics.record_error(error_type="consumer_error", component="normalization")

    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        total = self.messages_processed + self.messages_failed
        success_rate = self.messages_processed / total if total else 0.0
        return {
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "last_processing_time": self.last_processing_time,
            "success_rate": success_rate,
        }

    @staticmethod
    def _coerce_payload(payload: Any) -> Any:
        """Best-effort conversion of payload to dictionary."""
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, bytes):
            try:
                return json.loads(payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return {}
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except ValueError:
                return {}
        return payload
