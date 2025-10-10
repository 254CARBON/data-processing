"""
Entry point for the normalize-ticks service.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from aiohttp import web
import structlog
from datetime import datetime, timezone

# Ensure shared package is importable when running as standalone module
ROOT_PATH = Path(__file__).resolve().parents[3]
SHARED_PATH = ROOT_PATH / "shared"
if str(SHARED_PATH) not in sys.path:
    sys.path.insert(0, str(SHARED_PATH))

from shared.framework.consumer import ConsumerConfig, KafkaConsumer  # noqa: E402
from shared.framework.producer import KafkaProducer, ProducerConfig  # noqa: E402
from shared.framework.service import AsyncService  # noqa: E402
from shared.utils.logging import setup_logging  # noqa: E402
from shared.utils.tracing import (  # noqa: E402
    add_span_event,
    set_span_attribute,
    setup_tracing,
    trace_async_function,
)

from .backfill import ClickHouseBackfillRepository
from .config import NormalizeTicksConfig
from .idempotency import IdempotencyGuard
from .models import datetime_to_micros
from .processor import (
    ContractViolationError,
    NormalizationResult,
    NormalizedTickProcessor,
    SchemaValidationError,
)

logger = structlog.get_logger(__name__)


def _micros_from_any(value: Any) -> int:
    """Best-effort conversion to microseconds since epoch."""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            dt = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return datetime_to_micros(dt)
    return datetime_to_micros(datetime.now(timezone.utc))


class NormalizeTicksService(AsyncService):
    """Service responsible for normalizing raw market ticks."""

    def __init__(self, config: Optional[NormalizeTicksConfig] = None) -> None:
        config = config or NormalizeTicksConfig()
        super().__init__(config)
        self.config = config

        # Core components
        self.idempotency = IdempotencyGuard(
            ttl_seconds=config.idempotency_ttl_seconds,
            max_entries=config.idempotency_max_entries,
        )
        self.backfill_repository = ClickHouseBackfillRepository(config)
        self.processor = NormalizedTickProcessor(
            config=config,
            backfill_repository=self.backfill_repository,
        )

        # Kafka producers/consumers
        self.normalized_producer = KafkaProducer(
            config=ProducerConfig(
                topic=config.output_topic,
                batch_size=500,
                flush_timeout=1.0,
                retry_backoff_ms=100,
                max_retries=5,
            ),
            kafka_config=config.kafka,
            error_handler=self._handle_producer_error,
        )
        self.dlq_producer = KafkaProducer(
            config=ProducerConfig(
                topic=config.dlq_topic,
                batch_size=200,
                flush_timeout=2.0,
                retry_backoff_ms=250,
                max_retries=5,
            ),
            kafka_config=config.kafka,
            error_handler=self._handle_producer_error,
        )
        consumer_config = ConsumerConfig(
            topics=[config.input_topic],
            group_id=config.consumer_group,
            auto_offset_reset=config.kafka_auto_offset_reset,
            enable_auto_commit=config.kafka_enable_auto_commit,
            max_poll_records=config.kafka.max_poll_records,
            session_timeout_ms=config.kafka.session_timeout_ms,
            heartbeat_interval_ms=config.kafka.heartbeat_interval_ms,
        )
        self.consumer = KafkaConsumer(
            config=consumer_config,
            kafka_config=config.kafka,
            message_handler=self._handle_messages,
            error_handler=self._handle_consumer_error,
        )

        # Register with base service for coordinated shutdown
        self.consumers.append(self.consumer)
        self.producers.extend([self.normalized_producer, self.dlq_producer])

        # Metrics
        self.metrics_processed = self.metrics.create_counter(
            "normalized_messages_total",
            "Number of normalized messages processed by status",
            labels=["status"],
        )
        self.metrics_errors = self.metrics.create_counter(
            "normalized_errors_total",
            "Number of normalization errors by type",
            labels=["type"],
        )
        self.metrics_duplicates = self.metrics.create_counter(
            "normalized_duplicates_total",
            "Number of duplicate raw events skipped",
        )
        self.metrics_lag = self.metrics.create_histogram(
            "normalized_processing_lag_ms",
            "Processing lag measured in milliseconds",
            labels=["topic"],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000],
        )
        self.metrics_idempotency_size = self.metrics.create_gauge(
            "normalized_idempotency_cache_size",
            "Current size of the idempotency cache",
        )

    async def _startup_hook(self) -> None:
        """Execute service-specific startup logic."""
        await self.backfill_repository.startup()
        await self.normalized_producer.start()
        await self.dlq_producer.start()
        await self.consumer.start()
        logger.info(
            "Normalize-ticks service started",
            input_topic=self.config.input_topic,
            output_topic=self.config.output_topic,
        )

    async def _shutdown_hook(self) -> None:
        """Execute service-specific shutdown logic."""
        await self.consumer.stop()
        await self.normalized_producer.stop()
        await self.dlq_producer.stop()
        await self.backfill_repository.shutdown()
        logger.info("Normalize-ticks service stopped")

    def _setup_service_routes(self) -> None:
        """Expose service-specific health endpoints."""
        if not self.app:
            return

        self.app.router.add_get("/status", self._status_handler)

    async def _status_handler(self, request: web.Request) -> web.Response:
        """Return basic runtime information."""
        data = {
            "service": getattr(self.config, "service_slug", self.config.service_name),
            "input_topic": self.config.input_topic,
            "output_topic": self.config.output_topic,
            "dlq_topic": self.config.dlq_topic,
            "idempotency_cache_size": self.idempotency.size(),
            "backfill_enabled": self.backfill_repository.enabled,
        }
        return web.json_response(data)

    async def _handle_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Handle a batch of messages from Kafka."""
        for message in messages:
            await self._handle_message(message)

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        """Process an individual message."""
        event_id: Optional[str] = None
        mark_idempotency = False
        try:
            payload = message.get("payload") or {}
            event_id = payload.get("event_id")

            if not event_id:
                await self._handle_schema_error(message, SchemaValidationError("event_id missing"))
                return

            if await self.idempotency.seen(event_id):
                self.metrics_duplicates.inc()
                self.metrics_processed.labels(status="duplicate").inc()
                self.metrics.record_message_processed(
                    topic=self.config.input_topic,
                    status="duplicate",
                )
                logger.debug("Duplicate event skipped", event_id=event_id)
                return

            start = time.perf_counter()
            async with trace_async_function(
                "normalize_ticks.process",
                attributes={
                    "kafka.topic": message.get("topic"),
                    "kafka.partition": message.get("partition"),
                    "kafka.offset": message.get("offset"),
                    "raw.event_id": event_id,
                },
            ):
                try:
                    normalization_result = await self.processor.normalize(payload)
                    await self._emit_normalized_event(normalization_result)
                    mark_idempotency = True

                    duration = time.perf_counter() - start
                    self.metrics_processed.labels(status="success").inc()
                    self.metrics.record_message_processed(
                        topic=self.config.input_topic,
                        status="success",
                        duration=duration,
                        message_type="market_tick",
                    )
                    self.metrics_lag.labels(topic=self.config.output_topic).observe(
                        float(normalization_result.tick.envelope.payload.processing_lag_ms)
                    )
                    set_span_attribute(
                        "normalized.tick_id",
                        normalization_result.tick.envelope.payload.tick_id,
                    )
                    add_span_event(
                        "normalize.success",
                        {
                            "instrument_id": normalization_result.tick.envelope.payload.instrument_id,
                            "fallback": normalization_result.was_fallback,
                        },
                    )

                except SchemaValidationError as exc:
                    await self._handle_schema_error(message, exc)
                    mark_idempotency = True
                except ContractViolationError as exc:
                    await self._handle_contract_violation(message, exc)
                    mark_idempotency = True
                except Exception as exc:  # pragma: no cover - defensive logging
                    await self._handle_unexpected_error(message, exc)
                    mark_idempotency = True
        finally:
            if event_id and mark_idempotency:
                # Mark after handling to avoid reprocessing the same key within TTL
                await self.idempotency.mark(event_id)
            self.metrics_idempotency_size.set(float(self.idempotency.size()))

    async def _emit_normalized_event(self, result: NormalizationResult) -> None:
        """Emit the normalized event to Kafka."""
        event = result.tick
        payload = event.envelope.payload
        message = event.to_message()

        await self.normalized_producer.send_message(
            payload=message,
            key=payload.instrument_id,
        )

    async def _handle_schema_error(
        self,
        message: Dict[str, Any],
        error: SchemaValidationError,
    ) -> None:
        """Handle schema validation errors."""
        self.metrics_errors.labels(type="schema").inc()
        self.metrics_processed.labels(status="schema_error").inc()
        self.metrics.record_error(error_type="schema_error", component="normalize-ticks")
        logger.warning(
            "Schema validation error",
            error=str(error),
            topic=message.get("topic"),
            offset=message.get("offset"),
        )
        await self._emit_dead_letter(
            raw_message=message,
            error_code="VALIDATION_ERROR",
            error_message=str(error),
            issues=["schema_validation_failed"],
        )

    async def _handle_contract_violation(
        self,
        message: Dict[str, Any],
        error: ContractViolationError,
    ) -> None:
        """Handle contract violation errors and route to DLQ."""
        self.metrics_errors.labels(type="contract").inc()
        self.metrics_processed.labels(status="contract_error").inc()
        self.metrics.record_error(error_type="contract_error", component="normalize-ticks")
        logger.warning(
            "Contract violation",
            error=str(error),
            issues=getattr(error, "issues", []),
            topic=message.get("topic"),
            offset=message.get("offset"),
        )
        await self._emit_dead_letter(
            raw_message=message,
            error_code="CONTRACT_VIOLATION",
            error_message=str(error),
            issues=getattr(error, "issues", []),
        )

    async def _handle_unexpected_error(
        self,
        message: Dict[str, Any],
        error: Exception,
    ) -> None:
        """Handle unexpected processing errors."""
        self.metrics_errors.labels(type="unexpected").inc()
        self.metrics_processed.labels(status="unexpected_error").inc()
        self.metrics.record_error(error_type="unexpected_error", component="normalize-ticks")
        logger.error(
            "Unexpected normalization error",
            error=str(error),
            topic=message.get("topic"),
            offset=message.get("offset"),
            exc_info=True,
        )
        await self._emit_dead_letter(
            raw_message=message,
            error_code="UNEXPECTED_ERROR",
            error_message=str(error),
            issues=["unexpected_error"],
        )

    async def _emit_dead_letter(
        self,
        raw_message: Dict[str, Any],
        *,
        error_code: str,
        error_message: str,
        issues: Optional[List[str]],
    ) -> None:
        """Emit failed events to the DLQ."""
        payload = raw_message.get("payload") or {}
        occurred_at_micros = _micros_from_any(payload.get("occurred_at"))

        metadata = {
            "topic": raw_message.get("topic"),
            "partition": str(raw_message.get("partition")),
            "offset": str(raw_message.get("offset")),
        }
        if issues:
            metadata["issues"] = ",".join(issues)

        dead_letter = {
            "event_id": str(uuid4()),
            "trace_id": payload.get("trace_id", ""),
            "schema_version": "1.0.0",
            "tenant_id": payload.get("tenant_id", "default"),
            "producer": self.config.producer_id,
            "occurred_at": occurred_at_micros,
            "ingested_at": datetime_to_micros(datetime.now(timezone.utc)),
            "payload": {
                "failed_event_id": payload.get("event_id", ""),
                "failed_topic": raw_message.get("topic"),
                "failure_stage": "NORMALIZATION",
                "error_code": error_code,
                "error_message": error_message,
                "retry_count": int(payload.get("payload", {}).get("retry_count", 0))
                if isinstance(payload.get("payload"), dict)
                else 0,
                "last_retry_at": None,
                "raw_event": json.dumps(payload, default=str),
                "normalized_event": None,
                "metadata": metadata,
            },
        }

        await self.dlq_producer.send_message(
            payload=dead_letter,
            key=payload.get("event_id", ""),
        )

    async def _handle_producer_error(self, error: Exception) -> None:
        """Log producer errors."""
        logger.error("Producer error", error=str(error), exc_info=True)
        self.metrics.record_error(error_type="producer_error", component="normalize-ticks")

    async def _handle_consumer_error(self, error: Exception) -> None:
        """Log consumer errors."""
        logger.error("Consumer error", error=str(error), exc_info=True)
        self.metrics.record_error(error_type="consumer_error", component="normalize-ticks")


async def main() -> None:
    """Service entrypoint."""
    setup_logging("normalize-ticks")
    config = NormalizeTicksConfig()
    setup_tracing("normalize-ticks", endpoint=config.otel_endpoint)

    service = NormalizeTicksService(config=config)
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
