"""
Audit logging utilities.

Provides facilities for recording structured audit events with
an immutable storage trail in ClickHouse. Events are queued
asynchronously to avoid blocking critical data processing paths.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import structlog

from shared.framework.config import AuditConfig, KafkaConfig
from shared.framework.producer import KafkaProducer, ProducerConfig
from shared.storage.clickhouse import ClickHouseClient, ClickHouseConfig


logger = structlog.get_logger()


class AuditEventType(str, Enum):
    """Audit event categories."""

    SYSTEM = "system"
    DATA_PROCESSING = "data_processing"
    DATA_ACCESS = "data_access"
    SECURITY = "security"
    TASK = "task"


class AuditActorType(str, Enum):
    """Types of actors that can trigger audit events."""

    SERVICE = "service"
    USER = "user"
    SYSTEM = "system"
    EXTERNAL = "external"
    SCHEDULED_TASK = "scheduled_task"


@dataclass
class AuditEvent:
    """Structured representation of an audit event."""

    event_id: str
    event_time: datetime
    service_name: str
    environment: str
    event_type: AuditEventType
    action: str
    actor_type: AuditActorType
    actor_id: str
    tenant_id: str
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    correlation_id: Optional[str] = None
    request_id: Optional[str] = None
    ip_address: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> Dict[str, Any]:
        """Convert the audit event into a ClickHouse-compatible record."""

        # Create deterministic hash to detect duplicates.
        hash_input = "|".join(
            [
                self.event_id,
                self.service_name,
                self.environment,
                self.event_type.value,
                self.action,
                self.actor_type.value,
                self.actor_id,
                self.tenant_id,
                self.resource_type or "",
                self.resource_id or "",
                self.correlation_id or "",
                self.request_id or "",
            ]
        )
        event_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

        return {
            "event_id": self.event_id,
            "event_time": self.event_time.isoformat(timespec="milliseconds"),
            "service_name": self.service_name,
            "environment": self.environment,
            "event_type": self.event_type.value,
            "action": self.action,
            "actor_type": self.actor_type.value,
            "actor_id": self.actor_id,
            "tenant_id": self.tenant_id,
            "resource_type": self.resource_type or "",
            "resource_id": self.resource_id or "",
            "correlation_id": self.correlation_id or "",
            "request_id": self.request_id or "",
            "ip_address": self.ip_address or "",
            "metadata": self.metadata,
            "details": json.dumps(self.details, default=str),
            "event_hash": event_hash,
            "ingested_at": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds"),
        }


def _coerce_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not metadata:
        return {}

    coerced: Dict[str, str] = {}
    for key, value in metadata.items():
        try:
            coerced[str(key)] = json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value)
        except Exception:  # pragma: no cover - defensive fallback
            coerced[str(key)] = str(value)
    return coerced


class NullAuditLogger:
    """No-op audit logger used when auditing is disabled."""

    enabled: bool = False

    async def startup(self) -> None:  # pragma: no cover - trivial
        return

    async def shutdown(self) -> None:  # pragma: no cover - trivial
        return

    async def log_event(self, *args: Any, **kwargs: Any) -> Optional[str]:  # pragma: no cover - trivial
        return None

    async def log_service_event(self, *args: Any, **kwargs: Any) -> Optional[str]:  # pragma: no cover - trivial
        return None


class AuditLogger:
    """Asynchronous audit logger writing to ClickHouse (and optionally Kafka)."""

    def __init__(
        self,
        service_name: str,
        environment: str,
        audit_config: AuditConfig,
        *,
        clickhouse_url: Optional[str] = None,
        clickhouse_database: str = "data_processing",
        clickhouse_client: Optional[ClickHouseClient] = None,
        kafka_config: Optional[KafkaConfig] = None,
        kafka_producer: Optional[KafkaProducer] = None,
    ) -> None:
        self.service_name = service_name
        self.environment = environment
        self.config = audit_config
        self.enabled = audit_config.enabled

        if not self.enabled:
            # Allow constructing but mark as disabled.
            self._noop = True
            return

        if not clickhouse_client:
            if not clickhouse_url:
                raise ValueError("clickhouse_url must be provided when audit logging is enabled")
            ch_config = ClickHouseConfig(
                url=clickhouse_url,
                database=clickhouse_database,
                timeout=audit_config.clickhouse_timeout,
                max_connections=audit_config.clickhouse_max_connections,
            )
            clickhouse_client = ClickHouseClient(ch_config)

        self._noop = False
        self.clickhouse_client = clickhouse_client
        self.kafka_config = kafka_config

        if kafka_producer:
            self.kafka_producer = kafka_producer
        elif audit_config.kafka_topic and kafka_config:
            producer_config = ProducerConfig(
                topic=audit_config.kafka_topic,
                batch_size=audit_config.kafka_batch_size,
                flush_timeout=audit_config.kafka_flush_timeout,
                retry_backoff_ms=audit_config.kafka_retry_backoff_ms,
                max_retries=audit_config.kafka_max_retries,
            )
            self.kafka_producer = KafkaProducer(
                config=producer_config,
                kafka_config=kafka_config,
            )
        else:
            self.kafka_producer = None

        self._queue: asyncio.Queue[AuditEvent] = asyncio.Queue(maxsize=audit_config.queue_max_size)
        self._writer_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def startup(self) -> None:
        """Initialize audit logging resources."""

        if not self.enabled or self._noop:
            return

        logger.info("Starting audit logger", service=self.service_name)

        await self.clickhouse_client.connect()

        if self.kafka_producer:
            await self.kafka_producer.start()

        self._shutdown_event.clear()
        self._writer_task = asyncio.create_task(self._writer_loop(), name=f"audit-writer-{self.service_name}")

    async def shutdown(self) -> None:
        """Flush queued events and release resources."""

        if not self.enabled or self._noop:
            return

        logger.info("Shutting down audit logger", service=self.service_name)

        self._shutdown_event.set()

        if self._writer_task:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            finally:
                self._writer_task = None

        # Flush any remaining events synchronously.
        await self._flush_pending_events()

        if self.kafka_producer:
            await self.kafka_producer.stop()

        await self.clickhouse_client.disconnect()

    async def log_event(
        self,
        *,
        event_type: AuditEventType,
        action: str,
        actor_type: AuditActorType,
        actor_id: str,
        tenant_id: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        request_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Queue a new audit event for persistence."""

        if not self.enabled or self._noop:
            return None

        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            event_time=datetime.utcnow().replace(tzinfo=timezone.utc),
            service_name=self.service_name,
            environment=self.environment,
            event_type=event_type,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            tenant_id=tenant_id,
            resource_type=resource_type,
            resource_id=resource_id,
            correlation_id=correlation_id,
            request_id=request_id,
            ip_address=ip_address,
            metadata=_coerce_metadata(metadata),
            details=details or {},
        )

        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.error(
                "Audit queue full; dropping event",
                service=self.service_name,
                action=action,
                event_type=event_type.value,
            )
            return None

        return event.event_id

    async def log_service_event(
        self,
        action: str,
        *,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Convenience method for service lifecycle events."""

        return await self.log_event(
            event_type=AuditEventType.SYSTEM,
            action=action,
            actor_type=AuditActorType.SERVICE,
            actor_id=self.service_name,
            tenant_id="system",
            metadata={"status": status, **(metadata or {})},
            details=details,
        )

    async def _writer_loop(self) -> None:
        """Background loop that flushes audit events in batches."""

        buffer: List[AuditEvent] = []

        try:
            while not self._shutdown_event.is_set():
                try:
                    event = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=self.config.flush_interval_seconds,
                    )
                    buffer.append(event)

                    # Drain additional events up to batch size without blocking.
                    while len(buffer) < self.config.batch_size:
                        try:
                            buffer.append(self._queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break

                    await self._persist_events(buffer)
                    buffer.clear()
                except asyncio.TimeoutError:
                    if buffer:
                        await self._persist_events(buffer)
                        buffer.clear()
                except asyncio.CancelledError:
                    break
        finally:
            if buffer:
                await self._persist_events(buffer)

    async def _persist_events(self, events: Iterable[AuditEvent]) -> None:
        """Persist a collection of audit events to ClickHouse (and Kafka)."""

        events_list = list(events)
        if not events_list:
            return

        records = [event.to_record() for event in events_list]

        try:
            await self.clickhouse_client.insert(self.config.clickhouse_table, records)
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Failed to persist audit events", error=str(exc), count=len(records))
            return

        if self.kafka_producer:
            for event in events_list:
                try:
                    await self.kafka_producer.send_message(
                        payload=event.to_record(),
                        key=event.resource_id or event.event_id,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging path
                    logger.error(
                        "Failed to publish audit event to Kafka",
                        error=str(exc),
                        action=event.action,
                    )

    async def _flush_pending_events(self) -> None:
        """Flush any remaining events in the queue."""

        pending: List[AuditEvent] = []
        while not self._queue.empty():
            pending.append(self._queue.get_nowait())

        if pending:
            await self._persist_events(pending)


def build_audit_logger(
    service_name: str,
    service_environment: str,
    service_audit_config: AuditConfig,
    *,
    database_config: Optional[ClickHouseConfig] = None,
    clickhouse_url: Optional[str] = None,
    kafka_config: Optional[KafkaConfig] = None,
) -> AuditLogger | NullAuditLogger:
    """Factory helper for creating an audit logger based on configuration."""

    if not service_audit_config.enabled:
        return NullAuditLogger()

    if database_config:
        clickhouse_client = ClickHouseClient(database_config)
        clickhouse_url = database_config.url
        clickhouse_database = database_config.database
    else:
        clickhouse_client = None
        clickhouse_database = "data_processing"

    return AuditLogger(
        service_name=service_name,
        environment=service_environment,
        audit_config=service_audit_config,
        clickhouse_url=clickhouse_url,
        clickhouse_database=clickhouse_database,
        clickhouse_client=clickhouse_client,
        kafka_config=kafka_config,
    )


__all__ = [
    "AuditLogger",
    "AuditEvent",
    "AuditEventType",
    "AuditActorType",
    "NullAuditLogger",
    "build_audit_logger",
]


