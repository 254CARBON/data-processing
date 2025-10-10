"""
Configuration for the normalize-ticks service.
"""

from __future__ import annotations

import os

from shared.framework.config import ServiceConfig


class NormalizeTicksConfig(ServiceConfig):
    """Service configuration loaded from environment variables."""

    def __init__(self) -> None:
        super().__init__(service_name="normalize_ticks")

        # Human-readable slug used for logging/identifiers where hyphenated format is preferred
        self.service_slug = "normalize-ticks"

        # Topics
        self.input_topic = os.getenv(
            "NORMALIZE_TICKS_INPUT_TOPIC",
            "ingestion.market.ticks.raw.v1",
        )
        self.output_topic = os.getenv(
            "NORMALIZE_TICKS_OUTPUT_TOPIC",
            "normalized.market.ticks.v1",
        )
        self.dlq_topic = os.getenv(
            "NORMALIZE_TICKS_DLQ_TOPIC",
            "processing.deadletter.market.ticks.v1",
        )

        # Kafka consumer settings
        self.consumer_group = os.getenv(
            "NORMALIZE_TICKS_CONSUMER_GROUP",
            f"{self.service_name}-{self.environment}",
        )
        self.kafka_auto_offset_reset = os.getenv(
            "NORMALIZE_TICKS_AUTO_OFFSET_RESET",
            self.kafka.auto_offset_reset,
        )
        self.kafka_enable_auto_commit = (
            os.getenv(
                "NORMALIZE_TICKS_ENABLE_AUTO_COMMIT",
                "true" if self.kafka.enable_auto_commit else "false",
            ).lower()
            == "true"
        )

        # Idempotency cache
        self.idempotency_ttl_seconds = int(
            os.getenv("NORMALIZE_TICKS_IDEMPOTENCY_TTL_SECONDS", "900")
        )
        self.idempotency_max_entries = int(
            os.getenv("NORMALIZE_TICKS_IDEMPOTENCY_MAX_ENTRIES", "100_000")
        )

        # Optional ClickHouse backfill settings
        self.backfill_enabled = (
            os.getenv("NORMALIZE_TICKS_BACKFILL_ENABLED", "true").lower() == "true"
        )
        self.backfill_lookup_window_minutes = int(
            os.getenv("NORMALIZE_TICKS_BACKFILL_WINDOW_MINUTES", "240")
        )
        self.backfill_table = os.getenv(
            "NORMALIZE_TICKS_BACKFILL_TABLE",
            "normalized_market_ticks",
        )

        # Producer identity
        self.producer_id = os.getenv(
            "NORMALIZE_TICKS_PRODUCER_ID",
            "normalize-ticks@1.0.0",
        )

        # Tracing endpoint override
        self.otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

        # Convenience alias for bootstrap servers used throughout the service
        self.kafka_bootstrap_servers = self.kafka.bootstrap_servers
