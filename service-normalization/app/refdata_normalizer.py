#!/usr/bin/env python3
"""
Reference Data Normalization Service

Consumes raw reference data from Kafka and normalizes it according to the schema.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, Producer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
import structlog

from shared.utils.identifiers import (
    REFDATA_EVENT_NAMESPACE,
    REFDATA_INSTRUMENT_NAMESPACE,
    deterministic_uuid,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure structlog
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

# Configure OpenTelemetry
if os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
    trace.set_tracer_provider(TracerProvider())
    otlp_exporter = OTLPSpanExporter(endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'))
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

# Instrument Kafka
KafkaInstrumentor().instrument()

tracer = trace.get_tracer(__name__)


class RefDataNormalizer:
    """Reference data normalization service."""

    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = "ingestion.instruments.raw.v1"
        self.output_topic = "normalized.instruments.updated.v1"
        
        # Kafka consumer configuration
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'refdata-normalizer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
        })
        
        # Kafka producer configuration
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
        })

        self.logger = structlog.get_logger()

    def _delivery_callback(self, err, msg):
        """Delivery callback for Kafka messages."""
        if err:
            self.logger.error("Message delivery failed", error=str(err))
        else:
            self.logger.debug("Message delivered", topic=msg.topic(), partition=msg.partition())

    def _sanitize_slug(self, *values: Any) -> Optional[str]:
        """Build a deterministic slug identifier from provided components."""
        parts = []
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            parts.append(text.upper().replace(" ", "-"))
        if not parts:
            return None
        return "-".join(parts)

    def _epoch_millis(self, value: Any, fallback_ms: Optional[int] = None) -> int:
        """Normalize various timestamp representations to epoch milliseconds."""
        if value is None:
            return fallback_ms if fallback_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)

        if isinstance(value, (int, float)):
            # Treat very large values as already in milliseconds.
            if value > 10**12:
                return int(value)
            # Otherwise assume seconds.
            return int(value * 1000)

        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return fallback_ms if fallback_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
            normalized = normalized.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                try:
                    as_int = int(normalized)
                    return self._epoch_millis(as_int, fallback_ms=fallback_ms)
                except ValueError:
                    return fallback_ms if fallback_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return int(parsed.timestamp() * 1000)

        return fallback_ms if fallback_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)

    def _derive_instrument_id(self, raw_data: Dict[str, Any], instrument_data: Dict[str, Any]) -> str:
        """Return a stable instrument identifier for the normalized payload."""
        existing_id = instrument_data.get('instrument_id')
        if isinstance(existing_id, str) and existing_id.strip():
            return existing_id.strip()

        tenant_id = raw_data.get('tenant_id', 'default')
        symbol = instrument_data.get('symbol')
        exchange = instrument_data.get('exchange')
        asset_class = instrument_data.get('asset_class')

        slug = self._sanitize_slug(symbol, exchange, asset_class)
        if slug:
            return slug

        raw_payload = raw_data.get('raw_payload')
        return deterministic_uuid(
            REFDATA_INSTRUMENT_NAMESPACE,
            tenant_id,
            raw_payload if isinstance(raw_payload, str) else json.dumps(raw_payload or {}),
        )

    def normalize_instrument(self, raw_data: Dict[str, Any], instrument_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw instrument data."""
        try:
            instrument_id = self._derive_instrument_id(raw_data, instrument_data)
            tenant_id = raw_data.get('tenant_id', 'default')
            ingested_at = self._epoch_millis(raw_data.get('ingested_at'))
            effective_date = self._epoch_millis(
                instrument_data.get('effective_date'),
                fallback_ms=ingested_at,
            )
            normalized_at = int(datetime.now(timezone.utc).timestamp() * 1000)

            normalized = {
                'instrument_id': instrument_id,
                'symbol': instrument_data.get('symbol', ''),
                'name': instrument_data.get('name', ''),
                'exchange': instrument_data.get('exchange', ''),
                'asset_class': instrument_data.get('asset_class', 'equity'),
                'currency': instrument_data.get('currency', 'USD'),
                'is_active': bool(instrument_data.get('is_active', True)),
                'sector': instrument_data.get('sector'),
                'country': instrument_data.get('country'),
                'underlying_asset': instrument_data.get('underlying_asset'),
                'contract_size': instrument_data.get('contract_size'),
                'tick_size': instrument_data.get('tick_size'),
                'maturity_date': instrument_data.get('maturity_date'),
                'strike_price': instrument_data.get('strike_price'),
                'option_type': instrument_data.get('option_type'),
                'effective_date': effective_date,
                'ingested_at': ingested_at,
                'normalized_at': normalized_at,
                'source_system': raw_data.get('source_system', 'refdata-producer'),
                'tenant_id': tenant_id,
                'metadata': {
                    'source_event_id': raw_data.get('event_id', ''),
                }
            }
            
            return normalized
            
        except Exception as e:
            self.logger.error("Failed to normalize instrument", error=str(e), raw_data=raw_data)
            raise

    async def process_message(self, message) -> None:
        """Process a single Kafka message."""
        with tracer.start_as_current_span("normalize_instrument") as span:
            try:
                # Parse message
                raw_data = json.loads(message.value().decode('utf-8'))
                raw_payload = raw_data.get('raw_payload', '{}')
                instrument_data = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
                
                # Normalize data
                normalized = self.normalize_instrument(raw_data, instrument_data)
                span.set_attribute("instrument_id", normalized.get('instrument_id', 'unknown'))
                
                # Produce normalized event
                raw_event_id = raw_data.get('event_id')
                normalized_event_id = deterministic_uuid(
                    REFDATA_EVENT_NAMESPACE,
                    normalized['tenant_id'],
                    normalized['instrument_id'],
                    raw_event_id or normalized['ingested_at'],
                )
                trace_id = raw_data.get('trace_id') or deterministic_uuid(
                    REFDATA_EVENT_NAMESPACE,
                    normalized_event_id,
                    "trace",
                )

                normalized_event = {
                    'event_id': normalized_event_id,
                    'trace_id': trace_id,
                    'schema_version': '1.0.0',
                    'tenant_id': normalized['tenant_id'],
                    'producer': 'refdata-normalizer@0.1.0',
                    'occurred_at': normalized['effective_date'],
                    'ingested_at': normalized['ingested_at'],
                    'payload': normalized
                }
                
                # Send to output topic
                self.producer.produce(
                    self.output_topic,
                    key=normalized['instrument_id'],
                    value=json.dumps(normalized_event),
                    callback=self._delivery_callback
                )

                # Ensure message delivery before committing offsets for idempotency
                self.producer.flush(timeout=1.0)

                # Commit offset synchronously
                self.consumer.commit(message=message, asynchronous=False)
                
                self.logger.info(
                    "Instrument normalized successfully",
                    instrument_id=normalized['instrument_id'],
                    symbol=normalized['symbol']
                )
                
            except Exception as e:
                self.logger.error("Failed to process message", error=str(e))
                # TODO: Send to dead letter queue
                raise

    async def run(self) -> None:
        """Main run method."""
        self.logger.info("Starting reference data normalizer...")
        
        try:
            # Subscribe to input topic
            self.consumer.subscribe([self.input_topic])
            
            while True:
                # Poll for messages
                message = self.consumer.poll(timeout=1.0)
                
                if message is None:
                    continue
                    
                if message.error():
                    self.logger.error("Consumer error", error=str(message.error()))
                    continue
                
                # Process message
                await self.process_message(message)
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down normalizer...")
        except Exception as e:
            self.logger.error("Error in normalizer", error=str(e))
            raise
        finally:
            self.consumer.close()
            self.producer.flush()


async def main() -> None:
    """Main entry point."""
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    normalizer = RefDataNormalizer(kafka_bootstrap)
    await normalizer.run()


if __name__ == "__main__":
    asyncio.run(main())
