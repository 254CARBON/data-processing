#!/usr/bin/env python3
"""
Reference Data Projection Service

Consumes normalized reference data from Kafka and projects it to ClickHouse served tables.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional

import clickhouse_connect
from confluent_kafka import Consumer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
import structlog

from shared.utils.identifiers import (
    REFDATA_PROJECTION_NAMESPACE,
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


class RefDataProjector:
    """Reference data projection service."""

    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092", 
                 clickhouse_host: str = "localhost", clickhouse_port: int = 8123):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = "normalized.instruments.updated.v1"
        
        # Kafka consumer configuration
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'refdata-projector',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
        })
        
        # ClickHouse client
        self.clickhouse_client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            database='data_processing'
        )
        
        self.logger = structlog.get_logger()

    def _to_datetime(self, value: Any) -> Optional[datetime]:
        """Convert epoch millis or ISO strings to timezone-aware datetime."""
        if value is None:
            return None

        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        if isinstance(value, (int, float)):
            # Treat very large numbers as milliseconds.
            if value > 10**12:
                seconds = value / 1000.0
            else:
                seconds = float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)

        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None
            candidate = candidate.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                try:
                    numeric = int(candidate)
                except ValueError:
                    return None
                return self._to_datetime(numeric)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed

        return None

    def _to_date(self, value: Any) -> Optional[date]:
        """Convert supported inputs to a date object."""
        dt_value = self._to_datetime(value)
        return dt_value.date() if dt_value else None

    def prepare_clickhouse_data(self, normalized_event: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for ClickHouse insertion."""
        try:
            normalized_data = normalized_event.get('payload', {})
            instrument_id = normalized_data.get('instrument_id', '')
            tenant_id = normalized_event.get('tenant_id', 'default')
            effective_date = self._to_datetime(normalized_data.get('effective_date'))
            updated_at = self._to_datetime(normalized_data.get('normalized_at'))

            event_id = normalized_event.get('event_id') or deterministic_uuid(
                REFDATA_PROJECTION_NAMESPACE,
                tenant_id,
                instrument_id,
                normalized_data.get('ingested_at'),
            )
            event_version = (
                normalized_data.get('metadata', {}).get('event_version')
                or normalized_event.get('schema_version')
                or 1
            )

            # Convert timestamps and handle nullable fields
            clickhouse_data = {
                'instrument_id': instrument_id,
                'symbol': normalized_data.get('symbol', ''),
                'name': normalized_data.get('name', ''),
                'exchange': normalized_data.get('exchange', ''),
                'asset_class': normalized_data.get('asset_class', 'equity'),
                'currency': normalized_data.get('currency', 'USD'),
                'is_active': 1 if normalized_data.get('is_active', True) else 0,
                'sector': normalized_data.get('sector'),
                'country': normalized_data.get('country'),
                'underlying_asset': normalized_data.get('underlying_asset'),
                'contract_size': normalized_data.get('contract_size'),
                'tick_size': normalized_data.get('tick_size'),
                'maturity_date': self._to_date(normalized_data.get('maturity_date')),
                'strike_price': normalized_data.get('strike_price'),
                'option_type': normalized_data.get('option_type'),
                'effective_date': effective_date or datetime.now(timezone.utc),
                'updated_at': updated_at or datetime.now(timezone.utc),
                'event_id': event_id,
                'event_version': int(event_version) if str(event_version).isdigit() else 1
            }
            
            return clickhouse_data
            
        except Exception as e:
            self.logger.error("Failed to prepare ClickHouse data", error=str(e), data=normalized_event)
            raise

    async def project_to_clickhouse(self, clickhouse_data: Dict[str, Any]) -> None:
        """Project data to ClickHouse served table."""
        try:
            # Insert into served.reference_instruments table
            self.clickhouse_client.insert(
                'served.reference_instruments',
                [clickhouse_data],
                column_names=list(clickhouse_data.keys())
            )
            
            self.logger.info(
                "Instrument projected to ClickHouse successfully",
                instrument_id=clickhouse_data['instrument_id'],
                symbol=clickhouse_data['symbol']
            )
            
        except Exception as e:
            self.logger.error("Failed to project to ClickHouse", error=str(e), data=clickhouse_data)
            raise

    async def process_message(self, message) -> None:
        """Process a single Kafka message."""
        with tracer.start_as_current_span("project_instrument") as span:
            try:
                # Parse message
                normalized_event = json.loads(message.value().decode('utf-8'))
                normalized_data = normalized_event.get('payload', {})

                span.set_attribute("instrument_id", normalized_data.get('instrument_id', 'unknown'))
                
                # Prepare data for ClickHouse
                clickhouse_data = self.prepare_clickhouse_data(normalized_event)
                
                # Project to ClickHouse
                await self.project_to_clickhouse(clickhouse_data)
                
                # Commit offset
                self.consumer.commit(message=message, asynchronous=False)
                
                self.logger.info(
                    "Instrument projected successfully",
                    instrument_id=normalized_data.get('instrument_id'),
                    symbol=normalized_data.get('symbol')
                )
                
            except Exception as e:
                self.logger.error("Failed to process message", error=str(e))
                # TODO: Send to dead letter queue
                raise

    async def run(self) -> None:
        """Main run method."""
        self.logger.info("Starting reference data projector...")
        
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
            self.logger.info("Shutting down projector...")
        except Exception as e:
            self.logger.error("Error in projector", error=str(e))
            raise
        finally:
            self.consumer.close()
            self.clickhouse_client.close()


async def main() -> None:
    """Main entry point."""
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    
    projector = RefDataProjector(kafka_bootstrap, clickhouse_host, clickhouse_port)
    await projector.run()


if __name__ == "__main__":
    asyncio.run(main())
