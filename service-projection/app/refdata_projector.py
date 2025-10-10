#!/usr/bin/env python3
"""
Reference Data Projection Service

Consumes normalized reference data from Kafka and projects it to ClickHouse served tables.
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import clickhouse_connect
from confluent_kafka import Consumer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
import structlog

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

    def prepare_clickhouse_data(self, normalized_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for ClickHouse insertion."""
        try:
            # Convert timestamps and handle nullable fields
            clickhouse_data = {
                'instrument_id': normalized_data.get('instrument_id', ''),
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
                'maturity_date': normalized_data.get('maturity_date'),
                'strike_price': normalized_data.get('strike_price'),
                'option_type': normalized_data.get('option_type'),
                'effective_date': datetime.fromtimestamp(normalized_data.get('effective_date', 0) / 1000),
                'updated_at': datetime.fromtimestamp(normalized_data.get('normalized_at', 0) / 1000),
                'event_id': str(uuid.uuid4()),
                'event_version': 1
            }
            
            return clickhouse_data
            
        except Exception as e:
            self.logger.error("Failed to prepare ClickHouse data", error=str(e), data=normalized_data)
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
                clickhouse_data = self.prepare_clickhouse_data(normalized_data)
                
                # Project to ClickHouse
                await self.project_to_clickhouse(clickhouse_data)
                
                # Commit offset
                self.consumer.commit(message)
                
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
