#!/usr/bin/env python3
"""
Reference Data Normalization Service

Consumes raw reference data from Kafka and normalizes it according to the schema.
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from confluent_kafka import Consumer, Producer
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

    def normalize_instrument(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw instrument data."""
        try:
            # Extract instrument data from raw payload
            instrument_data = json.loads(raw_data.get('raw_payload', '{}'))
            
            # Normalize the data according to schema
            normalized = {
                'instrument_id': instrument_data.get('instrument_id', ''),
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
                'effective_date': int(datetime.now(timezone.utc).timestamp() * 1000),
                'ingested_at': raw_data.get('ingested_at', int(datetime.now(timezone.utc).timestamp() * 1000)),
                'normalized_at': int(datetime.now(timezone.utc).timestamp() * 1000),
                'source_system': raw_data.get('source_system', 'refdata-producer'),
                'tenant_id': raw_data.get('tenant_id', 'default'),
                'metadata': {}
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
                span.set_attribute("instrument_id", raw_data.get('raw_payload', {}).get('instrument_id', 'unknown'))
                
                # Normalize data
                normalized = self.normalize_instrument(raw_data)
                
                # Produce normalized event
                normalized_event = {
                    'event_id': str(uuid.uuid4()),
                    'trace_id': raw_data.get('trace_id', str(uuid.uuid4())),
                    'schema_version': '1.0.0',
                    'tenant_id': normalized['tenant_id'],
                    'producer': 'refdata-normalizer@0.1.0',
                    'occurred_at': normalized['effective_date'],
                    'ingested_at': int(datetime.now(timezone.utc).timestamp() * 1000),
                    'payload': normalized
                }
                
                # Send to output topic
                self.producer.produce(
                    self.output_topic,
                    key=normalized['instrument_id'],
                    value=json.dumps(normalized_event),
                    callback=self._delivery_callback
                )
                
                # Commit offset
                self.consumer.commit(message)
                
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
