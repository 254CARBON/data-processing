"""
Main entry point for normalization service.
"""

import asyncio
import sys
from pathlib import Path
from aiohttp import web
import json

# Add shared directory to path
shared_path = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from shared.framework.service import AsyncService
from shared.framework.producer import KafkaProducer
from shared.utils.audit import AuditActorType, AuditEventType
from shared.utils.logging import setup_logging
from shared.utils.tracing import setup_tracing

from .config import NormalizationConfig
from .consumers.raw_consumer import RawConsumer
from .processors.normalizer import Normalizer
from .validation.rules import ValidationRules
from .output.producer import NormalizedProducer
from .output.writer import ClickHouseWriter


class NormalizationService(AsyncService):
    """Normalization service implementation."""
    
    def __init__(self):
        config = NormalizationConfig()
        super().__init__(config)
        self.config = config
        
        # Service components
        self.normalizer = Normalizer(self.config)
        self.validation_rules = ValidationRules(self.config)
        self.writer = ClickHouseWriter(self.config, self.audit)
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            client_id="normalization-service"
        )
        
        # Kafka consumer
        self.raw_consumer = RawConsumer(
            config=self.config,
            normalizer=self.normalizer,
            writer=self.writer,
            producer=self.producer,
            audit_logger=self.audit,
        )
    
    async def _startup_hook(self) -> None:
        """Service-specific startup logic."""
        self.logger.info("Starting normalization service components")
        
        # Initialize components
        await self.normalizer.startup()
        await self.validation_rules.startup()
        await self.writer.startup()
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="normalizer_startup",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"component": "normalizer"}
        )
        
        # Start Kafka components
        await self.producer.start()
        await self.raw_consumer.start()
        
        self.logger.info("Normalization service startup complete")
    
    async def _shutdown_hook(self) -> None:
        """Service-specific shutdown logic."""
        self.logger.info("Shutting down normalization service components")
        
        # Stop Kafka components
        await self.raw_consumer.stop()
        await self.producer.stop()
        
        # Shutdown components
        await self.writer.shutdown()
        await self.validation_rules.shutdown()
        await self.normalizer.shutdown()
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="normalizer_shutdown",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"component": "normalizer"}
        )
        
        self.logger.info("Normalization service shutdown complete")
    
    def _setup_service_routes(self) -> None:
        """Setup service-specific HTTP routes."""
        if not self.app:
            return
        
        # Add normalization-specific endpoints
        self.app.router.add_get("/status", self._status_handler)
        self.app.router.add_post("/validate", self._validate_handler)
        self.app.router.add_get("/metrics/processing", self._processing_metrics_handler)
    
    async def _status_handler(self, request) -> web.Response:
        """Service status handler."""
        status = {
            "service": "normalization",
            "status": "running",
            "config": {
                "validation_enabled": self.config.validation_enabled,
                "max_batch_size": self.config.max_batch_size,
                "input_topics": self.config.input_topics,
                "output_topic": self.config.output_topic,
            },
            "components": {
                "normalizer": "running",
                "validation_rules": "running",
                "writer": "running",
            }
        }
        
        return web.json_response(status)
    
    async def _validate_handler(self, request) -> web.Response:
        """Validation test handler."""
        try:
            data = await request.json()
            result = await self.validation_rules.validate_tick(data)
            
            return web.json_response({
                "valid": result["valid"],
                "quality_flags": result["quality_flags"],
                "errors": result["errors"]
            })
        except Exception as e:
            return web.json_response({
                "error": str(e)
            }, status=400)
    
    async def _processing_metrics_handler(self, request) -> web.Response:
        """Processing metrics handler."""
        metrics = {
            "processed_ticks": self.normalizer.get_processed_count(),
            "failed_ticks": self.normalizer.get_failed_count(),
            "validation_errors": self.validation_rules.get_error_count(),
            "write_operations": self.writer.get_write_count(),
        }
        
        return web.json_response(metrics)


async def main():
    """Main entry point."""
    # Setup logging
    setup_logging("normalization-service")
    
    # Setup tracing
    setup_tracing("normalization-service")
    
    # Create and run service
    service = NormalizationService()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
