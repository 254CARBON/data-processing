"""
Main entry point for enrichment service.
"""

import asyncio
import sys
from pathlib import Path

# Add shared directory to path
shared_path = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from shared.framework.service import AsyncService
from shared.utils.audit import AuditEventType, AuditActorType
from shared.utils.logging import setup_logging
from shared.utils.tracing import setup_tracing

from .config import EnrichmentConfig
from .consumers.normalized_consumer import NormalizedConsumer
from .lookup.metadata import MetadataLookup
from .taxonomy.classifier import TaxonomyClassifier
from .processors.enricher import Enricher
from .output.producer import EnrichedProducer
from .output.writer import ClickHouseWriter


class EnrichmentService(AsyncService):
    """Enrichment service implementation."""
    
    def __init__(self):
        config = EnrichmentConfig()
        super().__init__(config)
        self.config = config
        
        # Service components
        self.metadata_lookup = MetadataLookup(self.config)
        self.taxonomy_classifier = TaxonomyClassifier(self.config)
        self.enricher = Enricher(self.config)
        self.writer = ClickHouseWriter(self.config)
        
        # Kafka components
        self.normalized_consumer = NormalizedConsumer(
            config=self.config,
            enricher=self.enricher,
            writer=self.writer,
            logger=self.logger
        )
        
        self.enriched_producer = EnrichedProducer(
            config=self.config,
            logger=self.logger
        )
        
        # Add to service
        self.add_consumer(self.normalized_consumer)
        self.add_producer(self.enriched_producer)
    
    async def _startup_hook(self) -> None:
        """Service-specific startup logic."""
        self.logger.info("Starting enrichment service components")
        
        # Initialize components
        await self.metadata_lookup.startup()
        await self.taxonomy_classifier.startup()
        await self.enricher.startup()
        await self.writer.startup()
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="enrichment_components_start",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"components": ["metadata_lookup", "taxonomy_classifier", "enricher", "writer"]}
        )
        
        # Start Kafka components
        await self.normalized_consumer.start()
        await self.enriched_producer.start()
        
        self.logger.info("Enrichment service startup complete")
    
    async def _shutdown_hook(self) -> None:
        """Service-specific shutdown logic."""
        self.logger.info("Shutting down enrichment service components")
        
        # Stop Kafka components
        await self.normalized_consumer.stop()
        await self.enriched_producer.stop()
        
        # Shutdown components
        await self.writer.shutdown()
        await self.enricher.shutdown()
        await self.taxonomy_classifier.shutdown()
        await self.metadata_lookup.shutdown()
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="enrichment_components_stop",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"components": ["metadata_lookup", "taxonomy_classifier", "enricher", "writer"]}
        )
        
        self.logger.info("Enrichment service shutdown complete")
    
    def _setup_service_routes(self) -> None:
        """Setup service-specific HTTP routes."""
        if not self.app:
            return
        
        # Add enrichment-specific endpoints
        self.app.router.add_get("/status", self._status_handler)
        self.app.router.add_post("/enrich", self._enrich_handler)
        self.app.router.add_get("/taxonomy", self._taxonomy_handler)
        self.app.router.add_get("/metadata", self._metadata_handler)
    
    async def _status_handler(self, request) -> web.Response:
        """Service status handler."""
        status = {
            "service": "enrichment",
            "status": "running",
            "config": {
                "cache_enabled": self.config.cache_enabled,
                "max_batch_size": self.config.max_batch_size,
                "input_topic": self.config.input_topic,
                "output_topic": self.config.output_topic,
            },
            "components": {
                "metadata_lookup": "running",
                "taxonomy_classifier": "running",
                "enricher": "running",
                "writer": "running",
            }
        }
        
        return web.json_response(status)
    
    async def _enrich_handler(self, request) -> web.Response:
        """Enrichment test handler."""
        try:
            data = await request.json()
            result = await self.enricher.enrich_tick(data)
            
            return web.json_response({
                "enriched": result["enriched"],
                "metadata": result["metadata"],
                "taxonomy": result["taxonomy"]
            })
        except Exception as e:
            return web.json_response({
                "error": str(e)
            }, status=400)
    
    async def _taxonomy_handler(self, request) -> web.Response:
        """Taxonomy status handler."""
        taxonomy_status = await self.taxonomy_classifier.get_status()
        return web.json_response(taxonomy_status)
    
    async def _metadata_handler(self, request) -> web.Response:
        """Metadata status handler."""
        metadata_status = await self.metadata_lookup.get_status()
        return web.json_response(metadata_status)


async def main():
    """Main entry point."""
    # Setup logging
    setup_logging("enrichment-service")
    
    # Setup tracing
    setup_tracing("enrichment-service")
    
    # Create and run service
    service = EnrichmentService()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
