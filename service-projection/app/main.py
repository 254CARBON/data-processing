"""Main entry point for projection service."""

import asyncio

import structlog
from aiohttp import web

from shared.framework.service import AsyncService
from shared.utils.audit import AuditEventType, AuditActorType
from shared.utils.logging import setup_logging
from shared.utils.tracing import setup_tracing

from .config import ProjectionConfig
from .consumers.aggregated_consumer import AggregatedDataConsumer
from .consumers.invalidation_consumer import InvalidationConsumer
from .builders.latest_price import LatestPriceBuilder
from .builders.curve_snapshot import CurveSnapshotBuilder
from .builders.custom import CustomProjectionBuilder
from .invalidation.manager import InvalidationManager
from .invalidation.triggers import InvalidationTriggers
from .refresh.scheduler import RefreshScheduler
from .refresh.executor import RefreshExecutor
from .apis.internal import InternalAPI
from .output.cache_writer import CacheWriter
from .output.mv_writer import MaterializedViewWriter


logger = structlog.get_logger(__name__)


class ProjectionService(AsyncService):
    """Projection service for cache builders and invalidation logic."""
    
    def __init__(self):
        config = ProjectionConfig()
        super().__init__(config)
        self.config = config
        self.consumers = []
        self.builders = {}
        self.invalidation_manager = None
        self.refresh_scheduler = None
        self.refresh_executor = None
        self.internal_api = None
        self.cache_writer = None
        self.mv_writer = None
        
    async def _startup_hook(self) -> None:
        setup_logging(self.config.service_name)
        setup_tracing(self.config.service_name)
        logger.info("Starting projection service components")
        
        # Initialize components
        self.builders = {
            "latest_price": LatestPriceBuilder(self.config),
            "curve_snapshot": CurveSnapshotBuilder(self.config),
            "custom": CustomProjectionBuilder(self.config)
        }
        
        self.cache_writer = CacheWriter(self.config)
        self.mv_writer = MaterializedViewWriter(self.config)
        self.invalidation_manager = InvalidationManager(self.config, served_cache=self.cache_writer.served_cache)
        self.refresh_scheduler = RefreshScheduler(self.config)
        self.refresh_executor = RefreshExecutor(self.config)
        self.internal_api = InternalAPI(self.config)
        
        # Start components
        await self.cache_writer.start()
        await self.mv_writer.start()
        await self.invalidation_manager.start()
        await self.refresh_scheduler.start()
        await self.refresh_executor.start()
        await self.internal_api.start()
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="projection_components_start",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"components": ["cache_writer", "mv_writer", "invalidation_manager", "refresh_scheduler", "refresh_executor", "internal_api", "consumers"]}
        )
        
        # Start consumers
        aggregated_consumer = AggregatedDataConsumer(
            self.config,
            self.builders,
            self.invalidation_manager,
            self.cache_writer,
            self.mv_writer
        )
        await aggregated_consumer.start()
        self.consumers.append(aggregated_consumer)
        
        invalidation_consumer = InvalidationConsumer(
            self.config,
            self.invalidation_manager,
            self.refresh_executor
        )
        await invalidation_consumer.start()
        self.consumers.append(invalidation_consumer)
        
        logger.info("Projection service started", port=self.config.observability.health_port)
        
    async def _shutdown_hook(self) -> None:
        logger.info("Stopping projection service components")
        
        # Stop consumers
        for consumer in self.consumers:
            await consumer.stop()
            
        # Stop components
        if self.cache_writer:
            await self.cache_writer.stop()
        if self.mv_writer:
            await self.mv_writer.stop()
        if self.invalidation_manager:
            await self.invalidation_manager.stop()
        if self.refresh_scheduler:
            await self.refresh_scheduler.stop()
        if self.refresh_executor:
            await self.refresh_executor.stop()
        if self.internal_api:
            await self.internal_api.stop()
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="projection_components_stop",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"components": ["cache_writer", "mv_writer", "invalidation_manager", "refresh_scheduler", "refresh_executor", "internal_api", "consumers"]}
        )
            
        logger.info("Projection service stopped")


async def main():
    """Main entry point."""
    service = ProjectionService()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
