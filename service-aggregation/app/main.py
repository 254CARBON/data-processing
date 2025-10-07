"""Main entry point for aggregation service."""

import asyncio
import logging
from aiohttp import web
from shared.framework.service import AsyncService
from shared.utils.logging import setup_logging
from shared.utils.tracing import setup_tracing
from shared.utils.audit import AuditEventType, AuditActorType
from .config import AggregationConfig
from .consumers.enriched_consumer import EnrichedMarketTickConsumer
from .bars.builder import OHLCBarBuilder
from .curves.calculator import ForwardCurveCalculator
from .calculators.spreads import SpreadCalculator
from .calculators.basis import BasisCalculator
from .calculators.rolling import RollingStatisticsCalculator
from .schedulers.batch import BatchScheduler
from .schedulers.watermark import WatermarkManager
from .jobs.runner import JobRunner
from .output.producer import AggregatedDataProducer
from .output.writer import AggregatedDataWriter


logger = logging.getLogger(__name__)


class AggregationService(AsyncService):
    """Aggregation service for OHLC bars and curve calculations."""
    
    def __init__(self):
        config = AggregationConfig()
        super().__init__(config)
        self.config = config
        self.consumer = None
        self.bar_builder = None
        self.curve_calculator = None
        self.spread_calculator = None
        self.basis_calculator = None
        self.rolling_calculator = None
        self.batch_scheduler = None
        self.watermark_manager = None
        self.job_runner = None
        self.producer = None
        self.writer = None
        
    async def _startup_hook(self) -> None:
        """Start the aggregation service."""
        logger.info("Starting aggregation service")
        
        # Setup logging and tracing
        setup_logging()
        setup_tracing(self.config.service_name)
        
        # Initialize components
        self.bar_builder = OHLCBarBuilder(self.config)
        self.curve_calculator = ForwardCurveCalculator(self.config)
        self.spread_calculator = SpreadCalculator(self.config)
        self.basis_calculator = BasisCalculator(self.config)
        self.rolling_calculator = RollingStatisticsCalculator(self.config)
        self.batch_scheduler = BatchScheduler(self.config)
        self.watermark_manager = WatermarkManager(self.config)
        self.job_runner = JobRunner(self.config)
        self.producer = AggregatedDataProducer(self.config)
        self.writer = AggregatedDataWriter(self.config)
        
        # Start components
        await self.producer.start()
        await self.writer.start()
        await self.batch_scheduler.start()
        await self.watermark_manager.start()
        await self.job_runner.start()
        
        # Start consumer
        self.consumer = EnrichedMarketTickConsumer(
            self.config,
            self.bar_builder,
            self.curve_calculator,
            self.spread_calculator,
            self.basis_calculator,
            self.rolling_calculator,
            self.batch_scheduler,
            self.watermark_manager,
            self.job_runner,
            self.producer,
            self.writer
        )
        await self.consumer.start()
        
        # Start HTTP server
        app = web.Application()
        app.router.add_get("/health", self.health_check)
        app.router.add_get("/metrics", self.metrics)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.config.port)
        await site.start()
        
        logger.info(f"Aggregation service started on port {self.config.port}")
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="aggregation_components_start",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"components": ["jobs_runner", "bar_builder", "curve_calculator", "writer"]}
        )
        
    async def _shutdown_hook(self) -> None:
        """Stop the aggregation service."""
        logger.info("Stopping aggregation service")
        
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        if self.writer:
            await self.writer.stop()
        if self.batch_scheduler:
            await self.batch_scheduler.stop()
        if self.watermark_manager:
            await self.watermark_manager.stop()
        if self.job_runner:
            await self.job_runner.stop()
            
        logger.info("Aggregation service stopped")
        await self.audit.log_event(
            event_type=AuditEventType.SYSTEM,
            action="aggregation_components_stop",
            actor_type=AuditActorType.SERVICE,
            actor_id=self.config.service_name,
            tenant_id="system",
            metadata={"components": ["jobs_runner", "bar_builder", "curve_calculator", "writer"]}
        )


async def main():
    """Main entry point."""
    service = AggregationService()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
