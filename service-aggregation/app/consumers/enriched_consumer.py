"""Consumer for enriched market tick events."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.framework.consumer import KafkaConsumer
from shared.schemas.events import EnrichedMarketTick
from shared.utils.errors import DataProcessingError
from ..bars.builder import OHLCBarBuilder
from ..curves.calculator import ForwardCurveCalculator
from ..calculators.spreads import SpreadCalculator
from ..calculators.basis import BasisCalculator
from ..calculators.rolling import RollingStatisticsCalculator
from ..schedulers.batch import BatchScheduler
from ..schedulers.watermark import WatermarkManager
from ..jobs.runner import JobRunner
from ..output.producer import AggregatedDataProducer
from ..output.writer import AggregatedDataWriter


logger = logging.getLogger(__name__)


class EnrichedMarketTickConsumer(KafkaConsumer):
    """Consumer for enriched market tick events."""
    
    def __init__(
        self,
        config,
        bar_builder: OHLCBarBuilder,
        curve_calculator: ForwardCurveCalculator,
        spread_calculator: SpreadCalculator,
        basis_calculator: BasisCalculator,
        rolling_calculator: RollingStatisticsCalculator,
        batch_scheduler: BatchScheduler,
        watermark_manager: WatermarkManager,
        job_runner: JobRunner,
        producer: AggregatedDataProducer,
        writer: AggregatedDataWriter
    ):
        super().__init__(
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=config.kafka_group_id,
            topics=[config.enriched_topic]
        )
        self.config = config
        self.bar_builder = bar_builder
        self.curve_calculator = curve_calculator
        self.spread_calculator = spread_calculator
        self.basis_calculator = basis_calculator
        self.rolling_calculator = rolling_calculator
        self.batch_scheduler = batch_scheduler
        self.watermark_manager = watermark_manager
        self.job_runner = job_runner
        self.producer = producer
        self.writer = writer
        
        # Buffers for batching
        self.tick_buffer: Dict[str, List[EnrichedMarketTick]] = {}
        self.last_flush_time = asyncio.get_event_loop().time()
        
    async def consume(self, message: Dict[str, Any]) -> None:
        """Process enriched market tick event."""
        try:
            # Parse event
            tick = EnrichedMarketTick(**message)
            
            # Update watermark
            await self.watermark_manager.update_watermark(tick.ts)
            
            # Add to buffer
            instrument_key = f"{tick.instrument_id}_{tick.tenant_id}"
            if instrument_key not in self.tick_buffer:
                self.tick_buffer[instrument_key] = []
            self.tick_buffer[instrument_key].append(tick)
            
            # Check if we should flush
            current_time = asyncio.get_event_loop().time()
            if (current_time - self.last_flush_time) >= (self.config.batch_timeout_ms / 1000):
                await self._flush_buffers()
                
        except Exception as e:
            logger.error(f"Error processing enriched tick: {e}")
            raise DataProcessingError(f"Failed to process enriched tick: {e}")
            
    async def _flush_buffers(self):
        """Flush all buffered ticks for processing."""
        if not self.tick_buffer:
            return
            
        logger.info(f"Flushing {len(self.tick_buffer)} instrument buffers")
        
        # Process each instrument's ticks
        for instrument_key, ticks in self.tick_buffer.items():
            if not ticks:
                continue
                
            try:
                # Sort by timestamp
                ticks.sort(key=lambda t: t.ts)
                
                # Process ticks for bars
                await self._process_bars(instrument_key, ticks)
                
                # Process ticks for curves
                await self._process_curves(instrument_key, ticks)
                
                # Process ticks for spreads
                await self._process_spreads(instrument_key, ticks)
                
                # Process ticks for basis
                await self._process_basis(instrument_key, ticks)
                
                # Process ticks for rolling statistics
                await self._process_rolling_stats(instrument_key, ticks)
                
            except Exception as e:
                logger.error(f"Error processing instrument {instrument_key}: {e}")
                continue
                
        # Clear buffers
        self.tick_buffer.clear()
        self.last_flush_time = asyncio.get_event_loop().time()
        
    async def _process_bars(self, instrument_key: str, ticks: List[EnrichedMarketTick]):
        """Process ticks for OHLC bar generation."""
        try:
            bars = await self.bar_builder.build_bars(ticks)
            if bars:
                # Send bars to producer
                for bar in bars:
                    await self.producer.produce_bar(bar)
                    
                # Write bars to ClickHouse
                await self.writer.write_bars(bars)
                
        except Exception as e:
            logger.error(f"Error processing bars for {instrument_key}: {e}")
            
    async def _process_curves(self, instrument_key: str, ticks: List[EnrichedMarketTick]):
        """Process ticks for forward curve calculation."""
        try:
            curves = await self.curve_calculator.calculate_curves(ticks)
            if curves:
                # Send curves to producer
                for curve in curves:
                    await self.producer.produce_curve(curve)
                    
                # Write curves to ClickHouse
                await self.writer.write_curves(curves)
                
        except Exception as e:
            logger.error(f"Error processing curves for {instrument_key}: {e}")
            
    async def _process_spreads(self, instrument_key: str, ticks: List[EnrichedMarketTick]):
        """Process ticks for spread calculations."""
        try:
            spreads = await self.spread_calculator.calculate_spreads(ticks)
            if spreads:
                # Send spreads to producer
                for spread in spreads:
                    await self.producer.produce_spread(spread)
                    
                # Write spreads to ClickHouse
                await self.writer.write_spreads(spreads)
                
        except Exception as e:
            logger.error(f"Error processing spreads for {instrument_key}: {e}")
            
    async def _process_basis(self, instrument_key: str, ticks: List[EnrichedMarketTick]):
        """Process ticks for basis calculations."""
        try:
            basis = await self.basis_calculator.calculate_basis(ticks)
            if basis:
                # Send basis to producer
                for b in basis:
                    await self.producer.produce_basis(b)
                    
                # Write basis to ClickHouse
                await self.writer.write_basis(basis)
                
        except Exception as e:
            logger.error(f"Error processing basis for {instrument_key}: {e}")
            
    async def _process_rolling_stats(self, instrument_key: str, ticks: List[EnrichedMarketTick]):
        """Process ticks for rolling statistics."""
        try:
            stats = await self.rolling_calculator.calculate_stats(ticks)
            if stats:
                # Send stats to producer
                for stat in stats:
                    await self.producer.produce_rolling_stat(stat)
                    
                # Write stats to ClickHouse
                await self.writer.write_rolling_stats(stats)
                
        except Exception as e:
            logger.error(f"Error processing rolling stats for {instrument_key}: {e}")

