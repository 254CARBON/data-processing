"""Consumer for aggregated data events."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.framework.consumer import KafkaConsumer
from shared.utils.errors import DataProcessingError
from ..builders.latest_price import LatestPriceBuilder
from ..builders.curve_snapshot import CurveSnapshotBuilder
from ..builders.custom import CustomProjectionBuilder
from ..invalidation.manager import InvalidationManager
from ..output.cache_writer import CacheWriter
from ..output.mv_writer import MaterializedViewWriter


logger = logging.getLogger(__name__)


class AggregatedDataConsumer(KafkaConsumer):
    """Consumer for aggregated data events."""
    
    def __init__(
        self,
        config,
        builders: Dict[str, Any],
        invalidation_manager: InvalidationManager,
        cache_writer: CacheWriter,
        mv_writer: MaterializedViewWriter
    ):
        super().__init__(
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=config.kafka_group_id,
            topics=[config.aggregated_topic]
        )
        self.config = config
        self.builders = builders
        self.invalidation_manager = invalidation_manager
        self.cache_writer = cache_writer
        self.mv_writer = mv_writer
        
    async def consume(self, message: Dict[str, Any]) -> None:
        """Process aggregated data event."""
        try:
            # Determine event type
            event_type = message.get("type", "bar")
            
            if event_type == "bar":
                await self._process_bar_event(message)
            elif event_type == "curve":
                await self._process_curve_event(message)
            elif event_type == "spread":
                await self._process_spread_event(message)
            elif event_type == "basis":
                await self._process_basis_event(message)
            elif event_type == "rolling_stat":
                await self._process_rolling_stat_event(message)
            else:
                logger.warning(f"Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error processing aggregated event: {e}")
            raise DataProcessingError(f"Failed to process aggregated event: {e}")
            
    async def _process_bar_event(self, message: Dict[str, Any]):
        """Process OHLC bar event."""
        try:
            # Build latest price projection
            latest_price_projection = await self.builders["latest_price"].build_projection(message)
            if latest_price_projection:
                await self.cache_writer.write_latest_price(latest_price_projection)
                await self.mv_writer.write_latest_price(latest_price_projection)
                
            # Check for invalidation triggers
            await self.invalidation_manager.check_price_change_triggers(message)
            
        except Exception as e:
            logger.error(f"Error processing bar event: {e}")
            
    async def _process_curve_event(self, message: Dict[str, Any]):
        """Process forward curve event."""
        try:
            # Build curve snapshot projection
            curve_snapshot_projection = await self.builders["curve_snapshot"].build_projection(message)
            if curve_snapshot_projection:
                await self.cache_writer.write_curve_snapshot(curve_snapshot_projection)
                await self.mv_writer.write_curve_snapshot(curve_snapshot_projection)
                
        except Exception as e:
            logger.error(f"Error processing curve event: {e}")
            
    async def _process_spread_event(self, message: Dict[str, Any]):
        """Process spread event."""
        try:
            # Build custom projection for spreads
            spread_projection = await self.builders["custom"].build_projection(message, "spread")
            if spread_projection:
                await self.cache_writer.write_custom_projection(spread_projection)
                
        except Exception as e:
            logger.error(f"Error processing spread event: {e}")
            
    async def _process_basis_event(self, message: Dict[str, Any]):
        """Process basis event."""
        try:
            # Build custom projection for basis
            basis_projection = await self.builders["custom"].build_projection(message, "basis")
            if basis_projection:
                await self.cache_writer.write_custom_projection(basis_projection)
                
        except Exception as e:
            logger.error(f"Error processing basis event: {e}")
            
    async def _process_rolling_stat_event(self, message: Dict[str, Any]):
        """Process rolling statistics event."""
        try:
            # Build custom projection for rolling stats
            rolling_stat_projection = await self.builders["custom"].build_projection(message, "rolling_stat")
            if rolling_stat_projection:
                await self.cache_writer.write_custom_projection(rolling_stat_projection)
                
        except Exception as e:
            logger.error(f"Error processing rolling stat event: {e}")

