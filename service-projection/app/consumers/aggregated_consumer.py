"""Consumer for aggregated data events."""

from dataclasses import replace
from typing import Dict, Any, List

import structlog

from shared.framework.consumer import KafkaConsumer, ConsumerConfig
from shared.framework.config import KafkaConfig
from shared.utils.errors import DataProcessingError

from ..builders.latest_price import LatestPriceBuilder
from ..builders.curve_snapshot import CurveSnapshotBuilder
from ..builders.custom import CustomProjectionBuilder
from ..invalidation.manager import InvalidationManager
from ..output.cache_writer import CacheWriter
from ..output.mv_writer import MaterializedViewWriter


logger = structlog.get_logger(__name__)


class AggregatedDataConsumer(KafkaConsumer):
    """Consumer for aggregated data events."""

    def __init__(
        self,
        config,
        builders: Dict[str, Any],
        invalidation_manager: InvalidationManager,
        cache_writer: CacheWriter,
        mv_writer: MaterializedViewWriter,
    ):
        self.config = config
        self.builders = builders
        self.invalidation_manager = invalidation_manager
        self.cache_writer = cache_writer
        self.mv_writer = mv_writer

        kafka_config: KafkaConfig = replace(
            config.kafka,
            bootstrap_servers=config.kafka_bootstrap_servers,
            consumer_group=config.kafka_group_id,
        )

        consumer_config = ConsumerConfig(
            topics=[config.aggregated_topic],
            group_id=f"{config.kafka_group_id}-aggregated",
            auto_offset_reset=kafka_config.auto_offset_reset,
            enable_auto_commit=kafka_config.enable_auto_commit,
            max_poll_records=kafka_config.max_poll_records,
            session_timeout_ms=kafka_config.session_timeout_ms,
            heartbeat_interval_ms=getattr(kafka_config, "heartbeat_interval_ms", 3000),
        )

        super().__init__(
            config=consumer_config,
            kafka_config=kafka_config,
            message_handler=self._handle_messages,
            error_handler=self._handle_error,
        )

    async def _handle_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Handle a batch of Kafka messages."""
        for message in messages:
            payload = message.get("payload") if isinstance(message, dict) else None
            event = payload if isinstance(payload, dict) else message
            logger.debug("Received aggregated message", raw=message)
            await self.consume(event)

    async def _handle_error(self, exc: Exception) -> None:
        """Default error handler for consumer loop."""
        logger.error("Aggregated consumer error", error=str(exc))

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
                logger.warning("Unknown event type", event_type=event_type)

        except Exception as e:
            logger.error("Error processing aggregated event", error=str(e))
            raise DataProcessingError(f"Failed to process aggregated event: {e}")

    async def _process_bar_event(self, message: Dict[str, Any]):
        """Process OHLC bar event."""
        try:
            # Build latest price projection
            latest_price_projection = await self.builders["latest_price"].build_projection(message)
            if latest_price_projection:
                await self.cache_writer.write_latest_price(latest_price_projection)
                await self.mv_writer.write_latest_price(latest_price_projection)
                logger.info(
                    "Processed bar projection",
                    instrument_id=latest_price_projection.instrument_id,
                    tenant_id=latest_price_projection.tenant_id,
                )

            # Check for invalidation triggers
            await self.invalidation_manager.check_price_change_triggers(message)

        except Exception as e:
            logger.error("Error processing bar event", error=str(e))
            
    async def _process_curve_event(self, message: Dict[str, Any]):
        """Process forward curve event."""
        try:
            # Build curve snapshot projection
            curve_snapshot_projection = await self.builders["curve_snapshot"].build_projection(message)
            if curve_snapshot_projection:
                await self.cache_writer.write_curve_snapshot(curve_snapshot_projection)
                await self.mv_writer.write_curve_snapshot(curve_snapshot_projection)
                
        except Exception as e:
            logger.error("Error processing curve event", error=str(e))
            
    async def _process_spread_event(self, message: Dict[str, Any]):
        """Process spread event."""
        try:
            # Build custom projection for spreads
            spread_projection = await self.builders["custom"].build_projection(message, "spread")
            if spread_projection:
                await self.cache_writer.write_custom_projection(spread_projection)
                
        except Exception as e:
            logger.error("Error processing spread event", error=str(e))
            
    async def _process_basis_event(self, message: Dict[str, Any]):
        """Process basis event."""
        try:
            # Build custom projection for basis
            basis_projection = await self.builders["custom"].build_projection(message, "basis")
            if basis_projection:
                await self.cache_writer.write_custom_projection(basis_projection)
                
        except Exception as e:
            logger.error("Error processing basis event", error=str(e))
            
    async def _process_rolling_stat_event(self, message: Dict[str, Any]):
        """Process rolling statistics event."""
        try:
            # Build custom projection for rolling stats
            rolling_stat_projection = await self.builders["custom"].build_projection(message, "rolling_stat")
            if rolling_stat_projection:
                await self.cache_writer.write_custom_projection(rolling_stat_projection)
                
        except Exception as e:
            logger.error("Error processing rolling stat event", error=str(e))
