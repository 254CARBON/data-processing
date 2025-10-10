"""Consumer for invalidation events."""

from dataclasses import replace
from typing import Dict, Any, List

import structlog

from shared.framework.consumer import KafkaConsumer, ConsumerConfig
from shared.framework.config import KafkaConfig
from shared.utils.errors import DataProcessingError

from ..invalidation.manager import InvalidationManager
from ..refresh.executor import RefreshExecutor


logger = structlog.get_logger(__name__)


class InvalidationConsumer(KafkaConsumer):
    """Consumer for invalidation events."""

    def __init__(
        self,
        config,
        invalidation_manager: InvalidationManager,
        refresh_executor: RefreshExecutor,
    ):
        self.config = config
        self.invalidation_manager = invalidation_manager
        self.refresh_executor = refresh_executor

        kafka_config: KafkaConfig = replace(
            config.kafka,
            bootstrap_servers=config.kafka_bootstrap_servers,
            consumer_group=config.kafka_group_id,
        )

        consumer_config = ConsumerConfig(
            topics=[config.invalidation_topic],
            group_id=f"{config.kafka_group_id}-invalidation",
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
        for message in messages:
            payload = message.get("payload") if isinstance(message, dict) else None
            event = payload if isinstance(payload, dict) else message
            await self.consume(event)

    async def _handle_error(self, exc: Exception) -> None:
        logger.error("Invalidation consumer error", error=str(exc))

    async def consume(self, message: Dict[str, Any]) -> None:
        """Process invalidation event."""
        try:
            # Parse invalidation event
            invalidation_type = message.get("type")
            target_projection = message.get("target_projection")
            reason = message.get("reason")
            
            logger.info(
                "Processing invalidation",
                invalidation_type=invalidation_type,
                target_projection=target_projection,
            )
            
            # Handle different invalidation types
            if invalidation_type == "price_change":
                await self._handle_price_change_invalidation(message)
            elif invalidation_type == "time_based":
                await self._handle_time_based_invalidation(message)
            elif invalidation_type == "manual":
                await self._handle_manual_invalidation(message)
            else:
                logger.warning("Unknown invalidation type", invalidation_type=invalidation_type)
                
        except Exception as e:
            logger.error("Error processing invalidation event", error=str(e))
            raise DataProcessingError(f"Failed to process invalidation event: {e}")
            
    async def _handle_price_change_invalidation(self, message: Dict[str, Any]):
        """Handle price change invalidation."""
        try:
            target_projection = message.get("target_projection")
            old_price = message.get("old_price")
            new_price = message.get("new_price")
            
            # Calculate price change percentage
            if old_price and new_price:
                price_change_pct = abs(new_price - old_price) / old_price
                
                if price_change_pct >= self.config.invalidation_threshold:
                    # Trigger refresh for affected projections
                    await self.refresh_executor.refresh_projection(target_projection)
                    
        except Exception as e:
            logger.error("Error handling price change invalidation", error=str(e))
            
    async def _handle_time_based_invalidation(self, message: Dict[str, Any]):
        """Handle time-based invalidation."""
        try:
            target_projection = message.get("target_projection")
            last_updated = message.get("last_updated")
            
            # Check if projection is stale
            if self._is_projection_stale(last_updated):
                await self.refresh_executor.refresh_projection(target_projection)
                
        except Exception as e:
            logger.error("Error handling time-based invalidation", error=str(e))
            
    async def _handle_manual_invalidation(self, message: Dict[str, Any]):
        """Handle manual invalidation."""
        try:
            target_projection = message.get("target_projection")
            reason = message.get("reason")
            
            logger.info("Manual invalidation", target_projection=target_projection, reason=reason)
            
            # Always refresh for manual invalidations
            await self.refresh_executor.refresh_projection(target_projection)
            
        except Exception as e:
            logger.error("Error handling manual invalidation", error=str(e))
            
    def _is_projection_stale(self, last_updated: str) -> bool:
        """Check if projection is stale based on last update time."""
        # Simplified staleness check
        # In reality, this would parse the timestamp and compare with current time
        return True  # Always consider stale for now
