"""Consumer for invalidation events."""

import asyncio
import logging
from typing import Dict, Any
from shared.framework.consumer import KafkaConsumer
from shared.utils.errors import DataProcessingError
from ..invalidation.manager import InvalidationManager
from ..refresh.executor import RefreshExecutor


logger = logging.getLogger(__name__)


class InvalidationConsumer(KafkaConsumer):
    """Consumer for invalidation events."""
    
    def __init__(
        self,
        config,
        invalidation_manager: InvalidationManager,
        refresh_executor: RefreshExecutor
    ):
        super().__init__(
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=config.kafka_group_id,
            topics=[config.invalidation_topic]
        )
        self.config = config
        self.invalidation_manager = invalidation_manager
        self.refresh_executor = refresh_executor
        
    async def consume(self, message: Dict[str, Any]) -> None:
        """Process invalidation event."""
        try:
            # Parse invalidation event
            invalidation_type = message.get("type")
            target_projection = message.get("target_projection")
            reason = message.get("reason")
            
            logger.info(f"Processing invalidation: {invalidation_type} for {target_projection}")
            
            # Handle different invalidation types
            if invalidation_type == "price_change":
                await self._handle_price_change_invalidation(message)
            elif invalidation_type == "time_based":
                await self._handle_time_based_invalidation(message)
            elif invalidation_type == "manual":
                await self._handle_manual_invalidation(message)
            else:
                logger.warning(f"Unknown invalidation type: {invalidation_type}")
                
        except Exception as e:
            logger.error(f"Error processing invalidation event: {e}")
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
            logger.error(f"Error handling price change invalidation: {e}")
            
    async def _handle_time_based_invalidation(self, message: Dict[str, Any]):
        """Handle time-based invalidation."""
        try:
            target_projection = message.get("target_projection")
            last_updated = message.get("last_updated")
            
            # Check if projection is stale
            if self._is_projection_stale(last_updated):
                await self.refresh_executor.refresh_projection(target_projection)
                
        except Exception as e:
            logger.error(f"Error handling time-based invalidation: {e}")
            
    async def _handle_manual_invalidation(self, message: Dict[str, Any]):
        """Handle manual invalidation."""
        try:
            target_projection = message.get("target_projection")
            reason = message.get("reason")
            
            logger.info(f"Manual invalidation for {target_projection}: {reason}")
            
            # Always refresh for manual invalidations
            await self.refresh_executor.refresh_projection(target_projection)
            
        except Exception as e:
            logger.error(f"Error handling manual invalidation: {e}")
            
    def _is_projection_stale(self, last_updated: str) -> bool:
        """Check if projection is stale based on last update time."""
        # Simplified staleness check
        # In reality, this would parse the timestamp and compare with current time
        return True  # Always consider stale for now

