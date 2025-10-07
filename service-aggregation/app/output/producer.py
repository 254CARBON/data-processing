"""Producer for aggregated data events."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.framework.producer import KafkaProducer
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class AggregatedDataProducer(KafkaProducer):
    """Producer for aggregated data events."""
    
    def __init__(self, config):
        super().__init__(
            bootstrap_servers=config.kafka_bootstrap_servers,
            topics=[config.bars_topic, config.curves_topic]
        )
        self.config = config
        
    async def produce_bar(self, bar_data: Dict[str, Any]):
        """Produce OHLC bar event."""
        try:
            await self.produce(
                topic=self.config.bars_topic,
                key=bar_data["instrument_id"],
                value=bar_data
            )
            logger.debug(f"Produced bar event for {bar_data['instrument_id']}")
        except Exception as e:
            logger.error(f"Error producing bar event: {e}")
            raise DataProcessingError(f"Failed to produce bar event: {e}")
            
    async def produce_curve(self, curve_data: Dict[str, Any]):
        """Produce forward curve event."""
        try:
            await self.produce(
                topic=self.config.curves_topic,
                key=curve_data["instrument_id"],
                value=curve_data
            )
            logger.debug(f"Produced curve event for {curve_data['instrument_id']}")
        except Exception as e:
            logger.error(f"Error producing curve event: {e}")
            raise DataProcessingError(f"Failed to produce curve event: {e}")
            
    async def produce_spread(self, spread_data: Dict[str, Any]):
        """Produce spread event."""
        try:
            await self.produce(
                topic=self.config.bars_topic,  # Reuse bars topic for now
                key=spread_data["instrument_id"],
                value=spread_data
            )
            logger.debug(f"Produced spread event for {spread_data['instrument_id']}")
        except Exception as e:
            logger.error(f"Error producing spread event: {e}")
            raise DataProcessingError(f"Failed to produce spread event: {e}")
            
    async def produce_basis(self, basis_data: Dict[str, Any]):
        """Produce basis event."""
        try:
            await self.produce(
                topic=self.config.bars_topic,  # Reuse bars topic for now
                key=basis_data["instrument_id"],
                value=basis_data
            )
            logger.debug(f"Produced basis event for {basis_data['instrument_id']}")
        except Exception as e:
            logger.error(f"Error producing basis event: {e}")
            raise DataProcessingError(f"Failed to produce basis event: {e}")
            
    async def produce_rolling_stat(self, stat_data: Dict[str, Any]):
        """Produce rolling statistics event."""
        try:
            await self.produce(
                topic=self.config.bars_topic,  # Reuse bars topic for now
                key=stat_data["instrument_id"],
                value=stat_data
            )
            logger.debug(f"Produced rolling stat event for {stat_data['instrument_id']}")
        except Exception as e:
            logger.error(f"Error producing rolling stat event: {e}")
            raise DataProcessingError(f"Failed to produce rolling stat event: {e}")

