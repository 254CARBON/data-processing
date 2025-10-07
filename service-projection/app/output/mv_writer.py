"""Materialized view writer for ClickHouse."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.storage.clickhouse import ClickHouseClient
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class MaterializedViewWriter:
    """Writer for materialized view updates."""
    
    def __init__(self, config):
        self.config = config
        self.client = ClickHouseClient(
            host=config.clickhouse_host,
            port=config.clickhouse_port,
            database=config.clickhouse_database,
            user=config.clickhouse_user,
            password=config.clickhouse_password
        )
        
    async def start(self):
        """Start the materialized view writer."""
        await self.client.connect()
        logger.info("Materialized view writer started")
        
    async def stop(self):
        """Stop the materialized view writer."""
        await self.client.close()
        logger.info("Materialized view writer stopped")
        
    async def write_latest_price(self, projection: Dict[str, Any]):
        """Write latest price projection to materialized view."""
        try:
            data = [{
                "instrument_id": projection["instrument_id"],
                "ts": projection["ts"],
                "price": projection["price"],
                "volume": projection["volume"],
                "quality_flags": projection["quality_flags"],
                "tenant_id": projection["tenant_id"]
            }]
            
            await self.client.insert("served_latest", data)
            logger.debug(f"Updated materialized view for latest price {projection['instrument_id']}")
        except Exception as e:
            logger.error(f"Error writing latest price to materialized view: {e}")
            raise DataProcessingError(f"Failed to write latest price to materialized view: {e}")
            
    async def write_curve_snapshot(self, projection: Dict[str, Any]):
        """Write curve snapshot projection to materialized view."""
        try:
            data = [{
                "instrument_id": projection["instrument_id"],
                "ts": projection["ts"],
                "horizon": projection["horizon"],
                "curve_points": projection["curve_points"],
                "interpolation_method": projection["interpolation_method"],
                "quality_flags": projection["quality_flags"],
                "tenant_id": projection["tenant_id"]
            }]
            
            await self.client.insert("served_curve_snapshots", data)
            logger.debug(f"Updated materialized view for curve snapshot {projection['instrument_id']}")
        except Exception as e:
            logger.error(f"Error writing curve snapshot to materialized view: {e}")
            raise DataProcessingError(f"Failed to write curve snapshot to materialized view: {e}")

