"""Writer for aggregated data to ClickHouse."""

import asyncio
import logging
from typing import Dict, Any, List
from shared.storage.clickhouse import ClickHouseClient
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class AggregatedDataWriter:
    """Writer for aggregated data to ClickHouse."""
    
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
        """Start the writer."""
        await self.client.connect()
        logger.info("Aggregated data writer started")
        
    async def stop(self):
        """Stop the writer."""
        await self.client.close()
        logger.info("Aggregated data writer stopped")
        
    async def write_bars(self, bars: List[Dict[str, Any]]):
        """Write OHLC bars to ClickHouse."""
        if not bars:
            return
            
        try:
            # Prepare data for insertion
            data = []
            for bar in bars:
                data.append({
                    "instrument_id": bar["instrument_id"],
                    "ts": bar["ts"],
                    "interval": bar["interval"],
                    "open_price": bar["open_price"],
                    "high_price": bar["high_price"],
                    "low_price": bar["low_price"],
                    "close_price": bar["close_price"],
                    "volume": bar["volume"],
                    "trade_count": bar["trade_count"],
                    "quality_flags": bar["quality_flags"],
                    "tenant_id": bar["tenant_id"]
                })
                
            # Insert into ClickHouse
            await self.client.insert("bars_5m", data)
            logger.info(f"Wrote {len(bars)} bars to ClickHouse")
            
        except Exception as e:
            logger.error(f"Error writing bars to ClickHouse: {e}")
            raise DataProcessingError(f"Failed to write bars: {e}")
            
    async def write_curves(self, curves: List[Dict[str, Any]]):
        """Write forward curves to ClickHouse."""
        if not curves:
            return
            
        try:
            # Prepare data for insertion
            data = []
            for curve in curves:
                data.append({
                    "instrument_id": curve["instrument_id"],
                    "ts": curve["ts"],
                    "horizon": curve["horizon"],
                    "curve_points": curve["curve_points"],
                    "interpolation_method": curve["interpolation_method"],
                    "quality_flags": curve["quality_flags"],
                    "tenant_id": curve["tenant_id"]
                })
                
            # Insert into ClickHouse
            await self.client.insert("curves_base", data)
            logger.info(f"Wrote {len(curves)} curves to ClickHouse")
            
        except Exception as e:
            logger.error(f"Error writing curves to ClickHouse: {e}")
            raise DataProcessingError(f"Failed to write curves: {e}")
            
    async def write_spreads(self, spreads: List[Dict[str, Any]]):
        """Write spreads to ClickHouse."""
        if not spreads:
            return
            
        try:
            # Prepare data for insertion
            data = []
            for spread in spreads:
                data.append({
                    "instrument_id": spread["instrument_id"],
                    "ts": spread["ts"],
                    "spread_type": spread["spread_type"],
                    "spread_value": spread["spread_value"],
                    "quality_flags": spread["quality_flags"],
                    "tenant_id": spread["tenant_id"]
                })
                
            # Insert into ClickHouse
            await self.client.insert("bars_5m", data)  # Reuse bars table for now
            logger.info(f"Wrote {len(spreads)} spreads to ClickHouse")
            
        except Exception as e:
            logger.error(f"Error writing spreads to ClickHouse: {e}")
            raise DataProcessingError(f"Failed to write spreads: {e}")
            
    async def write_basis(self, basis_data: List[Dict[str, Any]]):
        """Write basis data to ClickHouse."""
        if not basis_data:
            return
            
        try:
            # Prepare data for insertion
            data = []
            for basis in basis_data:
                data.append({
                    "instrument_id": basis["instrument_id"],
                    "ts": basis["ts"],
                    "basis_type": basis["basis_type"],
                    "basis_value": basis["basis_value"],
                    "quality_flags": basis["quality_flags"],
                    "tenant_id": basis["tenant_id"]
                })
                
            # Insert into ClickHouse
            await self.client.insert("bars_5m", data)  # Reuse bars table for now
            logger.info(f"Wrote {len(basis_data)} basis records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Error writing basis data to ClickHouse: {e}")
            raise DataProcessingError(f"Failed to write basis data: {e}")
            
    async def write_rolling_stats(self, stats: List[Dict[str, Any]]):
        """Write rolling statistics to ClickHouse."""
        if not stats:
            return
            
        try:
            # Prepare data for insertion
            data = []
            for stat in stats:
                data.append({
                    "instrument_id": stat["instrument_id"],
                    "ts": stat["ts"],
                    "stat_type": stat["stat_type"],
                    "stat_value": stat["stat_value"],
                    "window_size": stat["window_size"],
                    "quality_flags": stat["quality_flags"],
                    "tenant_id": stat["tenant_id"]
                })
                
            # Insert into ClickHouse
            await self.client.insert("bars_5m", data)  # Reuse bars table for now
            logger.info(f"Wrote {len(stats)} rolling stats to ClickHouse")
            
        except Exception as e:
            logger.error(f"Error writing rolling stats to ClickHouse: {e}")
            raise DataProcessingError(f"Failed to write rolling stats: {e}")

