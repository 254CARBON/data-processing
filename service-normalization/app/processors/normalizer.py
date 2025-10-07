"""
Data normalizer for raw market data.

Transforms raw market data into normalized tick format
with parsing, validation, and quality flagging.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from shared.schemas.models import TickData, QualityFlag
from shared.utils.errors import ProcessingError, create_error_context


logger = logging.getLogger(__name__)


class Normalizer:
    """
    Data normalizer for raw market data.
    
    Transforms raw market data into normalized tick format
    with parsing, validation, and quality flagging.
    """
    
    def __init__(self, config):
        self.config = config
        
        # Metrics
        self.processed_count = 0
        self.failed_count = 0
        self.last_processing_time = None
        
        # Market-specific parsers
        self.parsers = {
            "miso": self._parse_miso_data,
            "caiso": self._parse_caiso_data,
            "ercot": self._parse_ercot_data,
            "pjm": self._parse_pjm_data,
            "nyiso": self._parse_nyiso_data,
        }
        
        # Configuration defaults
        self.max_price = getattr(config, 'max_price', 10000.0)
        self.max_volume = getattr(config, 'max_volume', 1000000.0)
        self.late_arrival_threshold = getattr(config, 'late_arrival_threshold', 300)  # 5 minutes
    
    async def startup(self) -> None:
        """Initialize normalizer."""
        logger.info("Starting normalizer")
    
    async def shutdown(self) -> None:
        """Shutdown normalizer."""
        logger.info("Shutting down normalizer")
    
    async def normalize(self, raw_data: Dict[str, Any]) -> Optional[TickData]:
        """
        Normalize raw market data.
        
        Args:
            raw_data: Raw market data dictionary
            
        Returns:
            Normalized tick data or None if normalization failed
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Extract market and instrument from envelope/payload structure
            envelope = raw_data.get("envelope", {})
            payload = raw_data.get("payload", {})
            
            market = payload.get("market", "").lower()
            instrument_id = payload.get("instrument_id", "")
            tenant_id = envelope.get("tenant_id", "default")
            
            if not market or not instrument_id:
                raise ProcessingError(
                    "Missing required fields: market or instrument_id",
                    stage="normalization",
                    context=create_error_context(
                        service="normalization",
                        operation="normalize",
                        tenant_id=tenant_id,
                        instrument_id=instrument_id
                    )
                )
            
            # Parse market-specific data
            parser = self.parsers.get(market)
            if not parser:
                raise ProcessingError(
                    f"Unsupported market: {market}",
                    stage="normalization",
                    context=create_error_context(
                        service="normalization",
                        operation="normalize",
                        tenant_id=tenant_id,
                        instrument_id=instrument_id,
                        metadata={"market": market}
                    )
                )
            
            # Parse the data
            parsed_data = await parser(payload)
            
            # Create normalized tick
            tick = TickData(
                instrument_id=instrument_id,
                timestamp=parsed_data["timestamp"],
                price=parsed_data["price"],
                volume=parsed_data["volume"],
                tenant_id=tenant_id,
                source_id=envelope.get("source"),
                metadata=parsed_data.get("metadata", {})
            )
            
            # Add quality flags
            await self._add_quality_flags(tick, parsed_data)
            
            # Update metrics
            self.processed_count += 1
            self.last_processing_time = asyncio.get_event_loop().time() - start_time
            
            logger.debug(
                f"Data normalized successfully: {market} {instrument_id} @ {tick.price}"
            )
            
            return tick
            
        except Exception as e:
            self.failed_count += 1
            logger.error(
                f"Normalization failed: {e}",
                exc_info=True
            )
            return None
    
    async def _parse_miso_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Parse MISO market data."""
        try:
            # Extract MISO-specific fields
            miso_data = payload.get("raw_data", {})
            
            # Parse timestamp
            timestamp_str = miso_data.get("timestamp") or miso_data.get("time")
            if not timestamp_str:
                raise ProcessingError("Missing timestamp in MISO data")
            
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Parse price
            price = float(miso_data.get("price", 0))
            
            # Parse volume
            volume = float(miso_data.get("volume", 0))
            
            return {
                "timestamp": timestamp,
                "price": price,
                "volume": volume,
                "metadata": {
                    "market": "miso",
                    "node": miso_data.get("node"),
                    "zone": miso_data.get("zone"),
                    "market_status": miso_data.get("market_status", "unknown")
                }
            }
            
        except Exception as e:
            raise ProcessingError(
                f"MISO parsing error: {str(e)}",
                stage="parsing",
                context=create_error_context(
                    service="normalization",
                    operation="parse_miso"
                )
            )
    
    async def _parse_caiso_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Parse CAISO market data."""
        try:
            # Extract CAISO-specific fields
            caiso_data = payload.get("raw_data", {})
            
            # Parse timestamp
            timestamp_str = caiso_data.get("timestamp") or caiso_data.get("time")
            if not timestamp_str:
                raise ProcessingError("Missing timestamp in CAISO data")
            
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Parse price
            price = float(caiso_data.get("price", 0))
            
            # Parse volume
            volume = float(caiso_data.get("volume", 0))
            
            return {
                "timestamp": timestamp,
                "price": price,
                "volume": volume,
                "metadata": {
                    "market": "caiso",
                    "node": caiso_data.get("node"),
                    "hub": caiso_data.get("hub"),
                    "market_status": caiso_data.get("market_status", "unknown")
                }
            }
            
        except Exception as e:
            raise ProcessingError(
                f"CAISO parsing error: {str(e)}",
                stage="parsing",
                context=create_error_context(
                    service="normalization",
                    operation="parse_caiso"
                )
            )
    
    async def _parse_ercot_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Parse ERCOT market data."""
        try:
            # Extract ERCOT-specific fields
            ercot_data = payload.get("raw_data", {})
            
            # Parse timestamp
            timestamp_str = ercot_data.get("timestamp") or ercot_data.get("time")
            if not timestamp_str:
                raise ProcessingError("Missing timestamp in ERCOT data")
            
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Parse price
            price = float(ercot_data.get("price", 0))
            
            # Parse volume
            volume = float(ercot_data.get("volume", 0))
            
            return {
                "timestamp": timestamp,
                "price": price,
                "volume": volume,
                "metadata": {
                    "market": "ercot",
                    "node": ercot_data.get("node"),
                    "zone": ercot_data.get("zone"),
                    "market_status": ercot_data.get("market_status", "unknown")
                }
            }
            
        except Exception as e:
            raise ProcessingError(
                f"ERCOT parsing error: {str(e)}",
                stage="parsing",
                context=create_error_context(
                    service="normalization",
                    operation="parse_ercot"
                )
            )
    
    async def _parse_pjm_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Parse PJM market data."""
        try:
            # Extract PJM-specific fields
            pjm_data = payload.get("raw_data", {})
            
            # Parse timestamp
            timestamp_str = pjm_data.get("timestamp") or pjm_data.get("time")
            if not timestamp_str:
                raise ProcessingError("Missing timestamp in PJM data")
            
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Parse price
            price = float(pjm_data.get("price", 0))
            
            # Parse volume
            volume = float(pjm_data.get("volume", 0))
            
            return {
                "timestamp": timestamp,
                "price": price,
                "volume": volume,
                "metadata": {
                    "market": "pjm",
                    "node": pjm_data.get("node"),
                    "zone": pjm_data.get("zone"),
                    "market_status": pjm_data.get("market_status", "unknown")
                }
            }
            
        except Exception as e:
            raise ProcessingError(
                f"PJM parsing error: {str(e)}",
                stage="parsing",
                context=create_error_context(
                    service="normalization",
                    operation="parse_pjm"
                )
            )
    
    async def _parse_nyiso_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Parse NYISO market data."""
        try:
            # Extract NYISO-specific fields
            nyiso_data = payload.get("raw_data", {})
            
            # Parse timestamp
            timestamp_str = nyiso_data.get("timestamp") or nyiso_data.get("time")
            if not timestamp_str:
                raise ProcessingError("Missing timestamp in NYISO data")
            
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Parse price
            price = float(nyiso_data.get("price", 0))
            
            # Parse volume
            volume = float(nyiso_data.get("volume", 0))
            
            return {
                "timestamp": timestamp,
                "price": price,
                "volume": volume,
                "metadata": {
                    "market": "nyiso",
                    "node": nyiso_data.get("node"),
                    "zone": nyiso_data.get("zone"),
                    "market_status": nyiso_data.get("market_status", "unknown")
                }
            }
            
        except Exception as e:
            raise ProcessingError(
                f"NYISO parsing error: {str(e)}",
                stage="parsing",
                context=create_error_context(
                    service="normalization",
                    operation="parse_nyiso"
                )
            )
    
    async def _add_quality_flags(self, tick: TickData, parsed_data: Dict[str, Any]) -> None:
        """Add quality flags to tick."""
        # Check for negative prices
        if tick.price < 0:
            tick.add_quality_flag(QualityFlag.PRICE_NEGATIVE)
        
        # Check for price spikes
        if tick.price > self.max_price:
            tick.add_quality_flag(QualityFlag.OUT_OF_RANGE)
        
        # Check for volume spikes
        if tick.volume > 0:
            # Simple volume spike detection (in production, use historical data)
            if tick.volume > self.max_volume:
                tick.add_quality_flag(QualityFlag.VOLUME_SPIKE)
        
        # Check for late arrival
        now = datetime.now()
        if (now - tick.timestamp).total_seconds() > self.late_arrival_threshold:
            tick.add_quality_flag(QualityFlag.LATE_ARRIVAL)
        
        # Check for missing metadata
        metadata = parsed_data.get("metadata", {})
        if not metadata.get("node") or not metadata.get("zone"):
            tick.add_quality_flag(QualityFlag.MISSING_METADATA)
        
        # If no quality issues, mark as valid
        if not tick.quality_flags:
            tick.add_quality_flag(QualityFlag.VALID)
    
    def get_processed_count(self) -> int:
        """Get processed count."""
        return self.processed_count
    
    def get_failed_count(self) -> int:
        """Get failed count."""
        return self.failed_count
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get normalizer metrics."""
        return {
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "last_processing_time": self.last_processing_time,
            "success_rate": self.processed_count / (self.processed_count + self.failed_count) if (self.processed_count + self.failed_count) > 0 else 0,
        }
