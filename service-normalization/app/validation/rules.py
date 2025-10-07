"""
Validation rules engine for normalization service.

Provides comprehensive validation for normalized tick data
with configurable rules and quality flagging.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import structlog

from shared.schemas.models import TickData, QualityFlag
from shared.utils.errors import ValidationError, create_error_context


logger = structlog.get_logger()


class ValidationRules:
    """
    Validation rules engine for normalized tick data.
    
    Provides comprehensive validation with configurable rules
    and quality flagging.
    """
    
    def __init__(self, config):
        self.config = config
        self.logger = structlog.get_logger("validation-rules")
        
        # Metrics
        self.validation_count = 0
        self.error_count = 0
        self.last_validation_time = None
        
        # Validation rules
        self.rules = {
            "price_range": self._validate_price_range,
            "volume_range": self._validate_volume_range,
            "timestamp": self._validate_timestamp,
            "instrument_id": self._validate_instrument_id,
            "duplicate": self._validate_duplicate,
        }
    
    async def startup(self) -> None:
        """Initialize validation rules."""
        self.logger.info("Starting validation rules engine")
    
    async def shutdown(self) -> None:
        """Shutdown validation rules."""
        self.logger.info("Shutting down validation rules engine")
    
    async def validate_tick(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate tick data.
        
        Args:
            tick_data: Tick data dictionary
            
        Returns:
            Validation result with quality flags and errors
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            quality_flags = []
            errors = []
            
            # Run all validation rules
            for rule_name, rule_func in self.rules.items():
                try:
                    result = await rule_func(tick_data)
                    if result["valid"]:
                        quality_flags.extend(result["quality_flags"])
                    else:
                        errors.extend(result["errors"])
                        
                except Exception as e:
                    self.logger.error(
                        "Validation rule error",
                        rule=rule_name,
                        error=str(e),
                        exc_info=True
                    )
                    errors.append(f"Validation rule {rule_name} failed: {str(e)}")
            
            # Update metrics
            self.validation_count += 1
            self.last_validation_time = asyncio.get_event_loop().time() - start_time
            
            if errors:
                self.error_count += 1
            
            return {
                "valid": len(errors) == 0,
                "quality_flags": quality_flags,
                "errors": errors
            }
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(
                "Validation failed",
                error=str(e),
                tick_data=tick_data,
                exc_info=True
            )
            
            return {
                "valid": False,
                "quality_flags": [],
                "errors": [str(e)]
            }
    
    async def _validate_price_range(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate price range."""
        price = tick_data.get("price")
        
        if price is None:
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Price is required"]
            }
        
        try:
            price = float(price)
        except (ValueError, TypeError):
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Price must be a number"]
            }
        
        quality_flags = []
        errors = []
        
        # Check minimum price
        if price < self.config.min_price:
            quality_flags.append(QualityFlag.PRICE_NEGATIVE.value)
            errors.append(f"Price {price} is below minimum {self.config.min_price}")
        
        # Check maximum price
        if price > self.config.max_price:
            quality_flags.append(QualityFlag.OUT_OF_RANGE.value)
            errors.append(f"Price {price} is above maximum {self.config.max_price}")
        
        return {
            "valid": len(errors) == 0,
            "quality_flags": quality_flags,
            "errors": errors
        }
    
    async def _validate_volume_range(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate volume range."""
        volume = tick_data.get("volume")
        
        if volume is None:
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Volume is required"]
            }
        
        try:
            volume = float(volume)
        except (ValueError, TypeError):
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Volume must be a number"]
            }
        
        quality_flags = []
        errors = []
        
        # Check for negative volume
        if volume < 0:
            errors.append(f"Volume {volume} cannot be negative")
        
        # Check for volume spikes (simple threshold)
        if volume > 1000000:  # 1M volume threshold
            quality_flags.append(QualityFlag.VOLUME_SPIKE.value)
        
        return {
            "valid": len(errors) == 0,
            "quality_flags": quality_flags,
            "errors": errors
        }
    
    async def _validate_timestamp(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate timestamp."""
        timestamp_str = tick_data.get("timestamp")
        
        if not timestamp_str:
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Timestamp is required"]
            }
        
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError:
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Invalid timestamp format"]
            }
        
        quality_flags = []
        errors = []
        
        # Check for future timestamps
        now = datetime.now()
        if timestamp > now:
            errors.append(f"Timestamp {timestamp} is in the future")
        
        # Check for late arrival
        if (now - timestamp).total_seconds() > 300:  # 5 minutes
            quality_flags.append(QualityFlag.LATE_ARRIVAL.value)
        
        return {
            "valid": len(errors) == 0,
            "quality_flags": quality_flags,
            "errors": errors
        }
    
    async def _validate_instrument_id(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate instrument ID."""
        instrument_id = tick_data.get("instrument_id")
        
        if not instrument_id:
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Instrument ID is required"]
            }
        
        if not isinstance(instrument_id, str):
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Instrument ID must be a string"]
            }
        
        if len(instrument_id) < 3:
            return {
                "valid": False,
                "quality_flags": [],
                "errors": ["Instrument ID must be at least 3 characters"]
            }
        
        return {
            "valid": True,
            "quality_flags": [],
            "errors": []
        }
    
    async def _validate_duplicate(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate for duplicates."""
        # Simple duplicate detection based on instrument_id and timestamp
        # In production, this would check against a database or cache
        
        instrument_id = tick_data.get("instrument_id")
        timestamp_str = tick_data.get("timestamp")
        
        if not instrument_id or not timestamp_str:
            return {
                "valid": True,
                "quality_flags": [],
                "errors": []
            }
        
        # For now, just return valid
        # In production, implement duplicate detection logic
        return {
            "valid": True,
            "quality_flags": [],
            "errors": []
        }
    
    def get_error_count(self) -> int:
        """Get error count."""
        return self.error_count
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get validation metrics."""
        return {
            "validation_count": self.validation_count,
            "error_count": self.error_count,
            "last_validation_time": self.last_validation_time,
            "error_rate": self.error_count / self.validation_count if self.validation_count > 0 else 0,
        }

