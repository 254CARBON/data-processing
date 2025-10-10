"""Invalidation management for projections."""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from shared.utils.errors import DataProcessingError

from ..output.served_cache import ServedProjectionCache


logger = logging.getLogger(__name__)


class InvalidationManager:
    """Manages projection invalidation."""
    
    def __init__(self, config, served_cache: Optional[ServedProjectionCache] = None):
        self.config = config
        self.invalidation_rules = getattr(config, 'invalidation_rules', {})
        self.threshold = getattr(config, 'invalidation_threshold', 0.05)  # 5% default
        self.price_history: Dict[str, List[Dict[str, Any]]] = {}
        self.is_running = False
        self.served_cache = served_cache
        
    async def start(self):
        """Start the invalidation manager."""
        self.is_running = True
        logger.info("Invalidation manager started")
        
    async def stop(self):
        """Stop the invalidation manager."""
        self.is_running = False
        logger.info("Invalidation manager stopped")
        
    async def check_price_change_triggers(self, event: Dict[str, Any]):
        """Check if price change triggers invalidation."""
        try:
            instrument_id = event.get("instrument_id")
            tenant_id = event.get("tenant_id")
            current_price = event.get("close_price") or event.get("price")
            ts = event.get("timestamp") or event.get("ts")
            
            if not all([instrument_id, tenant_id, current_price, ts]):
                return
                
            cache_key = f"{instrument_id}_{tenant_id}"
            
            # Get previous price
            previous_price = await self._get_previous_price(cache_key)
            
            if previous_price is not None:
                # Calculate price change percentage
                price_change_pct = abs(current_price - previous_price) / previous_price
                
                if price_change_pct >= self.threshold:
                    # Trigger invalidation
                    await self._trigger_invalidation(
                        "price_change",
                        cache_key,
                        {
                            "tenant_id": tenant_id,
                            "instrument_id": instrument_id,
                            "old_price": previous_price,
                            "new_price": current_price,
                            "change_pct": price_change_pct
                        }
                    )
                    logger.info(f"Price change invalidation triggered: {instrument_id} ({price_change_pct:.2%})")
                    
            # Update price history
            await self._update_price_history(cache_key, current_price, ts)
            
        except Exception as e:
            logger.error(f"Error checking price change triggers: {e}")
            
    async def _get_previous_price(self, cache_key: str) -> Optional[float]:
        """Get previous price from history."""
        if cache_key in self.price_history:
            history = self.price_history[cache_key]
            if history:
                return history[-1].get("price")
        return None
        
    async def _update_price_history(self, cache_key: str, price: float, ts):
        """Update price history."""
        if cache_key not in self.price_history:
            self.price_history[cache_key] = []
            
        # Convert timestamp to datetime if it's a string
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
            
        self.price_history[cache_key].append({
            "price": price,
            "ts": ts
        })
        
        # Keep only recent history (last 100 entries)
        if len(self.price_history[cache_key]) > 100:
            self.price_history[cache_key] = self.price_history[cache_key][-100:]
            
    async def _trigger_invalidation(self, invalidation_type: str, target: str, data: Dict[str, Any]):
        """Trigger invalidation event."""
        try:
            invalidation_event = {
                "type": invalidation_type,
                "target_projection": target,
                "timestamp": datetime.now().isoformat(),
                "data": data
            }
            
            # Best-effort cache invalidation for Served projections
            if self.served_cache and invalidation_type == "price_change":
                tenant_id = data.get("tenant_id")
                instrument_id = data.get("instrument_id")
                if tenant_id and instrument_id:
                    await self.served_cache.invalidate_latest_price(tenant_id, instrument_id)
                    self.logger.debug(
                        "Invalidated served latest price cache",
                        tenant_id=tenant_id,
                        instrument_id=instrument_id,
                    )
            
            # In a real implementation, this would send to Kafka
            logger.info(f"Triggered invalidation: {invalidation_type} for {target}")
            
        except Exception as e:
            logger.error(f"Error triggering invalidation: {e}")
            
    async def check_time_based_triggers(self):
        """Check if time-based triggers need invalidation."""
        try:
            current_time = datetime.now()
            
            # Check for stale projections
            for cache_key, history in self.price_history.items():
                if history:
                    last_update = history[-1].get("ts")
                    if isinstance(last_update, str):
                        last_update = datetime.fromisoformat(last_update)
                        
                    # If projection is older than 1 hour, trigger invalidation
                    if (current_time - last_update).total_seconds() > 3600:
                        await self._trigger_invalidation(
                            "time_based",
                            cache_key,
                            {"last_updated": last_update.isoformat()}
                        )
                        
        except Exception as e:
            logger.error(f"Error checking time-based triggers: {e}")
            
    async def manual_invalidation(self, target: str, reason: str):
        """Trigger manual invalidation."""
        try:
            await self._trigger_invalidation(
                "manual",
                target,
                {"reason": reason}
            )
            logger.info(f"Manual invalidation triggered for {target}: {reason}")
            
        except Exception as e:
            logger.error(f"Error triggering manual invalidation: {e}")
            
    async def cleanup_old_history(self, max_age_hours: int = 24):
        """Clean up old price history."""
        try:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            
            for cache_key, history in self.price_history.items():
                # Filter out old entries
                self.price_history[cache_key] = [
                    entry for entry in history
                    if entry.get("ts", datetime.min) > cutoff_time
                ]
                
        except Exception as e:
            logger.error(f"Error cleaning up old history: {e}")
