"""Invalidation triggers and rules."""

import asyncio
import logging
from typing import Dict, Any, List, Callable
from datetime import datetime, timedelta
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class InvalidationTriggers:
    """Invalidation triggers and rules engine."""
    
    def __init__(self, config):
        self.config = config
        self.triggers: Dict[str, Callable] = {}
        self.rules: List[Dict[str, Any]] = []
        
    def register_trigger(self, trigger_name: str, trigger_func: Callable):
        """Register a custom trigger."""
        self.triggers[trigger_name] = trigger_func
        logger.info(f"Registered trigger: {trigger_name}")
        
    def add_rule(self, rule: Dict[str, Any]):
        """Add an invalidation rule."""
        self.rules.append(rule)
        logger.info(f"Added invalidation rule: {rule.get('name', 'unnamed')}")
        
    async def evaluate_triggers(self, event: Dict[str, Any]) -> List[str]:
        """Evaluate all triggers for an event."""
        triggered = []
        
        for trigger_name, trigger_func in self.triggers.items():
            try:
                if await trigger_func(event):
                    triggered.append(trigger_name)
            except Exception as e:
                logger.error(f"Error evaluating trigger {trigger_name}: {e}")
                
        return triggered
        
    async def evaluate_rules(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate all rules for an event."""
        matched_rules = []
        
        for rule in self.rules:
            try:
                if await self._evaluate_rule(rule, event):
                    matched_rules.append(rule)
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.get('name', 'unnamed')}: {e}")
                
        return matched_rules
        
    async def _evaluate_rule(self, rule: Dict[str, Any], event: Dict[str, Any]) -> bool:
        """Evaluate a single rule."""
        rule_type = rule.get("type")
        conditions = rule.get("conditions", [])
        
        if rule_type == "price_change":
            return await self._evaluate_price_change_rule(rule, event)
        elif rule_type == "time_based":
            return await self._evaluate_time_based_rule(rule, event)
        elif rule_type == "volume":
            return await self._evaluate_volume_rule(rule, event)
        else:
            logger.warning(f"Unknown rule type: {rule_type}")
            return False
            
    async def _evaluate_price_change_rule(self, rule: Dict[str, Any], event: Dict[str, Any]) -> bool:
        """Evaluate price change rule."""
        threshold = rule.get("threshold", 0.01)
        current_price = event.get("close_price") or event.get("price")
        
        if current_price is None:
            return False
            
        # Simplified evaluation - in reality, this would compare with previous price
        return True  # Always trigger for now
        
    async def _evaluate_time_based_rule(self, rule: Dict[str, Any], event: Dict[str, Any]) -> bool:
        """Evaluate time-based rule."""
        max_age_seconds = rule.get("max_age_seconds", 3600)
        event_ts = event.get("ts")
        
        if event_ts is None:
            return False
            
        if isinstance(event_ts, str):
            event_ts = datetime.fromisoformat(event_ts)
            
        age_seconds = (datetime.now() - event_ts).total_seconds()
        return age_seconds > max_age_seconds
        
    async def _evaluate_volume_rule(self, rule: Dict[str, Any], event: Dict[str, Any]) -> bool:
        """Evaluate volume rule."""
        min_volume = rule.get("min_volume", 0)
        current_volume = event.get("volume", 0)
        
        return current_volume >= min_volume
        
    def get_trigger_names(self) -> List[str]:
        """Get list of registered trigger names."""
        return list(self.triggers.keys())
        
    def get_rule_names(self) -> List[str]:
        """Get list of rule names."""
        return [rule.get("name", "unnamed") for rule in self.rules]

