"""
Taxonomy classifier for enrichment service.

Provides commodity classification and region mapping
with confidence scoring and fallback mechanisms.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

from shared.storage.postgres import PostgresClient
from shared.storage.redis import RedisClient
from shared.utils.errors import ProcessingError, create_error_context


logger = logging.getLogger(__name__)


class TaxonomyClassifier:
    """
    Taxonomy classifier for enrichment service.
    
    Provides commodity classification and region mapping
    with confidence scoring and fallback mechanisms.
    """
    
    def __init__(self, config):
        self.config = config
        
        # Create database clients
        self.postgres = PostgresClient(config.postgres_dsn)
        self.redis = RedisClient(config.redis_url)
        
        # Metrics
        self.classification_count = 0
        self.high_confidence_count = 0
        self.low_confidence_count = 0
        self.fallback_count = 0
        self.last_classification_time = None
        
        # Classification rules
        self.commodity_rules = {
            "natural_gas": ["NG", "GAS", "NATURAL"],
            "electricity": ["ELEC", "POWER", "MW"],
            "crude_oil": ["CRUDE", "OIL", "WTI", "BRENT"],
            "refined_products": ["GASOLINE", "DIESEL", "HEATING"],
            "coal": ["COAL", "THERMAL"],
        }
        
        self.region_rules = {
            "northeast": ["NY", "NE", "ISO-NE", "PJM"],
            "southeast": ["SE", "SOUTH", "FLORIDA"],
            "midwest": ["MIDWEST", "MISO", "ILLINOIS"],
            "west": ["WEST", "CAISO", "CALIFORNIA"],
            "texas": ["TEXAS", "ERCOT", "TX"],
        }
        
        # Configuration defaults
        self.confidence_threshold = getattr(config, 'confidence_threshold', 0.8)
    
    async def startup(self) -> None:
        """Initialize taxonomy classifier."""
        logger.info("Starting taxonomy classifier")
        
        # Connect to databases
        await self.postgres.connect()
        await self.redis.connect()
        
        # Load classification rules
        await self._load_classification_rules()
    
    async def shutdown(self) -> None:
        """Shutdown taxonomy classifier."""
        logger.info("Shutting down taxonomy classifier")
        
        # Disconnect from databases
        await self.postgres.disconnect()
        await self.redis.disconnect()
    
    async def classify_instrument(self, instrument_id: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Classify instrument taxonomy.
        
        Args:
            instrument_id: Instrument identifier
            metadata: Optional metadata for classification
            
        Returns:
            Classification result with commodity, region, and confidence
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Try database lookup first
            db_classification = await self._lookup_database_classification(instrument_id)
            if db_classification:
                classification = db_classification
                confidence = 0.95  # High confidence for database lookup
            else:
                # Fallback to rule-based classification
                classification = await self._rule_based_classification(instrument_id, metadata)
                confidence = classification.get("confidence", 0.7)
            
            # Update metrics
            self.classification_count += 1
            if confidence >= self.confidence_threshold:
                self.high_confidence_count += 1
            else:
                self.low_confidence_count += 1
            
            self.last_classification_time = asyncio.get_event_loop().time() - start_time
            
            logger.debug(
                f"Instrument classified: {instrument_id} -> {classification.get('commodity')}/{classification.get('region')} ({confidence:.2f})"
            )
            
            return {
                "commodity": classification.get("commodity", "unknown"),
                "region": classification.get("region", "unknown"),
                "product_tier": classification.get("product_tier", "standard"),
                "confidence": confidence,
                "method": classification.get("method", "rule_based")
            }
            
        except Exception as e:
            logger.error(f"Classification error: {e}", exc_info=True)
            
            # Return fallback classification
            self.fallback_count += 1
            return {
                "commodity": "unknown",
                "region": "unknown",
                "product_tier": "standard",
                "confidence": 0.1,
                "method": "fallback"
            }
    
    async def _lookup_database_classification(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Lookup classification in database."""
        try:
            query = """
                SELECT 
                    commodity,
                    region,
                    product_tier
                FROM taxonomy_instruments 
                WHERE instrument_id = $1
            """
            
            row = await self.postgres.execute_one(query, instrument_id)
            
            if row:
                return {
                    "commodity": row["commodity"],
                    "region": row["region"],
                    "product_tier": row["product_tier"],
                    "method": "database"
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Database classification lookup error: {e}")
            return None
    
    async def _rule_based_classification(self, instrument_id: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Rule-based classification."""
        try:
            # Extract classification hints
            hints = self._extract_classification_hints(instrument_id, metadata)
            
            # Classify commodity
            commodity = self._classify_commodity(hints)
            
            # Classify region
            region = self._classify_region(hints)
            
            # Determine product tier
            product_tier = self._classify_product_tier(hints)
            
            # Calculate confidence
            confidence = self._calculate_confidence(hints)
            
            return {
                "commodity": commodity,
                "region": region,
                "product_tier": product_tier,
                "confidence": confidence,
                "method": "rule_based",
                "hints": hints
            }
            
        except Exception as e:
            logger.error(f"Rule-based classification error: {e}")
            return {
                "commodity": "unknown",
                "region": "unknown",
                "product_tier": "standard",
                "confidence": 0.3,
                "method": "rule_based"
            }
    
    def _extract_classification_hints(self, instrument_id: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract classification hints from instrument ID and metadata."""
        hints = {
            "instrument_id": instrument_id,
            "market": None,
            "node": None,
            "hub": None,
            "zone": None,
        }
        
        # Extract from instrument ID
        id_upper = instrument_id.upper()
        
        # Market hints
        if "MISO" in id_upper:
            hints["market"] = "miso"
        elif "CAISO" in id_upper:
            hints["market"] = "caiso"
        elif "ERCOT" in id_upper:
            hints["market"] = "ercot"
        elif "PJM" in id_upper:
            hints["market"] = "pjm"
        elif "ISO-NE" in id_upper:
            hints["market"] = "iso-ne"
        
        # Extract from metadata
        if metadata:
            hints.update({
                "market": metadata.get("market"),
                "node": metadata.get("node"),
                "hub": metadata.get("hub"),
                "zone": metadata.get("zone"),
            })
        
        return hints
    
    def _classify_commodity(self, hints: Dict[str, Any]) -> str:
        """Classify commodity based on hints."""
        instrument_id = hints["instrument_id"].upper()
        
        # Check commodity rules
        for commodity, keywords in self.commodity_rules.items():
            for keyword in keywords:
                if keyword in instrument_id:
                    return commodity
        
        # Default classification based on market
        market = hints.get("market", "").lower()
        if market in ["miso", "caiso", "ercot", "pjm", "iso-ne"]:
            return "electricity"
        
        return "unknown"
    
    def _classify_region(self, hints: Dict[str, Any]) -> str:
        """Classify region based on hints."""
        instrument_id = hints["instrument_id"].upper()
        market = hints.get("market", "").lower()
        
        # Check region rules
        for region, keywords in self.region_rules.items():
            for keyword in keywords:
                if keyword in instrument_id:
                    return region
        
        # Market-based region mapping
        market_region_map = {
            "miso": "midwest",
            "caiso": "west",
            "ercot": "texas",
            "pjm": "northeast",
            "iso-ne": "northeast",
        }
        
        return market_region_map.get(market, "unknown")
    
    def _classify_product_tier(self, hints: Dict[str, Any]) -> str:
        """Classify product tier based on hints."""
        instrument_id = hints["instrument_id"].upper()
        
        # Premium indicators
        premium_indicators = ["BALMO", "PROMO", "PREM", "PREMIUM"]
        for indicator in premium_indicators:
            if indicator in instrument_id:
                return "premium"
        
        # Standard indicators
        standard_indicators = ["STD", "STANDARD", "BASE"]
        for indicator in standard_indicators:
            if indicator in instrument_id:
                return "standard"
        
        # Default to standard
        return "standard"
    
    def _calculate_confidence(self, hints: Dict[str, Any]) -> float:
        """Calculate classification confidence."""
        confidence = 0.5  # Base confidence
        
        # Increase confidence for market identification
        if hints.get("market"):
            confidence += 0.2
        
        # Increase confidence for node/hub identification
        if hints.get("node") or hints.get("hub"):
            confidence += 0.1
        
        # Increase confidence for zone identification
        if hints.get("zone"):
            confidence += 0.1
        
        # Cap at 0.9 for rule-based classification
        return min(confidence, 0.9)
    
    async def _load_classification_rules(self) -> None:
        """Load classification rules from database."""
        try:
            # Load commodity rules
            commodity_query = """
                SELECT commodity, keywords
                FROM taxonomy_commodity_rules
                WHERE active = true
            """
            
            commodity_rows = await self.postgres.execute(commodity_query)
            for row in commodity_rows:
                commodity = row["commodity"]
                keywords = row["keywords"].split(",") if row["keywords"] else []
                self.commodity_rules[commodity] = [kw.strip().upper() for kw in keywords]
            
            # Load region rules
            region_query = """
                SELECT region, keywords
                FROM taxonomy_region_rules
                WHERE active = true
            """
            
            region_rows = await self.postgres.execute(region_query)
            for row in region_rows:
                region = row["region"]
                keywords = row["keywords"].split(",") if row["keywords"] else []
                self.region_rules[region] = [kw.strip().upper() for kw in keywords]
            
            logger.info(f"Classification rules loaded: {len(self.commodity_rules)} commodity, {len(self.region_rules)} region")
            
        except Exception as e:
            logger.error(f"Failed to load classification rules: {e}", exc_info=True)
    
    async def get_status(self) -> Dict[str, Any]:
        """Get taxonomy classifier status."""
        return {
            "classification_count": self.classification_count,
            "high_confidence_count": self.high_confidence_count,
            "low_confidence_count": self.low_confidence_count,
            "fallback_count": self.fallback_count,
            "last_classification_time": self.last_classification_time,
            "confidence_threshold": self.confidence_threshold,
            "commodity_rules": len(self.commodity_rules),
            "region_rules": len(self.region_rules),
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get taxonomy classifier metrics."""
        return {
            "classification_count": self.classification_count,
            "high_confidence_count": self.high_confidence_count,
            "low_confidence_count": self.low_confidence_count,
            "fallback_count": self.fallback_count,
            "last_classification_time": self.last_classification_time,
            "high_confidence_rate": self.high_confidence_count / self.classification_count if self.classification_count > 0 else 0,
        }
