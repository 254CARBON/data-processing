"""
Data enricher for enrichment service.

Combines metadata lookup and taxonomy classification
to enrich normalized tick data.
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from shared.schemas.models import TickData, QualityFlag, InstrumentMetadata
from shared.utils.errors import ProcessingError, create_error_context


logger = logging.getLogger(__name__)


class Enricher:
    """
    Data enricher for enrichment service.
    
    Combines metadata lookup and taxonomy classification
    to enrich normalized tick data.
    """
    
    def __init__(self, config):
        self.config = config
        
        # Metrics
        self.enrichment_count = 0
        self.enrichment_failures = 0
        self.last_enrichment_time = None
        
        # Components (will be injected)
        self.metadata_lookup = None
        self.taxonomy_classifier = None
        
        # Configuration defaults
        self.confidence_threshold = getattr(config, 'confidence_threshold', 0.8)
    
    async def startup(self) -> None:
        """Initialize enricher."""
        logger.info("Starting enricher")
    
    async def shutdown(self) -> None:
        """Shutdown enricher."""
        logger.info("Shutting down enricher")
    
    def set_components(self, metadata_lookup, taxonomy_classifier) -> None:
        """Set component references."""
        self.metadata_lookup = metadata_lookup
        self.taxonomy_classifier = taxonomy_classifier
    
    async def enrich_tick(self, tick: TickData) -> Optional[TickData]:
        """
        Enrich normalized tick data.
        
        Args:
            tick: Normalized tick data
            
        Returns:
            Enriched tick data or None if enrichment failed
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            instrument_id = tick.instrument_id
            
            # Lookup metadata
            metadata = await self.metadata_lookup.lookup_metadata(instrument_id)
            
            # Classify taxonomy
            taxonomy = await self.taxonomy_classifier.classify_instrument(instrument_id, metadata.to_dict() if metadata else None)
            
            # Create enriched tick data
            enriched_tick = TickData(
                instrument_id=instrument_id,
                timestamp=tick.timestamp,
                price=tick.price,
                volume=tick.volume,
                quality_flags=tick.quality_flags.copy(),
                tenant_id=tick.tenant_id,
                source_id=tick.source_id,
                metadata=tick.metadata.copy()
            )
            
            # Add enrichment metadata
            enriched_tick.metadata.update({
                "commodity": taxonomy["commodity"],
                "region": taxonomy["region"],
                "product_tier": taxonomy["product_tier"],
                "classification_confidence": taxonomy["confidence"],
                "classification_method": taxonomy["method"],
            })
            
            # Add instrument metadata if available
            if metadata:
                enriched_tick.metadata.update({
                    "unit": metadata.unit,
                    "contract_size": metadata.contract_size,
                    "tick_size": metadata.tick_size,
                })
            
            # Add quality flags for enrichment
            if not metadata:
                enriched_tick.add_quality_flag(QualityFlag.MISSING_METADATA)
            
            if taxonomy["confidence"] < self.confidence_threshold:
                # Add custom quality flag for low confidence
                enriched_tick.metadata["low_confidence_classification"] = True
            
            # Update metrics
            self.enrichment_count += 1
            self.last_enrichment_time = asyncio.get_event_loop().time() - start_time
            
            logger.debug(
                f"Tick enriched successfully: {instrument_id} -> {taxonomy['commodity']} ({taxonomy['confidence']:.2f})"
            )
            
            return enriched_tick
            
        except Exception as e:
            self.enrichment_failures += 1
            logger.error(f"Tick enrichment failed: {e}", exc_info=True)
            return None
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get enricher metrics."""
        return {
            "enrichment_count": self.enrichment_count,
            "enrichment_failures": self.enrichment_failures,
            "last_enrichment_time": self.last_enrichment_time,
            "success_rate": self.enrichment_count / (self.enrichment_count + self.enrichment_failures) if (self.enrichment_count + self.enrichment_failures) > 0 else 0,
        }
