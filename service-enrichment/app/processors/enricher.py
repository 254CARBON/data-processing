"""
Data enricher for enrichment service.

Combines metadata lookup and taxonomy classification
to enrich normalized tick data.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, timezone, timedelta

from shared.schemas.models import TickData, QualityFlag, InstrumentMetadata
from shared.utils.errors import ProcessingError, create_error_context


logger = logging.getLogger(__name__)


MICROSECONDS_IN_SECOND = 1_000_000


def _coerce_datetime(value: Union[datetime, int, float, str, None]) -> Optional[datetime]:
    """Coerce supported timestamp representations into timezone-aware datetimes."""
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        integer_value = int(value)
        if integer_value > 1_000_000_000_000:
            seconds, micros = divmod(integer_value, MICROSECONDS_IN_SECOND)
            return datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=micros)
        return datetime.fromtimestamp(integer_value, tz=timezone.utc)

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    return None


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
    
    async def enrich_tick(self, tick: Union[TickData, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Enrich normalized tick data.
        
        Args:
            tick: Normalized tick data
            
        Returns:
            Enriched tick data or None if enrichment failed
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            tick_payload = tick.to_dict() if hasattr(tick, "to_dict") else dict(tick)

            instrument_id = tick_payload.get("instrument_id")
            if not instrument_id:
                raise ValueError("instrument_id missing from tick data")
            tenant_id = tick_payload.get("tenant_id", "default")
            tick_timestamp = _coerce_datetime(
                tick_payload.get("timestamp") or tick_payload.get("ts")
            ) or datetime.now(timezone.utc)
            normalized_at_dt = _coerce_datetime(tick_payload.get("normalized_at"))
            price = tick_payload.get("price")
            volume = tick_payload.get("volume", 0.0)
            source_id = tick_payload.get("source_id")
            source_system = tick_payload.get("source_system")
            symbol = tick_payload.get("symbol")
            market = tick_payload.get("market")
            currency = tick_payload.get("currency", "USD")
            unit = tick_payload.get("unit", "ton_CO2e")
            tick_id = tick_payload.get("tick_id")
            source_event_id = tick_payload.get("source_event_id")
            tags = tick_payload.get("tags", [])

            # Prepare quality flags
            quality_flags_raw = tick_payload.get("quality_flags", [])
            quality_flags: List[QualityFlag] = []
            for flag in quality_flags_raw:
                if isinstance(flag, QualityFlag):
                    quality_flags.append(flag)
                else:
                    try:
                        quality_flags.append(QualityFlag(flag))
                    except ValueError:
                        logger.debug("Ignoring unknown quality flag", flag=flag)

            metadata = tick_payload.get("metadata", {}) or {}
            enriched_metadata = dict(metadata)
            
            # Lookup metadata
            metadata_record: Optional[InstrumentMetadata] = await self.metadata_lookup.lookup_metadata(instrument_id)
            metadata_dict: Optional[Dict[str, Any]] = None
            if metadata_record:
                metadata_dict = metadata_record.to_dict()
            
            # Classify taxonomy
            taxonomy = await self.taxonomy_classifier.classify_instrument(
                instrument_id, metadata_dict if metadata_dict else None
            )
            
            # Create enriched tick data
            enriched_tick = TickData(
                instrument_id=instrument_id,
                timestamp=tick_timestamp,
                price=price,
                volume=volume,
                quality_flags=quality_flags,
                tenant_id=tenant_id,
                source_id=source_id,
                metadata=enriched_metadata,
                tick_id=tick_id,
                source_event_id=source_event_id,
                symbol=symbol,
                market=market,
                currency=currency,
                unit=unit,
                normalized_at=normalized_at_dt,
                source_system=source_system,
                tags=list(tags),
                taxonomy=taxonomy,
            )
            
            # Add enrichment metadata
            if taxonomy:
                enriched_tick.metadata.update({
                    "commodity": taxonomy.get("commodity"),
                    "region": taxonomy.get("region"),
                    "product_tier": taxonomy.get("product_tier"),
                    "classification_confidence": taxonomy.get("confidence"),
                    "classification_method": taxonomy.get("method"),
                })
            
            # Add instrument metadata if available
            if metadata_dict:
                enriched_tick.metadata.update({
                    "unit": metadata_dict.get("unit"),
                    "contract_size": metadata_dict.get("contract_size"),
                    "tick_size": metadata_dict.get("tick_size"),
                })
            
            # Add quality flags for enrichment
            if not metadata_dict:
                enriched_tick.add_quality_flag(QualityFlag.MISSING_METADATA)
            
            if taxonomy and taxonomy.get("confidence", 1.0) < self.confidence_threshold:
                # Add custom quality flag for low confidence
                enriched_tick.metadata["low_confidence_classification"] = True
            
            # Update metrics
            self.enrichment_count += 1
            self.last_enrichment_time = asyncio.get_event_loop().time() - start_time
            
            if taxonomy:
                logger.debug(
                    "Tick enriched successfully",
                    instrument_id=instrument_id,
                    commodity=taxonomy.get("commodity"),
                    confidence=taxonomy.get("confidence"),
                )
            else:
                logger.debug(
                    "Tick enriched without taxonomy classification",
                    instrument_id=instrument_id,
                )
            
            enriched_dict = enriched_tick.to_dict()
            # Ensure metadata contains primitives only
            if enriched_dict.get("metadata") is None:
                enriched_dict["metadata"] = {}
            if enriched_dict.get("taxonomy") is None and taxonomy:
                enriched_dict["taxonomy"] = taxonomy
            if enriched_dict.get("normalized_at") is None and normalized_at_dt:
                enriched_dict["normalized_at"] = normalized_at_dt.isoformat()

            return {
                "enriched": enriched_dict,
                "metadata": metadata_dict,
                "taxonomy": taxonomy,
            }
            
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
