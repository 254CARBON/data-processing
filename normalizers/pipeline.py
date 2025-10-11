"""
Normalization and enrichment pipeline for Bronze → Silver → Gold processing.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import structlog

from ..base.base_processor import BaseNormalizer, BaseEnricher, NormalizationResult, EnrichmentResult
from ..base.exceptions import NormalizationError, EnrichmentError
from .caiso_oasis_normalizer import CAISOOASISNormalizer
from .edam_enricher import EDAMEnricher


logger = structlog.get_logger("normalization_pipeline")


class NormalizationPipeline:
    """Pipeline for normalizing Bronze data to Gold with enrichment."""

    def __init__(self):
        self.normalizers = {}
        self.enrichers = {}
        self.logger = logger.bind(pipeline="bronze_to_gold")
        self._initialize_components()

    def _initialize_components(self):
        """Initialize normalizers and enrichers."""
        # Normalizers
        self.normalizers["caiso_oasis"] = CAISOOASISNormalizer()

        # Enrichers
        self.enrichers["edam"] = EDAMEnricher()

    async def process_bronze_to_gold(
        self,
        bronze_records: List[Dict[str, Any]],
        data_type: str
    ) -> Dict[str, Any]:
        """Process Bronze records through normalization and enrichment to Gold."""
        try:
            self.logger.info(
                "Starting Bronze to Gold pipeline",
                record_count=len(bronze_records),
                data_type=data_type
            )

            # Step 1: Normalize Bronze → Silver
            silver_result = await self._normalize_to_silver(bronze_records, data_type)

            if silver_result.record_count == 0:
                return {
                    "status": "failed",
                    "stage": "normalization",
                    "error": "No records survived normalization",
                    "bronze_records": len(bronze_records),
                    "silver_records": 0,
                    "gold_records": 0,
                }

            # Step 2: Enrich Silver → Gold
            gold_result = await self._enrich_to_gold(silver_result.records, data_type)

            # Step 3: Prepare final result
            result = {
                "status": "success",
                "bronze_records": len(bronze_records),
                "silver_records": silver_result.record_count,
                "gold_records": gold_result.record_count,
                "normalization_errors": len(silver_result.validation_errors),
                "enrichment_applied": gold_result.enrichment_applied,
                "silver_records_data": silver_result.records,
                "silver_validation_errors": silver_result.validation_errors,
                "gold_records_data": gold_result.records,
                "pipeline_metadata": {
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "data_type": data_type,
                    "normalization_success_rate": silver_result.metadata.get("success_rate", 0),
                    "enrichment_types": gold_result.metadata.get("enrichment_type", ""),
                }
            }

            self.logger.info(
                "Bronze to Gold pipeline completed",
                bronze_count=result["bronze_records"],
                silver_count=result["silver_records"],
                gold_count=result["gold_records"]
            )

            return result

        except Exception as e:
            self.logger.error("Pipeline failed", error=str(e))
            return {
                "status": "failed",
                "stage": "pipeline",
                "error": str(e),
                "bronze_records": len(bronze_records),
                "silver_records": 0,
                "gold_records": 0,
            }

    async def _normalize_to_silver(
        self,
        bronze_records: List[Dict[str, Any]],
        data_type: str
    ) -> NormalizationResult:
        """Normalize Bronze records to Silver format."""
        # Route to appropriate normalizer based on data type
        if data_type.startswith(("lmp_", "as_", "crr")):
            normalizer = self.normalizers.get("caiso_oasis")
        else:
            raise NormalizationError(f"No normalizer found for data type: {data_type}")

        if not normalizer:
            raise NormalizationError(f"Normalizer not initialized for data type: {data_type}")

        return await normalizer.normalize_records(bronze_records)

    async def _enrich_to_gold(
        self,
        silver_records: List[Dict[str, Any]],
        data_type: str
    ) -> EnrichmentResult:
        """Enrich Silver records to Gold format."""
        # Apply EDAM enrichment for market data
        if data_type.startswith(("lmp_", "as_")):
            enricher = self.enrichers.get("edam")
            if enricher:
                return await enricher.enrich_records(silver_records)

        # No enrichment applied, return as-is
        return EnrichmentResult(
            records=silver_records,
            record_count=len(silver_records),
            enrichment_applied=0,
            metadata={"enrichment_type": "none"}
        )

    def get_supported_data_types(self) -> List[str]:
        """Get list of supported data types for normalization."""
        supported_types = []

        for normalizer in self.normalizers.values():
            # This would need to be implemented in each normalizer
            # For now, return known types
            if isinstance(normalizer, CAISOOASISNormalizer):
                supported_types.extend([
                    "lmp_dam", "lmp_fmm", "lmp_rtm",
                    "as_dam", "as_fmm", "as_rtm",
                    "crr"
                ])

        return supported_types

    def get_gold_topics(self) -> Dict[str, str]:
        """Get Gold output topics for different data types."""
        gold_topics = {}

        for normalizer in self.normalizers.values():
            output_topics = normalizer.get_output_topics()
            for data_type, topic in output_topics.items():
                # Transform Silver topics to Gold topics
                gold_topic = topic.replace("normalized.market.", "gold.market.")
                gold_topics[data_type] = gold_topic

        return gold_topics
