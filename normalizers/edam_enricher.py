"""
EDAM enricher for adding EDAM participation flags and transfer limits.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import structlog

from ..base.base_processor import BaseEnricher, EnrichmentResult
from ..base.exceptions import EnrichmentError


logger = structlog.get_logger("edam_enricher")


class EDAMEnricher(BaseEnricher):
    """Enricher for EDAM (Extended Day-Ahead Market) participation flags."""

    def __init__(self):
        super().__init__("edam_enricher")
        # EDAM configuration - in production this would come from a database
        self.edam_config = {
            "TH_NP15_GEN-APND": {
                "edam_eligible": True,
                "edam_participant": True,
                "edam_hurdle_rate_usd_per_mwh": 2.50,
                "edam_transfer_limit_mw": 1000,
                "edam_region": "NP15",
            },
            "TH_SP15_GEN-APND": {
                "edam_eligible": True,
                "edam_participant": True,
                "edam_hurdle_rate_usd_per_mwh": 2.50,
                "edam_transfer_limit_mw": 1200,
                "edam_region": "SP15",
            },
            "TH_ZP26_GEN-APND": {
                "edam_eligible": True,
                "edam_participant": True,
                "edam_hurdle_rate_usd_per_mwh": 3.00,
                "edam_transfer_limit_mw": 800,
                "edam_region": "ZP26",
            },
            "AS_NORTH": {
                "edam_eligible": True,
                "edam_participant": True,
                "edam_hurdle_rate_usd_per_mwh": 2.00,
                "edam_transfer_limit_mw": 500,
                "edam_region": "AS_NORTH",
            },
            "AS_SOUTH": {
                "edam_eligible": True,
                "edam_participant": True,
                "edam_hurdle_rate_usd_per_mwh": 2.00,
                "edam_transfer_limit_mw": 600,
                "edam_region": "AS_SOUTH",
            },
        }

    def get_edam_flags(self, location: str) -> Dict[str, Any]:
        """Get EDAM flags for a specific location."""
        # Check exact match first
        if location in self.edam_config:
            return self.edam_config[location].copy()

        # Check partial matches for regions
        for config_location, config in self.edam_config.items():
            if config_location in location or location in config_location:
                return config.copy()

        # Default: not EDAM eligible
        return {
            "edam_eligible": False,
            "edam_participant": False,
            "edam_hurdle_rate_usd_per_mwh": 0.0,
            "edam_transfer_limit_mw": 0,
            "edam_region": None,
        }

    async def enrich_records(
        self,
        silver_records: List[Dict[str, Any]]
    ) -> EnrichmentResult:
        """Enrich Silver records with EDAM flags."""
        try:
            self.logger.info(
                "Starting EDAM enrichment",
                record_count=len(silver_records)
            )

            enriched_records = []
            enrichment_applied = 0

            for record in silver_records:
                try:
                    # Get location identifier from record
                    location = self._get_location_from_record(record)

                    if location:
                        edam_flags = self.get_edam_flags(location)

                        # Add EDAM fields to record
                        enriched_record = record.copy()
                        enriched_record.update({
                            "edam_eligible": edam_flags["edam_eligible"],
                            "edam_participant": edam_flags["edam_participant"],
                            "edam_hurdle_rate_usd_per_mwh": edam_flags["edam_hurdle_rate_usd_per_mwh"],
                            "edam_transfer_limit_mw": edam_flags["edam_transfer_limit_mw"],
                            "edam_region": edam_flags["edam_region"],
                            "enriched_at": datetime.now(timezone.utc).isoformat(),
                            "enrichment_types": ["edam_flags"],
                        })

                        if edam_flags["edam_eligible"]:
                            enrichment_applied += 1

                        enriched_records.append(enriched_record)
                    else:
                        # No location found, add record as-is
                        enriched_records.append(record)

                except Exception as e:
                    self.logger.warning(
                        "Failed to enrich record",
                        record_id=record.get("record_id"),
                        error=str(e)
                    )
                    # Add record as-is if enrichment fails
                    enriched_records.append(record)

            result = EnrichmentResult(
                records=enriched_records,
                record_count=len(enriched_records),
                enrichment_applied=enrichment_applied,
                metadata={
                    "enrichment_type": "edam_flags",
                    "total_records": len(silver_records),
                    "edam_eligible_records": enrichment_applied,
                }
            )

            self.logger.info(
                "EDAM enrichment completed",
                enriched_count=result.record_count,
                enrichment_applied=enrichment_applied
            )

            return result

        except Exception as e:
            self.logger.error("Failed to enrich records with EDAM flags", error=str(e))
            raise EnrichmentError(f"EDAM enrichment failed: {e}") from e

    def _get_location_from_record(self, record: Dict[str, Any]) -> Optional[str]:
        """Extract location identifier from record for EDAM lookup."""
        # Try different location fields based on data type
        location_fields = [
            "node_id", "node", "location", "as_region",
            "crr_source", "crr_sink", "region"
        ]

        for field in location_fields:
            location = record.get(field)
            if location:
                return str(location)

        return None

    def get_enrichment_types(self) -> List[str]:
        """Get types of enrichment this enricher provides."""
        return ["edam_flags", "transfer_limits", "market_participation"]
