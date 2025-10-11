"""
CAISO OASIS normalizer for Silver layer processing.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

import structlog

from ..base.base_processor import BaseNormalizer, NormalizationResult
from ..base.exceptions import NormalizationError


logger = structlog.get_logger("caiso_oasis_normalizer")


class CAISOOASISNormalizer(BaseNormalizer):
    """Normalizer for CAISO OASIS data (LMP, AS, CRR)."""

    def __init__(self):
        super().__init__("caiso_oasis_normalizer")
        self.silver_topics = {
            "lmp_dam": "normalized.market.caiso.lmp.dam.v1",
            "lmp_fmm": "normalized.market.caiso.lmp.fmm.v1",
            "lmp_rtm": "normalized.market.caiso.lmp.rtm.v1",
            "as_dam": "normalized.market.caiso.as.dam.v1",
            "as_fmm": "normalized.market.caiso.as.fmm.v1",
            "as_rtm": "normalized.market.caiso.as.rtm.v1",
            "crr": "normalized.market.caiso.crr.v1",
        }

    def normalize_lmp_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize LMP record for Silver layer."""
        normalized = {
            # Primary keys
            "timestamp": record.get("timestamp"),
            "market_run_id": record.get("market_run_id"),
            "node_id": record.get("node"),
            "ba_id": "CAISO",

            # Price components (USD/MWh)
            "lmp_total": self._safe_float(record.get("lmp_usd_per_mwh")),
            "lmp_energy": self._safe_float(record.get("lmp_energy_usd_per_mwh")),
            "lmp_congestion": self._safe_float(record.get("lmp_congestion_usd_per_mwh")),
            "lmp_losses": self._safe_float(record.get("lmp_marginal_loss_usd_per_mwh")),

            # Metadata
            "data_type": record.get("data_type"),
            "source": record.get("source"),
            "extracted_at": record.get("extracted_at"),
            "bronze_metadata": record.get("bronze_metadata", {}),
        }

        # Add EDAM flags if available
        edam_flags = self._get_edam_flags(record.get("node"))
        normalized.update(edam_flags)

        return normalized

    def normalize_as_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Ancillary Services record for Silver layer."""
        normalized = {
            # Primary keys
            "timestamp": record.get("timestamp"),
            "market_run_id": record.get("market_run_id"),
            "as_type": record.get("as_type"),
            "as_region": record.get("as_region"),
            "ba_id": "CAISO",

            # AS data (USD/MW and MW)
            "as_clearing_price_usd_per_mw": self._safe_float(record.get("clearing_price_usd_per_mw")),
            "as_cleared_mw": self._safe_float(record.get("cleared_mw")),

            # Metadata
            "data_type": record.get("data_type"),
            "source": record.get("source"),
            "extracted_at": record.get("extracted_at"),
            "bronze_metadata": record.get("bronze_metadata", {}),
        }

        # Add EDAM flags if available
        edam_flags = self._get_edam_flags(record.get("as_region"))
        normalized.update(edam_flags)

        return normalized

    def normalize_crr_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CRR record for Silver layer."""
        normalized = {
            # Primary keys
            "timestamp": record.get("timestamp"),
            "crr_type": record.get("crr_type"),
            "source": record.get("source"),
            "sink": record.get("sink"),
            "ba_id": "CAISO",

            # CRR data (MW and USD/MW)
            "crr_mw": self._safe_float(record.get("mw")),
            "crr_price_usd_per_mw": self._safe_float(record.get("price_usd_per_mw")),

            # Metadata
            "data_type": record.get("data_type"),
            "source": record.get("source"),
            "extracted_at": record.get("extracted_at"),
            "bronze_metadata": record.get("bronze_metadata", {}),
        }

        return normalized

    def _get_edam_flags(self, location: Optional[str]) -> Dict[str, Any]:
        """Get EDAM participation flags for a location."""
        # This would query the EDAM flags service in a real implementation
        # For now, return placeholder flags based on location
        flags = {
            "edam_eligible": False,
            "edam_participant": False,
            "edam_hurdle_rate_usd_per_mwh": 0.0,
            "edam_transfer_limit_mw": 0,
        }

        if location:
            # Placeholder logic - in reality this would come from EDAM configuration
            if "TH_" in str(location) or "NP15" in str(location):
                flags.update({
                    "edam_eligible": True,
                    "edam_participant": True,
                    "edam_hurdle_rate_usd_per_mwh": 2.50,
                    "edam_transfer_limit_mw": 1000,
                })

        return flags

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float, return None if invalid."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def normalize_records(
        self,
        bronze_records: List[Dict[str, Any]]
    ) -> NormalizationResult:
        """Normalize Bronze records to Silver format."""
        try:
            self.logger.info(
                "Starting CAISO OASIS normalization",
                record_count=len(bronze_records)
            )

            normalized_records = []
            validation_errors = []

            for record in bronze_records:
                try:
                    data_type = record.get("data_type", "")

                    if data_type.startswith("lmp_"):
                        normalized = self.normalize_lmp_record(record)
                    elif data_type.startswith("as_"):
                        normalized = self.normalize_as_record(record)
                    elif data_type == "crr":
                        normalized = self.normalize_crr_record(record)
                    else:
                        validation_errors.append(f"Unknown data type: {data_type}")
                        continue

                    # Validate required fields
                    if not normalized.get("timestamp"):
                        validation_errors.append(f"Missing timestamp in record: {record.get('timestamp')}")
                        continue

                    normalized_records.append(normalized)

                except Exception as e:
                    validation_errors.append(f"Error normalizing record: {str(e)}")
                    continue

            # Calculate quality metrics
            success_rate = len(normalized_records) / max(1, len(bronze_records))

            result = NormalizationResult(
                records=normalized_records,
                record_count=len(normalized_records),
                validation_errors=validation_errors,
                metadata={
                    "normalization_type": "caiso_oasis_silver",
                    "original_records": len(bronze_records),
                    "success_rate": success_rate,
                    "edam_flags_applied": sum(1 for r in normalized_records if r.get("edam_eligible")),
                }
            )

            self.logger.info(
                "CAISO OASIS normalization completed",
                normalized_count=result.record_count,
                success_rate=success_rate,
                validation_errors=len(validation_errors)
            )

            return result

        except Exception as e:
            self.logger.error("Failed to normalize CAISO OASIS data", error=str(e))
            raise NormalizationError(f"CAISO OASIS normalization failed: {e}") from e

    def get_output_topics(self) -> Dict[str, str]:
        """Get Silver output topics for different data types."""
        return self.silver_topics.copy()

    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate a normalized record."""
        errors = []

        # Check required fields based on data type
        data_type = record.get("data_type", "")
        timestamp = record.get("timestamp")
        ba_id = record.get("ba_id")

        if not timestamp:
            errors.append("Missing timestamp")
        if not ba_id:
            errors.append("Missing ba_id")
        if not data_type:
            errors.append("Missing data_type")

        # Validate numeric fields are actually numeric
        numeric_fields = [
            "lmp_total", "lmp_energy", "lmp_congestion", "lmp_losses",
            "as_clearing_price_usd_per_mw", "as_cleared_mw",
            "crr_mw", "crr_price_usd_per_mw"
        ]

        for field in numeric_fields:
            if field in record and record[field] is not None:
                try:
                    float(record[field])
                except (ValueError, TypeError):
                    errors.append(f"Invalid numeric value for {field}: {record[field]}")

        return errors
