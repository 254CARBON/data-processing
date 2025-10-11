"""
WECC transmission paths loader for processing transmission data into unified catalog.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import structlog

from ..base.base_processor import BaseAggregator
from ..base.exceptions import NormalizationError


logger = structlog.get_logger("wecc_paths_loader")


class WECCPathsLoader(BaseAggregator):
    """Loader for WECC transmission paths data into unified catalog."""

    def __init__(self, clickhouse_client):
        super().__init__("wecc_paths_loader")
        self.clickhouse = clickhouse_client

    async def load_wecc_paths_data(self, paths_data: Dict[str, Any]) -> Dict[str, int]:
        """Load WECC transmission paths data into ClickHouse tables."""
        results = {}

        # Load transmission paths
        if "transmission_paths" in paths_data:
            paths_records = []
            for path in paths_data["transmission_paths"]:
                try:
                    path_record = {
                        "path_id": path.get("path_id"),
                        "path_name": path.get("path_name"),
                        "wecc_path_number": path.get("wecc_path_number"),
                        "source_ba": path.get("source_ba"),
                        "sink_ba": path.get("sink_ba"),
                        "source_zone": path.get("source_zone"),
                        "sink_zone": path.get("sink_zone"),
                        "ttc_mw": self._safe_float(path.get("ttc_mw")),
                        "ntc_mw": self._safe_float(path.get("ntc_mw")),
                        "atc_mw": self._safe_float(path.get("atc_mw")),
                        "loss_factor_percent": self._safe_float(path.get("loss_factor_percent")),
                        "loss_factor_direction": path.get("loss_factor_direction", "bidirectional"),
                        "wheeling_rate_usd_per_mwh": self._safe_float(path.get("wheeling_rate_usd_per_mwh")),
                        "edam_eligible": path.get("edam_eligible", False),
                        "edam_participant": path.get("edam_participant", False),
                        "edam_hurdle_rate_usd_per_mwh": self._safe_float(path.get("edam_hurdle_rate_usd_per_mwh")),
                        "edam_transfer_limit_mw": self._safe_int(path.get("edam_transfer_limit_mw")),
                        "scheduling_priority": self._safe_int(path.get("scheduling_priority", 0)),
                        "operational_status": path.get("operational_status", "operational"),
                        "outage_start": path.get("outage_start"),
                        "outage_end": path.get("outage_end"),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    paths_records.append(path_record)
                except Exception as e:
                    self.logger.warning(
                        "Failed to process transmission path",
                        path_id=path.get("path_id"),
                        error=str(e)
                    )

            if paths_records:
                await self._insert_transmission_paths(paths_records)
                results["transmission_paths_loaded"] = len(paths_records)

        # Load interties
        if "interties" in paths_data:
            interties_records = []
            for intertie in paths_data["interties"]:
                try:
                    intertie_record = {
                        "intertie_id": intertie.get("intertie_id"),
                        "intertie_name": intertie.get("intertie_name"),
                        "source_ba": intertie.get("source_ba"),
                        "sink_ba": intertie.get("sink_ba"),
                        "ttc_mw": self._safe_float(intertie.get("ttc_mw")),
                        "ntc_mw": self._safe_float(intertie.get("ntc_mw")),
                        "atc_mw": self._safe_float(intertie.get("atc_mw")),
                        "loss_factor_percent": self._safe_float(intertie.get("loss_factor_percent")),
                        "wheeling_rate_usd_per_mwh": self._safe_float(intertie.get("wheeling_rate_usd_per_mwh")),
                        "scheduling_priority": self._safe_int(intertie.get("scheduling_priority", 0)),
                        "edam_eligible": intertie.get("edam_eligible", False),
                        "edam_participant": intertie.get("edam_participant", False),
                        "edam_hurdle_rate_usd_per_mwh": self._safe_float(intertie.get("edam_hurdle_rate_usd_per_mwh")),
                        "operational_status": intertie.get("operational_status", "operational"),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    interties_records.append(intertie_record)
                except Exception as e:
                    self.logger.warning(
                        "Failed to process intertie",
                        intertie_id=intertie.get("intertie_id"),
                        error=str(e)
                    )

            if interties_records:
                await self._insert_interties(interties_records)
                results["interties_loaded"] = len(interties_records)

        # Load wheeling contracts
        if "wheeling_contracts" in paths_data:
            contracts_records = []
            for contract in paths_data["wheeling_contracts"]:
                try:
                    contract_record = {
                        "contract_id": contract.get("contract_id"),
                        "contract_name": contract.get("contract_name"),
                        "source_ba": contract.get("source_ba"),
                        "sink_ba": contract.get("sink_ba"),
                        "path_id": contract.get("path_id"),
                        "capacity_mw": self._safe_float(contract.get("capacity_mw")),
                        "rate_usd_per_mwh": self._safe_float(contract.get("rate_usd_per_mwh")),
                        "term_start": contract.get("term_start"),
                        "term_end": contract.get("term_end"),
                        "priority_level": self._safe_int(contract.get("priority_level", 0)),
                        "firm_contract": contract.get("firm_contract", False),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    contracts_records.append(contract_record)
                except Exception as e:
                    self.logger.warning(
                        "Failed to process wheeling contract",
                        contract_id=contract.get("contract_id"),
                        error=str(e)
                    )

            if contracts_records:
                await self._insert_wheeling_contracts(contracts_records)
                results["wheeling_contracts_loaded"] = len(contracts_records)

        # Load EDAM configurations
        if "edam_configurations" in paths_data:
            edam_records = []
            for edam in paths_data["edam_configurations"]:
                try:
                    edam_record = {
                        "ba_id": edam.get("ba_id"),
                        "zone_id": edam.get("zone_id"),
                        "node_id": edam.get("node_id"),
                        "edam_eligible": edam.get("edam_eligible", False),
                        "edam_participant": edam.get("edam_participant", False),
                        "edam_region": edam.get("edam_region"),
                        "edam_hurdle_rate_usd_per_mwh": self._safe_float(edam.get("edam_hurdle_rate_usd_per_mwh")),
                        "edam_transfer_limit_mw": self._safe_int(edam.get("edam_transfer_limit_mw")),
                        "edam_greenhouse_gas_rate_usd_per_ton": self._safe_float(edam.get("edam_greenhouse_gas_rate_usd_per_ton")),
                        "edam_min_transfer_mw": self._safe_float(edam.get("edam_min_transfer_mw")),
                        "edam_max_transfer_mw": self._safe_float(edam.get("edam_max_transfer_mw")),
                        "effective_date": edam.get("effective_date"),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    edam_records.append(edam_record)
                except Exception as e:
                    self.logger.warning(
                        "Failed to process EDAM configuration",
                        ba_id=edam.get("ba_id"),
                        error=str(e)
                    )

            if edam_records:
                await self._insert_edam_configurations(edam_records)
                results["edam_configurations_loaded"] = len(edam_records)

        return results

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    async def _insert_transmission_paths(self, records: List[Dict[str, Any]]) -> None:
        """Insert transmission paths into ClickHouse."""
        try:
            await self.clickhouse.insert(
                "markets.wecc_transmission_paths",
                records
            )
            self.logger.info("Inserted transmission paths", count=len(records))
        except Exception as e:
            self.logger.error("Failed to insert transmission paths", error=str(e))
            raise

    async def _insert_interties(self, records: List[Dict[str, Any]]) -> None:
        """Insert interties into ClickHouse."""
        try:
            await self.clickhouse.insert(
                "markets.wecc_interties",
                records
            )
            self.logger.info("Inserted interties", count=len(records))
        except Exception as e:
            self.logger.error("Failed to insert interties", error=str(e))
            raise

    async def _insert_wheeling_contracts(self, records: List[Dict[str, Any]]) -> None:
        """Insert wheeling contracts into ClickHouse."""
        try:
            await self.clickhouse.insert(
                "markets.wecc_wheeling_contracts",
                records
            )
            self.logger.info("Inserted wheeling contracts", count=len(records))
        except Exception as e:
            self.logger.error("Failed to insert wheeling contracts", error=str(e))
            raise

    async def _insert_edam_configurations(self, records: List[Dict[str, Any]]) -> None:
        """Insert EDAM configurations into ClickHouse."""
        try:
            await self.clickhouse.insert(
                "markets.wecc_edam_configurations",
                records
            )
            self.logger.info("Inserted EDAM configurations", count=len(records))
        except Exception as e:
            self.logger.error("Failed to insert EDAM configurations", error=str(e))
            raise

    async def aggregate_records(
        self,
        gold_records: List[Dict[str, Any]],
        time_window: str = "1h"
    ) -> List[Dict[str, Any]]:
        """Aggregate Gold records - placeholder implementation."""
        return gold_records

    def get_aggregation_types(self) -> List[str]:
        """Get types of aggregation this aggregator provides."""
        return ["transmission_summary", "edam_participation", "wheeling_contracts"]
