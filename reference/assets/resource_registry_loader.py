"""
Resource registry loader for processing EIA data into unified asset catalog.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import structlog

from ..base.base_processor import BaseAggregator, EnrichmentResult
from ..base.exceptions import NormalizationError


logger = structlog.get_logger("resource_registry_loader")


class ResourceRegistryLoader(BaseAggregator):
    """Loader for EIA data into unified resource registry."""

    def __init__(self, clickhouse_client):
        super().__init__("resource_registry_loader")
        self.clickhouse = clickhouse_client

    async def load_eia_860_data(self, eia_860_records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load EIA 860 generator data into thermal resources table."""
        thermal_records = []

        for record in eia_860_records:
            try:
                thermal_record = {
                    "resource_id": f"EIA860_{record.get('plant_code', '')}_{record.get('generator_id', '')}",
                    "plant_code": record.get("plant_code", ""),
                    "unit_code": record.get("generator_id", ""),
                    "resource_name": record.get("plant_name", f"Plant {record.get('plant_code', '')}"),
                    "ba_id": self._map_eia_to_ba(record.get("state", "")),
                    "zone_id": self._map_state_to_zone(record.get("state", "")),
                    "node_id": self._derive_node_id(record),
                    "technology_type": self._map_eia_tech_to_standard(record.get("technology", "")),
                    "fuel_type": self._map_eia_fuel_to_standard(record.get("fuel_type", "")),
                    "nameplate_capacity_mw": self._safe_float(record.get("nameplate_capacity_mw")),
                    "summer_capacity_mw": self._safe_float(record.get("summer_capacity_mw")),
                    "winter_capacity_mw": self._safe_float(record.get("winter_capacity_mw")),
                    "heat_rate_mmbtu_per_mwh": self._calculate_heat_rate(record),
                    "variable_cost_usd_per_mwh": self._calculate_variable_cost(record),
                    "startup_cost_usd": self._calculate_startup_cost(record),
                    "min_up_time_hours": self._safe_float(record.get("min_up_time_hours", 4)),
                    "min_down_time_hours": self._safe_float(record.get("min_down_time_hours", 4)),
                    "ramp_rate_mw_per_min": self._calculate_ramp_rate(record),
                    "eford_percent": self._safe_float(record.get("eford_percent", 8.0)),
                    "co2_tons_per_mwh": self._calculate_co2_emissions(record),
                    "nox_lbs_per_mwh": self._calculate_nox_emissions(record),
                    "so2_lbs_per_mwh": self._calculate_so2_emissions(record),
                    "interconnection_status": self._map_status(record.get("status", "operational")),
                    "commercial_operation_date": record.get("commercial_operation_date"),
                    "retirement_date": record.get("retirement_date"),
                    "elcc_class": self._derive_elcc_class(record),
                    "elcc_factor": self._calculate_elcc_factor(record),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

                thermal_records.append(thermal_record)

            except Exception as e:
                self.logger.warning(
                    "Failed to process EIA 860 record",
                    plant_code=record.get("plant_code"),
                    error=str(e)
                )

        # Batch insert to ClickHouse
        if thermal_records:
            await self._insert_thermal_resources(thermal_records)

        return {"thermal_records_loaded": len(thermal_records)}

    async def load_eia_861_data(self, eia_861_records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load EIA 861 utility data into DER programs table."""
        der_records = []

        for record in eia_861_records:
            try:
                # Create DER program entries for demand response and efficiency programs
                if record.get("has_demand_response", False):
                    der_records.append({
                        "program_id": f"EIA861_DR_{record.get('utility_id', '')}_{record.get('state', '')}",
                        "program_name": f"Demand Response - {record.get('utility_name', '')}",
                        "ba_id": self._map_eia_to_ba(record.get("state", "")),
                        "zone_id": self._map_state_to_zone(record.get("state", "")),
                        "program_type": "demand_response",
                        "potential_capacity_mw": self._calculate_dr_potential(record),
                        "enrolled_capacity_mw": self._calculate_dr_enrolled(record),
                        "dispatchable_mw": self._calculate_dr_dispatchable(record),
                        "persistence_factor": 0.85,  # Typical DR persistence
                        "response_time_minutes": 30,  # Typical DR response time
                        "availability_hours_per_day": 4,  # Typical DR availability
                        "cost_usd_per_mw": self._calculate_dr_cost(record),
                        "trc_ratio": 1.5,  # Typical TRC ratio
                        "pac_ratio": 2.0,  # Typical PAC ratio
                        "customer_utility_split_percent": 50,
                        "wholesale_participation_eligible": True,
                        "dual_use_accounting": False,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })

            except Exception as e:
                self.logger.warning(
                    "Failed to process EIA 861 record",
                    utility_id=record.get("utility_id"),
                    error=str(e)
                )

        # Batch insert to ClickHouse
        if der_records:
            await self._insert_der_programs(der_records)

        return {"der_records_loaded": len(der_records)}

    async def load_eia_923_data(self, eia_923_records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load EIA 923 fuel consumption data to enhance thermal resources."""
        # This would typically be used to update existing thermal resources
        # with actual fuel consumption data
        updates = []

        for record in eia_923_records:
            try:
                resource_id = f"EIA860_{record.get('plant_code', '')}_{record.get('generator_id', '')}"

                update = {
                    "resource_id": resource_id,
                    "fuel_consumed_mmbtu": self._safe_float(record.get("fuel_consumed_mmbtu")),
                    "net_generation_mwh": self._safe_float(record.get("net_generation_mwh")),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

                updates.append(update)

            except Exception as e:
                self.logger.warning(
                    "Failed to process EIA 923 record",
                    plant_code=record.get("plant_code"),
                    error=str(e)
                )

        # Batch update thermal resources with fuel consumption data
        if updates:
            await self._update_thermal_resources_fuel_data(updates)

        return {"thermal_updates_applied": len(updates)}

    def _map_eia_to_ba(self, state: str) -> str:
        """Map EIA state codes to BA IDs."""
        state_to_ba = {
            "CA": "CAISO",
            "NV": "NEVP",
            "AZ": "AZPS",
            "NM": "PNM",
            "CO": "WACM",
            "UT": "PACW",
            "ID": "IPCO",
            "OR": "BPAT",
            "WA": "BPA",
        }
        return state_to_ba.get(state, "UNKNOWN")

    def _map_state_to_zone(self, state: str) -> str:
        """Map EIA state codes to load zones."""
        state_to_zone = {
            "CA": "CAISO_SP15",  # Simplified mapping
            "NV": "NEVP",
            "AZ": "AZPS",
            "NM": "PNM",
            "CO": "WACM",
            "UT": "PACW",
            "ID": "IPCO",
            "OR": "PACW",
            "WA": "BPA",
        }
        return state_to_zone.get(state, "UNKNOWN")

    def _derive_node_id(self, record: Dict[str, Any]) -> str:
        """Derive node ID from plant location."""
        plant_code = record.get("plant_code", "")
        state = record.get("state", "")
        return f"{plant_code}_{state}"

    def _map_eia_tech_to_standard(self, eia_tech: str) -> str:
        """Map EIA technology codes to standard categories."""
        tech_mapping = {
            "CC": "combined_cycle",
            "CT": "simple_cycle",
            "ST": "steam",
            "NU": "nuclear",
            "HY": "hydro",
            "PV": "solar_pv",
            "WT": "wind_onshore",
            "WS": "wind_offshore",
            "BT": "battery",
            "PS": "pumped_storage",
        }
        return tech_mapping.get(eia_tech, "unknown")

    def _map_eia_fuel_to_standard(self, eia_fuel: str) -> str:
        """Map EIA fuel codes to standard categories."""
        fuel_mapping = {
            "NG": "natural_gas",
            "COL": "coal",
            "NUC": "nuclear",
            "OIL": "oil",
            "WND": "wind",
            "SUN": "solar",
            "WAT": "hydro",
            "BIO": "biomass",
            "GEO": "geothermal",
            "WST": "waste",
        }
        return fuel_mapping.get(eia_fuel, "other")

    def _calculate_heat_rate(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate heat rate from EIA data."""
        # Placeholder calculation - would use actual EIA formulas
        return self._safe_float(record.get("heat_rate", 10.0))

    def _calculate_variable_cost(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate variable O&M cost."""
        # Placeholder - would use actual cost data
        return 5.0  # $5/MWh default

    def _calculate_startup_cost(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate startup cost."""
        # Placeholder - would use actual startup cost data
        return 10000.0  # $10k default

    def _calculate_ramp_rate(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate ramp rate."""
        # Placeholder - would use actual ramp rate data
        return 10.0  # 10 MW/min default

    def _calculate_co2_emissions(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate CO2 emissions factor."""
        fuel_type = record.get("fuel_type", "")
        if fuel_type == "natural_gas":
            return 0.4  # tons CO2/MWh
        elif fuel_type == "coal":
            return 0.9  # tons CO2/MWh
        return 0.0

    def _calculate_nox_emissions(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate NOx emissions factor."""
        fuel_type = record.get("fuel_type", "")
        if fuel_type == "natural_gas":
            return 0.3  # lbs NOx/MWh
        elif fuel_type == "coal":
            return 1.5  # lbs NOx/MWh
        return 0.0

    def _calculate_so2_emissions(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate SO2 emissions factor."""
        fuel_type = record.get("fuel_type", "")
        if fuel_type == "coal":
            return 2.0  # lbs SO2/MWh
        return 0.0

    def _map_status(self, status: str) -> str:
        """Map EIA status to standard categories."""
        status_mapping = {
            "OP": "operational",
            "OS": "operational",
            "OA": "operational",
            "SB": "standby",
            "RE": "retired",
            "PL": "planned",
            "UC": "under_construction",
        }
        return status_mapping.get(status, "operational")

    def _derive_elcc_class(self, record: Dict[str, Any]) -> str:
        """Derive ELCC class from technology and location."""
        tech = record.get("technology_type", "")
        state = record.get("state", "")

        if tech == "wind_onshore":
            return f"wind_{state.lower()}"
        elif tech == "solar_pv":
            return f"solar_{state.lower()}"
        elif tech == "combined_cycle":
            return "ccgt"
        elif tech == "nuclear":
            return "nuclear"
        return "other"

    def _calculate_elcc_factor(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate ELCC factor based on technology and location."""
        tech = record.get("technology_type", "")
        state = record.get("state", "")

        if tech == "nuclear":
            return 0.95
        elif tech == "coal":
            return 0.90
        elif tech == "combined_cycle":
            return 0.85
        elif tech == "wind_onshore":
            if state in ["CA", "TX"]:
                return 0.40
            return 0.35
        elif tech == "solar_pv":
            if state in ["CA", "AZ", "NV"]:
                return 0.50
            return 0.40
        return 0.80  # Default

    def _calculate_dr_potential(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate demand response potential."""
        # Placeholder - would use actual DR potential calculations
        return 50.0  # 50 MW default

    def _calculate_dr_enrolled(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate enrolled DR capacity."""
        # Placeholder - would use actual enrollment data
        return 25.0  # 25 MW default

    def _calculate_dr_dispatchable(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate dispatchable DR capacity."""
        # Placeholder - would use actual dispatch data
        return 20.0  # 20 MW default

    def _calculate_dr_cost(self, record: Dict[str, Any]) -> Optional[float]:
        """Calculate DR program cost."""
        # Placeholder - would use actual cost data
        return 100.0  # $100/MW default

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _insert_thermal_resources(self, records: List[Dict[str, Any]]) -> None:
        """Insert thermal resources into ClickHouse."""
        try:
            # Use the existing ClickHouse client to insert
            await self.clickhouse.insert(
                "assets.resource_registry_thermal",
                records
            )
            self.logger.info("Inserted thermal resources", count=len(records))
        except Exception as e:
            self.logger.error("Failed to insert thermal resources", error=str(e))
            raise

    async def _insert_der_programs(self, records: List[Dict[str, Any]]) -> None:
        """Insert DER programs into ClickHouse."""
        try:
            await self.clickhouse.insert(
                "assets.resource_registry_der_programs",
                records
            )
            self.logger.info("Inserted DER programs", count=len(records))
        except Exception as e:
            self.logger.error("Failed to insert DER programs", error=str(e))
            raise

    async def _update_thermal_resources_fuel_data(self, updates: List[Dict[str, Any]]) -> None:
        """Update thermal resources with fuel consumption data."""
        # This would implement UPDATE queries for existing thermal resources
        # For now, just log the updates
        self.logger.info("Would update thermal resources with fuel data", count=len(updates))

    async def aggregate_records(
        self,
        gold_records: List[Dict[str, Any]],
        time_window: str = "1h"
    ) -> List[Dict[str, Any]]:
        """Aggregate Gold records - placeholder implementation."""
        return gold_records

    def get_aggregation_types(self) -> List[str]:
        """Get types of aggregation this aggregator provides."""
        return ["resource_summary", "capacity_by_zone", "emissions_summary"]
