"""
Hydro cascade mass-balance calculations for IRP modeling.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("hydro.mass_balance")


@dataclass
class ReservoirState:
    """State of a reservoir at a given time."""
    reservoir_id: str
    timestamp: datetime
    storage_af: float  # Acre-feet
    elevation_feet: float
    inflow_cfs: float  # Cubic feet per second
    release_cfs: float
    generation_mw: float
    head_feet: float
    efficiency_percent: float


@dataclass
class CascadeTopology:
    """Hydro cascade topology and relationships."""
    cascade_id: str
    reservoirs: Dict[str, Dict]  # reservoir_id -> reservoir_data
    upstream_downstream: Dict[str, List[str]]  # reservoir_id -> [upstream_ids, downstream_ids]


class MassBalanceCalculator:
    """Calculates mass-balance for hydro cascades."""

    def __init__(self):
        self.cascade_topologies: Dict[str, CascadeTopology] = {}

    def load_cascade_topology(self, cascade_data: Dict[str, Any]) -> None:
        """Load cascade topology from hydro system data."""
        cascade_id = cascade_data["cascade_id"]

        # Build reservoir lookup
        reservoirs = {}
        upstream_downstream = {}

        for reservoir in cascade_data["reservoirs"]:
            res_id = reservoir["reservoir_id"]
            reservoirs[res_id] = reservoir

            # Build upstream/downstream relationships
            upstream_downstream[res_id] = {
                "upstream": reservoir.get("upstream_reservoirs", []),
                "downstream": reservoir.get("downstream_reservoirs", [])
            }

        topology = CascadeTopology(
            cascade_id=cascade_id,
            reservoirs=reservoirs,
            upstream_downstream=upstream_downstream
        )

        self.cascade_topologies[cascade_id] = topology
        logger.info("Loaded cascade topology", cascade_id=cascade_id, reservoirs=len(reservoirs))

    def calculate_mass_balance(
        self,
        cascade_id: str,
        inflow_data: pd.DataFrame,
        initial_states: Dict[str, ReservoirState],
        time_step_hours: float = 1.0
    ) -> List[ReservoirState]:
        """
        Calculate mass-balance for a cascade over time.

        Args:
            cascade_id: Cascade identifier
            inflow_data: DataFrame with columns [timestamp, reservoir_id, inflow_cfs]
            initial_states: Initial reservoir states
            time_step_hours: Hours per time step

        Returns:
            List of reservoir states over time
        """
        if cascade_id not in self.cascade_topologies:
            raise ValueError(f"Cascade {cascade_id} not loaded")

        topology = self.cascade_topologies[cascade_id]
        logger.info("Calculating mass balance", cascade_id=cascade_id, steps=len(inflow_data))

        # Convert time step to seconds
        time_step_seconds = time_step_hours * 3600

        states = []
        current_states = initial_states.copy()

        # Group inflow data by timestamp
        inflow_by_time = inflow_data.groupby('timestamp')

        for timestamp, inflow_group in inflow_by_time:
            timestamp_states = {}

            # Process each reservoir in topological order
            processed = set()

            def process_reservoir(reservoir_id: str):
                if reservoir_id in processed:
                    return current_states[reservoir_id]

                reservoir = topology.reservoirs[reservoir_id]

                # Get inflow for this reservoir
                inflow_row = inflow_group[inflow_group['reservoir_id'] == reservoir_id]
                if len(inflow_row) > 0:
                    inflow_cfs = inflow_row.iloc[0]['inflow_cfs']
                else:
                    inflow_cfs = 0.0

                # Calculate upstream contributions
                upstream_contribution = 0.0
                for upstream_id in topology.upstream_downstream[reservoir_id]["upstream"]:
                    upstream_state = process_reservoir(upstream_id)
                    upstream_contribution += upstream_state.release_cfs

                # Total inflow to this reservoir
                total_inflow_cfs = inflow_cfs + upstream_contribution

                # Get current state
                current_state = current_states.get(reservoir_id)
                if current_state is None:
                    # Initialize with default values
                    current_state = ReservoirState(
                        reservoir_id=reservoir_id,
                        timestamp=timestamp,
                        storage_af=reservoir.get("active_storage_mcm", 0) * 1233.48,  # Convert MCM to AF
                        elevation_feet=reservoir.get("elevation_range_meters", {}).get("min_elevation_m", 0) * 3.28084,  # Convert m to ft
                        inflow_cfs=0.0,
                        release_cfs=0.0,
                        generation_mw=0.0,
                        head_feet=0.0,
                        efficiency_percent=85.0  # Default efficiency
                    )

                # Calculate new state
                storage_af = current_state.storage_af + (total_inflow_cfs * time_step_seconds) / 43560  # Convert cfs*seconds to acre-feet

                # Apply storage constraints
                max_storage_af = reservoir.get("active_storage_mcm", 0) * 1233.48
                min_storage_af = reservoir.get("dead_storage_mcm", 0) * 1233.48 if reservoir.get("dead_storage_mcm") else 0

                storage_af = max(min_storage_af, min(max_storage_af, storage_af))

                # Calculate elevation from storage using simplified relationship
                elevation_feet = self._storage_to_elevation(storage_af, reservoir)

                # Calculate head (simplified)
                head_feet = elevation_feet - reservoir.get("tailwater_elevation_feet", 0)

                # Calculate release based on rule curves and downstream requirements
                release_cfs = self._calculate_release(
                    reservoir_id, storage_af, elevation_feet, total_inflow_cfs, reservoir, topology
                )

                # Calculate generation
                generation_mw, efficiency_percent = self._calculate_generation(
                    reservoir_id, head_feet, release_cfs, reservoir
                )

                # Create new state
                new_state = ReservoirState(
                    reservoir_id=reservoir_id,
                    timestamp=timestamp,
                    storage_af=storage_af,
                    elevation_feet=elevation_feet,
                    inflow_cfs=total_inflow_cfs,
                    release_cfs=release_cfs,
                    generation_mw=generation_mw,
                    head_feet=head_feet,
                    efficiency_percent=efficiency_percent
                )

                timestamp_states[reservoir_id] = new_state
                processed.add(reservoir_id)

                return new_state

            # Process all reservoirs
            for reservoir_id in topology.reservoirs.keys():
                process_reservoir(reservoir_id)

            # Update current states
            current_states.update(timestamp_states)
            states.extend(timestamp_states.values())

        return states

    def _storage_to_elevation(self, storage_af: float, reservoir: Dict) -> float:
        """Convert storage to elevation (simplified linear relationship)."""
        capacity_af = reservoir.get("active_storage_mcm", 0) * 1233.48
        if capacity_af == 0:
            return 0.0

        min_elevation = reservoir.get("elevation_range_meters", {}).get("min_elevation_m", 0) * 3.28084
        max_elevation = reservoir.get("elevation_range_meters", {}).get("max_elevation_m", 0) * 3.28084

        # Linear interpolation between min and max elevation
        elevation_range = max_elevation - min_elevation
        storage_ratio = storage_af / capacity_af if capacity_af > 0 else 0

        return min_elevation + (elevation_range * storage_ratio)

    def _calculate_release(
        self,
        reservoir_id: str,
        storage_af: float,
        elevation_feet: float,
        inflow_cfs: float,
        reservoir: Dict,
        topology: CascadeTopology
    ) -> float:
        """Calculate reservoir release based on rule curves."""
        # Get seasonal rules
        seasonal_rules = reservoir.get("seasonal_rules", {})

        # Determine current season (simplified - use month)
        current_month = datetime.now().month
        is_winter = current_month in [12, 1, 2, 3]

        if is_winter and "winter_rule_curve" in seasonal_rules:
            rule = seasonal_rules["winter_rule_curve"]
        elif "summer_rule_curve" in seasonal_rules:
            rule = seasonal_rules["summer_rule_curve"]
        else:
            # Default rule - release inflow
            return inflow_cfs

        # Apply rule curve logic (simplified)
        target_storage_percent = rule.get("target_storage_percent", 50.0)
        capacity_af = reservoir.get("active_storage_mcm", 0) * 1233.48
        target_storage_af = capacity_af * (target_storage_percent / 100.0)

        min_release_cfs = rule.get("min_release_cfs", 0.0)
        max_release_cfs = rule.get("max_release_cfs", inflow_cfs * 2.0)

        # Simple rule: release to maintain target storage
        if storage_af > target_storage_af:
            # Release more than inflow to draw down
            release_cfs = min(inflow_cfs * 1.5, max_release_cfs)
        elif storage_af < target_storage_af:
            # Release less than inflow to fill up
            release_cfs = max(inflow_cfs * 0.8, min_release_cfs)
        else:
            # At target - release inflow
            release_cfs = inflow_cfs

        # Apply flood control if needed
        if "flood_control_curve" in seasonal_rules:
            flood_rule = seasonal_rules["flood_control_curve"]
            max_storage_percent = flood_rule.get("max_storage_percent", 90.0)

            if (storage_af / capacity_af * 100) > max_storage_percent:
                # Flood control - release more
                release_cfs = max(release_cfs, inflow_cfs * 1.2)

        return max(min_release_cfs, min(release_cfs, max_release_cfs))

    def _calculate_generation(
        self,
        reservoir_id: str,
        head_feet: float,
        release_cfs: float,
        reservoir: Dict
    ) -> Tuple[float, float]:
        """Calculate generation and efficiency."""
        if head_feet <= 0 or release_cfs <= 0:
            return 0.0, 0.0

        # Get efficiency curve if available
        efficiency_curve = reservoir.get("efficiency_curve", {})
        flow_points = efficiency_curve.get("flow_points_cfs", [])
        efficiency_values = efficiency_curve.get("efficiency_values_percent", [])

        if flow_points and efficiency_values:
            # Interpolate efficiency
            efficiency_percent = np.interp(release_cfs, flow_points, efficiency_values)
        else:
            # Default efficiency
            efficiency_percent = 85.0

        # Calculate generation (simplified formula)
        # Power = flow * head * efficiency * density * gravity
        # Using simplified constants
        generation_mw = (release_cfs * head_feet * efficiency_percent / 100) / 11.8  # Simplified conversion

        return max(0.0, generation_mw), efficiency_percent
