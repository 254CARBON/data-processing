"""
Hydro cascade mass-balance calculations for IRP modeling.
"""

import logging
import math
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
    upstream_downstream: Dict[str, Dict[str, List[str]]]  # reservoir_id -> {upstream: [...], downstream: [...]}


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
        cascade_level_rules = cascade_data.get("seasonal_rules") or {}

        for reservoir in cascade_data["reservoirs"]:
            res_copy = dict(reservoir)
            res_id = res_copy["reservoir_id"]

            # Normalise seasonal rules so each reservoir has rule definitions available
            if "seasonal_rules" not in res_copy or not res_copy["seasonal_rules"]:
                res_copy["seasonal_rules"] = cascade_level_rules

            # Provide sane defaults for optional metadata
            res_copy.setdefault("reservoir_type", "storage")
            if "tailwater_elevation_feet" not in res_copy:
                tailwater_m = res_copy.get("tailwater_elevation_m")
                res_copy["tailwater_elevation_feet"] = (tailwater_m or 0.0) * 3.28084

            reservoirs[res_id] = res_copy

            # Build upstream/downstream relationships
            upstream_downstream[res_id] = {
                "upstream": list(res_copy.get("upstream_reservoirs", [])),
                "downstream": list(res_copy.get("downstream_reservoirs", [])),
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

        inflow_df = inflow_data.copy()
        if not inflow_df.empty:
            inflow_df["timestamp"] = pd.to_datetime(inflow_df["timestamp"], utc=True, errors="coerce")
            inflow_df = inflow_df.dropna(subset=["timestamp"])
            inflow_df.sort_values(["timestamp", "reservoir_id"], inplace=True)
        else:
            return []

        # Convert time step to seconds
        time_step_seconds = time_step_hours * 3600

        states = []
        current_states: Dict[str, ReservoirState] = initial_states.copy()

        # Group inflow data by timestamp
        inflow_by_time = inflow_df.groupby("timestamp", sort=True)

        # Ensure we have entries for reservoirs even without explicit inflow rows at a timestamp
        reservoir_ids = list(topology.reservoirs.keys())

        for timestamp, inflow_group in inflow_by_time:
            timestamp_states = {}

            # Process each reservoir in topological order
            processed = set()

            def process_reservoir(reservoir_id: str):
                if reservoir_id in processed:
                    if reservoir_id in timestamp_states:
                        return timestamp_states[reservoir_id]
                    return current_states[reservoir_id]

                reservoir = topology.reservoirs.get(reservoir_id)
                if reservoir is None:
                    raise ValueError(f"Reservoir {reservoir_id} missing from cascade {cascade_id}")

                # Get inflow for this reservoir
                inflow_row = inflow_group[inflow_group["reservoir_id"] == reservoir_id]
                inflow_cfs = float(inflow_row.iloc[0]["inflow_cfs"]) if not inflow_row.empty else 0.0

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
                    current_state = self._default_reservoir_state(reservoir_id, timestamp, reservoir)

                # Calculate release based on rule curves and downstream requirements
                release_cfs = self._calculate_release(
                    reservoir_id=reservoir_id,
                    timestamp=timestamp,
                    storage_af=current_state.storage_af,
                    inflow_cfs=total_inflow_cfs,
                    reservoir=reservoir,
                    time_step_hours=time_step_hours,
                )

                # Update storage applying mass balance
                storage_af = current_state.storage_af + ((total_inflow_cfs - release_cfs) * time_step_seconds) / 43560.0

                # Apply storage constraints with corrective action if bounds are exceeded
                min_storage_af, max_storage_af = self._storage_bounds(reservoir)
                storage_af, release_cfs = self._apply_storage_bounds(
                    storage_af=storage_af,
                    release_cfs=release_cfs,
                    min_storage_af=min_storage_af,
                    max_storage_af=max_storage_af,
                    current_storage=current_state.storage_af,
                    total_inflow_cfs=total_inflow_cfs,
                    time_step_seconds=time_step_seconds,
                )

                # Calculate elevation from storage using simplified relationship
                elevation_feet = self._storage_to_elevation(storage_af, reservoir)

                # Calculate head using available metadata
                head_feet = self._head_from_elevation(elevation_feet, reservoir)

                # Calculate generation
                generation_mw, efficiency_percent = self._calculate_generation(
                    reservoir_id=reservoir_id,
                    head_feet=head_feet,
                    release_cfs=release_cfs,
                    reservoir=reservoir,
                )

                # Create new state
                new_state = ReservoirState(
                    reservoir_id=reservoir_id,
                    timestamp=self._ensure_datetime(timestamp),
                    storage_af=storage_af,
                    elevation_feet=elevation_feet,
                    inflow_cfs=total_inflow_cfs,
                    release_cfs=release_cfs,
                    generation_mw=generation_mw,
                    head_feet=head_feet,
                    efficiency_percent=efficiency_percent
                )

                timestamp_states[reservoir_id] = new_state
                current_states[reservoir_id] = new_state
                processed.add(reservoir_id)

                return new_state

            # Process all reservoirs
            for reservoir_id in reservoir_ids:
                process_reservoir(reservoir_id)

            states.extend(timestamp_states.values())

        return states

    def _ensure_datetime(self, timestamp: Any) -> datetime:
        """Normalise timestamps to naive UTC datetimes."""
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        return pd.Timestamp(timestamp, tz="UTC").to_pydatetime().replace(tzinfo=None)

    def _storage_bounds(self, reservoir: Dict) -> Tuple[Optional[float], Optional[float]]:
        """Return (min_storage_af, max_storage_af) for a reservoir."""
        max_storage = reservoir.get("active_storage_mcm")
        max_storage_af = max_storage * 1233.48 if max_storage is not None else None

        min_storage = reservoir.get("dead_storage_mcm")
        if min_storage is None:
            min_storage = reservoir.get("min_storage_mcm")
        min_storage_af = min_storage * 1233.48 if min_storage is not None else None

        if min_storage_af is None:
            min_storage_af = 0.0 if max_storage_af is not None else None

        return min_storage_af, max_storage_af

    def _apply_storage_bounds(
        self,
        storage_af: float,
        release_cfs: float,
        min_storage_af: Optional[float],
        max_storage_af: Optional[float],
        current_storage: float,
        total_inflow_cfs: float,
        time_step_seconds: float,
    ) -> Tuple[float, float]:
        """Adjust storage/release to respect min/max limits."""
        adjusted_storage = storage_af
        adjusted_release = release_cfs

        def _volume_to_flow(volume_af: float) -> float:
            return (volume_af * 43560.0) / time_step_seconds

        if max_storage_af is not None and adjusted_storage > max_storage_af:
            excess_af = adjusted_storage - max_storage_af
            adjusted_release += _volume_to_flow(excess_af)
            adjusted_storage = max_storage_af

        if min_storage_af is not None and adjusted_storage < min_storage_af:
            deficit_af = min_storage_af - adjusted_storage
            adjusted_release = max(0.0, adjusted_release - _volume_to_flow(deficit_af))
            adjusted_storage = min_storage_af

        # Ensure release is not negative or NaN
        if not math.isfinite(adjusted_release) or adjusted_release < 0.0:
            adjusted_release = 0.0

        # Recompute storage to ensure consistency after adjustments
        adjusted_storage = current_storage + ((total_inflow_cfs - adjusted_release) * time_step_seconds) / 43560.0

        if max_storage_af is not None:
            adjusted_storage = min(adjusted_storage, max_storage_af)
        if min_storage_af is not None:
            adjusted_storage = max(adjusted_storage, min_storage_af)

        return adjusted_storage, adjusted_release

    def _default_reservoir_state(self, reservoir_id: str, timestamp: Any, reservoir: Dict) -> ReservoirState:
        """Construct a default reservoir state when historical data is unavailable."""
        min_storage_af, max_storage_af = self._storage_bounds(reservoir)

        if max_storage_af is None and min_storage_af is None:
            storage_af = 0.0
        elif max_storage_af is None:
            storage_af = min_storage_af
        elif min_storage_af is None:
            storage_af = max_storage_af * 0.5
        else:
            storage_af = min_storage_af + 0.5 * (max_storage_af - min_storage_af)

        elevation_feet = self._storage_to_elevation(storage_af, reservoir)
        head_feet = self._head_from_elevation(elevation_feet, reservoir)

        return ReservoirState(
            reservoir_id=reservoir_id,
            timestamp=self._ensure_datetime(timestamp),
            storage_af=storage_af or 0.0,
            elevation_feet=elevation_feet,
            inflow_cfs=0.0,
            release_cfs=0.0,
            generation_mw=0.0,
            head_feet=head_feet,
            efficiency_percent=0.0,
        )

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

        elevation = min_elevation + (elevation_range * storage_ratio)
        return max(min_elevation, min(max_elevation if max_elevation else elevation, elevation))

    def _head_from_elevation(self, elevation_feet: float, reservoir: Dict) -> float:
        """Estimate hydraulic head given reservoir elevation and metadata."""
        head_curve = reservoir.get("head_curve", {})
        head_feet: Optional[float] = None

        elevation_points_m = head_curve.get("elevation_points_m", []) if head_curve else []
        head_values_m = head_curve.get("head_values_m", []) if head_curve else []

        if elevation_points_m and head_values_m and len(elevation_points_m) == len(head_values_m):
            elevation_points_ft = [point * 3.28084 for point in elevation_points_m]
            head_points_ft = [value * 3.28084 for value in head_values_m]
            head_feet = float(np.interp(elevation_feet, elevation_points_ft, head_points_ft))

        if head_feet is None:
            tailwater = reservoir.get("tailwater_elevation_feet", 0.0)
            head_feet = elevation_feet - tailwater

        return max(head_feet, 0.0)

    def _calculate_release(
        self,
        reservoir_id: str,
        timestamp: Any,
        storage_af: float,
        inflow_cfs: float,
        reservoir: Dict,
        time_step_hours: float,
    ) -> float:
        """Calculate reservoir release based on rule curves."""
        seasonal_rules = reservoir.get("seasonal_rules", {})

        # Determine current season (simplified - use month)
        month = pd.Timestamp(timestamp).month
        is_winter = month in (12, 1, 2, 3)

        if is_winter and "winter_rule_curve" in seasonal_rules:
            rule = seasonal_rules["winter_rule_curve"]
        elif "summer_rule_curve" in seasonal_rules:
            rule = seasonal_rules["summer_rule_curve"]
        else:
            # Default rule - release inflow
            return inflow_cfs

        # Apply rule curve logic (simplified)
        capacity_af = reservoir.get("active_storage_mcm", 0) * 1233.48
        min_storage_af, max_storage_af = self._storage_bounds(reservoir)

        target_storage_percent = rule.get("target_storage_percent")
        if target_storage_percent is None:
            # Default target based on reservoir type
            if reservoir.get("reservoir_type") == "run_of_river":
                target_storage_percent = 20.0
            else:
                target_storage_percent = 60.0

        target_storage_af = capacity_af * (target_storage_percent / 100.0) if capacity_af else storage_af

        min_release_cfs = max(
            0.0,
            rule.get("min_release_cfs", 0.0),
            reservoir.get("environmental_min_release_cfs", 0.0),
        )

        rule_max_release = rule.get("max_release_cfs")
        reservoir_max_release = reservoir.get("max_release_cfs")
        if rule_max_release is not None:
            max_release_cfs = rule_max_release
        elif reservoir_max_release is not None:
            max_release_cfs = reservoir_max_release
        else:
            # Allow limited draft relative to inflow
            max_release_cfs = inflow_cfs * 2.0 if inflow_cfs else 0.0

        # Prevent unrealistic zero upper bound
        max_release_cfs = max(max_release_cfs, min_release_cfs + 1.0)

        # Simple rule: release to maintain target storage
        release_cfs = inflow_cfs
        tolerance_af = 0.05 * capacity_af if capacity_af else 0.0

        if storage_af > target_storage_af + tolerance_af:
            surplus_af = storage_af - target_storage_af
            surplus_cfs = (surplus_af * 43560.0) / (time_step_hours * 3600.0)
            release_cfs = min(max_release_cfs, inflow_cfs + surplus_cfs)
        elif storage_af < target_storage_af - tolerance_af:
            deficit_af = target_storage_af - storage_af
            deficit_cfs = (deficit_af * 43560.0) / (time_step_hours * 3600.0)
            release_cfs = max(min_release_cfs, inflow_cfs - deficit_cfs)
        else:
            # At target - release inflow
            release_cfs = inflow_cfs

        # Apply flood control if needed
        if "flood_control_curve" in seasonal_rules:
            flood_rule = seasonal_rules["flood_control_curve"]
            max_storage_percent = flood_rule.get("max_storage_percent", 90.0)

            if (storage_af / capacity_af * 100) > max_storage_percent:
                # Flood control - release more
                release_cfs = max(release_cfs, min(max_release_cfs, inflow_cfs * 1.2))

        downstream_requirement = reservoir.get("downstream_min_release_cfs")
        if downstream_requirement is not None:
            release_cfs = max(release_cfs, downstream_requirement)

        return max(min_release_cfs, min(max_release_cfs, release_cfs))

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

        if release_cfs <= 0.0 or head_feet <= 0.0:
            return 0.0, 0.0

        # Calculate generation (simplified formula)
        generation_mw = (release_cfs * head_feet * efficiency_percent / 100.0) / 11.8  # Simplified conversion

        max_generation_mw = reservoir.get("max_generation_mw")
        if max_generation_mw:
            generation_mw = min(generation_mw, max_generation_mw)
            if generation_mw <= 0.0:
                efficiency_percent = 0.0

        return max(0.0, generation_mw), efficiency_percent
