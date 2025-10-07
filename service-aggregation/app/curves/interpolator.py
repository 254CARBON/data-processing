"""Curve interpolation utilities."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import numpy as np
from scipy.interpolate import interp1d
from shared.utils.errors import DataProcessingError


logger = logging.getLogger(__name__)


class CurveInterpolator:
    """Interpolator for forward curves."""
    
    def __init__(self, config):
        self.config = config
        
    def interpolate_curve(
        self, 
        curve_points: List[Dict[str, Any]], 
        target_times: List[datetime],
        method: str = "linear"
    ) -> List[Dict[str, Any]]:
        """Interpolate curve at target times."""
        if not curve_points or not target_times:
            return []
            
        try:
            # Extract times and prices
            times = [datetime.fromisoformat(point["time"]) for point in curve_points]
            prices = [point["price"] for point in curve_points]
            
            # Convert to timestamps for interpolation
            time_stamps = [t.timestamp() for t in times]
            target_stamps = [t.timestamp() for t in target_times]
            
            # Perform interpolation
            if method == "linear":
                interpolator = interp1d(time_stamps, prices, kind="linear", bounds_error=False, fill_value="extrapolate")
            elif method == "cubic":
                interpolator = interp1d(time_stamps, prices, kind="cubic", bounds_error=False, fill_value="extrapolate")
            else:
                raise ValueError(f"Unsupported interpolation method: {method}")
                
            interpolated_prices = interpolator(target_stamps)
            
            # Build result
            result = []
            for i, target_time in enumerate(target_times):
                result.append({
                    "time": target_time.isoformat(),
                    "price": float(interpolated_prices[i]),
                    "confidence": self._calculate_interpolation_confidence(curve_points, target_time)
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Error interpolating curve: {e}")
            raise DataProcessingError(f"Curve interpolation failed: {e}")
            
    def _calculate_interpolation_confidence(
        self, 
        curve_points: List[Dict[str, Any]], 
        target_time: datetime
    ) -> float:
        """Calculate confidence for interpolated point."""
        if not curve_points:
            return 0.0
            
        # Find closest points
        target_stamp = target_time.timestamp()
        closest_points = []
        
        for point in curve_points:
            point_time = datetime.fromisoformat(point["time"])
            point_stamp = point_time.timestamp()
            diff = abs(target_stamp - point_stamp)
            closest_points.append((diff, point.get("confidence", 1.0)))
            
        # Sort by time difference
        closest_points.sort(key=lambda x: x[0])
        
        # Use confidence of closest point
        if closest_points:
            return closest_points[0][1]
        else:
            return 0.0
            
    def smooth_curve(
        self, 
        curve_points: List[Dict[str, Any]], 
        smoothing_factor: float = 0.1
    ) -> List[Dict[str, Any]]:
        """Smooth curve using moving average."""
        if not curve_points or len(curve_points) < 3:
            return curve_points
            
        try:
            prices = [point["price"] for point in curve_points]
            
            # Apply moving average smoothing
            window_size = max(3, int(len(prices) * smoothing_factor))
            smoothed_prices = []
            
            for i in range(len(prices)):
                start_idx = max(0, i - window_size // 2)
                end_idx = min(len(prices), i + window_size // 2 + 1)
                window_prices = prices[start_idx:end_idx]
                smoothed_prices.append(sum(window_prices) / len(window_prices))
                
            # Build result
            result = []
            for i, point in enumerate(curve_points):
                result.append({
                    "time": point["time"],
                    "price": smoothed_prices[i],
                    "confidence": point.get("confidence", 1.0)
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Error smoothing curve: {e}")
            return curve_points
            
    def extrapolate_curve(
        self, 
        curve_points: List[Dict[str, Any]], 
        extrapolation_horizon: int,
        method: str = "linear"
    ) -> List[Dict[str, Any]]:
        """Extrapolate curve beyond available data."""
        if not curve_points or len(curve_points) < 2:
            return []
            
        try:
            # Extract times and prices
            times = [datetime.fromisoformat(point["time"]) for point in curve_points]
            prices = [point["price"] for point in curve_points]
            
            # Calculate time step
            time_step = (times[-1] - times[-2]).total_seconds()
            
            # Extrapolate
            result = []
            for i in range(1, extrapolation_horizon + 1):
                extrapolated_time = times[-1] + timedelta(seconds=time_step * i)
                
                if method == "linear":
                    # Linear extrapolation
                    price_slope = (prices[-1] - prices[-2]) / time_step
                    extrapolated_price = prices[-1] + price_slope * time_step * i
                else:
                    # Constant extrapolation
                    extrapolated_price = prices[-1]
                    
                result.append({
                    "time": extrapolated_time.isoformat(),
                    "price": extrapolated_price,
                    "confidence": max(0.0, 1.0 - (i * 0.1))  # Decreasing confidence
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Error extrapolating curve: {e}")
            return []
