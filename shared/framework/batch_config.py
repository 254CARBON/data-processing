"""Batch configuration and optimization for data processing."""

import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class BatchStrategy(str, Enum):
    """Batch strategy enumeration."""
    FIXED_SIZE = "fixed_size"
    TIME_BASED = "time_based"
    ADAPTIVE = "adaptive"
    MEMORY_BASED = "memory_based"


@dataclass
class BatchConfig:
    """Batch configuration."""
    strategy: BatchStrategy = BatchStrategy.FIXED_SIZE
    size: int = 1000
    timeout: float = 30.0
    max_memory_mb: int = 100
    min_size: int = 100
    max_size: int = 10000
    adaptive_factor: float = 1.2
    performance_window: int = 100  # Number of batches to consider for adaptation


class BatchOptimizer:
    """Optimizes batch sizes based on performance metrics."""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self.logger = structlog.get_logger("batch-optimizer")
        self.performance_history: List[Dict[str, Any]] = []
        self.current_batch_size = config.size
        self.optimization_stats = {
            "total_batches": 0,
            "optimized_batches": 0,
            "performance_improvements": 0,
            "current_batch_size": config.size
        }
    
    def record_batch_performance(self, batch_size: int, processing_time: float, 
                               memory_usage: float, success: bool) -> None:
        """Record batch performance metrics."""
        try:
            performance_data = {
                "batch_size": batch_size,
                "processing_time": processing_time,
                "memory_usage": memory_usage,
                "success": success,
                "throughput": batch_size / processing_time if processing_time > 0 else 0,
                "memory_efficiency": batch_size / memory_usage if memory_usage > 0 else 0
            }
            
            self.performance_history.append(performance_data)
            
            # Keep only recent history
            if len(self.performance_history) > self.config.performance_window:
                self.performance_history.pop(0)
            
            self.optimization_stats["total_batches"] += 1
            
            # Optimize batch size if adaptive strategy is enabled
            if self.config.strategy == BatchStrategy.ADAPTIVE:
                self._optimize_batch_size()
            
        except Exception as e:
            self.logger.error("Failed to record batch performance", error=str(e))
    
    def _optimize_batch_size(self) -> None:
        """Optimize batch size based on performance history."""
        try:
            if len(self.performance_history) < 10:
                return
            
            # Calculate average performance metrics
            recent_batches = self.performance_history[-10:]
            avg_throughput = sum(b["throughput"] for b in recent_batches) / len(recent_batches)
            avg_memory_efficiency = sum(b["memory_efficiency"] for b in recent_batches) / len(recent_batches)
            success_rate = sum(1 for b in recent_batches if b["success"]) / len(recent_batches)
            
            # Determine if optimization is needed
            if success_rate < 0.95:  # Low success rate
                self.current_batch_size = max(
                    self.config.min_size,
                    int(self.current_batch_size * 0.8)
                )
                self.logger.info("Batch size reduced due to low success rate", 
                               new_size=self.current_batch_size)
            elif avg_throughput > self._get_target_throughput() * 1.1:  # High throughput
                self.current_batch_size = min(
                    self.config.max_size,
                    int(self.current_batch_size * self.config.adaptive_factor)
                )
                self.logger.info("Batch size increased due to high throughput", 
                               new_size=self.current_batch_size)
            elif avg_memory_efficiency < self._get_target_memory_efficiency() * 0.9:  # Low memory efficiency
                self.current_batch_size = max(
                    self.config.min_size,
                    int(self.current_batch_size * 0.9)
                )
                self.logger.info("Batch size reduced due to low memory efficiency", 
                               new_size=self.current_batch_size)
            
            self.optimization_stats["current_batch_size"] = self.current_batch_size
            self.optimization_stats["optimized_batches"] += 1
            
        except Exception as e:
            self.logger.error("Failed to optimize batch size", error=str(e))
    
    def _get_target_throughput(self) -> float:
        """Get target throughput for optimization."""
        # This would be based on service requirements
        return 1000.0  # items per second
    
    def _get_target_memory_efficiency(self) -> float:
        """Get target memory efficiency for optimization."""
        # This would be based on available memory
        return 100.0  # items per MB
    
    def get_optimal_batch_size(self) -> int:
        """Get the optimal batch size."""
        return self.current_batch_size
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        return self.optimization_stats.copy()


class BatchProcessor:
    """Processes data in batches with optimization."""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self.optimizer = BatchOptimizer(config)
        self.logger = structlog.get_logger("batch-processor")
        self.processing_stats = {
            "total_items": 0,
            "total_batches": 0,
            "total_time": 0.0,
            "avg_batch_size": 0.0,
            "avg_processing_time": 0.0
        }
    
    async def process_batch(self, items: List[Any], processor_func: callable) -> List[Any]:
        """Process a batch of items."""
        import time
        
        start_time = time.time()
        batch_size = len(items)
        
        try:
            # Process the batch
            results = await processor_func(items)
            
            # Record performance
            processing_time = time.time() - start_time
            memory_usage = self._estimate_memory_usage(items)
            
            self.optimizer.record_batch_performance(
                batch_size, processing_time, memory_usage, True
            )
            
            # Update statistics
            self._update_stats(batch_size, processing_time)
            
            self.logger.debug("Batch processed successfully", 
                            batch_size=batch_size, 
                            processing_time=processing_time)
            
            return results
            
        except Exception as e:
            # Record failed batch
            processing_time = time.time() - start_time
            memory_usage = self._estimate_memory_usage(items)
            
            self.optimizer.record_batch_performance(
                batch_size, processing_time, memory_usage, False
            )
            
            self.logger.error("Batch processing failed", 
                            batch_size=batch_size, 
                            error=str(e))
            raise
    
    def _estimate_memory_usage(self, items: List[Any]) -> float:
        """Estimate memory usage for a batch."""
        try:
            import sys
            total_size = sum(sys.getsizeof(item) for item in items)
            return total_size / (1024 * 1024)  # Convert to MB
        except Exception:
            return len(items) * 0.001  # Rough estimate
    
    def _update_stats(self, batch_size: int, processing_time: float) -> None:
        """Update processing statistics."""
        self.processing_stats["total_items"] += batch_size
        self.processing_stats["total_batches"] += 1
        self.processing_stats["total_time"] += processing_time
        
        # Calculate averages
        self.processing_stats["avg_batch_size"] = (
            self.processing_stats["total_items"] / self.processing_stats["total_batches"]
        )
        self.processing_stats["avg_processing_time"] = (
            self.processing_stats["total_time"] / self.processing_stats["total_batches"]
        )
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return self.processing_stats.copy()
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        return self.optimizer.get_optimization_stats()


class BatchSizeCalculator:
    """Calculates optimal batch sizes for different scenarios."""
    
    def __init__(self):
        self.logger = structlog.get_logger("batch-size-calculator")
    
    def calculate_for_throughput(self, target_throughput: float, 
                               avg_processing_time: float) -> int:
        """Calculate batch size for target throughput."""
        try:
            # Batch size = target throughput * processing time
            batch_size = int(target_throughput * avg_processing_time)
            
            # Apply constraints
            batch_size = max(100, min(10000, batch_size))
            
            self.logger.info("Batch size calculated for throughput", 
                           target_throughput=target_throughput,
                           avg_processing_time=avg_processing_time,
                           calculated_size=batch_size)
            
            return batch_size
            
        except Exception as e:
            self.logger.error("Failed to calculate batch size for throughput", error=str(e))
            return 1000
    
    def calculate_for_memory(self, available_memory_mb: int, 
                           avg_item_size_kb: float) -> int:
        """Calculate batch size for available memory."""
        try:
            # Reserve 20% of memory for overhead
            usable_memory_mb = available_memory_mb * 0.8
            
            # Convert to KB
            usable_memory_kb = usable_memory_mb * 1024
            
            # Calculate batch size
            batch_size = int(usable_memory_kb / avg_item_size_kb)
            
            # Apply constraints
            batch_size = max(100, min(10000, batch_size))
            
            self.logger.info("Batch size calculated for memory", 
                           available_memory_mb=available_memory_mb,
                           avg_item_size_kb=avg_item_size_kb,
                           calculated_size=batch_size)
            
            return batch_size
            
        except Exception as e:
            self.logger.error("Failed to calculate batch size for memory", error=str(e))
            return 1000
    
    def calculate_for_latency(self, max_latency_ms: float, 
                            avg_processing_time_ms: float) -> int:
        """Calculate batch size for maximum latency."""
        try:
            # Batch size = max latency / processing time per item
            batch_size = int(max_latency_ms / avg_processing_time_ms)
            
            # Apply constraints
            batch_size = max(100, min(10000, batch_size))
            
            self.logger.info("Batch size calculated for latency", 
                           max_latency_ms=max_latency_ms,
                           avg_processing_time_ms=avg_processing_time_ms,
                           calculated_size=batch_size)
            
            return batch_size
            
        except Exception as e:
            self.logger.error("Failed to calculate batch size for latency", error=str(e))
            return 1000
    
    def calculate_optimal(self, constraints: Dict[str, Any]) -> int:
        """Calculate optimal batch size considering multiple constraints."""
        try:
            batch_sizes = []
            
            # Calculate for each constraint
            if "throughput" in constraints:
                batch_sizes.append(
                    self.calculate_for_throughput(
                        constraints["throughput"]["target"],
                        constraints["throughput"]["avg_processing_time"]
                    )
                )
            
            if "memory" in constraints:
                batch_sizes.append(
                    self.calculate_for_memory(
                        constraints["memory"]["available_mb"],
                        constraints["memory"]["avg_item_size_kb"]
                    )
                )
            
            if "latency" in constraints:
                batch_sizes.append(
                    self.calculate_for_latency(
                        constraints["latency"]["max_ms"],
                        constraints["latency"]["avg_processing_time_ms"]
                    )
                )
            
            if not batch_sizes:
                return 1000
            
            # Use the minimum batch size that satisfies all constraints
            optimal_size = min(batch_sizes)
            
            self.logger.info("Optimal batch size calculated", 
                           constraints=constraints,
                           calculated_sizes=batch_sizes,
                           optimal_size=optimal_size)
            
            return optimal_size
            
        except Exception as e:
            self.logger.error("Failed to calculate optimal batch size", error=str(e))
            return 1000
