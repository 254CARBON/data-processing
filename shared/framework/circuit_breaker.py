"""
Circuit Breaker implementation for the 254Carbon Data Processing Pipeline.

This module implements the Circuit Breaker pattern to prevent cascading failures
and improve system resilience.
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Callable, Any, Optional, Dict, List
from dataclasses import dataclass
from functools import wraps
import statistics

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, requests are rejected
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5          # Number of failures before opening
    success_threshold: int = 3          # Number of successes to close from half-open
    timeout: float = 60.0               # Timeout before trying half-open state
    expected_exception: type = Exception # Exception type to count as failure
    failure_rate_threshold: float = 0.5 # Failure rate threshold (0.0-1.0)
    min_requests: int = 10              # Minimum requests before calculating failure rate
    sliding_window_size: int = 100       # Number of requests in sliding window
    enable_metrics: bool = True          # Enable metrics collection


@dataclass
class CircuitBreakerMetrics:
    """Circuit breaker metrics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rejected_requests: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    current_failure_rate: float = 0.0
    request_history: List[bool] = None  # True for success, False for failure
    
    def __post_init__(self):
        if self.request_history is None:
            self.request_history = []


class CircuitBreaker:
    """Circuit breaker implementation."""
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_state_change = time.time()
        
        logger.info(f"Circuit breaker '{name}' initialized with state: {self.state.value}")
    
    def _is_failure(self, exception: Exception) -> bool:
        """Check if exception should be counted as failure."""
        return isinstance(exception, self.config.expected_exception)
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt reset."""
        if self.state != CircuitState.OPEN:
            return False
        
        return time.time() - self.last_failure_time >= self.config.timeout
    
    def _calculate_failure_rate(self) -> float:
        """Calculate current failure rate."""
        if len(self.metrics.request_history) < self.config.min_requests:
            return 0.0
        
        # Use sliding window
        recent_requests = self.metrics.request_history[-self.config.sliding_window_size:]
        if not recent_requests:
            return 0.0
        
        failures = sum(1 for success in recent_requests if not success)
        return failures / len(recent_requests)
    
    def _update_metrics(self, success: bool, exception: Optional[Exception] = None):
        """Update circuit breaker metrics."""
        self.metrics.total_requests += 1
        
        if success:
            self.metrics.successful_requests += 1
            self.metrics.last_success_time = time.time()
        else:
            self.metrics.failed_requests += 1
            self.metrics.last_failure_time = time.time()
        
        # Update request history
        self.metrics.request_history.append(success)
        
        # Keep only recent requests
        if len(self.metrics.request_history) > self.config.sliding_window_size:
            self.metrics.request_history = self.metrics.request_history[-self.config.sliding_window_size:]
        
        # Update failure rate
        self.metrics.current_failure_rate = self._calculate_failure_rate()
    
    def _change_state(self, new_state: CircuitState):
        """Change circuit breaker state."""
        if self.state != new_state:
            old_state = self.state
            self.state = new_state
            self.last_state_change = time.time()
            self.metrics.state_changes += 1
            
            logger.info(f"Circuit breaker '{self.name}' state changed: {old_state.value} -> {new_state.value}")
    
    def _on_success(self):
        """Handle successful request."""
        self.success_count += 1
        self.failure_count = 0
        
        if self.state == CircuitState.HALF_OPEN:
            if self.success_count >= self.config.success_threshold:
                self._change_state(CircuitState.CLOSED)
                logger.info(f"Circuit breaker '{self.name}' closed after {self.success_count} successes")
    
    def _on_failure(self, exception: Exception):
        """Handle failed request."""
        self.failure_count += 1
        self.success_count = 0
        
        if self.state == CircuitState.HALF_OPEN:
            self._change_state(CircuitState.OPEN)
            logger.warning(f"Circuit breaker '{self.name}' opened after failure in half-open state")
        elif self.state == CircuitState.CLOSED:
            # Check if we should open the circuit
            should_open = False
            
            # Check failure count threshold
            if self.failure_count >= self.config.failure_threshold:
                should_open = True
                logger.warning(f"Circuit breaker '{self.name}' opened due to failure count: {self.failure_count}")
            
            # Check failure rate threshold
            if self.metrics.current_failure_rate >= self.config.failure_rate_threshold:
                should_open = True
                logger.warning(f"Circuit breaker '{self.name}' opened due to failure rate: {self.metrics.current_failure_rate:.2%}")
            
            if should_open:
                self._change_state(CircuitState.OPEN)
                self.last_failure_time = time.time()
    
    def _should_allow_request(self) -> bool:
        """Check if request should be allowed."""
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._change_state(CircuitState.HALF_OPEN)
                logger.info(f"Circuit breaker '{self.name}' moved to half-open state")
                return True
            return False
        elif self.state == CircuitState.HALF_OPEN:
            return True
        
        return False
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if not self._should_allow_request():
            self.metrics.rejected_requests += 1
            raise CircuitBreakerOpenException(f"Circuit breaker '{self.name}' is open")
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            self._on_success()
            self._update_metrics(True)
            return result
            
        except Exception as e:
            if self._is_failure(e):
                self._on_failure(e)
                self._update_metrics(False, e)
            else:
                # Don't count non-expected exceptions as failures
                self._update_metrics(True)
            
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get circuit breaker metrics."""
        return {
            'name': self.name,
            'state': self.state.value,
            'total_requests': self.metrics.total_requests,
            'successful_requests': self.metrics.successful_requests,
            'failed_requests': self.metrics.failed_requests,
            'rejected_requests': self.metrics.rejected_requests,
            'state_changes': self.metrics.state_changes,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'current_failure_rate': self.metrics.current_failure_rate,
            'last_failure_time': self.metrics.last_failure_time,
            'last_success_time': self.metrics.last_success_time,
            'last_state_change': self.last_state_change,
            'config': {
                'failure_threshold': self.config.failure_threshold,
                'success_threshold': self.config.success_threshold,
                'timeout': self.config.timeout,
                'failure_rate_threshold': self.config.failure_rate_threshold,
                'min_requests': self.config.min_requests,
                'sliding_window_size': self.config.sliding_window_size
            }
        }
    
    def reset(self):
        """Reset circuit breaker to closed state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_state_change = time.time()
        
        logger.info(f"Circuit breaker '{self.name}' manually reset to closed state")


class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerManager:
    """Manager for multiple circuit breakers."""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
    
    def get_circuit_breaker(self, name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get or create circuit breaker."""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(name, config)
        
        return self.circuit_breakers[name]
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all circuit breakers."""
        return {name: cb.get_metrics() for name, cb in self.circuit_breakers.items()}
    
    def reset_all(self):
        """Reset all circuit breakers."""
        for cb in self.circuit_breakers.values():
            cb.reset()
    
    def reset(self, name: str):
        """Reset specific circuit breaker."""
        if name in self.circuit_breakers:
            self.circuit_breakers[name].reset()


# Decorator for circuit breaker
def circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None):
    """Decorator to add circuit breaker to function."""
    def decorator(func):
        cb = CircuitBreakerManager().get_circuit_breaker(name, config)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await cb.call(func, *args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return cb.call(func, *args, **kwargs)
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Global circuit breaker manager
_circuit_breaker_manager = CircuitBreakerManager()


def get_circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
    """Get global circuit breaker instance."""
    return _circuit_breaker_manager.get_circuit_breaker(name, config)


def get_all_circuit_breaker_metrics() -> Dict[str, Dict[str, Any]]:
    """Get metrics for all circuit breakers."""
    return _circuit_breaker_manager.get_all_metrics()


def reset_circuit_breaker(name: str):
    """Reset specific circuit breaker."""
    _circuit_breaker_manager.reset(name)


def reset_all_circuit_breakers():
    """Reset all circuit breakers."""
    _circuit_breaker_manager.reset_all()


# Example usage and testing
if __name__ == "__main__":
    import random
    
    # Example circuit breaker configuration
    config = CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout=5.0,
        failure_rate_threshold=0.6,
        min_requests=5
    )
    
    # Create circuit breaker
    cb = CircuitBreaker("test-service", config)
    
    # Simulate service calls
    async def simulate_service_call(success_rate: float = 0.7):
        """Simulate service call with given success rate."""
        await asyncio.sleep(0.1)  # Simulate network delay
        
        if random.random() < success_rate:
            return "success"
        else:
            raise Exception("Service error")
    
    async def test_circuit_breaker():
        """Test circuit breaker functionality."""
        print("Testing circuit breaker...")
        
        # Test successful calls
        for i in range(10):
            try:
                result = await cb.call(simulate_service_call, success_rate=0.8)
                print(f"Call {i+1}: {result}")
            except Exception as e:
                print(f"Call {i+1}: {e}")
            
            await asyncio.sleep(0.1)
        
        # Test with high failure rate
        print("\nTesting with high failure rate...")
        for i in range(10):
            try:
                result = await cb.call(simulate_service_call, success_rate=0.2)
                print(f"Call {i+1}: {result}")
            except Exception as e:
                print(f"Call {i+1}: {e}")
            
            await asyncio.sleep(0.1)
        
        # Print metrics
        print("\nCircuit breaker metrics:")
        metrics = cb.get_metrics()
        for key, value in metrics.items():
            print(f"  {key}: {value}")
    
    # Run test
    asyncio.run(test_circuit_breaker())
