"""
Health check system for microservices.

Provides comprehensive health checking including:
- Service health status
- Readiness checks
- Liveness checks
- Dependency health
"""

import asyncio
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import time

import structlog


logger = structlog.get_logger()


class HealthStatus(Enum):
    """Health status enumeration."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class HealthCheck:
    """Individual health check definition."""
    name: str
    check_func: Callable[[], bool]
    timeout: float = 5.0
    critical: bool = True
    description: Optional[str] = None


class HealthChecker:
    """
    Health checker for microservices.
    
    Manages multiple health checks and provides
    aggregated health status.
    """
    
    def __init__(self, config):
        self.config = config
        self.logger = structlog.get_logger("health-checker")
        self.checks: List[HealthCheck] = []
        self.last_check_time: Optional[float] = None
        self.last_status: Optional[HealthStatus] = None
        
        # Add default checks
        self._add_default_checks()
    
    def _add_default_checks(self) -> None:
        """Add default health checks."""
        # Service configuration check
        self.add_check(
            HealthCheck(
                name="config",
                check_func=self._check_config,
                description="Service configuration validation"
            )
        )
        
        # Basic system check
        self.add_check(
            HealthCheck(
                name="system",
                check_func=self._check_system,
                description="Basic system health"
            )
        )
    
    def add_check(self, check: HealthCheck) -> None:
        """Add a health check."""
        self.checks.append(check)
        self.logger.debug("Added health check", name=check.name)
    
    def remove_check(self, name: str) -> None:
        """Remove a health check by name."""
        self.checks = [check for check in self.checks if check.name != name]
        self.logger.debug("Removed health check", name=name)
    
    async def check_health(self) -> Dict[str, Any]:
        """Perform all health checks and return aggregated status."""
        start_time = time.time()
        results = {}
        overall_status = HealthStatus.HEALTHY
        critical_failures = 0
        
        for check in self.checks:
            try:
                # Run check with timeout
                result = await asyncio.wait_for(
                    self._run_check(check),
                    timeout=check.timeout
                )
                
                results[check.name] = {
                    "status": "healthy" if result else "unhealthy",
                    "description": check.description,
                    "critical": check.critical,
                    "duration_ms": (time.time() - start_time) * 1000,
                }
                
                if not result and check.critical:
                    critical_failures += 1
                    overall_status = HealthStatus.UNHEALTHY
                elif not result and overall_status == HealthStatus.HEALTHY:
                    overall_status = HealthStatus.DEGRADED
                
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Health check timeout",
                    name=check.name,
                    timeout=check.timeout
                )
                
                results[check.name] = {
                    "status": "unhealthy",
                    "description": check.description,
                    "critical": check.critical,
                    "error": "timeout",
                    "duration_ms": check.timeout * 1000,
                }
                
                if check.critical:
                    critical_failures += 1
                    overall_status = HealthStatus.UNHEALTHY
                elif overall_status == HealthStatus.HEALTHY:
                    overall_status = HealthStatus.DEGRADED
                
            except Exception as e:
                self.logger.error(
                    "Health check error",
                    name=check.name,
                    error=str(e),
                    exc_info=True
                )
                
                results[check.name] = {
                    "status": "unhealthy",
                    "description": check.description,
                    "critical": check.critical,
                    "error": str(e),
                    "duration_ms": (time.time() - start_time) * 1000,
                }
                
                if check.critical:
                    critical_failures += 1
                    overall_status = HealthStatus.UNHEALTHY
                elif overall_status == HealthStatus.HEALTHY:
                    overall_status = HealthStatus.DEGRADED
        
        self.last_check_time = time.time()
        self.last_status = overall_status
        
        return {
            "healthy": overall_status == HealthStatus.HEALTHY,
            "status": overall_status.value,
            "checks": results,
            "critical_failures": critical_failures,
            "total_checks": len(self.checks),
            "timestamp": self.last_check_time,
        }
    
    async def check_readiness(self) -> Dict[str, Any]:
        """Check if service is ready to accept traffic."""
        health_result = await self.check_health()
        
        # Service is ready if no critical failures
        ready = health_result["critical_failures"] == 0
        
        return {
            "ready": ready,
            "status": "ready" if ready else "not_ready",
            "health": health_result,
            "timestamp": time.time(),
        }
    
    async def _run_check(self, check: HealthCheck) -> bool:
        """Run a single health check."""
        try:
            if asyncio.iscoroutinefunction(check.check_func):
                return await check.check_func()
            else:
                return check.check_func()
        except Exception as e:
            self.logger.error(
                "Health check execution error",
                name=check.name,
                error=str(e),
                exc_info=True
            )
            return False
    
    def _check_config(self) -> bool:
        """Check service configuration."""
        try:
            # Validate required configuration
            if not self.config.service_name:
                return False
            
            if self.config.environment not in ["local", "dev", "staging", "prod"]:
                return False
            
            return True
        except Exception:
            return False
    
    def _check_system(self) -> bool:
        """Check basic system health."""
        try:
            # Check if we can get current time
            time.time()
            return True
        except Exception:
            return False
    
    def get_last_status(self) -> Optional[Dict[str, Any]]:
        """Get the last health check status."""
        if self.last_check_time is None:
            return None
        
        return {
            "status": self.last_status.value if self.last_status else None,
            "timestamp": self.last_check_time,
        }

