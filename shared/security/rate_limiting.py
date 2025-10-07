"""Rate limiting middleware using Redis."""

import time
import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class RateLimitType(str, Enum):
    """Rate limit type enumeration."""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_minute: int = 60
    requests_per_hour: int = 1000
    requests_per_day: int = 10000
    burst_limit: int = 100
    window_size: int = 60  # seconds
    rate_limit_type: RateLimitType = RateLimitType.SLIDING_WINDOW


class RateLimiter:
    """Redis-based rate limiter."""
    
    def __init__(self, redis_client, config: RateLimitConfig = None):
        self.redis = redis_client
        self.config = config or RateLimitConfig()
        self.logger = structlog.get_logger("rate-limiter")
    
    async def is_allowed(self, identifier: str, limit_type: str = "minute") -> Tuple[bool, Dict[str, Any]]:
        """Check if request is allowed based on rate limits."""
        current_time = int(time.time())
        
        if limit_type == "minute":
            return await self._check_minute_limit(identifier, current_time)
        elif limit_type == "hour":
            return await self._check_hour_limit(identifier, current_time)
        elif limit_type == "day":
            return await self._check_day_limit(identifier, current_time)
        else:
            return await self._check_minute_limit(identifier, current_time)
    
    async def _check_minute_limit(self, identifier: str, current_time: int) -> Tuple[bool, Dict[str, Any]]:
        """Check minute-based rate limit."""
        window_start = current_time - (current_time % 60)
        key = f"rate_limit:minute:{identifier}:{window_start}"
        
        # Get current count
        current_count = await self.redis.get(key)
        current_count = int(current_count) if current_count else 0
        
        # Check if limit exceeded
        if current_count >= self.config.requests_per_minute:
            return False, {
                "limit": self.config.requests_per_minute,
                "remaining": 0,
                "reset_time": window_start + 60,
                "retry_after": window_start + 60 - current_time
            }
        
        # Increment counter
        await self.redis.set(key, current_count + 1, ttl=60)
        
        return True, {
            "limit": self.config.requests_per_minute,
            "remaining": self.config.requests_per_minute - current_count - 1,
            "reset_time": window_start + 60,
            "retry_after": 0
        }
    
    async def _check_hour_limit(self, identifier: str, current_time: int) -> Tuple[bool, Dict[str, Any]]:
        """Check hour-based rate limit."""
        window_start = current_time - (current_time % 3600)
        key = f"rate_limit:hour:{identifier}:{window_start}"
        
        # Get current count
        current_count = await self.redis.get(key)
        current_count = int(current_count) if current_count else 0
        
        # Check if limit exceeded
        if current_count >= self.config.requests_per_hour:
            return False, {
                "limit": self.config.requests_per_hour,
                "remaining": 0,
                "reset_time": window_start + 3600,
                "retry_after": window_start + 3600 - current_time
            }
        
        # Increment counter
        await self.redis.set(key, current_count + 1, ttl=3600)
        
        return True, {
            "limit": self.config.requests_per_hour,
            "remaining": self.config.requests_per_hour - current_count - 1,
            "reset_time": window_start + 3600,
            "retry_after": 0
        }
    
    async def _check_day_limit(self, identifier: str, current_time: int) -> Tuple[bool, Dict[str, Any]]:
        """Check day-based rate limit."""
        window_start = current_time - (current_time % 86400)
        key = f"rate_limit:day:{identifier}:{window_start}"
        
        # Get current count
        current_count = await self.redis.get(key)
        current_count = int(current_count) if current_count else 0
        
        # Check if limit exceeded
        if current_count >= self.config.requests_per_day:
            return False, {
                "limit": self.config.requests_per_day,
                "remaining": 0,
                "reset_time": window_start + 86400,
                "retry_after": window_start + 86400 - current_time
            }
        
        # Increment counter
        await self.redis.set(key, current_count + 1, ttl=86400)
        
        return True, {
            "limit": self.config.requests_per_day,
            "remaining": self.config.requests_per_day - current_count - 1,
            "reset_time": window_start + 86400,
            "retry_after": 0
        }
    
    async def get_rate_limit_info(self, identifier: str) -> Dict[str, Any]:
        """Get current rate limit information for an identifier."""
        current_time = int(time.time())
        
        # Get minute limit info
        minute_allowed, minute_info = await self._check_minute_limit(identifier, current_time)
        
        # Get hour limit info
        hour_allowed, hour_info = await self._check_hour_limit(identifier, current_time)
        
        # Get day limit info
        day_allowed, day_info = await self._check_day_limit(identifier, current_time)
        
        return {
            "minute": minute_info,
            "hour": hour_info,
            "day": day_info,
            "overall_allowed": minute_allowed and hour_allowed and day_allowed
        }
    
    async def reset_rate_limit(self, identifier: str) -> None:
        """Reset rate limits for an identifier."""
        current_time = int(time.time())
        
        # Reset all time windows
        minute_window = current_time - (current_time % 60)
        hour_window = current_time - (current_time % 3600)
        day_window = current_time - (current_time % 86400)
        
        await self.redis.delete(f"rate_limit:minute:{identifier}:{minute_window}")
        await self.redis.delete(f"rate_limit:hour:{identifier}:{hour_window}")
        await self.redis.delete(f"rate_limit:day:{identifier}:{day_window}")
        
        self.logger.info("Rate limits reset", identifier=identifier)


class RateLimitMiddleware:
    """Rate limiting middleware for HTTP requests."""
    
    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter
        self.logger = structlog.get_logger("rate-limit-middleware")
    
    async def check_rate_limit(self, request, identifier: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Check rate limit for a request."""
        # Get identifier (IP address, user ID, etc.)
        if not identifier:
            # Try to get from request headers or IP
            identifier = request.headers.get('X-Forwarded-For', request.remote)
            if identifier:
                identifier = identifier.split(',')[0].strip()
            else:
                identifier = request.remote
        
        # Check rate limits
        allowed, info = await self.rate_limiter.is_allowed(identifier, "minute")
        
        if not allowed:
            self.logger.warning("Rate limit exceeded", identifier=identifier, info=info)
        
        return allowed, info
    
    def get_rate_limit_headers(self, info: Dict[str, Any]) -> Dict[str, str]:
        """Get rate limit headers for response."""
        return {
            "X-RateLimit-Limit": str(info["limit"]),
            "X-RateLimit-Remaining": str(info["remaining"]),
            "X-RateLimit-Reset": str(info["reset_time"]),
            "X-RateLimit-Retry-After": str(info["retry_after"]) if info["retry_after"] > 0 else "0"
        }
