"""API key management endpoints."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from aiohttp import web
from aiohttp.web import Request, Response
import structlog

from .auth import APIKeyManager, APIKeyValidator, APIKeyStatus
from ..security.rate_limiting import RateLimitMiddleware

logger = structlog.get_logger()


class APIKeyEndpoints:
    """API key management endpoints."""
    
    def __init__(self, api_key_manager: APIKeyManager, rate_limit_middleware: RateLimitMiddleware):
        self.api_key_manager = api_key_manager
        self.rate_limit_middleware = rate_limit_middleware
        self.logger = structlog.get_logger("api-key-endpoints")
    
    async def create_api_key(self, request: Request) -> Response:
        """Create a new API key."""
        try:
            # Check rate limit
            allowed, rate_info = await self.rate_limit_middleware.check_rate_limit(request)
            if not allowed:
                return web.json_response(
                    {"error": "Rate limit exceeded"},
                    status=429,
                    headers=self.rate_limit_middleware.get_rate_limit_headers(rate_info)
                )
            
            # Parse request data
            data = await request.json()
            tenant_id = data.get("tenant_id")
            name = data.get("name")
            permissions = data.get("permissions", [])
            expires_in_days = data.get("expires_in_days")
            metadata = data.get("metadata", {})
            
            # Validate required fields
            if not tenant_id or not name:
                return web.json_response(
                    {"error": "tenant_id and name are required"},
                    status=400
                )
            
            # Generate API key
            key_value = await self.api_key_manager.generate_key(
                tenant_id=tenant_id,
                name=name,
                permissions=permissions,
                expires_in_days=expires_in_days,
                metadata=metadata
            )
            
            self.logger.info("API key created", tenant_id=tenant_id, name=name)
            
            return web.json_response({
                "api_key": key_value,
                "tenant_id": tenant_id,
                "name": name,
                "permissions": permissions,
                "expires_in_days": expires_in_days,
                "created_at": datetime.utcnow().isoformat()
            }, status=201)
            
        except Exception as e:
            self.logger.error("Failed to create API key", error=str(e))
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )
    
    async def list_api_keys(self, request: Request) -> Response:
        """List API keys for a tenant."""
        try:
            # Check rate limit
            allowed, rate_info = await self.rate_limit_middleware.check_rate_limit(request)
            if not allowed:
                return web.json_response(
                    {"error": "Rate limit exceeded"},
                    status=429,
                    headers=self.rate_limit_middleware.get_rate_limit_headers(rate_info)
                )
            
            # Get tenant ID from query params
            tenant_id = request.query.get("tenant_id")
            if not tenant_id:
                return web.json_response(
                    {"error": "tenant_id query parameter is required"},
                    status=400
                )
            
            # List API keys
            keys = await self.api_key_manager.list_keys(tenant_id)
            
            # Convert to response format
            response_data = []
            for key in keys:
                response_data.append({
                    "key_id": key.key_id,
                    "name": key.name,
                    "status": key.status.value,
                    "created_at": key.created_at.isoformat(),
                    "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                    "last_used_at": key.last_used_at.isoformat() if key.last_used_at else None,
                    "permissions": key.permissions,
                    "metadata": key.metadata
                })
            
            return web.json_response({
                "api_keys": response_data,
                "count": len(response_data)
            })
            
        except Exception as e:
            self.logger.error("Failed to list API keys", error=str(e))
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )
    
    async def get_api_key(self, request: Request) -> Response:
        """Get API key information."""
        try:
            # Check rate limit
            allowed, rate_info = await self.rate_limit_middleware.check_rate_limit(request)
            if not allowed:
                return web.json_response(
                    {"error": "Rate limit exceeded"},
                    status=429,
                    headers=self.rate_limit_middleware.get_rate_limit_headers(rate_info)
                )
            
            # Get key ID from URL
            key_id = request.match_info.get("key_id")
            if not key_id:
                return web.json_response(
                    {"error": "key_id is required"},
                    status=400
                )
            
            # Get API key info
            key_info = await self.api_key_manager.get_key_info(key_id)
            if not key_info:
                return web.json_response(
                    {"error": "API key not found"},
                    status=404
                )
            
            return web.json_response({
                "key_id": key_info.key_id,
                "name": key_info.name,
                "status": key_info.status.value,
                "created_at": key_info.created_at.isoformat(),
                "expires_at": key_info.expires_at.isoformat() if key_info.expires_at else None,
                "last_used_at": key_info.last_used_at.isoformat() if key_info.last_used_at else None,
                "permissions": key_info.permissions,
                "metadata": key_info.metadata
            })
            
        except Exception as e:
            self.logger.error("Failed to get API key", error=str(e))
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )
    
    async def revoke_api_key(self, request: Request) -> Response:
        """Revoke an API key."""
        try:
            # Check rate limit
            allowed, rate_info = await self.rate_limit_middleware.check_rate_limit(request)
            if not allowed:
                return web.json_response(
                    {"error": "Rate limit exceeded"},
                    status=429,
                    headers=self.rate_limit_middleware.get_rate_limit_headers(rate_info)
                )
            
            # Get key ID from URL
            key_id = request.match_info.get("key_id")
            if not key_id:
                return web.json_response(
                    {"error": "key_id is required"},
                    status=400
                )
            
            # Revoke API key
            success = await self.api_key_manager.revoke_key(key_id)
            if not success:
                return web.json_response(
                    {"error": "Failed to revoke API key"},
                    status=500
                )
            
            self.logger.info("API key revoked", key_id=key_id)
            
            return web.json_response({
                "message": "API key revoked successfully",
                "key_id": key_id
            })
            
        except Exception as e:
            self.logger.error("Failed to revoke API key", error=str(e))
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )
    
    async def validate_api_key(self, request: Request) -> Response:
        """Validate an API key."""
        try:
            # Check rate limit
            allowed, rate_info = await self.rate_limit_middleware.check_rate_limit(request)
            if not allowed:
                return web.json_response(
                    {"error": "Rate limit exceeded"},
                    status=429,
                    headers=self.rate_limit_middleware.get_rate_limit_headers(rate_info)
                )
            
            # Get API key from headers
            api_key = request.headers.get("X-API-Key") or request.headers.get("Authorization")
            if api_key and api_key.startswith("Bearer "):
                api_key = api_key[7:]
            
            if not api_key:
                return web.json_response(
                    {"error": "API key is required"},
                    status=400
                )
            
            # Validate API key
            key_info = await self.api_key_manager.validate_key(api_key)
            if not key_info:
                return web.json_response(
                    {"error": "Invalid API key"},
                    status=401
                )
            
            return web.json_response({
                "valid": True,
                "tenant_id": key_info.tenant_id,
                "permissions": key_info.permissions,
                "metadata": key_info.metadata
            })
            
        except Exception as e:
            self.logger.error("Failed to validate API key", error=str(e))
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )
    
    async def get_rate_limit_info(self, request: Request) -> Response:
        """Get rate limit information."""
        try:
            # Get identifier from request
            identifier = request.headers.get('X-Forwarded-For', request.remote)
            if identifier:
                identifier = identifier.split(',')[0].strip()
            else:
                identifier = request.remote
            
            # Get rate limit info
            rate_info = await self.rate_limit_middleware.rate_limiter.get_rate_limit_info(identifier)
            
            return web.json_response(rate_info)
            
        except Exception as e:
            self.logger.error("Failed to get rate limit info", error=str(e))
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )


def setup_api_key_routes(app: web.Application, api_key_endpoints: APIKeyEndpoints) -> None:
    """Setup API key management routes."""
    
    # API key management routes
    app.router.add_post("/api/keys", api_key_endpoints.create_api_key)
    app.router.add_get("/api/keys", api_key_endpoints.list_api_keys)
    app.router.add_get("/api/keys/{key_id}", api_key_endpoints.get_api_key)
    app.router.add_delete("/api/keys/{key_id}", api_key_endpoints.revoke_api_key)
    app.router.add_post("/api/keys/validate", api_key_endpoints.validate_api_key)
    app.router.add_get("/api/rate-limit", api_key_endpoints.get_rate_limit_info)
