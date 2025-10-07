"""Security middleware for HTTP services."""

import logging
from typing import Dict, Any, Optional
from aiohttp import web
from aiohttp.web import Request, Response
from aiohttp.web_middlewares import middleware

logger = logging.getLogger(__name__)


@middleware
async def security_headers_middleware(request: Request, handler) -> Response:
    """Add security headers to all responses."""
    response = await handler(request)
    
    # OWASP security headers
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'
    
    return response


@middleware
async def cors_middleware(request: Request, handler) -> Response:
    """Handle CORS requests."""
    response = await handler(request)
    
    # Allow CORS for development
    if request.headers.get('Origin'):
        response.headers['Access-Control-Allow-Origin'] = request.headers['Origin']
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-API-Key'
        response.headers['Access-Control-Allow-Credentials'] = 'true'
    
    return response


class SecurityHeaders:
    """Security headers configuration."""
    
    @staticmethod
    def get_headers() -> Dict[str, str]:
        """Get standard security headers."""
        return {
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
            'Referrer-Policy': 'strict-origin-when-cross-origin',
            'Content-Security-Policy': "default-src 'self'",
            'Permissions-Policy': 'geolocation=(), microphone=(), camera=()'
        }


class AuthMiddleware:
    """Authentication middleware for API endpoints."""
    
    def __init__(self, api_keys: Optional[Dict[str, str]] = None):
        self.api_keys = api_keys or {}
    
    @middleware
    async def api_key_auth(self, request: Request, handler) -> Response:
        """Validate API key for protected endpoints."""
        # Skip auth for health checks and metrics
        if request.path in ['/health', '/metrics', '/health/ready', '/health/live']:
            return await handler(request)
        
        api_key = request.headers.get('X-API-Key') or request.headers.get('Authorization')
        
        if not api_key:
            return web.json_response(
                {'error': 'Missing API key'}, 
                status=401,
                headers={'WWW-Authenticate': 'ApiKey'}
            )
        
        # Remove 'Bearer ' prefix if present
        if api_key.startswith('Bearer '):
            api_key = api_key[7:]
        
        if api_key not in self.api_keys:
            return web.json_response(
                {'error': 'Invalid API key'}, 
                status=401
            )
        
        # Add tenant info to request
        request['tenant_id'] = self.api_keys[api_key]
        
        return await handler(request)
    
    def add_api_key(self, api_key: str, tenant_id: str) -> None:
        """Add an API key for a tenant."""
        self.api_keys[api_key] = tenant_id
        logger.info(f"Added API key for tenant: {tenant_id}")
    
    def remove_api_key(self, api_key: str) -> None:
        """Remove an API key."""
        if api_key in self.api_keys:
            tenant_id = self.api_keys.pop(api_key)
            logger.info(f"Removed API key for tenant: {tenant_id}")
