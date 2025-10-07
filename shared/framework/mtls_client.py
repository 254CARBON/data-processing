"""mTLS client for service-to-service communication."""

import ssl
import aiohttp
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

import structlog

from ..security.mtls import MTLSConfig, CertificateManager

logger = structlog.get_logger()


@dataclass
class ServiceEndpoint:
    """Service endpoint configuration."""
    name: str
    host: str
    port: int
    path: str = "/"
    timeout: int = 30


class MTLSClient:
    """mTLS-enabled HTTP client for service-to-service communication."""
    
    def __init__(self, cert_manager: CertificateManager, service_name: str):
        self.cert_manager = cert_manager
        self.service_name = service_name
        self.logger = structlog.get_logger("mtls-client")
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def start(self) -> None:
        """Start the mTLS client."""
        if self.session:
            return
        
        # Get mTLS configuration
        mtls_config = self.cert_manager.get_service_config(self.service_name)
        ssl_context = mtls_config.to_ssl_context()
        
        # Create connector with mTLS
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        # Create session
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
        
        self.logger.info("mTLS client started", service=self.service_name)
    
    async def stop(self) -> None:
        """Stop the mTLS client."""
        if self.session:
            await self.session.close()
            self.session = None
            self.logger.info("mTLS client stopped", service=self.service_name)
    
    async def get(self, endpoint: ServiceEndpoint, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make GET request with mTLS."""
        if not self.session:
            await self.start()
        
        url = f"https://{endpoint.host}:{endpoint.port}{endpoint.path}"
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
                    
        except Exception as e:
            self.logger.error("mTLS GET request failed", 
                            service=self.service_name, 
                            endpoint=endpoint.name, 
                            error=str(e))
            raise
    
    async def post(self, endpoint: ServiceEndpoint, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make POST request with mTLS."""
        if not self.session:
            await self.start()
        
        url = f"https://{endpoint.host}:{endpoint.port}{endpoint.path}"
        
        try:
            async with self.session.post(url, json=data) as response:
                if response.status in [200, 201]:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
                    
        except Exception as e:
            self.logger.error("mTLS POST request failed", 
                            service=self.service_name, 
                            endpoint=endpoint.name, 
                            error=str(e))
            raise
    
    async def put(self, endpoint: ServiceEndpoint, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make PUT request with mTLS."""
        if not self.session:
            await self.start()
        
        url = f"https://{endpoint.host}:{endpoint.port}{endpoint.path}"
        
        try:
            async with self.session.put(url, json=data) as response:
                if response.status in [200, 201, 204]:
                    if response.status == 204:
                        return {}
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
                    
        except Exception as e:
            self.logger.error("mTLS PUT request failed", 
                            service=self.service_name, 
                            endpoint=endpoint.name, 
                            error=str(e))
            raise
    
    async def delete(self, endpoint: ServiceEndpoint) -> Dict[str, Any]:
        """Make DELETE request with mTLS."""
        if not self.session:
            await self.start()
        
        url = f"https://{endpoint.host}:{endpoint.port}{endpoint.path}"
        
        try:
            async with self.session.delete(url) as response:
                if response.status in [200, 204]:
                    if response.status == 204:
                        return {}
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
                    
        except Exception as e:
            self.logger.error("mTLS DELETE request failed", 
                            service=self.service_name, 
                            endpoint=endpoint.name, 
                            error=str(e))
            raise
    
    async def health_check(self, endpoint: ServiceEndpoint) -> bool:
        """Check service health with mTLS."""
        try:
            health_endpoint = ServiceEndpoint(
                name=f"{endpoint.name}-health",
                host=endpoint.host,
                port=endpoint.port,
                path="/health"
            )
            
            result = await self.get(health_endpoint)
            return result.get("healthy", False)
            
        except Exception as e:
            self.logger.error("mTLS health check failed", 
                            service=self.service_name, 
                            endpoint=endpoint.name, 
                            error=str(e))
            return False


class ServiceRegistry:
    """Registry for service endpoints."""
    
    def __init__(self):
        self.services: Dict[str, ServiceEndpoint] = {}
        self.logger = structlog.get_logger("service-registry")
    
    def register_service(self, name: str, host: str, port: int, path: str = "/") -> None:
        """Register a service endpoint."""
        self.services[name] = ServiceEndpoint(
            name=name,
            host=host,
            port=port,
            path=path
        )
        self.logger.info("Service registered", name=name, host=host, port=port)
    
    def get_service(self, name: str) -> Optional[ServiceEndpoint]:
        """Get service endpoint by name."""
        return self.services.get(name)
    
    def list_services(self) -> Dict[str, ServiceEndpoint]:
        """List all registered services."""
        return self.services.copy()


class ServiceClient:
    """High-level service client with mTLS."""
    
    def __init__(self, cert_manager: CertificateManager, service_name: str):
        self.cert_manager = cert_manager
        self.service_name = service_name
        self.mtls_client = MTLSClient(cert_manager, service_name)
        self.service_registry = ServiceRegistry()
        self.logger = structlog.get_logger("service-client")
    
    async def start(self) -> None:
        """Start the service client."""
        await self.mtls_client.start()
        self.logger.info("Service client started", service=self.service_name)
    
    async def stop(self) -> None:
        """Stop the service client."""
        await self.mtls_client.stop()
        self.logger.info("Service client stopped", service=self.service_name)
    
    def register_service(self, name: str, host: str, port: int, path: str = "/") -> None:
        """Register a service endpoint."""
        self.service_registry.register_service(name, host, port, path)
    
    async def call_service(self, service_name: str, method: str, path: str, 
                          data: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Call another service with mTLS."""
        endpoint = self.service_registry.get_service(service_name)
        if not endpoint:
            raise ValueError(f"Service {service_name} not registered")
        
        # Update path
        endpoint.path = path
        
        # Make request based on method
        if method.upper() == "GET":
            return await self.mtls_client.get(endpoint, params)
        elif method.upper() == "POST":
            return await self.mtls_client.post(endpoint, data)
        elif method.upper() == "PUT":
            return await self.mtls_client.put(endpoint, data)
        elif method.upper() == "DELETE":
            return await self.mtls_client.delete(endpoint)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
    
    async def health_check_all(self) -> Dict[str, bool]:
        """Check health of all registered services."""
        results = {}
        
        for name, endpoint in self.service_registry.list_services().items():
            results[name] = await self.mtls_client.health_check(endpoint)
        
        return results
