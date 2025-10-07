"""Service identity verification for mTLS."""

import hashlib
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class ServiceRole(str, Enum):
    """Service role enumeration."""
    NORMALIZATION = "normalization"
    ENRICHMENT = "enrichment"
    AGGREGATION = "aggregation"
    PROJECTION = "projection"
    MONITORING = "monitoring"
    GATEWAY = "gateway"


@dataclass
class ServiceIdentity:
    """Service identity information."""
    service_name: str
    service_id: str
    role: ServiceRole
    tenant_id: Optional[str]
    permissions: List[str]
    certificate_fingerprint: str
    is_verified: bool
    metadata: Dict[str, Any]


class ServiceIdentityVerifier:
    """Verifies service identity for mTLS communication."""
    
    def __init__(self):
        self.logger = structlog.get_logger("service-identity-verifier")
        self.known_services: Dict[str, ServiceIdentity] = {}
        self.service_permissions: Dict[ServiceRole, List[str]] = {
            ServiceRole.NORMALIZATION: ["read:raw_data", "write:normalized_data"],
            ServiceRole.ENRICHMENT: ["read:normalized_data", "write:enriched_data"],
            ServiceRole.AGGREGATION: ["read:enriched_data", "write:aggregated_data"],
            ServiceRole.PROJECTION: ["read:aggregated_data", "write:projected_data"],
            ServiceRole.MONITORING: ["read:metrics", "read:logs"],
            ServiceRole.GATEWAY: ["read:all", "write:all"]
        }
    
    def register_service(self, service_name: str, service_id: str, 
                        role: ServiceRole, tenant_id: Optional[str] = None,
                        certificate_fingerprint: str = None,
                        metadata: Dict[str, Any] = None) -> ServiceIdentity:
        """Register a service identity."""
        service_identity = ServiceIdentity(
            service_name=service_name,
            service_id=service_id,
            role=role,
            tenant_id=tenant_id,
            permissions=self.service_permissions.get(role, []),
            certificate_fingerprint=certificate_fingerprint or "",
            is_verified=False,
            metadata=metadata or {}
        )
        
        self.known_services[service_id] = service_identity
        
        self.logger.info("Service registered", 
                        service_name=service_name,
                        service_id=service_id,
                        role=role.value)
        
        return service_identity
    
    def verify_service_identity(self, service_id: str, 
                               certificate_fingerprint: str = None) -> Optional[ServiceIdentity]:
        """Verify service identity."""
        if service_id not in self.known_services:
            self.logger.warning("Unknown service ID", service_id=service_id)
            return None
        
        service_identity = self.known_services[service_id]
        
        # Verify certificate fingerprint if provided
        if certificate_fingerprint:
            if service_identity.certificate_fingerprint != certificate_fingerprint:
                self.logger.warning("Certificate fingerprint mismatch", 
                                  service_id=service_id,
                                  expected=service_identity.certificate_fingerprint,
                                  actual=certificate_fingerprint)
                return None
        
        # Mark as verified
        service_identity.is_verified = True
        
        self.logger.info("Service identity verified", 
                        service_name=service_identity.service_name,
                        service_id=service_id)
        
        return service_identity
    
    def check_service_permission(self, service_id: str, permission: str) -> bool:
        """Check if service has a specific permission."""
        if service_id not in self.known_services:
            return False
        
        service_identity = self.known_services[service_id]
        return permission in service_identity.permissions
    
    def get_service_permissions(self, service_id: str) -> List[str]:
        """Get permissions for a service."""
        if service_id not in self.known_services:
            return []
        
        return self.known_services[service_id].permissions
    
    def list_services(self) -> Dict[str, ServiceIdentity]:
        """List all registered services."""
        return self.known_services.copy()
    
    def get_service_by_name(self, service_name: str) -> Optional[ServiceIdentity]:
        """Get service identity by name."""
        for service_identity in self.known_services.values():
            if service_identity.service_name == service_name:
                return service_identity
        return None
    
    def get_services_by_role(self, role: ServiceRole) -> List[ServiceIdentity]:
        """Get services by role."""
        return [si for si in self.known_services.values() if si.role == role]
    
    def get_services_by_tenant(self, tenant_id: str) -> List[ServiceIdentity]:
        """Get services by tenant."""
        return [si for si in self.known_services.values() if si.tenant_id == tenant_id]


class ServiceIdentityMiddleware:
    """Middleware for service identity verification."""
    
    def __init__(self, identity_verifier: ServiceIdentityVerifier):
        self.identity_verifier = identity_verifier
        self.logger = structlog.get_logger("service-identity-middleware")
    
    async def verify_request_identity(self, request) -> Tuple[bool, Optional[ServiceIdentity]]:
        """Verify service identity from request."""
        try:
            # Get service ID from headers
            service_id = request.headers.get("X-Service-ID")
            if not service_id:
                return False, None
            
            # Get certificate fingerprint from headers (if available)
            cert_fingerprint = request.headers.get("X-Certificate-Fingerprint")
            
            # Verify service identity
            service_identity = self.identity_verifier.verify_service_identity(
                service_id, cert_fingerprint
            )
            
            if not service_identity:
                return False, None
            
            return True, service_identity
            
        except Exception as e:
            self.logger.error("Service identity verification error", error=str(e))
            return False, None
    
    def check_request_permission(self, service_identity: ServiceIdentity, 
                               required_permission: str) -> bool:
        """Check if service has required permission."""
        return self.identity_verifier.check_service_permission(
            service_identity.service_id, required_permission
        )
    
    def get_identity_headers(self, service_identity: ServiceIdentity) -> Dict[str, str]:
        """Get service identity headers."""
        return {
            "X-Service-ID": service_identity.service_id,
            "X-Service-Name": service_identity.service_name,
            "X-Service-Role": service_identity.role.value,
            "X-Service-Tenant": service_identity.tenant_id or "",
            "X-Service-Permissions": ",".join(service_identity.permissions)
        }


class ServiceDiscovery:
    """Service discovery for mTLS communication."""
    
    def __init__(self, identity_verifier: ServiceIdentityVerifier):
        self.identity_verifier = identity_verifier
        self.logger = structlog.get_logger("service-discovery")
        self.service_endpoints: Dict[str, Dict[str, Any]] = {}
    
    def register_service_endpoint(self, service_id: str, host: str, port: int, 
                                 path: str = "/", protocol: str = "https") -> None:
        """Register a service endpoint."""
        self.service_endpoints[service_id] = {
            "host": host,
            "port": port,
            "path": path,
            "protocol": protocol
        }
        
        self.logger.info("Service endpoint registered", 
                        service_id=service_id,
                        host=host,
                        port=port)
    
    def get_service_endpoint(self, service_id: str) -> Optional[Dict[str, Any]]:
        """Get service endpoint by ID."""
        return self.service_endpoints.get(service_id)
    
    def get_service_endpoint_by_name(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Get service endpoint by name."""
        service_identity = self.identity_verifier.get_service_by_name(service_name)
        if not service_identity:
            return None
        
        return self.get_service_endpoint(service_identity.service_id)
    
    def list_service_endpoints(self) -> Dict[str, Dict[str, Any]]:
        """List all service endpoints."""
        return self.service_endpoints.copy()
    
    def discover_services(self, role: ServiceRole = None) -> List[Dict[str, Any]]:
        """Discover services by role."""
        services = []
        
        if role:
            service_identities = self.identity_verifier.get_services_by_role(role)
        else:
            service_identities = list(self.identity_verifier.known_services.values())
        
        for service_identity in service_identities:
            endpoint = self.get_service_endpoint(service_identity.service_id)
            if endpoint:
                services.append({
                    "service_id": service_identity.service_id,
                    "service_name": service_identity.service_name,
                    "role": service_identity.role.value,
                    "tenant_id": service_identity.tenant_id,
                    "endpoint": endpoint
                })
        
        return services
