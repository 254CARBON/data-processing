"""Certificate validation middleware for mTLS."""

import ssl
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class CertificateInfo:
    """Certificate information."""
    subject: str
    issuer: str
    not_before: datetime
    not_after: datetime
    serial_number: str
    fingerprint: str
    is_valid: bool
    days_until_expiry: int


class CertificateValidator:
    """Validates SSL certificates for mTLS."""
    
    def __init__(self):
        self.logger = structlog.get_logger("cert-validator")
        self.cert_cache: Dict[str, CertificateInfo] = {}
    
    def validate_certificate(self, cert_data: bytes) -> CertificateInfo:
        """Validate a certificate."""
        try:
            # Parse certificate
            cert = ssl.DER_cert_to_PEM_cert(cert_data)
            x509 = ssl.PEM_cert_to_DER_cert(cert)
            
            # Extract certificate information
            cert_info = self._extract_cert_info(x509)
            
            # Check validity
            now = datetime.utcnow()
            cert_info.is_valid = cert_info.not_before <= now <= cert_info.not_after
            
            # Calculate days until expiry
            cert_info.days_until_expiry = (cert_info.not_after - now).days
            
            # Cache certificate info
            self.cert_cache[cert_info.fingerprint] = cert_info
            
            self.logger.info("Certificate validated", 
                           subject=cert_info.subject,
                           is_valid=cert_info.is_valid,
                           days_until_expiry=cert_info.days_until_expiry)
            
            return cert_info
            
        except Exception as e:
            self.logger.error("Certificate validation failed", error=str(e))
            raise
    
    def validate_certificate_chain(self, cert_chain: list) -> bool:
        """Validate a certificate chain."""
        try:
            if not cert_chain:
                return False
            
            # Validate each certificate in the chain
            for i, cert_data in enumerate(cert_chain):
                cert_info = self.validate_certificate(cert_data)
                
                if not cert_info.is_valid:
                    self.logger.warning("Invalid certificate in chain", 
                                      position=i, 
                                      subject=cert_info.subject)
                    return False
            
            # TODO: Implement proper chain validation
            # For now, just check that all certificates are valid
            return True
            
        except Exception as e:
            self.logger.error("Certificate chain validation failed", error=str(e))
            return False
    
    def check_certificate_expiry(self, cert_info: CertificateInfo, 
                                warning_days: int = 30) -> Tuple[bool, str]:
        """Check if certificate is expiring soon."""
        if cert_info.days_until_expiry <= 0:
            return False, "Certificate has expired"
        elif cert_info.days_until_expiry <= warning_days:
            return True, f"Certificate expires in {cert_info.days_until_expiry} days"
        else:
            return True, "Certificate is valid"
    
    def get_certificate_info(self, fingerprint: str) -> Optional[CertificateInfo]:
        """Get cached certificate information."""
        return self.cert_cache.get(fingerprint)
    
    def _extract_cert_info(self, cert_data: bytes) -> CertificateInfo:
        """Extract information from certificate."""
        # This is a simplified implementation
        # In production, you'd use a proper X.509 library like cryptography
        
        # For now, return mock data
        return CertificateInfo(
            subject="CN=service",
            issuer="CN=254Carbon CA",
            not_before=datetime.utcnow() - timedelta(days=365),
            not_after=datetime.utcnow() + timedelta(days=365),
            serial_number="123456789",
            fingerprint="mock_fingerprint",
            is_valid=True,
            days_until_expiry=365
        )


class CertificateMonitoring:
    """Monitors certificate health and expiration."""
    
    def __init__(self, cert_validator: CertificateValidator):
        self.cert_validator = cert_validator
        self.logger = structlog.get_logger("cert-monitoring")
        self.monitored_certs: Dict[str, CertificateInfo] = {}
    
    def add_certificate(self, name: str, cert_info: CertificateInfo) -> None:
        """Add a certificate to monitoring."""
        self.monitored_certs[name] = cert_info
        self.logger.info("Certificate added to monitoring", name=name)
    
    def remove_certificate(self, name: str) -> None:
        """Remove a certificate from monitoring."""
        if name in self.monitored_certs:
            del self.monitored_certs[name]
            self.logger.info("Certificate removed from monitoring", name=name)
    
    def check_all_certificates(self, warning_days: int = 30) -> Dict[str, Dict[str, Any]]:
        """Check all monitored certificates."""
        results = {}
        
        for name, cert_info in self.monitored_certs.items():
            is_healthy, message = self.cert_validator.check_certificate_expiry(
                cert_info, warning_days
            )
            
            results[name] = {
                "healthy": is_healthy,
                "message": message,
                "days_until_expiry": cert_info.days_until_expiry,
                "expires_at": cert_info.not_after.isoformat(),
                "subject": cert_info.subject,
                "issuer": cert_info.issuer
            }
            
            if not is_healthy:
                self.logger.warning("Certificate health issue", 
                                  name=name, 
                                  message=message)
        
        return results
    
    def get_expiring_certificates(self, days: int = 30) -> Dict[str, CertificateInfo]:
        """Get certificates expiring within specified days."""
        expiring = {}
        
        for name, cert_info in self.monitored_certs.items():
            if 0 <= cert_info.days_until_expiry <= days:
                expiring[name] = cert_info
        
        return expiring
    
    def get_expired_certificates(self) -> Dict[str, CertificateInfo]:
        """Get expired certificates."""
        expired = {}
        
        for name, cert_info in self.monitored_certs.items():
            if cert_info.days_until_expiry < 0:
                expired[name] = cert_info
        
        return expired


class CertificateMiddleware:
    """Middleware for certificate validation in HTTP requests."""
    
    def __init__(self, cert_validator: CertificateValidator):
        self.cert_validator = cert_validator
        self.logger = structlog.get_logger("cert-middleware")
    
    async def validate_client_certificate(self, request) -> Tuple[bool, Optional[str]]:
        """Validate client certificate from request."""
        try:
            # Get client certificate from request
            # This depends on your web framework
            # For aiohttp, you'd need to configure SSL context
            
            # Mock implementation
            client_cert = request.get("client_cert")
            if not client_cert:
                return False, "No client certificate provided"
            
            # Validate certificate
            cert_info = self.cert_validator.validate_certificate(client_cert)
            
            if not cert_info.is_valid:
                return False, "Invalid client certificate"
            
            # Check expiry
            is_healthy, message = self.cert_validator.check_certificate_expiry(cert_info)
            if not is_healthy:
                return False, message
            
            return True, cert_info.subject
            
        except Exception as e:
            self.logger.error("Certificate validation error", error=str(e))
            return False, "Certificate validation failed"
    
    def get_certificate_headers(self, cert_info: CertificateInfo) -> Dict[str, str]:
        """Get certificate information headers."""
        return {
            "X-Client-Cert-Subject": cert_info.subject,
            "X-Client-Cert-Issuer": cert_info.issuer,
            "X-Client-Cert-Expires": cert_info.not_after.isoformat(),
            "X-Client-Cert-Days-Until-Expiry": str(cert_info.days_until_expiry)
        }
