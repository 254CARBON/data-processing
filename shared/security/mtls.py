"""mTLS configuration and certificate management."""

import ssl
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MTLSConfig:
    """mTLS configuration for service communication."""
    
    ca_cert_path: str
    cert_path: str
    key_path: str
    verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED
    check_hostname: bool = True
    
    def to_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context from configuration."""
        context = ssl.create_default_context()
        context.load_verify_locations(self.ca_cert_path)
        context.load_cert_chain(self.cert_path, self.key_path)
        context.verify_mode = self.verify_mode
        context.check_hostname = self.check_hostname
        
        # Security settings
        context.set_ciphers('ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS')
        context.options |= ssl.OP_NO_SSLv2
        context.options |= ssl.OP_NO_SSLv3
        context.options |= ssl.OP_NO_TLSv1
        context.options |= ssl.OP_NO_TLSv1_1
        
        return context


class CertificateManager:
    """Manages certificates for mTLS communication."""
    
    def __init__(self, cert_dir: str = "certs"):
        self.cert_dir = Path(cert_dir)
        self.cert_dir.mkdir(exist_ok=True)
        
    def get_service_config(self, service_name: str) -> MTLSConfig:
        """Get mTLS configuration for a specific service."""
        return MTLSConfig(
            ca_cert_path=str(self.cert_dir / "ca-cert.pem"),
            cert_path=str(self.cert_dir / f"{service_name}-cert.pem"),
            key_path=str(self.cert_dir / f"{service_name}-key.pem")
        )
    
    def get_client_config(self) -> MTLSConfig:
        """Get mTLS configuration for client connections."""
        return MTLSConfig(
            ca_cert_path=str(self.cert_dir / "ca-cert.pem"),
            cert_path=str(self.cert_dir / "client-cert.pem"),
            key_path=str(self.cert_dir / "client-key.pem")
        )
    
    def validate_certificates(self) -> bool:
        """Validate that all required certificates exist."""
        required_files = [
            "ca-cert.pem",
            "client-cert.pem", 
            "client-key.pem"
        ]
        
        # Check for service certificates
        services = ["normalization-service", "enrichment-service", "aggregation-service", "projection-service"]
        for service in services:
            required_files.extend([
                f"{service}-cert.pem",
                f"{service}-key.pem"
            ])
        
        missing_files = []
        for file in required_files:
            if not (self.cert_dir / file).exists():
                missing_files.append(file)
        
        if missing_files:
            logger.error(f"Missing certificate files: {missing_files}")
            return False
            
        logger.info("All required certificates found")
        return True
    
    def get_certificate_info(self, cert_path: str) -> Dict[str, Any]:
        """Get information about a certificate."""
        import ssl
        import socket
        from datetime import datetime
        
        try:
            with open(cert_path, 'rb') as f:
                cert_data = f.read()
            
            cert = ssl.DER_cert_to_PEM_cert(cert_data)
            x509 = ssl.PEM_cert_to_DER_cert(cert)
            
            # Parse certificate (simplified)
            return {
                "path": cert_path,
                "exists": True,
                "size": len(cert_data)
            }
        except Exception as e:
            logger.error(f"Error reading certificate {cert_path}: {e}")
            return {
                "path": cert_path,
                "exists": False,
                "error": str(e)
            }
