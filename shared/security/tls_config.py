"""
TLS configuration and management for the 254Carbon Data Processing Pipeline.
Handles TLS certificates, SSL contexts, and secure communication setup.
"""

import os
import ssl
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import datetime

logger = logging.getLogger(__name__)


class TLSConfig:
    """Manages TLS configuration for secure communication."""
    
    def __init__(self, cert_dir: str = "/etc/certs"):
        self.cert_dir = Path(cert_dir)
        self.cert_dir.mkdir(parents=True, exist_ok=True)
        
        # TLS configuration
        self.tls_version = ssl.PROTOCOL_TLSv1_2
        self.cipher_suites = [
            'ECDHE-RSA-AES256-GCM-SHA384',
            'ECDHE-RSA-AES128-GCM-SHA256',
            'ECDHE-RSA-AES256-SHA384',
            'ECDHE-RSA-AES128-SHA256',
            'AES256-GCM-SHA384',
            'AES128-GCM-SHA256'
        ]
        
        # Certificate paths
        self.ca_cert_path = self.cert_dir / "ca-cert.pem"
        self.ca_key_path = self.cert_dir / "ca-key.pem"
        self.server_cert_path = self.cert_dir / "server-cert.pem"
        self.server_key_path = self.cert_dir / "server-key.pem"
        self.client_cert_path = self.cert_dir / "client-cert.pem"
        self.client_key_path = self.cert_dir / "client-key.pem"
    
    def create_ssl_context(self, 
                          server_side: bool = False,
                          verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED,
                          check_hostname: bool = True) -> ssl.SSLContext:
        """Create an SSL context with proper security settings."""
        
        # Create SSL context
        context = ssl.create_default_context()
        
        if server_side:
            # Server-side configuration
            context.load_cert_chain(
                str(self.server_cert_path),
                str(self.server_key_path)
            )
            context.load_verify_locations(str(self.ca_cert_path))
        else:
            # Client-side configuration
            context.load_cert_chain(
                str(self.client_cert_path),
                str(self.client_key_path)
            )
            context.load_verify_locations(str(self.ca_cert_path))
        
        # Security settings
        context.verify_mode = verify_mode
        context.check_hostname = check_hostname
        
        # Disable weak protocols and ciphers
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        
        # Set cipher suites
        context.set_ciphers(':'.join(self.cipher_suites))
        
        # Additional security options
        context.options |= ssl.OP_NO_SSLv2
        context.options |= ssl.OP_NO_SSLv3
        context.options |= ssl.OP_NO_TLSv1
        context.options |= ssl.OP_NO_TLSv1_1
        context.options |= ssl.OP_NO_COMPRESSION
        context.options |= ssl.OP_SINGLE_DH_USE
        context.options |= ssl.OP_SINGLE_ECDH_USE
        
        return context
    
    def create_server_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for server-side connections."""
        return self.create_ssl_context(
            server_side=True,
            verify_mode=ssl.CERT_REQUIRED,
            check_hostname=False
        )
    
    def create_client_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for client-side connections."""
        return self.create_ssl_context(
            server_side=False,
            verify_mode=ssl.CERT_REQUIRED,
            check_hostname=True
        )
    
    def verify_certificate(self, cert_path: Path) -> Dict[str, Any]:
        """Verify a certificate and return its information."""
        try:
            with open(cert_path, 'rb') as f:
                cert_data = f.read()
            
            cert = x509.load_pem_x509_certificate(cert_data)
            
            # Check expiration
            now = datetime.datetime.utcnow()
            is_expired = cert.not_valid_after < now
            days_until_expiry = (cert.not_valid_after - now).days
            
            # Extract certificate information
            cert_info = {
                'subject': cert.subject.rfc4514_string(),
                'issuer': cert.issuer.rfc4514_string(),
                'serial_number': str(cert.serial_number),
                'not_valid_before': cert.not_valid_before.isoformat(),
                'not_valid_after': cert.not_valid_after.isoformat(),
                'is_expired': is_expired,
                'days_until_expiry': days_until_expiry,
                'version': cert.version.name,
                'signature_algorithm': cert.signature_algorithm_oid._name,
                'public_key_algorithm': cert.public_key().key_size if hasattr(cert.public_key(), 'key_size') else 'Unknown'
            }
            
            return cert_info
            
        except Exception as e:
            logger.error(f"Failed to verify certificate {cert_path}: {e}")
            return {'error': str(e)}
    
    def check_certificate_health(self) -> Dict[str, Any]:
        """Check the health of all certificates."""
        health_status = {
            'ca_cert': self.verify_certificate(self.ca_cert_path),
            'server_cert': self.verify_certificate(self.server_cert_path),
            'client_cert': self.verify_certificate(self.client_cert_path),
            'overall_status': 'healthy'
        }
        
        # Check for expired certificates
        expired_certs = []
        for cert_name, cert_info in health_status.items():
            if cert_name != 'overall_status' and cert_info.get('is_expired', False):
                expired_certs.append(cert_name)
        
        if expired_certs:
            health_status['overall_status'] = 'unhealthy'
            health_status['expired_certificates'] = expired_certs
        
        return health_status
    
    def get_tls_config_for_service(self, service_name: str) -> Dict[str, Any]:
        """Get TLS configuration for a specific service."""
        return {
            'cert_file': str(self.server_cert_path),
            'key_file': str(self.server_key_path),
            'ca_file': str(self.ca_cert_path),
            'verify_mode': 'required',
            'check_hostname': False,
            'min_version': 'TLSv1.2',
            'max_version': 'TLSv1.3',
            'cipher_suites': self.cipher_suites
        }


class CertificateManager:
    """Manages certificate generation, renewal, and validation."""
    
    def __init__(self, cert_dir: str = "/etc/certs"):
        self.cert_dir = Path(cert_dir)
        self.cert_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_ca_certificate(self, 
                               common_name: str = "254CarbonCA",
                               validity_days: int = 3650) -> bool:
        """Generate a Certificate Authority (CA) certificate."""
        try:
            # Generate private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            
            # Create certificate
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "254Carbon"),
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            ])
            
            cert = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                issuer
            ).public_key(
                private_key.public_key()
            ).serial_number(
                x509.random_serial_number()
            ).not_valid_before(
                datetime.datetime.utcnow()
            ).not_valid_after(
                datetime.datetime.utcnow() + datetime.timedelta(days=validity_days)
            ).add_extension(
                x509.BasicConstraints(ca=True, path_length=None),
                critical=True,
            ).sign(private_key, hashes.SHA256())
            
            # Save certificate and key
            with open(self.cert_dir / "ca-cert.pem", "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))
            
            with open(self.cert_dir / "ca-key.pem", "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))
            
            logger.info(f"CA certificate generated successfully: {common_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate CA certificate: {e}")
            return False
    
    def generate_server_certificate(self,
                                   common_name: str,
                                   san_list: List[str] = None,
                                   validity_days: int = 365) -> bool:
        """Generate a server certificate signed by the CA."""
        try:
            # Load CA certificate and key
            with open(self.cert_dir / "ca-cert.pem", "rb") as f:
                ca_cert = x509.load_pem_x509_certificate(f.read())
            
            with open(self.cert_dir / "ca-key.pem", "rb") as f:
                ca_key = serialization.load_pem_private_key(f.read(), password=None)
            
            # Generate server private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            
            # Create certificate
            subject = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "254Carbon"),
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            ])
            
            cert_builder = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                ca_cert.subject
            ).public_key(
                private_key.public_key()
            ).serial_number(
                x509.random_serial_number()
            ).not_valid_before(
                datetime.datetime.utcnow()
            ).not_valid_after(
                datetime.datetime.utcnow() + datetime.timedelta(days=validity_days)
            )
            
            # Add Subject Alternative Names (SAN)
            if san_list:
                san_extension = x509.SubjectAlternativeName([
                    x509.DNSName(name) for name in san_list
                ])
                cert_builder = cert_builder.add_extension(san_extension, critical=False)
            
            cert = cert_builder.sign(ca_key, hashes.SHA256())
            
            # Save certificate and key
            with open(self.cert_dir / "server-cert.pem", "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))
            
            with open(self.cert_dir / "server-key.pem", "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))
            
            logger.info(f"Server certificate generated successfully: {common_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate server certificate: {e}")
            return False
    
    def generate_client_certificate(self,
                                  common_name: str,
                                  validity_days: int = 365) -> bool:
        """Generate a client certificate signed by the CA."""
        try:
            # Load CA certificate and key
            with open(self.cert_dir / "ca-cert.pem", "rb") as f:
                ca_cert = x509.load_pem_x509_certificate(f.read())
            
            with open(self.cert_dir / "ca-key.pem", "rb") as f:
                ca_key = serialization.load_pem_private_key(f.read(), password=None)
            
            # Generate client private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            
            # Create certificate
            subject = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "254Carbon"),
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            ])
            
            cert = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                ca_cert.subject
            ).public_key(
                private_key.public_key()
            ).serial_number(
                x509.random_serial_number()
            ).not_valid_before(
                datetime.datetime.utcnow()
            ).not_valid_after(
                datetime.datetime.utcnow() + datetime.timedelta(days=validity_days)
            ).add_extension(
                x509.KeyUsage(
                    key_cert_sign=False,
                    crl_sign=False,
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False
                ),
                critical=True,
            ).add_extension(
                x509.ExtendedKeyUsage([
                    x509.ExtendedKeyUsageOID.CLIENT_AUTH
                ]),
                critical=True,
            ).sign(ca_key, hashes.SHA256())
            
            # Save certificate and key
            with open(self.cert_dir / "client-cert.pem", "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))
            
            with open(self.cert_dir / "client-key.pem", "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))
            
            logger.info(f"Client certificate generated successfully: {common_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate client certificate: {e}")
            return False
    
    def renew_certificate(self, cert_type: str) -> bool:
        """Renew a certificate."""
        if cert_type == "server":
            return self.generate_server_certificate("254Carbon Server")
        elif cert_type == "client":
            return self.generate_client_certificate("254Carbon Client")
        else:
            logger.error(f"Unknown certificate type: {cert_type}")
            return False


class TLSSecurityHeaders:
    """Manages TLS-related security headers for HTTP responses."""
    
    @staticmethod
    def get_security_headers() -> Dict[str, str]:
        """Get TLS-related security headers."""
        return {
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block',
            'Referrer-Policy': 'strict-origin-when-cross-origin',
            'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
            'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
            'Cross-Origin-Embedder-Policy': 'require-corp',
            'Cross-Origin-Opener-Policy': 'same-origin',
            'Cross-Origin-Resource-Policy': 'same-origin'
        }
    
    @staticmethod
    def apply_security_headers(response):
        """Apply security headers to a response object."""
        headers = TLSSecurityHeaders.get_security_headers()
        for header, value in headers.items():
            response.headers[header] = value
        return response


# Global TLS configuration instance
_tls_config = None

def get_tls_config() -> TLSConfig:
    """Get the global TLS configuration instance."""
    global _tls_config
    if _tls_config is None:
        cert_dir = os.getenv('DATA_PROC_CERT_DIR', '/etc/certs')
        _tls_config = TLSConfig(cert_dir)
    return _tls_config

def get_certificate_manager() -> CertificateManager:
    """Get a certificate manager instance."""
    cert_dir = os.getenv('DATA_PROC_CERT_DIR', '/etc/certs')
    return CertificateManager(cert_dir)


# Example usage and testing
if __name__ == "__main__":
    import tempfile
    import shutil
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Test certificate manager
        cert_manager = CertificateManager(temp_dir)
        
        # Generate CA certificate
        print("Generating CA certificate...")
        if cert_manager.generate_ca_certificate():
            print("✅ CA certificate generated successfully")
        else:
            print("❌ Failed to generate CA certificate")
        
        # Generate server certificate
        print("\nGenerating server certificate...")
        if cert_manager.generate_server_certificate("254Carbon Server", ["localhost", "127.0.0.1"]):
            print("✅ Server certificate generated successfully")
        else:
            print("❌ Failed to generate server certificate")
        
        # Generate client certificate
        print("\nGenerating client certificate...")
        if cert_manager.generate_client_certificate("254Carbon Client"):
            print("✅ Client certificate generated successfully")
        else:
            print("❌ Failed to generate client certificate")
        
        # Test TLS configuration
        print("\nTesting TLS configuration...")
        tls_config = TLSConfig(temp_dir)
        
        # Check certificate health
        health_status = tls_config.check_certificate_health()
        print(f"Certificate health: {health_status['overall_status']}")
        
        # Test SSL context creation
        try:
            server_context = tls_config.create_server_ssl_context()
            client_context = tls_config.create_client_ssl_context()
            print("✅ SSL contexts created successfully")
        except Exception as e:
            print(f"❌ Failed to create SSL contexts: {e}")
        
        # Test security headers
        print("\nTesting security headers...")
        headers = TLSSecurityHeaders.get_security_headers()
        print(f"Security headers: {len(headers)} headers configured")
        
        print("\nTLS configuration test completed successfully!")
        
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
