"""
Certificate validation utilities for mutual TLS enforcement.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import structlog
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import dsa, ec, rsa, padding
from cryptography.hazmat.primitives.serialization import Encoding


logger = structlog.get_logger()


def _format_name(name: x509.Name) -> str:
    parts = []
    for attribute in name:
        label = attribute.oid._name or attribute.oid.dotted_string  # type: ignore[attr-defined]
        parts.append(f"{label}={attribute.value}")
    return ", ".join(parts)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _compute_days_until(expiry: datetime, reference: datetime) -> int:
    delta: timedelta = expiry - reference
    return math.floor(delta.total_seconds() / 86400)


@dataclass
class CertificateInfo:
    """Materialized certificate metadata."""

    subject: str
    issuer: str
    not_before: datetime
    not_after: datetime
    serial_number: str
    fingerprint: str
    is_valid: bool
    days_until_expiry: int


class CertificateValidator:
    """Validates X.509 certificates and chains."""

    def __init__(self, trusted_certificates: Optional[Iterable[bytes]] = None) -> None:
        self.logger = structlog.get_logger("cert-validator")
        self.cert_cache: Dict[str, CertificateInfo] = {}
        self._trusted_roots: List[x509.Certificate] = []

        if trusted_certificates:
            for cert_bytes in trusted_certificates:
                try:
                    cert = self._load_certificate(cert_bytes)
                    self._trusted_roots.append(cert)
                    self.logger.info("Loaded trusted certificate", subject=_format_name(cert.subject))
                except Exception as exc:  # pragma: no cover - configuration issue
                    self.logger.error("Failed to load trusted certificate", error=str(exc))

    def validate_certificate(self, cert_data: bytes) -> CertificateInfo:
        """Parse and validate a single certificate."""
        certificate = self._load_certificate(cert_data)
        cert_info = self._extract_cert_info(certificate)

        now = datetime.now(timezone.utc)
        cert_info.is_valid = cert_info.not_before <= now <= cert_info.not_after
        cert_info.days_until_expiry = _compute_days_until(cert_info.not_after, now)

        self.cert_cache[cert_info.fingerprint] = cert_info

        if not cert_info.is_valid:
            self.logger.warning(
                "Certificate outside validity window",
                subject=cert_info.subject,
                not_before=cert_info.not_before.isoformat(),
                not_after=cert_info.not_after.isoformat(),
            )
        else:
            self.logger.info(
                "Certificate validated",
                subject=cert_info.subject,
                expires_at=cert_info.not_after.isoformat(),
                days_until_expiry=cert_info.days_until_expiry,
            )

        return cert_info

    def validate_certificate_chain(self, cert_chain: Sequence[bytes]) -> bool:
        """Validate a certificate chain from leaf → root."""
        if not cert_chain:
            self.logger.warning("Empty certificate chain provided")
            return False

        try:
            certificates = [self._load_certificate(cert) for cert in cert_chain]
        except Exception as exc:
            self.logger.error("Failed to parse certificate chain", error=str(exc))
            return False

        # Ensure each certificate is individually valid
        for cert in certificates:
            info = self.validate_certificate(cert.public_bytes(Encoding.DER))
            if not info.is_valid:
                self.logger.warning("Certificate in chain failed validity window check", subject=info.subject)
                return False

        # Verify signatures along the chain
        for idx in range(len(certificates) - 1):
            subject = certificates[idx]
            issuer = certificates[idx + 1]
            if not self._verify_signature(subject, issuer):
                self.logger.warning(
                    "Signature verification failed for certificate",
                    subject=_format_name(subject.subject),
                    issuer=_format_name(issuer.subject),
                    position=idx,
                )
                return False

        root_certificate = certificates[-1]
        if self._trusted_roots:
            root_fingerprint = root_certificate.fingerprint(hashes.SHA256()).hex()
            if not any(root_fingerprint == trusted.fingerprint(hashes.SHA256()).hex() for trusted in self._trusted_roots):
                self.logger.warning(
                    "Root certificate is not trusted",
                    subject=_format_name(root_certificate.subject),
                )
                return False
        else:
            if not self._verify_signature(root_certificate, root_certificate):
                self.logger.warning(
                    "Self-signed root verification failed",
                    subject=_format_name(root_certificate.subject),
                )
                return False

        self.logger.info(
            "Certificate chain validated",
            length=len(certificates),
            leaf_subject=_format_name(certificates[0].subject),
        )
        return True

    def check_certificate_expiry(self, cert_info: CertificateInfo, warning_days: int = 30) -> Tuple[bool, str]:
        """Check expiry window for a certificate."""
        if cert_info.days_until_expiry <= 0:
            return False, "Certificate has expired"
        if cert_info.days_until_expiry <= warning_days:
            return True, f"Certificate expires in {cert_info.days_until_expiry} days"
        return True, "Certificate is valid"

    def get_certificate_info(self, fingerprint: str) -> Optional[CertificateInfo]:
        """Retrieve cached certificate information."""
        return self.cert_cache.get(fingerprint)

    def _load_certificate(self, cert_data: bytes) -> x509.Certificate:
        """Attempt to load a certificate from DER or PEM bytes."""
        try:
            return x509.load_der_x509_certificate(cert_data)
        except ValueError:
            try:
                return x509.load_pem_x509_certificate(cert_data)
            except ValueError as exc:
                raise ValueError("Failed to decode certificate data") from exc

    def _extract_cert_info(self, certificate: x509.Certificate) -> CertificateInfo:
        """Build CertificateInfo from an x509 certificate."""
        subject = _format_name(certificate.subject)
        issuer = _format_name(certificate.issuer)
        fingerprint = certificate.fingerprint(hashes.SHA256()).hex()
        not_before = _to_utc(certificate.not_valid_before)
        not_after = _to_utc(certificate.not_valid_after)

        return CertificateInfo(
            subject=subject,
            issuer=issuer,
            not_before=not_before,
            not_after=not_after,
            serial_number=hex(certificate.serial_number),
            fingerprint=fingerprint,
            is_valid=True,
            days_until_expiry=_compute_days_until(not_after, datetime.now(timezone.utc)),
        )

    def _verify_signature(self, certificate: x509.Certificate, issuer: x509.Certificate) -> bool:
        """Verify certificate signature against issuer public key."""
        public_key = issuer.public_key()
        try:
            if isinstance(public_key, rsa.RSAPublicKey):
                public_key.verify(
                    certificate.signature,
                    certificate.tbs_certificate_bytes,
                    padding.PKCS1v15(),
                    certificate.signature_hash_algorithm,
                )
            elif isinstance(public_key, dsa.DSAPublicKey):
                public_key.verify(
                    certificate.signature,
                    certificate.tbs_certificate_bytes,
                    certificate.signature_hash_algorithm,
                )
            elif isinstance(public_key, ec.EllipticCurvePublicKey):
                public_key.verify(
                    certificate.signature,
                    certificate.tbs_certificate_bytes,
                    ec.ECDSA(certificate.signature_hash_algorithm),
                )
            else:  # pragma: no cover - uncommon key types
                self.logger.error(
                    "Unsupported public key type for certificate verification",
                    key_type=type(public_key).__name__,
                )
                return False
        except Exception as exc:
            self.logger.error(
                "Certificate signature verification failed",
                error=str(exc),
                subject=_format_name(certificate.subject),
            )
            return False
        return True


class CertificateMonitoring:
    """Monitors certificate health and expiration."""

    def __init__(self, cert_validator: CertificateValidator):
        self.cert_validator = cert_validator
        self.logger = structlog.get_logger("cert-monitoring")
        self.monitored_certs: Dict[str, CertificateInfo] = {}

    def add_certificate(self, name: str, cert_info: CertificateInfo) -> None:
        self.monitored_certs[name] = cert_info
        self.logger.info("Certificate added to monitoring", name=name)

    def remove_certificate(self, name: str) -> None:
        if name in self.monitored_certs:
            del self.monitored_certs[name]
            self.logger.info("Certificate removed from monitoring", name=name)

    def check_all_certificates(self, warning_days: int = 30) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}

        for name, cert_info in self.monitored_certs.items():
            is_healthy, message = self.cert_validator.check_certificate_expiry(cert_info, warning_days)

            results[name] = {
                "healthy": is_healthy,
                "message": message,
                "days_until_expiry": cert_info.days_until_expiry,
                "expires_at": cert_info.not_after.isoformat(),
                "subject": cert_info.subject,
                "issuer": cert_info.issuer,
            }

            if not is_healthy:
                self.logger.warning("Certificate health issue", name=name, message=message)

        return results

    def get_expiring_certificates(self, days: int = 30) -> Dict[str, CertificateInfo]:
        return {
            name: cert
            for name, cert in self.monitored_certs.items()
            if 0 <= cert.days_until_expiry <= days
        }

    def get_expired_certificates(self) -> Dict[str, CertificateInfo]:
        return {
            name: cert
            for name, cert in self.monitored_certs.items()
            if cert.days_until_expiry < 0
        }


class CertificateMiddleware:
    """Middleware helper for certificate validation in HTTP requests."""

    def __init__(self, cert_validator: CertificateValidator):
        self.cert_validator = cert_validator
        self.logger = structlog.get_logger("cert-middleware")

    async def validate_client_certificate(self, request: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate client certificate from a request dict-like payload."""
        client_cert = request.get("client_cert")
        if not client_cert:
            return False, "No client certificate provided"

        try:
            cert_info = self.cert_validator.validate_certificate(client_cert)
        except Exception as exc:
            self.logger.error("Client certificate validation failed", error=str(exc))
            return False, "Certificate validation failed"

        if not cert_info.is_valid:
            return False, "Invalid client certificate"

        chain: Optional[Sequence[bytes]] = request.get("client_cert_chain")
        if chain:
            chain_bytes: List[bytes] = [client_cert]
            chain_bytes.extend(cert for cert in chain if isinstance(cert, (bytes, bytearray)))
            if not self.cert_validator.validate_certificate_chain(chain_bytes):
                return False, "Certificate chain validation failed"

        is_healthy, message = self.cert_validator.check_certificate_expiry(cert_info)
        if not is_healthy:
            return False, message

        return True, cert_info.subject

    def get_certificate_headers(self, cert_info: CertificateInfo) -> Dict[str, str]:
        """Generate standard headers for propagating certificate metadata."""
        return {
            "X-Client-Cert-Subject": cert_info.subject,
            "X-Client-Cert-Issuer": cert_info.issuer,
            "X-Client-Cert-Expires": cert_info.not_after.isoformat(),
            "X-Client-Cert-Fingerprint": cert_info.fingerprint,
            "X-Client-Cert-Days-Until-Expiry": str(cert_info.days_until_expiry),
        }
