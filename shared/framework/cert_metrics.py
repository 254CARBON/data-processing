"""Certificate metrics for Prometheus monitoring."""

import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
import structlog

from ..security.cert_validation import CertificateInfo, CertificateMonitoring

logger = structlog.get_logger()


class CertificateMetrics:
    """Certificate metrics for monitoring."""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self.logger = structlog.get_logger("cert-metrics")
        
        # Certificate validation metrics
        self.cert_validation_total = Counter(
            'certificate_validation_total',
            'Total certificate validations',
            ['service_name', 'result'],
            registry=self.registry
        )
        
        self.cert_validation_duration = Histogram(
            'certificate_validation_duration_seconds',
            'Certificate validation duration',
            ['service_name'],
            registry=self.registry
        )
        
        self.cert_validation_errors = Counter(
            'certificate_validation_errors_total',
            'Certificate validation errors',
            ['service_name', 'error_type'],
            registry=self.registry
        )
        
        # Certificate expiry metrics
        self.cert_expiry_days = Gauge(
            'certificate_expiry_days',
            'Days until certificate expires',
            ['certificate_name', 'service_name'],
            registry=self.registry
        )
        
        self.cert_valid = Gauge(
            'certificate_valid',
            'Certificate validity status',
            ['certificate_name', 'service_name'],
            registry=self.registry
        )
        
        # Certificate chain metrics
        self.cert_chain_valid = Gauge(
            'certificate_chain_valid',
            'Certificate chain validity status',
            ['service_name'],
            registry=self.registry
        )
        
        self.cert_chain_length = Gauge(
            'certificate_chain_length',
            'Certificate chain length',
            ['service_name'],
            registry=self.registry
        )
        
        # mTLS connection metrics
        self.mtls_connections_total = Counter(
            'mtls_connections_total',
            'Total mTLS connections',
            ['service_name', 'result'],
            registry=self.registry
        )
        
        self.mtls_connection_failures = Counter(
            'mtls_connection_failures_total',
            'mTLS connection failures',
            ['service_name', 'error_type'],
            registry=self.registry
        )
        
        self.mtls_connection_duration = Histogram(
            'mtls_connection_duration_seconds',
            'mTLS connection duration',
            ['service_name'],
            registry=self.registry
        )
        
        # Service identity metrics
        self.service_identity_verifications = Counter(
            'service_identity_verification_total',
            'Service identity verifications',
            ['service_name', 'result'],
            registry=self.registry
        )
        
        self.service_identity_failures = Counter(
            'service_identity_verification_failures_total',
            'Service identity verification failures',
            ['service_name', 'error_type'],
            registry=self.registry
        )
        
        # Certificate storage metrics
        self.cert_storage_usage = Gauge(
            'certificate_storage_usage_percent',
            'Certificate storage usage percentage',
            registry=self.registry
        )
        
        self.cert_storage_total = Gauge(
            'certificate_storage_total_bytes',
            'Total certificate storage size',
            registry=self.registry
        )
        
        self.cert_storage_free = Gauge(
            'certificate_storage_free_bytes',
            'Free certificate storage space',
            registry=self.registry
        )
    
    def record_certificate_validation(self, service_name: str, result: str, 
                                    duration: float, error_type: str = None) -> None:
        """Record certificate validation metrics."""
        self.cert_validation_total.labels(service_name=service_name, result=result).inc()
        self.cert_validation_duration.labels(service_name=service_name).observe(duration)
        
        if result == "error" and error_type:
            self.cert_validation_errors.labels(
                service_name=service_name, 
                error_type=error_type
            ).inc()
    
    def update_certificate_info(self, cert_name: str, service_name: str, 
                              cert_info: CertificateInfo) -> None:
        """Update certificate information metrics."""
        self.cert_expiry_days.labels(
            certificate_name=cert_name,
            service_name=service_name
        ).set(cert_info.days_until_expiry)
        
        self.cert_valid.labels(
            certificate_name=cert_name,
            service_name=service_name
        ).set(1 if cert_info.is_valid else 0)
    
    def update_certificate_chain(self, service_name: str, is_valid: bool, 
                               chain_length: int) -> None:
        """Update certificate chain metrics."""
        self.cert_chain_valid.labels(service_name=service_name).set(1 if is_valid else 0)
        self.cert_chain_length.labels(service_name=service_name).set(chain_length)
    
    def record_mtls_connection(self, service_name: str, result: str, 
                             duration: float, error_type: str = None) -> None:
        """Record mTLS connection metrics."""
        self.mtls_connections_total.labels(service_name=service_name, result=result).inc()
        self.mtls_connection_duration.labels(service_name=service_name).observe(duration)
        
        if result == "error" and error_type:
            self.mtls_connection_failures.labels(
                service_name=service_name,
                error_type=error_type
            ).inc()
    
    def record_service_identity_verification(self, service_name: str, result: str, 
                                            error_type: str = None) -> None:
        """Record service identity verification metrics."""
        self.service_identity_verifications.labels(
            service_name=service_name, 
            result=result
        ).inc()
        
        if result == "error" and error_type:
            self.service_identity_failures.labels(
                service_name=service_name,
                error_type=error_type
            ).inc()
    
    def update_storage_metrics(self, total_bytes: int, free_bytes: int) -> None:
        """Update certificate storage metrics."""
        self.cert_storage_total.set(total_bytes)
        self.cert_storage_free.set(free_bytes)
        
        if total_bytes > 0:
            usage_percent = ((total_bytes - free_bytes) / total_bytes) * 100
            self.cert_storage_usage.set(usage_percent)


class CertificateMetricsCollector:
    """Collects certificate metrics from monitoring."""
    
    def __init__(self, cert_monitoring: CertificateMonitoring, 
                 cert_metrics: CertificateMetrics):
        self.cert_monitoring = cert_monitoring
        self.cert_metrics = cert_metrics
        self.logger = structlog.get_logger("cert-metrics-collector")
    
    def collect_metrics(self) -> None:
        """Collect all certificate metrics."""
        try:
            # Get certificate health information
            cert_health = self.cert_monitoring.check_all_certificates()
            
            for cert_name, health_info in cert_health.items():
                # Update certificate metrics
                cert_info = self.cert_monitoring.monitored_certs.get(cert_name)
                if cert_info:
                    self.cert_metrics.update_certificate_info(
                        cert_name, "unknown", cert_info
                    )
                
                # Record validation result
                result = "success" if health_info["healthy"] else "error"
                self.cert_metrics.record_certificate_validation(
                    "unknown", result, 0.1
                )
            
            # Get expiring certificates
            expiring_certs = self.cert_monitoring.get_expiring_certificates(30)
            for cert_name, cert_info in expiring_certs.items():
                self.logger.warning("Certificate expiring soon", 
                                  cert_name=cert_name,
                                  days_until_expiry=cert_info.days_until_expiry)
            
            # Get expired certificates
            expired_certs = self.cert_monitoring.get_expired_certificates()
            for cert_name, cert_info in expired_certs.items():
                self.logger.error("Certificate expired", 
                                cert_name=cert_name,
                                days_since_expiry=abs(cert_info.days_until_expiry))
            
        except Exception as e:
            self.logger.error("Failed to collect certificate metrics", error=str(e))
    
    def start_collection(self, interval: int = 60) -> None:
        """Start periodic metric collection."""
        import asyncio
        
        async def collect_loop():
            while True:
                try:
                    self.collect_metrics()
                    await asyncio.sleep(interval)
                except Exception as e:
                    self.logger.error("Error in metric collection loop", error=str(e))
                    await asyncio.sleep(interval)
        
        # Start collection loop
        asyncio.create_task(collect_loop())
        self.logger.info("Certificate metrics collection started", interval=interval)
