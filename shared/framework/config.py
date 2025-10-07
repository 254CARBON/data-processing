"""
Configuration management for microservices.

Provides typed configuration classes with environment variable
injection and validation.
"""

import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = field(default_factory=lambda: os.getenv("DATA_PROC_KAFKA_BOOTSTRAP", "localhost:9092"))
    consumer_group: str = field(default_factory=lambda: os.getenv("DATA_PROC_CONSUMER_GROUP", "default-group"))
    auto_offset_reset: str = field(default_factory=lambda: os.getenv("DATA_PROC_KAFKA_AUTO_OFFSET_RESET", "latest"))
    enable_auto_commit: bool = field(default_factory=lambda: os.getenv("DATA_PROC_KAFKA_AUTO_COMMIT", "true").lower() == "true")
    max_poll_records: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_KAFKA_MAX_POLL_RECORDS", "500")))
    session_timeout_ms: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_KAFKA_SESSION_TIMEOUT_MS", "30000")))


@dataclass
class DatabaseConfig:
    """Database configuration."""
    clickhouse_url: str = field(default_factory=lambda: os.getenv("DATA_PROC_CLICKHOUSE_URL", "http://localhost:8123"))
    postgres_dsn: str = field(default_factory=lambda: os.getenv("DATA_PROC_POSTGRES_DSN", "postgresql://localhost:5432/data_proc"))
    redis_url: str = field(default_factory=lambda: os.getenv("DATA_PROC_REDIS_URL", "redis://localhost:6379/0"))


@dataclass
class AuditConfig:
    """Audit logging configuration."""

    enabled: bool = field(default_factory=lambda: os.getenv("DATA_PROC_AUDIT_ENABLED", "false").lower() == "true")
    clickhouse_table: str = field(default_factory=lambda: os.getenv("DATA_PROC_AUDIT_TABLE", "audit_events"))
    queue_max_size: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_QUEUE_SIZE", "5000")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_BATCH_SIZE", "200")))
    flush_interval_seconds: float = field(default_factory=lambda: float(os.getenv("DATA_PROC_AUDIT_FLUSH_INTERVAL", "2.0")))
    clickhouse_timeout: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_CLICKHOUSE_TIMEOUT", "30")))
    clickhouse_max_connections: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_CLICKHOUSE_MAX_CONN", "4")))
    kafka_topic: Optional[str] = field(default_factory=lambda: os.getenv("DATA_PROC_AUDIT_KAFKA_TOPIC"))
    kafka_batch_size: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_KAFKA_BATCH_SIZE", "100")))
    kafka_flush_timeout: float = field(default_factory=lambda: float(os.getenv("DATA_PROC_AUDIT_KAFKA_FLUSH_TIMEOUT", "1.0")))
    kafka_retry_backoff_ms: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_KAFKA_RETRY_BACKOFF_MS", "100")))
    kafka_max_retries: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_AUDIT_KAFKA_MAX_RETRIES", "3")))


@dataclass
class ObservabilityConfig:
    """Observability configuration."""
    log_level: str = field(default_factory=lambda: os.getenv("DATA_PROC_LOG_LEVEL", "info"))
    trace_enabled: bool = field(default_factory=lambda: os.getenv("DATA_PROC_TRACE_ENABLED", "true").lower() == "true")
    metrics_port: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_METRICS_PORT", "9090")))
    health_port: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_HEALTH_PORT", "8080")))


@dataclass
class ServiceConfig:
    """Base service configuration."""
    service_name: str
    environment: str = field(default_factory=lambda: os.getenv("DATA_PROC_ENV", "local"))
    tenant_id: str = field(default_factory=lambda: os.getenv("DATA_PROC_TENANT_ID", "default"))
    
    # Sub-configurations
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    audit: AuditConfig = field(default_factory=AuditConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    
    # Service-specific settings
    max_batch_size: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_MAX_BATCH_SIZE", "1000")))
    processing_timeout: int = field(default_factory=lambda: int(os.getenv("DATA_PROC_PROCESSING_TIMEOUT", "30")))
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.service_name:
            raise ValueError("service_name is required")
        
        if self.environment not in ["local", "dev", "staging", "prod"]:
            raise ValueError(f"Invalid environment: {self.environment}")
    
    @classmethod
    def from_env(cls, service_name: str) -> "ServiceConfig":
        """Create configuration from environment variables."""
        return cls(service_name=service_name)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "service_name": self.service_name,
            "environment": self.environment,
            "tenant_id": self.tenant_id,
            "kafka": {
                "bootstrap_servers": self.kafka.bootstrap_servers,
                "consumer_group": self.kafka.consumer_group,
                "auto_offset_reset": self.kafka.auto_offset_reset,
                "enable_auto_commit": self.kafka.enable_auto_commit,
                "max_poll_records": self.kafka.max_poll_records,
                "session_timeout_ms": self.kafka.session_timeout_ms,
            },
            "database": {
                "clickhouse_url": self.database.clickhouse_url,
                "postgres_dsn": self.database.postgres_dsn,
                "redis_url": self.database.redis_url,
            },
            "audit": {
                "enabled": self.audit.enabled,
                "clickhouse_table": self.audit.clickhouse_table,
                "queue_max_size": self.audit.queue_max_size,
                "batch_size": self.audit.batch_size,
                "flush_interval_seconds": self.audit.flush_interval_seconds,
                "kafka_topic": self.audit.kafka_topic,
            },
            "observability": {
                "log_level": self.observability.log_level,
                "trace_enabled": self.observability.trace_enabled,
                "metrics_port": self.observability.metrics_port,
                "health_port": self.observability.health_port,
            },
            "max_batch_size": self.max_batch_size,
            "processing_timeout": self.processing_timeout,
        }
