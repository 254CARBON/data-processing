"""
Core framework components for async microservices.

Provides base classes and abstractions for building scalable,
observable microservices with Kafka integration.
"""

from .service import AsyncService
from .consumer import KafkaConsumer
from .producer import KafkaProducer
from .config import ServiceConfig, AuditConfig
from .health import HealthChecker
from .metrics import MetricsCollector

__all__ = [
    "AsyncService",
    "KafkaConsumer", 
    "KafkaProducer",
    "ServiceConfig",
    "AuditConfig",
    "HealthChecker",
    "MetricsCollector",
]
