"""
Custom error classes for data processing services.

Provides structured error handling with error codes,
context information, and proper exception chaining.
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class ErrorContext:
    """Error context information."""
    service: str
    operation: str
    tenant_id: Optional[str] = None
    instrument_id: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class DataProcessingError(Exception):
    """Base exception for data processing errors."""
    
    def __init__(
        self,
        message: str,
        error_code: str,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context
        self.details = details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary."""
        result = {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
        }
        
        if self.context:
            result["context"] = {
                "service": self.context.service,
                "operation": self.context.operation,
                "tenant_id": self.context.tenant_id,
                "instrument_id": self.context.instrument_id,
                "correlation_id": self.context.correlation_id,
                "metadata": self.context.metadata,
            }
        
        return result


class ValidationError(DataProcessingError):
    """Error raised when data validation fails."""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            context=context,
            details=details or {}
        )
        self.field = field
        self.value = value
        
        if field:
            self.details["field"] = field
        if value is not None:
            self.details["value"] = str(value)


class ProcessingError(DataProcessingError):
    """Error raised during data processing."""
    
    def __init__(
        self,
        message: str,
        stage: Optional[str] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="PROCESSING_ERROR",
            context=context,
            details=details or {}
        )
        self.stage = stage
        
        if stage:
            self.details["stage"] = stage


class SchemaError(DataProcessingError):
    """Error raised when schema validation fails."""
    
    def __init__(
        self,
        message: str,
        schema_name: Optional[str] = None,
        schema_version: Optional[str] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="SCHEMA_ERROR",
            context=context,
            details=details or {}
        )
        self.schema_name = schema_name
        self.schema_version = schema_version
        
        if schema_name:
            self.details["schema_name"] = schema_name
        if schema_version:
            self.details["schema_version"] = schema_version


class StorageError(DataProcessingError):
    """Error raised when storage operations fail."""
    
    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        table: Optional[str] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="STORAGE_ERROR",
            context=context,
            details=details or {}
        )
        self.operation = operation
        self.table = table
        
        if operation:
            self.details["operation"] = operation
        if table:
            self.details["table"] = table


class KafkaError(DataProcessingError):
    """Error raised when Kafka operations fail."""
    
    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="KAFKA_ERROR",
            context=context,
            details=details or {}
        )
        self.topic = topic
        self.partition = partition
        self.offset = offset
        
        if topic:
            self.details["topic"] = topic
        if partition is not None:
            self.details["partition"] = partition
        if offset is not None:
            self.details["offset"] = offset


class ConfigurationError(DataProcessingError):
    """Error raised when configuration is invalid."""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="CONFIGURATION_ERROR",
            context=context,
            details=details or {}
        )
        self.config_key = config_key
        self.config_value = config_value
        
        if config_key:
            self.details["config_key"] = config_key
        if config_value is not None:
            self.details["config_value"] = str(config_value)


class TimeoutError(DataProcessingError):
    """Error raised when operations timeout."""
    
    def __init__(
        self,
        message: str,
        timeout_seconds: Optional[float] = None,
        operation: Optional[str] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="TIMEOUT_ERROR",
            context=context,
            details=details or {}
        )
        self.timeout_seconds = timeout_seconds
        self.operation = operation
        
        if timeout_seconds is not None:
            self.details["timeout_seconds"] = timeout_seconds
        if operation:
            self.details["operation"] = operation


class CircuitBreakerError(DataProcessingError):
    """Error raised when circuit breaker is open."""
    
    def __init__(
        self,
        message: str,
        service: Optional[str] = None,
        failure_count: Optional[int] = None,
        context: Optional[ErrorContext] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="CIRCUIT_BREAKER_ERROR",
            context=context,
            details=details or {}
        )
        self.service = service
        self.failure_count = failure_count
        
        if service:
            self.details["service"] = service
        if failure_count is not None:
            self.details["failure_count"] = failure_count


def create_error_context(
    service: str,
    operation: str,
    tenant_id: Optional[str] = None,
    instrument_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> ErrorContext:
    """Create error context."""
    return ErrorContext(
        service=service,
        operation=operation,
        tenant_id=tenant_id,
        instrument_id=instrument_id,
        correlation_id=correlation_id,
        metadata=metadata or {}
    )

