"""
Structured logging setup for microservices.

Provides consistent logging configuration across all services
with structured output and correlation IDs.
"""

import logging
import sys
from typing import Dict, Any, Optional
import structlog
from structlog.stdlib import LoggerFactory


def setup_logging(
    service_name: str,
    log_level: str = "info",
    format_type: str = "json"
) -> None:
    """
    Setup structured logging for the service.
    
    Args:
        service_name: Name of the service
        log_level: Logging level (debug, info, warning, error)
        format_type: Output format (json, console)
    """
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )
    
    # Configure structlog
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if format_type == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Set default context
    structlog.get_logger().bind(service=service_name)


def get_logger(name: Optional[str] = None) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


def add_correlation_id(logger: structlog.BoundLogger, correlation_id: str) -> structlog.BoundLogger:
    """Add correlation ID to logger context."""
    return logger.bind(correlation_id=correlation_id)


def add_tenant_id(logger: structlog.BoundLogger, tenant_id: str) -> structlog.BoundLogger:
    """Add tenant ID to logger context."""
    return logger.bind(tenant_id=tenant_id)


def add_instrument_id(logger: structlog.BoundLogger, instrument_id: str) -> structlog.BoundLogger:
    """Add instrument ID to logger context."""
    return logger.bind(instrument_id=instrument_id)

