"""
OpenTelemetry tracing setup for microservices.

Provides distributed tracing configuration with
automatic instrumentation and span management.
"""

import asyncio
from typing import Optional, Dict, Any, Callable
from contextlib import asynccontextmanager
import structlog

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

logger = structlog.get_logger()


def setup_tracing(
    service_name: str,
    endpoint: Optional[str] = None,
    enabled: bool = True
) -> None:
    """
    Setup OpenTelemetry tracing for the service.
    
    Args:
        service_name: Name of the service
        endpoint: OTLP endpoint URL
        enabled: Whether tracing is enabled
    """
    if not OPENTELEMETRY_AVAILABLE:
        logger.warning("OpenTelemetry not available, tracing disabled")
        return
    
    if not enabled:
        logger.info("Tracing disabled by configuration")
        return
    
    try:
        # Create tracer provider
        tracer_provider = TracerProvider()
        trace.set_tracer_provider(tracer_provider)
        
        # Create span exporter
        if endpoint:
            span_exporter = OTLPSpanExporter(endpoint=endpoint)
        else:
            # Use default OTLP endpoint
            span_exporter = OTLPSpanExporter()
        
        # Create span processor
        span_processor = BatchSpanProcessor(span_exporter)
        tracer_provider.add_span_processor(span_processor)
        
        # Get tracer
        tracer = trace.get_tracer(service_name)
        
        # Instrument libraries
        AioHttpClientInstrumentor().instrument()
        AsyncioInstrumentor().instrument()
        LoggingInstrumentor().instrument()
        
        logger.info("Tracing setup complete", service=service_name, endpoint=endpoint)
        
    except Exception as e:
        logger.error("Failed to setup tracing", error=str(e), exc_info=True)


def get_tracer(name: Optional[str] = None):
    """Get OpenTelemetry tracer."""
    if not OPENTELEMETRY_AVAILABLE:
        return None
    
    return trace.get_tracer(name)


def get_current_span():
    """Get current OpenTelemetry span."""
    if not OPENTELEMETRY_AVAILABLE:
        return None
    
    return trace.get_current_span()


def set_span_attribute(key: str, value: Any) -> None:
    """Set attribute on current span."""
    if not OPENTELEMETRY_AVAILABLE:
        return
    
    span = get_current_span()
    if span:
        span.set_attribute(key, value)


def add_span_event(name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
    """Add event to current span."""
    if not OPENTELEMETRY_AVAILABLE:
        return
    
    span = get_current_span()
    if span:
        span.add_event(name, attributes or {})


@asynccontextmanager
async def trace_async_function(
    name: str,
    attributes: Optional[Dict[str, Any]] = None
):
    """Trace an async function."""
    if not OPENTELEMETRY_AVAILABLE:
        yield
        return
    
    tracer = get_tracer()
    if not tracer:
        yield
        return
    
    with tracer.start_as_current_span(name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        try:
            yield span
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise


def trace_function(
    name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None
):
    """Decorator to trace a function."""
    def decorator(func: Callable) -> Callable:
        if not OPENTELEMETRY_AVAILABLE:
            return func
        
        tracer = get_tracer()
        if not tracer:
            return func
        
        span_name = name or f"{func.__module__}.{func.__name__}"
        
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                with tracer.start_as_current_span(span_name) as span:
                    if attributes:
                        for key, value in attributes.items():
                            span.set_attribute(key, value)
                    
                    try:
                        result = await func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                        raise
            
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                with tracer.start_as_current_span(span_name) as span:
                    if attributes:
                        for key, value in attributes.items():
                            span.set_attribute(key, value)
                    
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                        raise
            
            return sync_wrapper
    
    return decorator


def trace_kafka_consumer(
    topic: str,
    partition: int,
    offset: int
) -> None:
    """Add Kafka consumer tracing attributes."""
    set_span_attribute("kafka.topic", topic)
    set_span_attribute("kafka.partition", partition)
    set_span_attribute("kafka.offset", offset)


def trace_database_operation(
    operation: str,
    table: str,
    duration_ms: float
) -> None:
    """Add database operation tracing attributes."""
    set_span_attribute("db.operation", operation)
    set_span_attribute("db.table", table)
    set_span_attribute("db.duration_ms", duration_ms)

