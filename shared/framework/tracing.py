"""
Distributed tracing implementation using Jaeger for the 254Carbon Data Processing Pipeline.
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
from functools import wraps

try:
    from jaeger_client import Config as JaegerConfig
    from opentelemetry import trace
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.kafka import KafkaInstrumentor
    TRACING_AVAILABLE = True
except ImportError:
    TRACING_AVAILABLE = False
    logging.warning("Tracing dependencies not available. Install with: pip install jaeger-client opentelemetry-instrumentation")

logger = logging.getLogger(__name__)


class TracingConfig:
    """Configuration for distributed tracing."""
    
    def __init__(self):
        self.enabled = os.getenv('DATA_PROC_TRACING_ENABLED', 'true').lower() == 'true'
        self.jaeger_agent_host = os.getenv('DATA_PROC_JAEGER_AGENT_HOST', 'jaeger-agent')
        self.jaeger_agent_port = int(os.getenv('DATA_PROC_JAEGER_AGENT_PORT', '6831'))
        self.service_name = os.getenv('DATA_PROC_SERVICE_NAME', 'data-processing-service')
        self.sample_rate = float(os.getenv('DATA_PROC_TRACING_SAMPLE_RATE', '0.1'))
        self.max_tag_value_length = int(os.getenv('DATA_PROC_TRACING_MAX_TAG_VALUE_LENGTH', '1024'))


class TracingManager:
    """Manages distributed tracing for the data processing pipeline."""
    
    def __init__(self, config: Optional[TracingConfig] = None):
        self.config = config or TracingConfig()
        self.tracer = None
        self._initialized = False
        
    def initialize(self) -> bool:
        """Initialize the tracing system."""
        if not TRACING_AVAILABLE:
            logger.warning("Tracing dependencies not available. Skipping initialization.")
            return False
            
        if not self.config.enabled:
            logger.info("Tracing disabled by configuration.")
            return False
            
        try:
            # Configure OpenTelemetry
            trace.set_tracer_provider(TracerProvider())
            tracer_provider = trace.get_tracer_provider()
            
            # Configure Jaeger exporter
            jaeger_exporter = JaegerExporter(
                agent_host_name=self.config.jaeger_agent_host,
                agent_port=self.config.jaeger_agent_port,
            )
            
            # Add span processor
            span_processor = BatchSpanProcessor(jaeger_exporter)
            tracer_provider.add_span_processor(span_processor)
            
            # Get tracer
            self.tracer = trace.get_tracer(self.config.service_name)
            
            # Auto-instrument libraries
            self._setup_auto_instrumentation()
            
            self._initialized = True
            logger.info(f"Tracing initialized for service: {self.config.service_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize tracing: {e}")
            return False
    
    def _setup_auto_instrumentation(self):
        """Setup automatic instrumentation for common libraries."""
        try:
            # Instrument HTTP clients
            RequestsInstrumentor().instrument()
            HTTPXClientInstrumentor().instrument()
            
            # Instrument databases
            SQLAlchemyInstrumentor().instrument()
            RedisInstrumentor().instrument()
            
            # Instrument Kafka
            KafkaInstrumentor().instrument()
            
            logger.info("Auto-instrumentation setup completed")
            
        except Exception as e:
            logger.warning(f"Failed to setup auto-instrumentation: {e}")
    
    def get_tracer(self):
        """Get the tracer instance."""
        if not self._initialized:
            self.initialize()
        return self.tracer
    
    def start_span(self, name: str, parent_span=None, tags: Optional[Dict[str, Any]] = None):
        """Start a new span."""
        if not self._initialized:
            return None
            
        try:
            span = self.tracer.start_span(name, parent=parent_span)
            
            if tags:
                for key, value in tags.items():
                    # Truncate long tag values
                    if isinstance(value, str) and len(value) > self.config.max_tag_value_length:
                        value = value[:self.config.max_tag_value_length] + "..."
                    span.set_attribute(key, value)
            
            return span
            
        except Exception as e:
            logger.error(f"Failed to start span '{name}': {e}")
            return None
    
    @contextmanager
    def span(self, name: str, parent_span=None, tags: Optional[Dict[str, Any]] = None):
        """Context manager for creating spans."""
        span = self.start_span(name, parent_span, tags)
        if span:
            try:
                yield span
            finally:
                span.end()
        else:
            yield None
    
    def trace_function(self, name: Optional[str] = None, tags: Optional[Dict[str, Any]] = None):
        """Decorator to trace function execution."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                span_name = name or f"{func.__module__}.{func.__name__}"
                
                with self.span(span_name, tags=tags) as span:
                    if span:
                        # Add function arguments as tags (be careful with sensitive data)
                        if args:
                            span.set_attribute("function.args_count", len(args))
                        if kwargs:
                            span.set_attribute("function.kwargs_count", len(kwargs))
                    
                    try:
                        result = func(*args, **kwargs)
                        if span:
                            span.set_attribute("function.success", True)
                        return result
                    except Exception as e:
                        if span:
                            span.set_attribute("function.success", False)
                            span.set_attribute("function.error", str(e))
                        raise
            
            return wrapper
        return decorator
    
    def trace_async_function(self, name: Optional[str] = None, tags: Optional[Dict[str, Any]] = None):
        """Decorator to trace async function execution."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                span_name = name or f"{func.__module__}.{func.__name__}"
                
                with self.span(span_name, tags=tags) as span:
                    if span:
                        # Add function arguments as tags (be careful with sensitive data)
                        if args:
                            span.set_attribute("function.args_count", len(args))
                        if kwargs:
                            span.set_attribute("function.kwargs_count", len(kwargs))
                    
                    try:
                        result = await func(*args, **kwargs)
                        if span:
                            span.set_attribute("function.success", True)
                        return result
                    except Exception as e:
                        if span:
                            span.set_attribute("function.success", False)
                            span.set_attribute("function.error", str(e))
                        raise
            
            return wrapper
        return decorator


# Global tracing manager instance
tracing_manager = TracingManager()


def get_tracer():
    """Get the global tracer instance."""
    return tracing_manager.get_tracer()


def start_span(name: str, parent_span=None, tags: Optional[Dict[str, Any]] = None):
    """Start a new span using the global tracer."""
    return tracing_manager.start_span(name, parent_span, tags)


def span(name: str, parent_span=None, tags: Optional[Dict[str, Any]] = None):
    """Context manager for creating spans using the global tracer."""
    return tracing_manager.span(name, parent_span, tags)


def trace_function(name: Optional[str] = None, tags: Optional[Dict[str, Any]] = None):
    """Decorator to trace function execution using the global tracer."""
    return tracing_manager.trace_function(name, tags)


def trace_async_function(name: Optional[str] = None, tags: Optional[Dict[str, Any]] = None):
    """Decorator to trace async function execution using the global tracer."""
    return tracing_manager.trace_async_function(name, tags)


def initialize_tracing() -> bool:
    """Initialize the global tracing system."""
    return tracing_manager.initialize()


# Example usage:
if __name__ == "__main__":
    # Initialize tracing
    initialize_tracing()
    
    # Example of tracing a function
    @trace_function(tags={"component": "example"})
    def process_data(data: str) -> str:
        """Example function with tracing."""
        with span("data_validation", tags={"data_length": len(data)}):
            # Simulate some work
            import time
            time.sleep(0.1)
        
        with span("data_transformation", tags={"input_size": len(data)}):
            # Simulate transformation
            result = data.upper()
            time.sleep(0.05)
        
        return result
    
    # Example of tracing async function
    @trace_async_function(tags={"component": "async_example"})
    async def async_process_data(data: str) -> str:
        """Example async function with tracing."""
        import asyncio
        
        with span("async_validation", tags={"data_length": len(data)}):
            await asyncio.sleep(0.1)
        
        with span("async_transformation", tags={"input_size": len(data)}):
            result = data.upper()
            await asyncio.sleep(0.05)
        
        return result
    
    # Test the functions
    result = process_data("hello world")
    print(f"Sync result: {result}")
    
    import asyncio
    async_result = asyncio.run(async_process_data("hello async"))
    print(f"Async result: {async_result}")
