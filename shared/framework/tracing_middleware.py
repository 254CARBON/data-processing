"""
Tracing middleware for FastAPI applications in the 254Carbon Data Processing Pipeline.
"""

import time
import logging
from typing import Callable, Optional, Dict, Any
from functools import wraps

try:
    from fastapi import Request, Response
    from opentelemetry import trace
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.trace import Status, StatusCode
    FASTAPI_TRACING_AVAILABLE = True
except ImportError:
    FASTAPI_TRACING_AVAILABLE = False
    logging.warning("FastAPI tracing dependencies not available")

logger = logging.getLogger(__name__)


class TracingMiddleware:
    """Middleware for adding distributed tracing to FastAPI applications."""
    
    def __init__(self, app, service_name: str, tracing_enabled: bool = True):
        self.app = app
        self.service_name = service_name
        self.tracing_enabled = tracing_enabled
        self.tracer = None
        
        if self.tracing_enabled and FASTAPI_TRACING_AVAILABLE:
            self._setup_tracing()
    
    def _setup_tracing(self):
        """Setup tracing for the FastAPI application."""
        try:
            # Get the global tracer
            self.tracer = trace.get_tracer(self.service_name)
            
            # Instrument FastAPI
            FastAPIInstrumentor.instrument_app(
                self.app,
                tracer_provider=trace.get_tracer_provider(),
                excluded_urls="health,metrics"
            )
            
            logger.info(f"Tracing middleware initialized for service: {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup tracing middleware: {e}")
            self.tracing_enabled = False
    
    async def __call__(self, request: Request, call_next: Callable) -> Response:
        """Process the request with tracing."""
        if not self.tracing_enabled or not self.tracer:
            return await call_next(request)
        
        # Start span for the request
        span_name = f"{request.method} {request.url.path}"
        span = self.tracer.start_span(span_name)
        
        # Add request attributes
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.url", str(request.url))
        span.set_attribute("http.user_agent", request.headers.get("user-agent", ""))
        span.set_attribute("http.request_id", request.headers.get("x-request-id", ""))
        
        # Add service name
        span.set_attribute("service.name", self.service_name)
        
        # Add client IP
        client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown")
        span.set_attribute("http.client_ip", client_ip)
        
        start_time = time.time()
        
        try:
            # Process the request
            response = await call_next(request)
            
            # Add response attributes
            span.set_attribute("http.status_code", response.status_code)
            span.set_attribute("http.response_size", response.headers.get("content-length", 0))
            
            # Set span status based on HTTP status code
            if response.status_code >= 400:
                span.set_status(Status(StatusCode.ERROR, f"HTTP {response.status_code}"))
            else:
                span.set_status(Status(StatusCode.OK))
            
            return response
            
        except Exception as e:
            # Set span status to error
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.set_attribute("error", True)
            span.set_attribute("error.message", str(e))
            
            # Re-raise the exception
            raise
            
        finally:
            # Calculate duration
            duration = time.time() - start_time
            span.set_attribute("http.duration", duration)
            
            # End the span
            span.end()


def trace_endpoint(
    operation_name: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None
):
    """Decorator to trace FastAPI endpoint execution."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Try to get the tracer
            try:
                tracer = trace.get_tracer("fastapi")
                span_name = operation_name or f"{func.__name__}"
                
                with tracer.start_as_current_span(span_name) as span:
                    # Add custom tags
                    if tags:
                        for key, value in tags.items():
                            span.set_attribute(key, value)
                    
                    # Add function name
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)
                    
                    try:
                        result = await func(*args, **kwargs)
                        span.set_attribute("function.success", True)
                        return result
                    except Exception as e:
                        span.set_attribute("function.success", False)
                        span.set_attribute("function.error", str(e))
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        raise
            
            except Exception as e:
                logger.warning(f"Failed to trace endpoint {func.__name__}: {e}")
                # Continue without tracing
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def trace_sync_endpoint(
    operation_name: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None
):
    """Decorator to trace synchronous FastAPI endpoint execution."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get the tracer
            try:
                tracer = trace.get_tracer("fastapi")
                span_name = operation_name or f"{func.__name__}"
                
                with tracer.start_as_current_span(span_name) as span:
                    # Add custom tags
                    if tags:
                        for key, value in tags.items():
                            span.set_attribute(key, value)
                    
                    # Add function name
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)
                    
                    try:
                        result = func(*args, **kwargs)
                        span.set_attribute("function.success", True)
                        return result
                    except Exception as e:
                        span.set_attribute("function.success", False)
                        span.set_attribute("function.error", str(e))
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        raise
            
            except Exception as e:
                logger.warning(f"Failed to trace endpoint {func.__name__}: {e}")
                # Continue without tracing
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


def trace_database_operation(
    operation_name: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None
):
    """Decorator to trace database operations."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                tracer = trace.get_tracer("database")
                span_name = operation_name or f"db.{func.__name__}"
                
                with tracer.start_as_current_span(span_name) as span:
                    # Add custom tags
                    if tags:
                        for key, value in tags.items():
                            span.set_attribute(key, value)
                    
                    # Add database operation attributes
                    span.set_attribute("db.operation", func.__name__)
                    span.set_attribute("db.system", "unknown")  # Will be set by specific implementations
                    
                    try:
                        result = await func(*args, **kwargs)
                        span.set_attribute("db.success", True)
                        return result
                    except Exception as e:
                        span.set_attribute("db.success", False)
                        span.set_attribute("db.error", str(e))
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        raise
            
            except Exception as e:
                logger.warning(f"Failed to trace database operation {func.__name__}: {e}")
                # Continue without tracing
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def trace_kafka_operation(
    operation_name: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None
):
    """Decorator to trace Kafka operations."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                tracer = trace.get_tracer("kafka")
                span_name = operation_name or f"kafka.{func.__name__}"
                
                with tracer.start_as_current_span(span_name) as span:
                    # Add custom tags
                    if tags:
                        for key, value in tags.items():
                            span.set_attribute(key, value)
                    
                    # Add Kafka operation attributes
                    span.set_attribute("messaging.system", "kafka")
                    span.set_attribute("messaging.operation", func.__name__)
                    
                    try:
                        result = await func(*args, **kwargs)
                        span.set_attribute("messaging.success", True)
                        return result
                    except Exception as e:
                        span.set_attribute("messaging.success", False)
                        span.set_attribute("messaging.error", str(e))
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        raise
            
            except Exception as e:
                logger.warning(f"Failed to trace Kafka operation {func.__name__}: {e}")
                # Continue without tracing
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator


# Example usage:
if __name__ == "__main__":
    from fastapi import FastAPI
    
    app = FastAPI()
    
    # Add tracing middleware
    tracing_middleware = TracingMiddleware(app, "example-service")
    
    # Example traced endpoint
    @app.get("/health")
    @trace_endpoint(operation_name="health_check", tags={"component": "health"})
    async def health_check():
        """Health check endpoint with tracing."""
        return {"status": "healthy"}
    
    # Example traced database operation
    @trace_database_operation(operation_name="get_user", tags={"table": "users"})
    async def get_user(user_id: int):
        """Example database operation with tracing."""
        # Simulate database query
        import asyncio
        await asyncio.sleep(0.1)
        return {"user_id": user_id, "name": "John Doe"}
    
    # Example traced Kafka operation
    @trace_kafka_operation(operation_name="publish_message", tags={"topic": "events"})
    async def publish_message(topic: str, message: dict):
        """Example Kafka operation with tracing."""
        # Simulate Kafka publish
        import asyncio
        await asyncio.sleep(0.05)
        return {"published": True, "topic": topic}
