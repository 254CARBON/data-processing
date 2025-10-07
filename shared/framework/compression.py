"""Request/response compression middleware."""

import gzip
import zlib
import logging
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class CompressionType(str, Enum):
    """Compression type enumeration."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    BROTLI = "br"
    NONE = "identity"


@dataclass
class CompressionConfig:
    """Compression configuration."""
    min_size: int = 1024  # Minimum size to compress
    max_size: int = 10 * 1024 * 1024  # Maximum size to compress
    compression_level: int = 6  # Compression level (1-9)
    enabled_types: List[CompressionType] = None
    exclude_types: List[str] = None  # Content types to exclude


class CompressionMiddleware:
    """Compression middleware for HTTP requests/responses."""
    
    def __init__(self, config: CompressionConfig = None):
        self.config = config or CompressionConfig()
        if self.config.enabled_types is None:
            self.config.enabled_types = [CompressionType.GZIP, CompressionType.DEFLATE]
        if self.config.exclude_types is None:
            self.config.exclude_types = ["image/", "video/", "audio/"]
        
        self.logger = structlog.get_logger("compression-middleware")
        self.compression_stats = {
            "requests_compressed": 0,
            "responses_compressed": 0,
            "bytes_saved": 0,
            "compression_ratio": 0.0
        }
    
    def should_compress(self, content_type: str, content_length: int) -> bool:
        """Determine if content should be compressed."""
        # Check size constraints
        if content_length < self.config.min_size or content_length > self.config.max_size:
            return False
        
        # Check excluded content types
        for excluded_type in self.config.exclude_types:
            if content_type.startswith(excluded_type):
                return False
        
        # Check if content type is compressible
        compressible_types = [
            "text/",
            "application/json",
            "application/xml",
            "application/javascript",
            "application/css"
        ]
        
        return any(content_type.startswith(ct) for ct in compressible_types)
    
    def get_best_compression_type(self, accept_encoding: str) -> CompressionType:
        """Get the best compression type based on client support."""
        if not accept_encoding:
            return CompressionType.NONE
        
        # Parse Accept-Encoding header
        encodings = [e.strip().split(';')[0] for e in accept_encoding.split(',')]
        
        # Check for Brotli support
        if CompressionType.BROTLI in self.config.enabled_types and 'br' in encodings:
            return CompressionType.BROTLI
        
        # Check for Gzip support
        if CompressionType.GZIP in self.config.enabled_types and 'gzip' in encodings:
            return CompressionType.GZIP
        
        # Check for Deflate support
        if CompressionType.DEFLATE in self.config.enabled_types and 'deflate' in encodings:
            return CompressionType.DEFLATE
        
        return CompressionType.NONE
    
    def compress_data(self, data: bytes, compression_type: CompressionType) -> bytes:
        """Compress data using specified compression type."""
        try:
            if compression_type == CompressionType.GZIP:
                return gzip.compress(data, compresslevel=self.config.compression_level)
            elif compression_type == CompressionType.DEFLATE:
                return zlib.compress(data, level=self.config.compression_level)
            elif compression_type == CompressionType.BROTLI:
                # Brotli compression would require brotli library
                # For now, fall back to gzip
                return gzip.compress(data, compresslevel=self.config.compression_level)
            else:
                return data
                
        except Exception as e:
            self.logger.error("Compression failed", 
                            compression_type=compression_type.value, 
                            error=str(e))
            return data
    
    def decompress_data(self, data: bytes, compression_type: CompressionType) -> bytes:
        """Decompress data using specified compression type."""
        try:
            if compression_type == CompressionType.GZIP:
                return gzip.decompress(data)
            elif compression_type == CompressionType.DEFLATE:
                return zlib.decompress(data)
            elif compression_type == CompressionType.BROTLI:
                # Brotli decompression would require brotli library
                # For now, fall back to gzip
                return gzip.decompress(data)
            else:
                return data
                
        except Exception as e:
            self.logger.error("Decompression failed", 
                            compression_type=compression_type.value, 
                            error=str(e))
            return data
    
    def get_compression_ratio(self, original_size: int, compressed_size: int) -> float:
        """Calculate compression ratio."""
        if original_size == 0:
            return 0.0
        return (1.0 - compressed_size / original_size) * 100
    
    def update_stats(self, original_size: int, compressed_size: int, is_response: bool = True) -> None:
        """Update compression statistics."""
        if is_response:
            self.compression_stats["responses_compressed"] += 1
        else:
            self.compression_stats["requests_compressed"] += 1
        
        bytes_saved = original_size - compressed_size
        self.compression_stats["bytes_saved"] += bytes_saved
        
        # Update compression ratio
        if self.compression_stats["responses_compressed"] > 0:
            total_original = self.compression_stats["responses_compressed"] * original_size
            total_compressed = total_original - self.compression_stats["bytes_saved"]
            self.compression_stats["compression_ratio"] = self.get_compression_ratio(
                total_original, total_compressed
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        return self.compression_stats.copy()


class RequestCompressor:
    """Compresses HTTP requests."""
    
    def __init__(self, compression_middleware: CompressionMiddleware):
        self.compression_middleware = compression_middleware
        self.logger = structlog.get_logger("request-compressor")
    
    async def compress_request(self, request) -> Optional[bytes]:
        """Compress request body."""
        try:
            # Get request body
            if hasattr(request, 'body'):
                body = request.body
            elif hasattr(request, 'content'):
                body = request.content
            else:
                return None
            
            if not body:
                return None
            
            # Check if compression is needed
            content_type = request.headers.get('content-type', '')
            content_length = len(body)
            
            if not self.compression_middleware.should_compress(content_type, content_length):
                return None
            
            # Determine compression type
            compression_type = self.compression_middleware.get_best_compression_type(
                request.headers.get('accept-encoding', '')
            )
            
            if compression_type == CompressionType.NONE:
                return None
            
            # Compress body
            compressed_body = self.compression_middleware.compress_data(body, compression_type)
            
            # Update statistics
            self.compression_middleware.update_stats(
                content_length, len(compressed_body), is_response=False
            )
            
            # Update request headers
            request.headers['content-encoding'] = compression_type.value
            request.headers['content-length'] = str(len(compressed_body))
            
            self.logger.debug("Request compressed", 
                            compression_type=compression_type.value,
                            original_size=content_length,
                            compressed_size=len(compressed_body))
            
            return compressed_body
            
        except Exception as e:
            self.logger.error("Request compression failed", error=str(e))
            return None


class ResponseCompressor:
    """Compresses HTTP responses."""
    
    def __init__(self, compression_middleware: CompressionMiddleware):
        self.compression_middleware = compression_middleware
        self.logger = structlog.get_logger("response-compressor")
    
    async def compress_response(self, response) -> Optional[bytes]:
        """Compress response body."""
        try:
            # Get response body
            if hasattr(response, 'body'):
                body = response.body
            elif hasattr(response, 'content'):
                body = response.content
            else:
                return None
            
            if not body:
                return None
            
            # Check if compression is needed
            content_type = response.headers.get('content-type', '')
            content_length = len(body)
            
            if not self.compression_middleware.should_compress(content_type, content_length):
                return None
            
            # Determine compression type
            compression_type = self.compression_middleware.get_best_compression_type(
                response.headers.get('accept-encoding', '')
            )
            
            if compression_type == CompressionType.NONE:
                return None
            
            # Compress body
            compressed_body = self.compression_middleware.compress_data(body, compression_type)
            
            # Update statistics
            self.compression_middleware.update_stats(
                content_length, len(compressed_body), is_response=True
            )
            
            # Update response headers
            response.headers['content-encoding'] = compression_type.value
            response.headers['content-length'] = str(len(compressed_body))
            
            self.logger.debug("Response compressed", 
                            compression_type=compression_type.value,
                            original_size=content_length,
                            compressed_size=len(compressed_body))
            
            return compressed_body
            
        except Exception as e:
            self.logger.error("Response compression failed", error=str(e))
            return None


class CompressionMetrics:
    """Metrics for compression monitoring."""
    
    def __init__(self, compression_middleware: CompressionMiddleware):
        self.compression_middleware = compression_middleware
        self.logger = structlog.get_logger("compression-metrics")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get compression metrics."""
        stats = self.compression_middleware.get_stats()
        
        return {
            "compression_requests_total": stats["requests_compressed"],
            "compression_responses_total": stats["responses_compressed"],
            "compression_bytes_saved_total": stats["bytes_saved"],
            "compression_ratio": stats["compression_ratio"],
            "compression_enabled": len(self.compression_middleware.config.enabled_types) > 0,
            "compression_min_size": self.compression_middleware.config.min_size,
            "compression_max_size": self.compression_middleware.config.max_size
        }
    
    def get_performance_impact(self) -> Dict[str, Any]:
        """Get performance impact metrics."""
        stats = self.compression_middleware.get_stats()
        
        total_compressions = stats["requests_compressed"] + stats["responses_compressed"]
        avg_bytes_saved = stats["bytes_saved"] / total_compressions if total_compressions > 0 else 0
        
        return {
            "total_compressions": total_compressions,
            "average_bytes_saved": avg_bytes_saved,
            "compression_ratio": stats["compression_ratio"],
            "performance_impact": "positive" if stats["compression_ratio"] > 20 else "neutral"
        }
