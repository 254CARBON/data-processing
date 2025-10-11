"""
Custom exceptions for data processing components.
"""


class DataProcessingError(Exception):
    """Base exception for data processing errors."""
    pass


class NormalizationError(DataProcessingError):
    """Exception raised during data normalization."""
    pass


class EnrichmentError(DataProcessingError):
    """Exception raised during data enrichment."""
    pass


class AggregationError(DataProcessingError):
    """Exception raised during data aggregation."""
    pass


class ValidationError(DataProcessingError):
    """Exception raised during data validation."""
    pass


class SchemaError(DataProcessingError):
    """Exception raised for schema-related errors."""
    pass
