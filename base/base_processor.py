"""
Base classes for data processing components.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any

import structlog


logger = structlog.get_logger("base_processor")


@dataclass
class NormalizationResult:
    """Result of normalization operation."""
    records: List[Dict[str, Any]]
    record_count: int
    validation_errors: List[str]
    metadata: Dict[str, Any]


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""
    records: List[Dict[str, Any]]
    record_count: int
    enrichment_applied: int
    metadata: Dict[str, Any]


class BaseNormalizer(ABC):
    """Base class for data normalizers."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logger.bind(processor=name)

    @abstractmethod
    async def normalize_records(
        self,
        bronze_records: List[Dict[str, Any]]
    ) -> NormalizationResult:
        """Normalize Bronze records to Silver format."""
        pass

    @abstractmethod
    def get_output_topics(self) -> Dict[str, str]:
        """Get output topics for normalized data."""
        pass

    @abstractmethod
    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate a normalized record."""
        pass


class BaseEnricher(ABC):
    """Base class for data enrichers."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logger.bind(enricher=name)

    @abstractmethod
    async def enrich_records(
        self,
        silver_records: List[Dict[str, Any]]
    ) -> EnrichmentResult:
        """Enrich Silver records to Gold format."""
        pass

    @abstractmethod
    def get_enrichment_types(self) -> List[str]:
        """Get types of enrichment this enricher provides."""
        pass


class BaseAggregator(ABC):
    """Base class for data aggregators."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logger.bind(aggregator=name)

    @abstractmethod
    async def aggregate_records(
        self,
        gold_records: List[Dict[str, Any]],
        time_window: str = "1h"
    ) -> List[Dict[str, Any]]:
        """Aggregate Gold records."""
        pass

    @abstractmethod
    def get_aggregation_types(self) -> List[str]:
        """Get types of aggregation this aggregator provides."""
        pass
