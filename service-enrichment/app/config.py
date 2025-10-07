"""
Configuration for enrichment service.
"""

import os
from shared.framework.config import ServiceConfig


class EnrichmentConfig(ServiceConfig):
    """Enrichment service configuration."""
    
    def __init__(self):
        super().__init__(service_name="enrichment")
        
        # Service-specific settings
        self.max_batch_size = int(os.getenv("DATA_PROC_ENRICHMENT_MAX_BATCH_SIZE", "1000"))
        self.processing_timeout = int(os.getenv("DATA_PROC_ENRICHMENT_TIMEOUT", "30"))
        self.cache_enabled = os.getenv("DATA_PROC_ENRICHMENT_CACHE_ENABLED", "true").lower() == "true"
        self.cache_ttl = int(os.getenv("DATA_PROC_ENRICHMENT_CACHE_TTL", "300"))
        
        # Input topic
        self.input_topic = "normalized.market.ticks.v1"
        
        # Output topic
        self.output_topic = "enriched.market.ticks.v1"
        
        # Dead letter queue
        self.dlq_topic = "processing.deadletter.enrichment.v1"
        
        # Taxonomy settings
        self.taxonomy_refresh_interval = int(os.getenv("DATA_PROC_ENRICHMENT_TAXONOMY_REFRESH_INTERVAL", "3600"))
        self.metadata_refresh_interval = int(os.getenv("DATA_PROC_ENRICHMENT_METADATA_REFRESH_INTERVAL", "1800"))
        
        # Classification thresholds
        self.confidence_threshold = float(os.getenv("DATA_PROC_ENRICHMENT_CONFIDENCE_THRESHOLD", "0.8"))
        self.max_classification_time = float(os.getenv("DATA_PROC_ENRICHMENT_MAX_CLASSIFICATION_TIME", "5.0"))

        self.audit.enabled = os.getenv(
            "DATA_PROC_ENRICHMENT_AUDIT_ENABLED",
            "true" if self.audit.enabled else "false",
        ).lower() == "true"
        
        # Performance optimization settings
        self.redis_pool_min = int(os.getenv("DATA_PROC_REDIS_POOL_MIN", "10"))
        self.redis_pool_max = int(os.getenv("DATA_PROC_REDIS_POOL_MAX", "50"))
        self.postgres_pool_min = int(os.getenv("DATA_PROC_POSTGRES_POOL_MIN", "5"))
        self.postgres_pool_max = int(os.getenv("DATA_PROC_POSTGRES_POOL_MAX", "20"))
        
        # Caching optimization
        self.taxonomy_cache_ttl = int(os.getenv("DATA_PROC_TAXONOMY_CACHE_TTL", "3600"))  # 1 hour
        self.instrument_cache_ttl = int(os.getenv("DATA_PROC_INSTRUMENT_CACHE_TTL", "1800"))  # 30 minutes
        self.metadata_cache_ttl = int(os.getenv("DATA_PROC_METADATA_CACHE_TTL", "600"))  # 10 minutes
        
        # Batch processing
        self.metadata_batch_size = int(os.getenv("DATA_PROC_METADATA_BATCH_SIZE", "100"))
        self.batch_lookup_enabled = os.getenv("DATA_PROC_BATCH_LOOKUP_ENABLED", "true").lower() == "true"