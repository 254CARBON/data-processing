"""
Configuration for normalization service.
"""

import os
from shared.framework.config import ServiceConfig


class NormalizationConfig(ServiceConfig):
    """Normalization service configuration."""
    
    def __init__(self):
        super().__init__(service_name="normalization")
        
        # Service-specific settings
        self.max_batch_size = int(os.getenv("DATA_PROC_NORMALIZATION_MAX_BATCH_SIZE", "1000"))
        self.processing_timeout = int(os.getenv("DATA_PROC_NORMALIZATION_TIMEOUT", "30"))
        self.validation_enabled = os.getenv("DATA_PROC_NORMALIZATION_VALIDATION_ENABLED", "true").lower() == "true"
        
        # Input topics
        self.input_topics = [
            "ingestion.miso.raw.v1",
            "ingestion.caiso.raw.v1",
            "ingestion.ercot.raw.v1",
        ]
        
        # Output topic
        self.output_topic = "normalized.market.ticks.v1"
        
        # Dead letter queue
        self.dlq_topic = "processing.deadletter.normalization.v1"
        
        # Quality thresholds
        self.price_spike_threshold = float(os.getenv("DATA_PROC_NORMALIZATION_PRICE_SPIKE_THRESHOLD", "0.1"))
        self.volume_spike_threshold = float(os.getenv("DATA_PROC_NORMALIZATION_VOLUME_SPIKE_THRESHOLD", "0.5"))
        self.max_price = float(os.getenv("DATA_PROC_NORMALIZATION_MAX_PRICE", "10000.0"))
        self.min_price = float(os.getenv("DATA_PROC_NORMALIZATION_MIN_PRICE", "0.0"))

        self.audit.enabled = os.getenv(
            "DATA_PROC_NORMALIZATION_AUDIT_ENABLED",
            "true" if self.audit.enabled else "false",
        ).lower() == "true"
        
        # Performance optimization settings
        self.clickhouse_pool_min = int(os.getenv("DATA_PROC_CLICKHOUSE_POOL_MIN", "5"))
        self.clickhouse_pool_max = int(os.getenv("DATA_PROC_CLICKHOUSE_POOL_MAX", "20"))
        self.batch_size = int(os.getenv("DATA_PROC_NORMALIZATION_BATCH_SIZE", "1000"))
        self.batch_flush_interval = int(os.getenv("DATA_PROC_NORMALIZATION_FLUSH_INTERVAL", "5"))  # seconds
        
        # Kafka consumer optimization
        self.kafka_fetch_min_bytes = int(os.getenv("DATA_PROC_KAFKA_FETCH_MIN_BYTES", "102400"))  # 100KB
        self.kafka_fetch_max_wait_ms = int(os.getenv("DATA_PROC_KAFKA_FETCH_MAX_WAIT_MS", "500"))
        self.kafka_max_poll_records = int(os.getenv("DATA_PROC_KAFKA_MAX_POLL_RECORDS", "500"))
        
        # Parser caching
        self.parser_cache_size = int(os.getenv("DATA_PROC_PARSER_CACHE_SIZE", "100"))
        self.parser_cache_ttl = int(os.getenv("DATA_PROC_PARSER_CACHE_TTL", "3600"))  # 1 hour