"""Configuration for projection service."""

import os
from shared.framework.config import ServiceConfig


class ProjectionConfig(ServiceConfig):
    """Configuration for projection service."""

    def __init__(self) -> None:
        super().__init__(service_name="projection")

        self.port = int(os.getenv("DATA_PROC_PROJECTION_PORT", "8084"))
        self.kafka_bootstrap_servers = os.getenv("DATA_PROC_KAFKA_BOOTSTRAP", "localhost:9092")
        self.kafka_group_id = os.getenv("DATA_PROC_PROJECTION_GROUP_ID", f"projection-{self.environment}")

        self.aggregated_topic = os.getenv("DATA_PROC_PROJECTION_AGGREGATED_TOPIC", "aggregation.bars.v1")
        self.invalidation_topic = os.getenv("DATA_PROC_PROJECTION_INVALIDATION_TOPIC", "projection.invalidation.v1")
        self.projection_topic = os.getenv("DATA_PROC_PROJECTION_OUTPUT_TOPIC", "projection.updates.v1")

        self.clickhouse_host = os.getenv("DATA_PROC_CLICKHOUSE_HOST", "localhost")
        self.clickhouse_port = int(os.getenv("DATA_PROC_CLICKHOUSE_PORT", "9000"))
        self.clickhouse_database = os.getenv("DATA_PROC_CLICKHOUSE_DATABASE", "data_processing")
        self.clickhouse_user = os.getenv("DATA_PROC_CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.getenv("DATA_PROC_CLICKHOUSE_PASSWORD", "")

        self.redis_host = os.getenv("DATA_PROC_REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("DATA_PROC_REDIS_PORT", "6379"))
        self.redis_database = int(os.getenv("DATA_PROC_REDIS_DB", "0"))
        self.redis_password = os.getenv("DATA_PROC_REDIS_PASSWORD", "")

        self.internal_api_port = int(os.getenv("DATA_PROC_INTERNAL_API_PORT", "8085"))

        self.projection_types = os.getenv(
            "DATA_PROC_PROJECTION_TYPES",
            "latest_price,curve_snapshot,custom",
        ).split(",")
        self.cache_ttl_seconds = int(os.getenv("DATA_PROC_PROJECTION_CACHE_TTL", "3600"))
        self.market_cache_ttl_min = int(os.getenv("DATA_PROC_MARKET_CACHE_TTL_MIN", "15"))
        self.market_cache_ttl_max = int(os.getenv("DATA_PROC_MARKET_CACHE_TTL_MAX", "60"))
        self.market_cache_refresh_margin = int(os.getenv("DATA_PROC_MARKET_CACHE_REFRESH_MARGIN", "5"))
        self.market_cache_lock_timeout = int(os.getenv("DATA_PROC_MARKET_CACHE_LOCK_TIMEOUT", "5"))
        self.refresh_interval_seconds = int(os.getenv("DATA_PROC_PROJECTION_REFRESH_INTERVAL", "300"))

        self.invalidation_rules = os.getenv(
            "DATA_PROC_PROJECTION_INVALIDATION_RULES",
            "price_change,time_based,manual",
        ).split(",")
        self.invalidation_threshold = float(os.getenv("DATA_PROC_PROJECTION_INVALIDATION_THRESHOLD", "0.01"))

        self.audit.enabled = os.getenv(
            "DATA_PROC_PROJECTION_AUDIT_ENABLED",
            "true" if self.audit.enabled else "false",
        ).lower() == "true"
        
        # Performance optimization settings
        self.redis_pool_min = int(os.getenv("DATA_PROC_REDIS_POOL_MIN", "10"))
        self.redis_pool_max = int(os.getenv("DATA_PROC_REDIS_POOL_MAX", "50"))
        self.clickhouse_pool_min = int(os.getenv("DATA_PROC_CLICKHOUSE_POOL_MIN", "5"))
        self.clickhouse_pool_max = int(os.getenv("DATA_PROC_CLICKHOUSE_POOL_MAX", "20"))
        
        # Redis pipelining
        self.redis_pipeline_size = int(os.getenv("DATA_PROC_REDIS_PIPELINE_SIZE", "100"))
        self.use_redis_pipeline = os.getenv("DATA_PROC_USE_REDIS_PIPELINE", "true").lower() == "true"
        
        # Partial cache updates
        self.partial_updates_enabled = os.getenv("DATA_PROC_PARTIAL_UPDATES_ENABLED", "true").lower() == "true"
        self.update_batch_size = int(os.getenv("DATA_PROC_UPDATE_BATCH_SIZE", "50"))
        
        # Cache warming
        self.cache_warming_enabled = os.getenv("DATA_PROC_CACHE_WARMING_ENABLED", "true").lower() == "true"
        self.warming_batch_size = int(os.getenv("DATA_PROC_WARMING_BATCH_SIZE", "1000"))
        
        # Pub/Sub for invalidation
        self.pubsub_enabled = os.getenv("DATA_PROC_PUBSUB_ENABLED", "true").lower() == "true"
        self.invalidation_channel = os.getenv("DATA_PROC_INVALIDATION_CHANNEL", "cache:invalidation")
