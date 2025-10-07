"""Configuration for aggregation service."""

import os
from shared.framework.config import ServiceConfig


class AggregationConfig(ServiceConfig):
    """Configuration for aggregation service."""

    def __init__(self) -> None:
        super().__init__(service_name="aggregation")

        self.port = int(os.getenv("DATA_PROC_AGGREGATION_PORT", "8083"))
        self.kafka_bootstrap_servers = os.getenv("DATA_PROC_KAFKA_BOOTSTRAP", "localhost:9092")
        self.kafka_group_id = os.getenv("DATA_PROC_AGGREGATION_GROUP_ID", f"aggregation-{self.environment}")

        self.enriched_topic = os.getenv("DATA_PROC_AGGREGATION_ENRICHED_TOPIC", "enriched.market.ticks.v1")
        self.bars_topic = os.getenv("DATA_PROC_AGGREGATION_BARS_TOPIC", "aggregation.bars.v1")
        self.curves_topic = os.getenv("DATA_PROC_AGGREGATION_CURVES_TOPIC", "aggregation.curves.v1")

        self.clickhouse_host = os.getenv("DATA_PROC_CLICKHOUSE_HOST", "localhost")
        self.clickhouse_port = int(os.getenv("DATA_PROC_CLICKHOUSE_PORT", "9000"))
        self.clickhouse_database = os.getenv("DATA_PROC_CLICKHOUSE_DATABASE", "data_processing")
        self.clickhouse_user = os.getenv("DATA_PROC_CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.getenv("DATA_PROC_CLICKHOUSE_PASSWORD", "")

        self.bar_intervals = os.getenv(
            "DATA_PROC_AGGREGATION_BAR_INTERVALS",
            "5m,1h,1d",
        ).split(",")
        self.curve_horizons = os.getenv(
            "DATA_PROC_AGGREGATION_CURVE_HORIZONS",
            "1m,3m,6m,1y",
        ).split(",")

        self.batch_size = int(os.getenv("DATA_PROC_AGGREGATION_BATCH_SIZE", "1000"))
        self.batch_timeout_ms = int(os.getenv("DATA_PROC_AGGREGATION_BATCH_TIMEOUT_MS", "5000"))
        self.watermark_delay_ms = int(os.getenv("DATA_PROC_AGGREGATION_WATERMARK_DELAY_MS", "30000"))
        self.max_late_data_ms = int(os.getenv("DATA_PROC_AGGREGATION_MAX_LATE_MS", "300000"))

        self.audit.enabled = os.getenv(
            "DATA_PROC_AGGREGATION_AUDIT_ENABLED",
            "true" if self.audit.enabled else "false",
        ).lower() == "true"
        
        # Performance optimization settings
        self.clickhouse_pool_min = int(os.getenv("DATA_PROC_CLICKHOUSE_POOL_MIN", "5"))
        self.clickhouse_pool_max = int(os.getenv("DATA_PROC_CLICKHOUSE_POOL_MAX", "20"))
        
        # In-memory aggregation buffer
        self.aggregation_buffer_size = int(os.getenv("DATA_PROC_AGGREGATION_BUFFER_SIZE", "10000"))
        self.aggregation_flush_interval = int(os.getenv("DATA_PROC_AGGREGATION_FLUSH_INTERVAL", "10"))  # seconds
        
        # Parallel processing
        self.parallel_windows = int(os.getenv("DATA_PROC_PARALLEL_WINDOWS", "4"))
        self.worker_threads = int(os.getenv("DATA_PROC_WORKER_THREADS", "4"))
        
        # Curve calculation optimization
        self.use_vectorization = os.getenv("DATA_PROC_USE_VECTORIZATION", "true").lower() == "true"
        self.numpy_optimization = os.getenv("DATA_PROC_NUMPY_OPTIMIZATION", "true").lower() == "true"