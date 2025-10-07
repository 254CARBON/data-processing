-- ClickHouse migration: Create served layer views and tables
-- Created: 2025-01-27
-- Description: Creates served layer tables for API consumption

-- Latest prices table (for serving layer)
CREATE TABLE IF NOT EXISTS served_latest (
    instrument_id String,
    price Float64,
    volume Float64,
    timestamp DateTime64(3),
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, instrument_id)
TTL updated_at + INTERVAL 1 DAY
SETTINGS index_granularity = 8192;

-- Curve snapshots table (for serving layer)
CREATE TABLE IF NOT EXISTS served_curve_snapshots (
    curve_id String,
    as_of_date Date,
    snapshot_data String, -- JSON string of curve points
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, curve_id, as_of_date)
TTL updated_at + INTERVAL 1 DAY
SETTINGS index_granularity = 8192;

-- Market summary table (for serving layer)
CREATE TABLE IF NOT EXISTS served_market_summary (
    market String,
    commodity String,
    region String,
    instrument_count UInt64,
    avg_price Float64,
    total_volume Float64,
    last_update DateTime64(3),
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, market, commodity, region)
TTL updated_at + INTERVAL 1 HOUR
SETTINGS index_granularity = 8192;

-- Served layer indexes
CREATE INDEX IF NOT EXISTS idx_served_latest_instrument 
ON served_latest (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_curve_snapshots_curve 
ON served_curve_snapshots (curve_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_market_summary_market 
ON served_market_summary (market) TYPE set(50) GRANULARITY 1;

-- Latest prices materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_prices_updater
TO served_latest
AS SELECT
    instrument_id,
    price,
    volume,
    timestamp,
    tenant_id,
    metadata,
    now64() as updated_at
FROM (
    SELECT
        instrument_id,
        price,
        volume,
        timestamp,
        tenant_id,
        metadata,
        row_number() OVER (PARTITION BY tenant_id, instrument_id ORDER BY timestamp DESC) as rn
    FROM enriched_ticks
)
WHERE rn = 1;

-- Market summary materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS market_summary_updater
TO served_market_summary
AS SELECT
    metadata['market'] as market,
    metadata['commodity'] as commodity,
    metadata['region'] as region,
    countDistinct(instrument_id) as instrument_count,
    avg(price) as avg_price,
    sum(volume) as total_volume,
    max(timestamp) as last_update,
    tenant_id,
    map('source', 'aggregation') as metadata,
    now64() as updated_at
FROM enriched_ticks
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY tenant_id, market, commodity, region;

-- Processing metrics table (for monitoring)
CREATE TABLE IF NOT EXISTS metrics_processing (
    service_name String,
    metric_name String,
    metric_value Float64,
    labels Map(String, String),
    timestamp DateTime64(3),
    tenant_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(timestamp)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, service_name, metric_name, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Processing metrics index
CREATE INDEX IF NOT EXISTS idx_metrics_processing_service 
ON metrics_processing (service_name) TYPE set(20) GRANULARITY 1;

-- Processing metrics materialized view for real-time monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS processing_metrics_realtime
TO processing_metrics_realtime_summary
AS SELECT
    service_name,
    metric_name,
    avg(metric_value) as avg_value,
    max(metric_value) as max_value,
    min(metric_value) as min_value,
    count() as sample_count,
    toStartOfMinute(timestamp) as minute_start,
    tenant_id
FROM metrics_processing
GROUP BY tenant_id, service_name, metric_name, minute_start;

-- Processing metrics realtime summary table
CREATE TABLE IF NOT EXISTS processing_metrics_realtime_summary (
    service_name String,
    metric_name String,
    avg_value Float64,
    max_value Float64,
    min_value Float64,
    sample_count UInt64,
    minute_start DateTime64(3),
    tenant_id String
) ENGINE = ReplacingMergeTree(minute_start)
PARTITION BY toYYYYMMDD(minute_start)
ORDER BY (tenant_id, service_name, metric_name, minute_start)
TTL minute_start + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

