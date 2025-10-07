-- ClickHouse migration: Initialize Silver layer tables
-- Created: 2025-01-27
-- Description: Creates normalized tick tables for Silver layer

-- Silver ticks table (normalized market data)
CREATE TABLE IF NOT EXISTS silver_ticks (
    instrument_id String,
    timestamp DateTime64(3),
    price Float64,
    volume Float64,
    quality_flags Array(String),
    tenant_id String DEFAULT 'default',
    source_id Nullable(String),
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, instrument_id, timestamp)
TTL timestamp + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Silver ticks indexes
CREATE INDEX IF NOT EXISTS idx_silver_ticks_instrument 
ON silver_ticks (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_silver_ticks_timestamp 
ON silver_ticks (timestamp) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_silver_ticks_tenant 
ON silver_ticks (tenant_id) TYPE set(100) GRANULARITY 1;

-- Silver ticks materialized view for real-time aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ticks_realtime
TO silver_ticks_realtime_summary
AS SELECT
    tenant_id,
    instrument_id,
    toStartOfMinute(timestamp) as minute_start,
    count() as tick_count,
    avg(price) as avg_price,
    max(price) as max_price,
    min(price) as min_price,
    sum(volume) as total_volume,
    now64() as updated_at
FROM silver_ticks
GROUP BY tenant_id, instrument_id, minute_start;

-- Silver ticks realtime summary table
CREATE TABLE IF NOT EXISTS silver_ticks_realtime_summary (
    tenant_id String,
    instrument_id String,
    minute_start DateTime64(3),
    tick_count UInt64,
    avg_price Float64,
    max_price Float64,
    min_price Float64,
    total_volume Float64,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(minute_start)
ORDER BY (tenant_id, instrument_id, minute_start)
TTL minute_start + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

