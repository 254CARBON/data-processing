-- ClickHouse migration: Create enriched layer tables and indexes
-- Created: 2025-01-27
-- Description: Creates enriched tick tables for Gold layer

-- Enriched ticks table (Gold layer with metadata)
CREATE TABLE IF NOT EXISTS enriched_ticks (
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

-- Enriched ticks indexes
CREATE INDEX IF NOT EXISTS idx_enriched_ticks_instrument 
ON enriched_ticks (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_enriched_ticks_timestamp 
ON enriched_ticks (timestamp) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_enriched_ticks_tenant 
ON enriched_ticks (tenant_id) TYPE set(100) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_enriched_ticks_commodity 
ON enriched_ticks (metadata['commodity']) TYPE set(50) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_enriched_ticks_region 
ON enriched_ticks (metadata['region']) TYPE set(50) GRANULARITY 1;

-- Enriched ticks materialized view for real-time aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_ticks_realtime
TO enriched_ticks_realtime_summary
AS SELECT
    tenant_id,
    instrument_id,
    metadata['commodity'] as commodity,
    metadata['region'] as region,
    toStartOfMinute(timestamp) as minute_start,
    count() as tick_count,
    avg(price) as avg_price,
    max(price) as max_price,
    min(price) as min_price,
    sum(volume) as total_volume,
    now64() as updated_at
FROM enriched_ticks
GROUP BY tenant_id, instrument_id, commodity, region, minute_start;

-- Enriched ticks realtime summary table
CREATE TABLE IF NOT EXISTS enriched_ticks_realtime_summary (
    tenant_id String,
    instrument_id String,
    commodity String,
    region String,
    minute_start DateTime64(3),
    tick_count UInt64,
    avg_price Float64,
    max_price Float64,
    min_price Float64,
    total_volume Float64,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(minute_start)
ORDER BY (tenant_id, instrument_id, commodity, region, minute_start)
TTL minute_start + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

