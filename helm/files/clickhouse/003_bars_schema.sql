-- ClickHouse migration: Create OHLC bars tables
-- Created: 2025-01-27
-- Description: Creates aggregated OHLC bar tables for different intervals

-- 5-minute bars table
CREATE TABLE IF NOT EXISTS bars_5m (
    instrument_id String,
    interval_start DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    trade_count UInt64,
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(interval_start)
ORDER BY (tenant_id, instrument_id, interval_start)
TTL interval_start + INTERVAL 3 YEAR
SETTINGS index_granularity = 8192;

-- 1-hour bars table
CREATE TABLE IF NOT EXISTS bars_1h (
    instrument_id String,
    interval_start DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    trade_count UInt64,
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(interval_start)
ORDER BY (tenant_id, instrument_id, interval_start)
TTL interval_start + INTERVAL 3 YEAR
SETTINGS index_granularity = 8192;

-- Daily bars table
CREATE TABLE IF NOT EXISTS bars_1d (
    instrument_id String,
    interval_start DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    trade_count UInt64,
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(interval_start)
ORDER BY (tenant_id, instrument_id, interval_start)
TTL interval_start + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192;

-- Bars indexes
CREATE INDEX IF NOT EXISTS idx_bars_5m_instrument 
ON bars_5m (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bars_5m_timestamp 
ON bars_5m (interval_start) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bars_1h_instrument 
ON bars_1h (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bars_1h_timestamp 
ON bars_1h (interval_start) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bars_1d_instrument 
ON bars_1d (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bars_1d_timestamp 
ON bars_1d (interval_start) TYPE minmax GRANULARITY 1;

-- Bars aggregation materialized views
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_5m_aggregator
TO bars_5m
AS SELECT
    instrument_id,
    toStartOfFiveMinutes(timestamp) as interval_start,
    any(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    anyLast(price) as close_price,
    sum(volume) as volume,
    count() as trade_count,
    tenant_id,
    map('interval', '5m') as metadata,
    now64() as created_at
FROM enriched_ticks
GROUP BY tenant_id, instrument_id, interval_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1h_aggregator
TO bars_1h
AS SELECT
    instrument_id,
    toStartOfHour(timestamp) as interval_start,
    any(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    anyLast(price) as close_price,
    sum(volume) as volume,
    count() as trade_count,
    tenant_id,
    map('interval', '1h') as metadata,
    now64() as created_at
FROM enriched_ticks
GROUP BY tenant_id, instrument_id, interval_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1d_aggregator
TO bars_1d
AS SELECT
    instrument_id,
    toStartOfDay(timestamp) as interval_start,
    any(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    anyLast(price) as close_price,
    sum(volume) as volume,
    count() as trade_count,
    tenant_id,
    map('interval', '1d') as metadata,
    now64() as created_at
FROM enriched_ticks
GROUP BY tenant_id, instrument_id, interval_start;

