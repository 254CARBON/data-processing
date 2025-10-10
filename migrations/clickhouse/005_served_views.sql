-- ClickHouse migration: Create served layer tables, indexes, and materialized views
-- Created: 2025-01-27
-- Description: Defines served-layer datasets for API consumption backed by projections

/* ======================================================================
   SERVED MARKET TICKS (FROM NORMALIZED STREAM)
   Mirrors canonical DDL in sql/clickhouse/served_market_ticks.sql
   ====================================================================== */

CREATE TABLE IF NOT EXISTS served_market_ticks (
    tenant_id LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    instrument_id String,
    tick_timestamp DateTime64(3),
    price Float64,
    volume Float64,
    quality_flags Array(String),
    source_id Nullable(String),
    metadata Map(String, String),
    ingested_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree
PARTITION BY toDate(tick_timestamp)
ORDER BY (tenant_id, market, symbol, tick_timestamp)
TTL toDateTime(tick_timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_symbol
ON served_market_ticks (symbol) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_instrument
ON served_market_ticks (instrument_id) TYPE minmax GRANULARITY 1;

CREATE TABLE IF NOT EXISTS served_market_ticks_latest (
    tenant_id LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    instrument_id String,
    tick_timestamp DateTime64(3),
    price Float64,
    volume Float64,
    quality_flags Array(String),
    source_id Nullable(String),
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toDate(tick_timestamp)
ORDER BY (tenant_id, market, symbol)
TTL toDateTime(updated_at) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_latest_symbol
ON served_market_ticks_latest (symbol) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_latest_instrument
ON served_market_ticks_latest (instrument_id) TYPE minmax GRANULARITY 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_served_market_ticks
TO served_market_ticks
AS
SELECT
    tenant_id,
    coalesce(metadata['market'], 'unknown') AS market,
    coalesce(metadata['symbol'], instrument_id) AS symbol,
    instrument_id,
    timestamp AS tick_timestamp,
    price,
    volume,
    quality_flags,
    source_id,
    metadata,
    now64() AS ingested_at,
    now64() AS updated_at
FROM silver_ticks;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_served_market_ticks_latest
TO served_market_ticks_latest
AS
SELECT
    tenant_id,
    argMax(coalesce(metadata['market'], 'unknown'), ordering_key) AS market,
    argMax(coalesce(metadata['symbol'], instrument_id), ordering_key) AS symbol,
    instrument_id,
    argMax(timestamp, ordering_key) AS tick_timestamp,
    argMax(price, ordering_key) AS price,
    argMax(volume, ordering_key) AS volume,
    argMax(quality_flags, ordering_key) AS quality_flags,
    argMax(source_id, ordering_key) AS source_id,
    max(updated_at) AS updated_at
FROM (
    SELECT
        tenant_id,
        instrument_id,
        price,
        volume,
        quality_flags,
        source_id,
        timestamp,
        toUnixTimestamp64Milli(timestamp) AS ordering_key,
        now64() AS updated_at,
        metadata
    FROM silver_ticks
)
GROUP BY tenant_id, instrument_id;

/* ======================================================================
   SERVED LATEST PRICE SNAPSHOTS
   ====================================================================== */

-- Change-log table that stores every latest price projection emitted by the projection service.
CREATE TABLE IF NOT EXISTS served_latest (
    tenant_id LowCardinality(String),
    instrument_id String,
    price Float64,
    volume Float64,
    quality_flags Array(String),
    source LowCardinality(String) DEFAULT 'unknown',
    snapshot_at DateTime64(3) DEFAULT now64(),
    projection_type LowCardinality(String) DEFAULT 'latest_price',
    metadata String DEFAULT '{}',
    processed_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (tenant_id, instrument_id, snapshot_at, processed_at)
TTL toDateTime(snapshot_at) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_latest_instrument
ON served_latest (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_latest_snapshot
ON served_latest (snapshot_at) TYPE minmax GRANULARITY 1;

-- Serving-optimised table that keeps only the most recent snapshot per (tenant, instrument).
CREATE TABLE IF NOT EXISTS served_latest_current (
    tenant_id LowCardinality(String),
    instrument_id String,
    price Float64,
    volume Float64,
    quality_flags Array(String),
    source LowCardinality(String),
    snapshot_at DateTime64(3),
    projection_type LowCardinality(String),
    metadata String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, instrument_id)
TTL toDateTime(snapshot_at) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_latest_current_instrument
ON served_latest_current (instrument_id) TYPE minmax GRANULARITY 1;

-- Materialized view that collapses the change-log table into the latest snapshot table.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_served_latest_current
TO served_latest_current
AS
SELECT
    tenant_id,
    instrument_id,
    argMax(price, ordering_key) AS price,
    argMax(volume, ordering_key) AS volume,
    argMax(quality_flags, ordering_key) AS quality_flags,
    argMax(source, ordering_key) AS source,
    argMax(snapshot_at, ordering_key) AS snapshot_at,
    argMax(projection_type, ordering_key) AS projection_type,
    argMax(metadata, ordering_key) AS metadata,
    max(processed_at) AS updated_at
FROM (
    SELECT
        tenant_id,
        instrument_id,
        price,
        volume,
        quality_flags,
        source,
        snapshot_at,
        projection_type,
        metadata,
        processed_at,
        toUnixTimestamp64Milli(processed_at) AS ordering_key
    FROM served_latest
)
GROUP BY tenant_id, instrument_id;

/* ======================================================================
   SERVED CURVE SNAPSHOTS
   ====================================================================== */

-- Change-log table with curve snapshot projections (per tenant/instrument/horizon).
CREATE TABLE IF NOT EXISTS served_curve_snapshots (
    tenant_id LowCardinality(String),
    instrument_id String,
    horizon LowCardinality(String),
    curve_points String,
    interpolation_method LowCardinality(String) DEFAULT 'linear',
    quality_flags Array(String),
    snapshot_at DateTime64(3) DEFAULT now64(),
    projection_type LowCardinality(String) DEFAULT 'curve_snapshot',
    metadata String DEFAULT '{}',
    processed_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (tenant_id, instrument_id, horizon, snapshot_at, processed_at)
TTL toDateTime(snapshot_at) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_curve_snapshots_instrument
ON served_curve_snapshots (instrument_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_curve_snapshots_horizon
ON served_curve_snapshots (horizon) TYPE set(100) GRANULARITY 1;

-- Serving-optimised table with the latest curve snapshot per (tenant, instrument, horizon).
CREATE TABLE IF NOT EXISTS served_curve_snapshots_current (
    tenant_id LowCardinality(String),
    instrument_id String,
    horizon LowCardinality(String),
    curve_points String,
    interpolation_method LowCardinality(String),
    quality_flags Array(String),
    snapshot_at DateTime64(3),
    projection_type LowCardinality(String),
    metadata String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, instrument_id, horizon)
TTL toDateTime(snapshot_at) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_curve_snapshots_current_instrument
ON served_curve_snapshots_current (instrument_id) TYPE minmax GRANULARITY 1;

-- Materialized view collapsing curve snapshot change-log entries.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_served_curve_snapshots_current
TO served_curve_snapshots_current
AS
SELECT
    tenant_id,
    instrument_id,
    horizon,
    argMax(curve_points, ordering_key) AS curve_points,
    argMax(interpolation_method, ordering_key) AS interpolation_method,
    argMax(quality_flags, ordering_key) AS quality_flags,
    argMax(snapshot_at, ordering_key) AS snapshot_at,
    argMax(projection_type, ordering_key) AS projection_type,
    argMax(metadata, ordering_key) AS metadata,
    max(processed_at) AS updated_at
FROM (
    SELECT
        tenant_id,
        instrument_id,
        horizon,
        curve_points,
        interpolation_method,
        quality_flags,
        snapshot_at,
        projection_type,
        metadata,
        processed_at,
        toUnixTimestamp64Milli(processed_at) AS ordering_key
    FROM served_curve_snapshots
)
GROUP BY tenant_id, instrument_id, horizon;

/* ======================================================================
   PROCESSING METRICS (unchanged from previous revision)
   ====================================================================== */

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
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_metrics_processing_service
ON metrics_processing (service_name) TYPE set(20) GRANULARITY 1;

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
TTL toDateTime(minute_start) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS processing_metrics_realtime
TO processing_metrics_realtime_summary
AS
SELECT
    service_name,
    metric_name,
    avg(metric_value) AS avg_value,
    max(metric_value) AS max_value,
    min(metric_value) AS min_value,
    count() AS sample_count,
    toStartOfMinute(timestamp) AS minute_start,
    tenant_id
FROM metrics_processing
GROUP BY tenant_id, service_name, metric_name, minute_start;
