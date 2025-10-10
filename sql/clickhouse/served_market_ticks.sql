-- Served layer schema for market tick queries.
-- Source: normalized (silver) stream written to `silver_ticks`.

-- Base served table retains 90 days of tick history, partitioned by event date
-- to keep pruning predictable and cold partitions small.
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
PARTITION BY toDate(tick_timestamp) -- daily partitions for efficient pruning
ORDER BY (tenant_id, market, symbol, tick_timestamp)
TTL toDateTime(tick_timestamp) + INTERVAL 90 DAY -- retain 90 days of served tick history
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_symbol
ON served_market_ticks (symbol) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_instrument
ON served_market_ticks (instrument_id) TYPE minmax GRANULARITY 1;

-- Latest snapshot table keeps only the most recent value per symbol with a short TTL
-- so consumers can query a small, hot dataset.
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
TTL toDateTime(updated_at) + INTERVAL 7 DAY -- automatically purge stale snapshots after 7 days
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_latest_symbol
ON served_market_ticks_latest (symbol) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_served_market_ticks_latest_instrument
ON served_market_ticks_latest (instrument_id) TYPE minmax GRANULARITY 1;

-- Materialized view populating the served history table directly from the normalized stream.
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

-- Materialized view maintaining the latest-per-symbol snapshot for fast served reads.
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
        metadata,
        toUnixTimestamp64Milli(timestamp) AS ordering_key,
        now64() AS updated_at
    FROM silver_ticks
)
GROUP BY tenant_id, instrument_id;
