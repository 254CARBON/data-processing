-- Gold layer schemas for normalized and enriched market data

-- CAISO LMP Gold table (normalized + EDAM enriched)
CREATE TABLE IF NOT EXISTS gold.market.caiso.lmp (
    timestamp DateTime,
    market_run_id LowCardinality(String),
    node_id String,
    ba_id LowCardinality(String),

    -- Price components (USD/MWh)
    lmp_total Nullable(Float64),
    lmp_energy Nullable(Float64),
    lmp_congestion Nullable(Float64),
    lmp_losses Nullable(Float64),

    -- EDAM enrichment
    edam_eligible Bool DEFAULT false,
    edam_participant Bool DEFAULT false,
    edam_hurdle_rate_usd_per_mwh Nullable(Float64),
    edam_transfer_limit_mw Nullable(UInt32),
    edam_region Nullable(String),

    -- Metadata
    data_type LowCardinality(String),
    source LowCardinality(String),
    extracted_at DateTime,
    enriched_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_node node_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, node_id, market_run_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 2 YEAR;

-- CAISO AS Gold table (normalized + EDAM enriched)
CREATE TABLE IF NOT EXISTS gold.market.caiso.as (
    timestamp DateTime,
    market_run_id LowCardinality(String),
    as_type LowCardinality(String),
    as_region String,
    ba_id LowCardinality(String),

    -- AS data (USD/MW and MW)
    as_clearing_price_usd_per_mw Nullable(Float64),
    as_cleared_mw Nullable(Float64),

    -- EDAM enrichment
    edam_eligible Bool DEFAULT false,
    edam_participant Bool DEFAULT false,
    edam_hurdle_rate_usd_per_mwh Nullable(Float64),
    edam_transfer_limit_mw Nullable(UInt32),
    edam_region Nullable(String),

    -- Metadata
    data_type LowCardinality(String),
    source LowCardinality(String),
    extracted_at DateTime,
    enriched_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_region as_region TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, as_region, market_run_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 2 YEAR;

-- CAISO CRR Gold table (normalized)
CREATE TABLE IF NOT EXISTS gold.market.caiso.crr (
    timestamp DateTime,
    crr_type LowCardinality(String),
    source String,
    sink String,
    ba_id LowCardinality(String),

    -- CRR data (MW and USD/MW)
    crr_mw Nullable(Float64),
    crr_price_usd_per_mw Nullable(Float64),

    -- Metadata
    data_type LowCardinality(String),
    source LowCardinality(String),
    extracted_at DateTime,
    enriched_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_source source TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_sink sink TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, source, sink, crr_type, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 2 YEAR;

-- Materialized views for common queries

-- Daily LMP summary by node and market
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.market.caiso.lmp_daily_summary
ENGINE = SummingMergeTree()
ORDER BY (ba_id, node_id, market_run_id, date)
AS SELECT
    ba_id,
    node_id,
    market_run_id,
    toDate(timestamp) as date,
    count() as record_count,
    avg(lmp_total) as avg_lmp_total,
    min(lmp_total) as min_lmp_total,
    max(lmp_total) as max_lmp_total,
    avg(lmp_energy) as avg_lmp_energy,
    avg(lmp_congestion) as avg_lmp_congestion,
    avg(lmp_losses) as avg_lmp_losses,
    sum(edam_eligible) as edam_eligible_count,
    sum(edam_participant) as edam_participant_count,
    avg(edam_hurdle_rate_usd_per_mwh) as avg_edam_hurdle_rate,
    max(edam_transfer_limit_mw) as max_edam_transfer_limit
FROM gold.market.caiso.lmp
GROUP BY ba_id, node_id, market_run_id, date;

-- Hourly AS clearing by type and region
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.market.caiso.as_hourly_summary
ENGINE = SummingMergeTree()
ORDER BY (ba_id, as_region, market_run_id, as_type, hour)
AS SELECT
    ba_id,
    as_region,
    market_run_id,
    as_type,
    toHour(timestamp) as hour,
    count() as record_count,
    avg(as_clearing_price_usd_per_mw) as avg_clearing_price,
    sum(as_cleared_mw) as total_cleared_mw,
    sum(edam_eligible) as edam_eligible_count,
    sum(edam_participant) as edam_participant_count
FROM gold.market.caiso.as
GROUP BY ba_id, as_region, market_run_id, as_type, hour;

-- Daily CRR auction summary
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.market.caiso.crr_daily_summary
ENGINE = SummingMergeTree()
ORDER BY (ba_id, crr_type, source, sink, date)
AS SELECT
    ba_id,
    crr_type,
    source,
    sink,
    toDate(timestamp) as date,
    count() as record_count,
    sum(crr_mw) as total_crr_mw,
    avg(crr_price_usd_per_mw) as avg_crr_price,
    min(crr_price_usd_per_mw) as min_crr_price,
    max(crr_price_usd_per_mw) as max_crr_price
FROM gold.market.caiso.crr
GROUP BY ba_id, crr_type, source, sink, date;
