-- ClickHouse migration: Analytics curve base schema
-- Created: 2025-02-10
-- Description: Ensures analytics.curve_base and supporting objects exist for downstream consumers

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.curve_base (
    curve_id String,
    as_of_date Date,
    maturity Date,
    value Float64,
    instrument Nullable(String),
    source String DEFAULT 'unknown',
    created_at DateTime DEFAULT now(),
    tenant_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (curve_id, as_of_date, maturity, tenant_id)
SETTINGS index_granularity = 8192;

ALTER TABLE analytics.curve_base ADD INDEX IF NOT EXISTS idx_curve_date (curve_id, as_of_date) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE analytics.curve_base ADD INDEX IF NOT EXISTS idx_instrument (instrument) TYPE bloom_filter GRANULARITY 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.curve_base_latest
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (curve_id, maturity, tenant_id)
AS SELECT 
    curve_id,
    as_of_date,
    maturity,
    value,
    instrument,
    source,
    created_at,
    tenant_id
FROM analytics.curve_base
WHERE as_of_date = (
    SELECT max(as_of_date) 
    FROM analytics.curve_base cb2 
    WHERE cb2.curve_id = curve_base.curve_id 
      AND cb2.tenant_id = curve_base.tenant_id
);

ALTER TABLE analytics.curve_base COMMENT 'Raw curve observations from market data';
ALTER TABLE analytics.curve_base_latest COMMENT 'Latest curve data per curve_id for fast lookups';
