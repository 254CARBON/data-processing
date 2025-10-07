-- ClickHouse migration: Create forward curve tables
-- Created: 2025-01-27
-- Description: Creates forward curve tables for pricing data

-- Forward curve base table (raw curve points)
CREATE TABLE IF NOT EXISTS curves_base (
    curve_id String,
    as_of_date Date,
    contract_month String,
    price Float64,
    volume Float64,
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (tenant_id, curve_id, as_of_date, contract_month)
TTL as_of_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Forward curve computed table (interpolated results)
CREATE TABLE IF NOT EXISTS curves_computed (
    curve_id String,
    as_of_date Date,
    contract_month String,
    price Float64,
    volume Float64,
    interpolation_method String,
    confidence Float64,
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (tenant_id, curve_id, as_of_date, contract_month)
TTL as_of_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Curve indexes
CREATE INDEX IF NOT EXISTS idx_curves_base_curve 
ON curves_base (curve_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_curves_base_date 
ON curves_base (as_of_date) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_curves_computed_curve 
ON curves_computed (curve_id) TYPE minmax GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_curves_computed_date 
ON curves_computed (as_of_date) TYPE minmax GRANULARITY 1;

-- Curve snapshots table (for serving layer)
CREATE TABLE IF NOT EXISTS curve_snapshots (
    curve_id String,
    as_of_date Date,
    snapshot_data String, -- JSON string of curve points
    tenant_id String DEFAULT 'default',
    metadata Map(String, String),
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (tenant_id, curve_id, as_of_date)
TTL as_of_date + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Curve snapshots index
CREATE INDEX IF NOT EXISTS idx_curve_snapshots_curve 
ON curve_snapshots (curve_id) TYPE minmax GRANULARITY 1;

-- Curve interpolation materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS curve_interpolator
TO curves_computed
AS SELECT
    curve_id,
    as_of_date,
    contract_month,
    price,
    volume,
    'linear' as interpolation_method,
    0.8 as confidence,
    tenant_id,
    map('source', 'interpolation') as metadata,
    now64() as created_at
FROM curves_base
WHERE price > 0;

-- Curve snapshots materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS curve_snapshot_builder
TO curve_snapshots
AS SELECT
    curve_id,
    as_of_date,
    groupArray(map('contract_month', contract_month, 'price', toString(price), 'volume', toString(volume))) as snapshot_data,
    tenant_id,
    map('method', 'aggregation') as metadata,
    now64() as created_at
FROM curves_computed
GROUP BY tenant_id, curve_id, as_of_date;

