-- Served reference instruments table for ClickHouse
-- This table stores normalized instrument reference data for serving layer

CREATE TABLE IF NOT EXISTS served.reference_instruments (
    instrument_id String,
    symbol String,
    name String,
    exchange String,
    asset_class LowCardinality(String),
    currency FixedString(3),
    is_active UInt8,
    sector Nullable(String),
    country Nullable(String),
    underlying_asset Nullable(String),
    contract_size Nullable(Float64),
    tick_size Nullable(Float64),
    maturity_date Nullable(Date),
    strike_price Nullable(Float64),
    option_type Nullable(String),
    effective_date DateTime64(3),
    updated_at DateTime64(3),
    event_id UUID,
    event_version UInt32
) ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toDate(effective_date)
ORDER BY (instrument_id)
SETTINGS index_granularity = 8192;

-- Create materialized view for active instruments
CREATE MATERIALIZED VIEW IF NOT EXISTS served.active_instruments
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toDate(effective_date)
ORDER BY (instrument_id)
AS SELECT
    instrument_id,
    symbol,
    name,
    exchange,
    asset_class,
    currency,
    sector,
    country,
    underlying_asset,
    contract_size,
    tick_size,
    maturity_date,
    strike_price,
    option_type,
    effective_date,
    updated_at,
    event_id,
    event_version
FROM served.reference_instruments
WHERE is_active = 1;

-- Create index for symbol lookups
CREATE INDEX IF NOT EXISTS idx_symbol ON served.reference_instruments (symbol) TYPE bloom_filter GRANULARITY 1;

-- Create index for exchange lookups
CREATE INDEX IF NOT EXISTS idx_exchange ON served.reference_instruments (exchange) TYPE bloom_filter GRANULARITY 1;

-- Create index for asset class lookups
CREATE INDEX IF NOT EXISTS idx_asset_class ON served.reference_instruments (asset_class) TYPE bloom_filter GRANULARITY 1;
