-- Performance optimization indexes for ClickHouse
-- This migration adds indexes for common query patterns to improve query performance

-- ==================================================================
-- MARKET TICKS TABLE INDEXES
-- ==================================================================

-- Index for instrument_id + timestamp range queries (most common pattern)
ALTER TABLE market_ticks
    ADD INDEX IF NOT EXISTS idx_instrument_timestamp (instrument_id, timestamp)
    TYPE minmax
    GRANULARITY 8192;

-- Index for tenant_id filtering
ALTER TABLE market_ticks
    ADD INDEX IF NOT EXISTS idx_tenant_id (tenant_id)
    TYPE bloom_filter
    GRANULARITY 4096;

-- Index for quality flag filtering (to find high-quality data quickly)
ALTER TABLE market_ticks
    ADD INDEX IF NOT EXISTS idx_quality_flag (quality_flag)
    TYPE set(10)
    GRANULARITY 4096;

-- Index for market filtering
ALTER TABLE market_ticks
    ADD INDEX IF NOT EXISTS idx_market (market)
    TYPE set(20)
    GRANULARITY 4096;

-- Compound index for common filtering patterns
ALTER TABLE market_ticks
    ADD INDEX IF NOT EXISTS idx_tenant_instrument (tenant_id, instrument_id)
    TYPE bloom_filter
    GRANULARITY 8192;

-- ==================================================================
-- ENRICHED TICKS TABLE INDEXES
-- ==================================================================

-- Index for commodity classification queries
ALTER TABLE enriched_ticks
    ADD INDEX IF NOT EXISTS idx_commodity_tier (commodity_tier)
    TYPE set(10)
    GRANULARITY 4096;

-- Index for region filtering
ALTER TABLE enriched_ticks
    ADD INDEX IF NOT EXISTS idx_region (region)
    TYPE set(50)
    GRANULARITY 4096;

-- Index for product tier
ALTER TABLE enriched_ticks
    ADD INDEX IF NOT EXISTS idx_product_tier (product_tier)
    TYPE set(10)
    GRANULARITY 4096;

-- Compound index for taxonomy queries
ALTER TABLE enriched_ticks
    ADD INDEX IF NOT EXISTS idx_taxonomy (commodity_tier, region, product_tier)
    TYPE bloom_filter
    GRANULARITY 8192;

-- Index for confidence score filtering
ALTER TABLE enriched_ticks
    ADD INDEX IF NOT EXISTS idx_confidence_score (confidence_score)
    TYPE minmax
    GRANULARITY 4096;

-- ==================================================================
-- OHLC BARS TABLE INDEXES
-- ==================================================================

-- Index for instrument and time window queries
ALTER TABLE ohlc_bars
    ADD INDEX IF NOT EXISTS idx_instrument_window (instrument_id, time_window)
    TYPE minmax
    GRANULARITY 4096;

-- Index for bar start time range queries
ALTER TABLE ohlc_bars
    ADD INDEX IF NOT EXISTS idx_bar_start_time (bar_start_time)
    TYPE minmax
    GRANULARITY 2048;

-- Index for completed bars
ALTER TABLE ohlc_bars
    ADD INDEX IF NOT EXISTS idx_is_complete (is_complete)
    TYPE set(2)
    GRANULARITY 2048;

-- Compound index for querying bars by tenant and instrument
ALTER TABLE ohlc_bars
    ADD INDEX IF NOT EXISTS idx_tenant_instrument_bars (tenant_id, instrument_id, bar_start_time)
    TYPE bloom_filter
    GRANULARITY 8192;

-- ==================================================================
-- CURVE POINTS TABLE INDEXES
-- ==================================================================

-- Index for curve_id and tenor queries
ALTER TABLE curve_points
    ADD INDEX IF NOT EXISTS idx_curve_tenor (curve_id, tenor)
    TYPE minmax
    GRANULARITY 4096;

-- Index for snapshot time
ALTER TABLE curve_points
    ADD INDEX IF NOT EXISTS idx_snapshot_time (snapshot_time)
    TYPE minmax
    GRANULARITY 2048;

-- Index for curve type
ALTER TABLE curve_points
    ADD INDEX IF NOT EXISTS idx_curve_type (curve_type)
    TYPE set(10)
    GRANULARITY 2048;

-- ==================================================================
-- LATEST PRICES TABLE INDEXES
-- ==================================================================

-- Index for fast instrument lookups
ALTER TABLE latest_prices
    ADD INDEX IF NOT EXISTS idx_instrument_latest (instrument_id)
    TYPE bloom_filter
    GRANULARITY 1024;

-- Index for updated_at to find recently updated prices
ALTER TABLE latest_prices
    ADD INDEX IF NOT EXISTS idx_updated_at (updated_at)
    TYPE minmax
    GRANULARITY 1024;

-- ==================================================================
-- AUDIT EVENTS TABLE INDEXES
-- ==================================================================

-- Index for event type filtering
ALTER TABLE audit_events
    ADD INDEX IF NOT EXISTS idx_event_type (event_type)
    TYPE set(50)
    GRANULARITY 4096;

-- Index for tenant audit queries
ALTER TABLE audit_events
    ADD INDEX IF NOT EXISTS idx_audit_tenant (tenant_id)
    TYPE bloom_filter
    GRANULARITY 4096;

-- Index for actor filtering
ALTER TABLE audit_events
    ADD INDEX IF NOT EXISTS idx_actor_id (actor_id)
    TYPE bloom_filter
    GRANULARITY 4096;

-- Index for severity-based queries
ALTER TABLE audit_events
    ADD INDEX IF NOT EXISTS idx_severity (severity)
    TYPE set(10)
    GRANULARITY 2048;

-- Index for correlation tracking
ALTER TABLE audit_events
    ADD INDEX IF NOT EXISTS idx_correlation_id (correlation_id)
    TYPE bloom_filter
    GRANULARITY 8192;

-- ==================================================================
-- OPTIMIZE TABLES AFTER INDEX CREATION
-- ==================================================================

-- Optimize tables to apply indexes and merge parts
OPTIMIZE TABLE market_ticks FINAL;
OPTIMIZE TABLE enriched_ticks FINAL;
OPTIMIZE TABLE ohlc_bars FINAL;
OPTIMIZE TABLE curve_points FINAL;
OPTIMIZE TABLE latest_prices FINAL;
OPTIMIZE TABLE audit_events FINAL;

-- ==================================================================
-- PERFORMANCE TUNING SETTINGS
-- ==================================================================

-- Set optimal merge tree settings for write performance
ALTER TABLE market_ticks 
    MODIFY SETTING 
        merge_with_ttl_timeout = 3600,
        max_bytes_to_merge_at_max_space_usage = 104857600,  -- 100MB
        max_bytes_to_merge_at_min_space_in_pool = 10485760;  -- 10MB

ALTER TABLE enriched_ticks 
    MODIFY SETTING 
        merge_with_ttl_timeout = 3600,
        max_bytes_to_merge_at_max_space_usage = 104857600;

ALTER TABLE ohlc_bars 
    MODIFY SETTING 
        merge_with_ttl_timeout = 7200,
        max_bytes_to_merge_at_max_space_usage = 209715200;  -- 200MB

ALTER TABLE curve_points 
    MODIFY SETTING 
        merge_with_ttl_timeout = 7200,
        max_bytes_to_merge_at_max_space_usage = 209715200;

-- Verify indexes were created
SELECT 
    table,
    name,
    type,
    granularity
FROM system.data_skipping_indices
WHERE database = currentDatabase()
ORDER BY table, name;
