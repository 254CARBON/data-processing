-- Materialized views for common aggregations and query patterns
-- These views precompute expensive aggregations to improve query performance

-- ==================================================================
-- HOURLY MARKET STATISTICS MATERIALIZED VIEW
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_market_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour_start)
ORDER BY (tenant_id, market, instrument_id, hour_start)
TTL hour_start + INTERVAL 90 DAY
AS
SELECT
    tenant_id,
    market,
    instrument_id,
    toStartOfHour(timestamp) AS hour_start,
    count() AS tick_count,
    avg(price) AS avg_price,
    min(price) AS min_price,
    max(price) AS max_price,
    sum(volume) AS total_volume,
    countIf(quality_flag = 'VALID') AS valid_count,
    countIf(quality_flag = 'SUSPECT') AS suspect_count,
    countIf(quality_flag = 'INVALID') AS invalid_count
FROM market_ticks
WHERE timestamp >= now() - INTERVAL 90 DAY
GROUP BY tenant_id, market, instrument_id, hour_start;

-- ==================================================================
-- DAILY INSTRUMENT SUMMARY MATERIALIZED VIEW
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_instrument_summary
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, instrument_id, day)
TTL day + INTERVAL 365 DAY
AS
SELECT
    tenant_id,
    instrument_id,
    toDate(timestamp) AS day,
    count() AS tick_count,
    avg(price) AS avg_price,
    min(price) AS min_price,
    max(price) AS max_price,
    sum(volume) AS total_volume,
    stddevPop(price) AS price_volatility,
    quantile(0.5)(price) AS median_price,
    quantile(0.95)(price) AS p95_price,
    quantile(0.99)(price) AS p99_price,
    argMin(price, timestamp) AS open_price,
    argMax(price, timestamp) AS close_price,
    now() AS updated_at
FROM market_ticks
WHERE timestamp >= now() - INTERVAL 365 DAY
GROUP BY tenant_id, instrument_id, day;

-- ==================================================================
-- ENRICHMENT TAXONOMY DISTRIBUTION
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_taxonomy_distribution
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, commodity_tier, region, product_tier, day)
TTL day + INTERVAL 180 DAY
AS
SELECT
    tenant_id,
    commodity_tier,
    region,
    product_tier,
    toDate(timestamp) AS day,
    count() AS tick_count,
    avg(price) AS avg_price,
    avg(confidence_score) AS avg_confidence,
    sum(volume) AS total_volume
FROM enriched_ticks
WHERE timestamp >= now() - INTERVAL 180 DAY
GROUP BY tenant_id, commodity_tier, region, product_tier, day;

-- ==================================================================
-- LATEST PRICES BY INSTRUMENT (REAL-TIME VIEW)
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_prices_fast
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (tenant_id, instrument_id)
AS
SELECT
    tenant_id,
    instrument_id,
    market,
    price,
    volume,
    timestamp,
    quality_flag
FROM market_ticks
WHERE timestamp >= now() - INTERVAL 1 HOUR;

-- ==================================================================
-- BAR COMPLETION STATISTICS
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_bar_completion_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(day)
ORDER BY (tenant_id, instrument_id, time_window, day)
TTL day + INTERVAL 90 DAY
AS
SELECT
    tenant_id,
    instrument_id,
    time_window,
    toDate(bar_start_time) AS day,
    countIf(is_complete = 1) AS completed_bars,
    countIf(is_complete = 0) AS incomplete_bars,
    avg(tick_count) AS avg_ticks_per_bar,
    avg(close_price - open_price) AS avg_price_change
FROM ohlc_bars
WHERE bar_start_time >= now() - INTERVAL 90 DAY
GROUP BY tenant_id, instrument_id, time_window, day;

-- ==================================================================
-- CURVE POINT HISTORY FOR ANALYSIS
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_curve_history
ENGINE = ReplacingMergeTree(snapshot_time)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (tenant_id, curve_id, tenor, snapshot_date)
TTL snapshot_date + INTERVAL 365 DAY
AS
SELECT
    tenant_id,
    curve_id,
    curve_type,
    tenor,
    toDate(snapshot_time) AS snapshot_date,
    avg(rate) AS avg_rate,
    min(rate) AS min_rate,
    max(rate) AS max_rate,
    stddevPop(rate) AS rate_volatility,
    max(snapshot_time) AS snapshot_time
FROM curve_points
WHERE snapshot_time >= now() - INTERVAL 365 DAY
GROUP BY tenant_id, curve_id, curve_type, tenor, snapshot_date;

-- ==================================================================
-- AUDIT EVENT SUMMARY BY HOUR
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_audit_summary_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour_start)
ORDER BY (tenant_id, event_type, severity, hour_start)
TTL hour_start + INTERVAL 90 DAY
AS
SELECT
    tenant_id,
    event_type,
    severity,
    toStartOfHour(event_time) AS hour_start,
    count() AS event_count,
    uniqExact(actor_id) AS unique_actors,
    uniqExact(correlation_id) AS unique_correlations
FROM audit_events
WHERE event_time >= now() - INTERVAL 90 DAY
GROUP BY tenant_id, event_type, severity, hour_start;

-- ==================================================================
-- PROCESSING LATENCY TRACKING
-- ==================================================================

CREATE TABLE IF NOT EXISTS processing_latency_log
(
    tenant_id String,
    service_name LowCardinality(String),
    operation LowCardinality(String),
    latency_ms UInt32,
    timestamp DateTime64(3),
    success Bool,
    error_message String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, service_name, operation, timestamp)
TTL timestamp + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latency_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(minute_start)
ORDER BY (tenant_id, service_name, operation, minute_start)
TTL minute_start + INTERVAL 30 DAY
AS
SELECT
    tenant_id,
    service_name,
    operation,
    toStartOfMinute(timestamp) AS minute_start,
    count() AS request_count,
    countIf(success = 1) AS success_count,
    countIf(success = 0) AS error_count,
    avg(latency_ms) AS avg_latency,
    quantile(0.50)(latency_ms) AS p50_latency,
    quantile(0.95)(latency_ms) AS p95_latency,
    quantile(0.99)(latency_ms) AS p99_latency,
    max(latency_ms) AS max_latency
FROM processing_latency_log
GROUP BY tenant_id, service_name, operation, minute_start;

-- ==================================================================
-- VERIFY MATERIALIZED VIEWS
-- ==================================================================

SELECT 
    database,
    name,
    engine,
    total_rows,
    total_bytes
FROM system.tables
WHERE database = currentDatabase() 
AND name LIKE 'mv_%'
ORDER BY name;
