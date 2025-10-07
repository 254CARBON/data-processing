-- Migration: Optimized materialized views for projection service
-- Description: Creates optimized materialized views for common projection queries

-- Materialized view for latest prices
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_prices
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (tenant_id, instrument_id)
PARTITION BY toDate(timestamp)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    instrument_id,
    price,
    volume,
    timestamp,
    market,
    quality_flag,
    commodity_tier,
    region_tier,
    product_tier,
    _version
FROM (
    SELECT
        tenant_id,
        instrument_id,
        price,
        volume,
        timestamp,
        market,
        quality_flag,
        commodity_tier,
        region_tier,
        product_tier,
        _version,
        ROW_NUMBER() OVER (PARTITION BY tenant_id, instrument_id ORDER BY timestamp DESC) as rn
    FROM enriched_ticks
    WHERE quality_flag = 'valid'
) t
WHERE rn = 1;

-- Materialized view for price snapshots (hourly)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_price_snapshots_hourly
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, snapshot_time, instrument_id)
PARTITION BY toDate(snapshot_time)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    toStartOfHour(timestamp) as snapshot_time,
    instrument_id,
    argMax(price, timestamp) as latest_price,
    sum(volume) as total_volume,
    count() as tick_count,
    min(price) as min_price,
    max(price) as max_price,
    avg(price) as avg_price,
    stddevPop(price) as price_stddev,
    market,
    commodity_tier,
    region_tier,
    product_tier
FROM enriched_ticks
WHERE quality_flag = 'valid'
GROUP BY tenant_id, snapshot_time, instrument_id, market, commodity_tier, region_tier, product_tier;

-- Materialized view for price snapshots (daily)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_price_snapshots_daily
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, snapshot_date, instrument_id)
PARTITION BY toYearMonth(snapshot_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    toDate(timestamp) as snapshot_date,
    instrument_id,
    argMax(price, timestamp) as latest_price,
    sum(volume) as total_volume,
    count() as tick_count,
    min(price) as min_price,
    max(price) as max_price,
    avg(price) as avg_price,
    stddevPop(price) as price_stddev,
    market,
    commodity_tier,
    region_tier,
    product_tier
FROM enriched_ticks
WHERE quality_flag = 'valid'
GROUP BY tenant_id, snapshot_date, instrument_id, market, commodity_tier, region_tier, product_tier;

-- Materialized view for curve snapshots
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_curve_snapshots
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (tenant_id, curve_id, tenor)
PARTITION BY toDate(timestamp)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    curve_id,
    curve_type,
    tenor,
    price,
    timestamp,
    market,
    commodity_tier,
    region_tier,
    product_tier,
    _version
FROM (
    SELECT
        tenant_id,
        curve_id,
        curve_type,
        tenor,
        price,
        timestamp,
        market,
        commodity_tier,
        region_tier,
        product_tier,
        _version,
        ROW_NUMBER() OVER (PARTITION BY tenant_id, curve_id, tenor ORDER BY timestamp DESC) as rn
    FROM curve_points
) t
WHERE rn = 1;

-- Materialized view for market statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_market_stats
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, market, stat_date)
PARTITION BY toYearMonth(stat_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    market,
    toDate(timestamp) as stat_date,
    count() as total_ticks,
    countDistinct(instrument_id) as unique_instruments,
    sum(volume) as total_volume,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    stddevPop(price) as price_stddev,
    countIf(quality_flag = 'valid') as valid_ticks,
    countIf(quality_flag != 'valid') as invalid_ticks
FROM enriched_ticks
GROUP BY tenant_id, market, stat_date;

-- Materialized view for instrument statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_instrument_stats
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, instrument_id, stat_date)
PARTITION BY toYearMonth(stat_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    instrument_id,
    toDate(timestamp) as stat_date,
    count() as total_ticks,
    sum(volume) as total_volume,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    stddevPop(price) as price_stddev,
    countIf(quality_flag = 'valid') as valid_ticks,
    countIf(quality_flag != 'valid') as invalid_ticks,
    market,
    commodity_tier,
    region_tier,
    product_tier
FROM enriched_ticks
GROUP BY tenant_id, instrument_id, stat_date, market, commodity_tier, region_tier, product_tier;

-- Materialized view for tenant statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_stats
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, stat_date)
PARTITION BY toYearMonth(stat_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    toDate(timestamp) as stat_date,
    count() as total_ticks,
    countDistinct(instrument_id) as unique_instruments,
    countDistinct(market) as unique_markets,
    sum(volume) as total_volume,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    stddevPop(price) as price_stddev,
    countIf(quality_flag = 'valid') as valid_ticks,
    countIf(quality_flag != 'valid') as invalid_ticks
FROM enriched_ticks
GROUP BY tenant_id, stat_date;

-- Materialized view for data quality metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_data_quality_metrics
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, quality_date)
PARTITION BY toYearMonth(quality_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    toDate(timestamp) as quality_date,
    count() as total_ticks,
    countIf(quality_flag = 'valid') as valid_ticks,
    countIf(quality_flag = 'price_negative') as negative_price_ticks,
    countIf(quality_flag = 'out_of_range') as out_of_range_ticks,
    countIf(quality_flag = 'volume_spike') as volume_spike_ticks,
    countIf(quality_flag = 'late_arrival') as late_arrival_ticks,
    countIf(quality_flag = 'missing_metadata') as missing_metadata_ticks,
    (countIf(quality_flag = 'valid') / count()) * 100 as data_quality_percentage
FROM enriched_ticks
GROUP BY tenant_id, quality_date;

-- Materialized view for performance metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_performance_metrics
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, service_name, metric_date)
PARTITION BY toYearMonth(metric_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    service_name,
    toDate(timestamp) as metric_date,
    count() as total_events,
    avg(processing_time_ms) as avg_processing_time,
    min(processing_time_ms) as min_processing_time,
    max(processing_time_ms) as max_processing_time,
    stddevPop(processing_time_ms) as processing_time_stddev,
    countIf(event_type = 'success') as success_events,
    countIf(event_type = 'error') as error_events,
    countIf(event_type = 'timeout') as timeout_events,
    (countIf(event_type = 'success') / count()) * 100 as success_rate
FROM audit_events
WHERE event_type IN ('success', 'error', 'timeout')
GROUP BY tenant_id, service_name, metric_date;

-- Materialized view for aggregation metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_aggregation_metrics
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, aggregation_date)
PARTITION BY toYearMonth(aggregation_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    toDate(timestamp) as aggregation_date,
    count() as total_bars,
    countDistinct(instrument_id) as unique_instruments,
    sum(volume) as total_volume,
    avg(open_price) as avg_open_price,
    avg(high_price) as avg_high_price,
    avg(low_price) as avg_low_price,
    avg(close_price) as avg_close_price,
    countIf(bar_type = '1m') as minute_bars,
    countIf(bar_type = '5m') as five_minute_bars,
    countIf(bar_type = '15m') as fifteen_minute_bars,
    countIf(bar_type = '1h') as hourly_bars,
    countIf(bar_type = '1d') as daily_bars
FROM ohlc_bars
GROUP BY tenant_id, aggregation_date;

-- Materialized view for curve metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_curve_metrics
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, curve_id, metric_date)
PARTITION BY toYearMonth(metric_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    curve_id,
    curve_type,
    toDate(timestamp) as metric_date,
    count() as total_points,
    countDistinct(tenor) as unique_tenors,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    stddevPop(price) as price_stddev,
    market,
    commodity_tier,
    region_tier,
    product_tier
FROM curve_points
GROUP BY tenant_id, curve_id, curve_type, metric_date, market, commodity_tier, region_tier, product_tier;

-- Materialized view for cache metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cache_metrics
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, cache_type, metric_date)
PARTITION BY toYearMonth(metric_date)
SETTINGS index_granularity = 8192
AS SELECT
    tenant_id,
    cache_type,
    toDate(timestamp) as metric_date,
    sum(cache_hits) as total_hits,
    sum(cache_misses) as total_misses,
    sum(cache_sets) as total_sets,
    sum(cache_deletes) as total_deletes,
    avg(cache_hit_rate) as avg_hit_rate,
    max(cache_size_bytes) as max_cache_size,
    avg(cache_size_bytes) as avg_cache_size
FROM cache_metrics
GROUP BY tenant_id, cache_type, metric_date;

-- Materialized view for system metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_system_metrics
ENGINE = SummingMergeTree()
ORDER BY (service_name, metric_date)
PARTITION BY toYearMonth(metric_date)
SETTINGS index_granularity = 8192
AS SELECT
    service_name,
    toDate(timestamp) as metric_date,
    count() as total_metrics,
    avg(cpu_usage_percent) as avg_cpu_usage,
    avg(memory_usage_percent) as avg_memory_usage,
    avg(disk_usage_percent) as avg_disk_usage,
    avg(network_usage_bytes) as avg_network_usage,
    max(cpu_usage_percent) as max_cpu_usage,
    max(memory_usage_percent) as max_memory_usage,
    max(disk_usage_percent) as max_disk_usage,
    max(network_usage_bytes) as max_network_usage
FROM system_metrics
GROUP BY service_name, metric_date;
