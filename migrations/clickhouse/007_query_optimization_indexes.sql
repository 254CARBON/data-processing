-- Migration: Add query optimization indexes
-- Description: Adds indexes based on query patterns and performance analysis

-- Indexes for market_ticks table
-- Optimize queries filtering by tenant_id and timestamp
ALTER TABLE market_ticks ADD INDEX idx_tenant_timestamp (tenant_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by instrument_id and timestamp
ALTER TABLE market_ticks ADD INDEX idx_instrument_timestamp (instrument_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, instrument_id, and timestamp
ALTER TABLE market_ticks ADD INDEX idx_tenant_instrument_timestamp (tenant_id, instrument_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by quality_flag
ALTER TABLE market_ticks ADD INDEX idx_quality_flag (quality_flag) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by market
ALTER TABLE market_ticks ADD INDEX idx_market (market) TYPE set(0) GRANULARITY 1;

-- Indexes for enriched_ticks table
-- Optimize queries filtering by tenant_id and timestamp
ALTER TABLE enriched_ticks ADD INDEX idx_tenant_timestamp (tenant_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by instrument_id and timestamp
ALTER TABLE enriched_ticks ADD INDEX idx_instrument_timestamp (instrument_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by commodity_tier
ALTER TABLE enriched_ticks ADD INDEX idx_commodity_tier (commodity_tier) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by region_tier
ALTER TABLE enriched_ticks ADD INDEX idx_region_tier (region_tier) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by product_tier
ALTER TABLE enriched_ticks ADD INDEX idx_product_tier (product_tier) TYPE set(0) GRANULARITY 1;

-- Indexes for ohlc_bars table
-- Optimize queries filtering by tenant_id and timestamp
ALTER TABLE ohlc_bars ADD INDEX idx_tenant_timestamp (tenant_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by instrument_id and timestamp
ALTER TABLE ohlc_bars ADD INDEX idx_instrument_timestamp (instrument_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by bar_type
ALTER TABLE ohlc_bars ADD INDEX idx_bar_type (bar_type) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by bar_size
ALTER TABLE ohlc_bars ADD INDEX idx_bar_size (bar_size) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by tenant_id, instrument_id, and bar_type
ALTER TABLE ohlc_bars ADD INDEX idx_tenant_instrument_bar_type (tenant_id, instrument_id, bar_type) TYPE minmax GRANULARITY 1;

-- Indexes for curve_points table
-- Optimize queries filtering by tenant_id and timestamp
ALTER TABLE curve_points ADD INDEX idx_tenant_timestamp (tenant_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by curve_id and timestamp
ALTER TABLE curve_points ADD INDEX idx_curve_timestamp (curve_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by curve_type
ALTER TABLE curve_points ADD INDEX idx_curve_type (curve_type) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by tenor
ALTER TABLE curve_points ADD INDEX idx_tenor (tenor) TYPE set(0) GRANULARITY 1;

-- Indexes for audit_events table
-- Optimize queries filtering by tenant_id and timestamp
ALTER TABLE audit_events ADD INDEX idx_tenant_timestamp (tenant_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by event_type
ALTER TABLE audit_events ADD INDEX idx_event_type (event_type) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by actor_type
ALTER TABLE audit_events ADD INDEX idx_actor_type (actor_type) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by service_name
ALTER TABLE audit_events ADD INDEX idx_service_name (service_name) TYPE set(0) GRANULARITY 1;

-- Composite indexes for common query patterns
-- Optimize queries filtering by tenant_id, instrument_id, and timestamp range
ALTER TABLE market_ticks ADD INDEX idx_tenant_instrument_time_range (tenant_id, instrument_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, market, and timestamp
ALTER TABLE market_ticks ADD INDEX idx_tenant_market_time (tenant_id, market, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, commodity_tier, and timestamp
ALTER TABLE enriched_ticks ADD INDEX idx_tenant_commodity_time (tenant_id, commodity_tier, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, region_tier, and timestamp
ALTER TABLE enriched_ticks ADD INDEX idx_tenant_region_time (tenant_id, region_tier, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, bar_type, and timestamp
ALTER TABLE ohlc_bars ADD INDEX idx_tenant_bar_type_time (tenant_id, bar_type, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, curve_type, and timestamp
ALTER TABLE curve_points ADD INDEX idx_tenant_curve_type_time (tenant_id, curve_type, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id, event_type, and timestamp
ALTER TABLE audit_events ADD INDEX idx_tenant_event_type_time (tenant_id, event_type, timestamp) TYPE minmax GRANULARITY 1;

-- Indexes for aggregation queries
-- Optimize GROUP BY queries on tenant_id and timestamp
ALTER TABLE market_ticks ADD INDEX idx_group_tenant_time (tenant_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize GROUP BY queries on instrument_id and timestamp
ALTER TABLE market_ticks ADD INDEX idx_group_instrument_time (instrument_id, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize GROUP BY queries on market and timestamp
ALTER TABLE market_ticks ADD INDEX idx_group_market_time (market, timestamp) TYPE minmax GRANULARITY 1;

-- Indexes for ORDER BY queries
-- Optimize ORDER BY timestamp queries
ALTER TABLE market_ticks ADD INDEX idx_order_timestamp (timestamp) TYPE minmax GRANULARITY 1;

-- Optimize ORDER BY price queries
ALTER TABLE market_ticks ADD INDEX idx_order_price (price) TYPE minmax GRANULARITY 1;

-- Optimize ORDER BY volume queries
ALTER TABLE market_ticks ADD INDEX idx_order_volume (volume) TYPE minmax GRANULARITY 1;

-- Indexes for JOIN operations
-- Optimize JOINs on instrument_id
ALTER TABLE market_ticks ADD INDEX idx_join_instrument (instrument_id) TYPE minmax GRANULARITY 1;

-- Optimize JOINs on tenant_id
ALTER TABLE market_ticks ADD INDEX idx_join_tenant (tenant_id) TYPE minmax GRANULARITY 1;

-- Indexes for subquery optimization
-- Optimize subqueries filtering by timestamp
ALTER TABLE market_ticks ADD INDEX idx_subquery_timestamp (timestamp) TYPE minmax GRANULARITY 1;

-- Optimize subqueries filtering by instrument_id
ALTER TABLE market_ticks ADD INDEX idx_subquery_instrument (instrument_id) TYPE minmax GRANULARITY 1;

-- Indexes for data quality queries
-- Optimize queries filtering by quality_flag and timestamp
ALTER TABLE market_ticks ADD INDEX idx_quality_time (quality_flag, timestamp) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by missing fields
ALTER TABLE market_ticks ADD INDEX idx_missing_fields (is_price_null, is_volume_null, is_timestamp_null) TYPE minmax GRANULARITY 1;

-- Indexes for performance monitoring
-- Optimize queries filtering by processing_time
ALTER TABLE market_ticks ADD INDEX idx_processing_time (processing_time_ms) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by batch_id
ALTER TABLE market_ticks ADD INDEX idx_batch_id (batch_id) TYPE minmax GRANULARITY 1;

-- Indexes for tenant isolation
-- Optimize queries filtering by tenant_id (primary isolation)
ALTER TABLE market_ticks ADD INDEX idx_tenant_isolation (tenant_id) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by tenant_id and data_source
ALTER TABLE market_ticks ADD INDEX idx_tenant_data_source (tenant_id, data_source) TYPE minmax GRANULARITY 1;

-- Indexes for time-based partitioning
-- Optimize queries filtering by date (for daily partitions)
ALTER TABLE market_ticks ADD INDEX idx_date_partition (toDate(timestamp)) TYPE minmax GRANULARITY 1;

-- Optimize queries filtering by hour (for hourly partitions)
ALTER TABLE market_ticks ADD INDEX idx_hour_partition (toHour(timestamp)) TYPE minmax GRANULARITY 1;

-- Indexes for data retention
-- Optimize queries filtering by data_age
ALTER TABLE market_ticks ADD INDEX idx_data_age (timestamp) TYPE minmax GRANULARITY 1;

-- Indexes for backup and recovery
-- Optimize queries filtering by backup_status
ALTER TABLE market_ticks ADD INDEX idx_backup_status (backup_status) TYPE set(0) GRANULARITY 1;

-- Indexes for compliance and auditing
-- Optimize queries filtering by compliance_status
ALTER TABLE market_ticks ADD INDEX idx_compliance_status (compliance_status) TYPE set(0) GRANULARITY 1;

-- Optimize queries filtering by audit_required
ALTER TABLE market_ticks ADD INDEX idx_audit_required (audit_required) TYPE set(0) GRANULARITY 1;
