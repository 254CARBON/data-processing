-- Migration 018: GHG emissions analytics schemas

-- Emissions factors table
CREATE TABLE IF NOT EXISTS ghg_emissions_factors (
    factor_id String,
    resource_id String,
    node_id String,
    zone_id String,
    ba_id LowCardinality(String),

    -- Emissions factors (tons/MWh)
    co2_tons_per_mwh Float64,
    ch4_tons_per_mwh Float64,
    n2o_tons_per_mwh Float64,
    co2e_tons_per_mwh Float64,

    -- Methodology
    calculation_method LowCardinality(String),
    data_source String,
    confidence_level Float64,

    -- Temporal coverage
    start_date Date,
    end_date Date,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_factor factor_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_node node_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_date start_date TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, node_id, resource_id, start_date)
PARTITION BY ba_id
TTL start_date + INTERVAL 5 YEAR;

-- Marginal emissions table
CREATE TABLE IF NOT EXISTS ghg_marginal_emissions (
    analysis_id String,
    node_id String,
    zone_id String,
    ba_id LowCardinality(String),

    -- Marginal emissions (tons/MWh)
    marginal_co2_tons_per_mwh Float64,
    marginal_co2e_tons_per_mwh Float64,

    -- Analysis parameters
    time_period_start DateTime,
    time_period_end DateTime,
    load_level_mw Float64,
    analysis_method LowCardinality(String),

    -- Confidence metrics
    confidence_interval_lower Float64,
    confidence_interval_upper Float64,
    standard_error Float64,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_analysis analysis_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_node node_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_method analysis_method TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_period time_period_start TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, node_id, time_period_start)
PARTITION BY ba_id
TTL time_period_start + INTERVAL 2 YEAR;

-- Carbon programs table
CREATE TABLE IF NOT EXISTS ghg_carbon_programs (
    program_id String,
    program_name String,
    jurisdiction LowCardinality(String),
    program_type LowCardinality(String),

    -- Program parameters
    allowance_price_usd_per_ton Float64,
    allowance_cap_tons Float64,
    baseline_year UInt16,
    benchmark_factor Float64,

    -- Program rules
    banking_allowed Bool,
    borrowing_allowed Bool,
    offset_usage_percent Float64,

    -- Compliance periods
    compliance_period_start Date,
    compliance_period_end Date,

    -- Status
    active Bool,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_program program_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_jurisdiction jurisdiction TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_type program_type TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_active active TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (jurisdiction, program_type, program_id)
PARTITION BY jurisdiction
TTL created_at + INTERVAL 10 YEAR;

-- Abatement curves table
CREATE TABLE IF NOT EXISTS ghg_abatement_curves (
    curve_id String,
    jurisdiction LowCardinality(String),
    sector String,

    -- Curve data points (JSON)
    curve_points String,

    -- Curve characteristics
    total_abatement_potential_tons Float64,
    marginal_abatement_cost_usd_per_ton Float64,
    average_abatement_cost_usd_per_ton Float64,

    -- Methodology
    analysis_method String,
    data_sources String,  -- JSON array

    -- Temporal validity
    valid_from Date,
    valid_to Date,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_curve curve_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_jurisdiction jurisdiction TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_sector sector TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_validity valid_from TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (jurisdiction, sector, curve_id)
PARTITION BY jurisdiction
TTL created_at + INTERVAL 5 YEAR;

-- Import attribution table
CREATE TABLE IF NOT EXISTS ghg_import_attribution (
    attribution_id String,
    import_node_id String,
    export_jurisdiction LowCardinality(String),

    -- Import details
    import_volume_mwh Float64,
    import_period_start DateTime,
    import_period_end DateTime,

    -- Attribution methodology
    attribution_method LowCardinality(String),

    -- Emissions attribution
    attributed_emissions_tons Float64,
    attribution_factor Float64,

    -- Supporting data (JSON)
    supporting_contracts String,
    dispatch_data_source String,

    -- Compliance tracking
    compliance_program String,
    compliance_year UInt16,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_attribution attribution_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_import_node import_node_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_export export_jurisdiction TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_method attribution_method TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_period import_period_start TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (export_jurisdiction, import_node_id, import_period_start)
PARTITION BY export_jurisdiction
TTL import_period_start + INTERVAL 3 YEAR;

-- Materialized views for common queries

-- Average emissions by node and time period
CREATE MATERIALIZED VIEW IF NOT EXISTS ghg_avg_emissions_by_node_period
ENGINE = SummingMergeTree()
ORDER BY (node_id, ba_id, period_year, period_month)
AS SELECT
    node_id,
    ba_id,
    toYear(time_period_start) as period_year,
    toMonth(time_period_start) as period_month,
    count() as analysis_count,
    avg(marginal_co2_tons_per_mwh) as avg_marginal_co2,
    avg(marginal_co2e_tons_per_mwh) as avg_marginal_co2e,
    avg(confidence_interval_lower) as avg_confidence_lower,
    avg(confidence_interval_upper) as avg_confidence_upper,
    avg(standard_error) as avg_standard_error
FROM ghg_marginal_emissions
GROUP BY node_id, ba_id, period_year, period_month;

-- Emissions factors by resource type and time
CREATE MATERIALIZED VIEW IF NOT EXISTS ghg_emissions_factors_by_resource_time
ENGINE = SummingMergeTree()
ORDER BY (resource_id, ba_id, factor_year)
AS SELECT
    resource_id,
    ba_id,
    toYear(start_date) as factor_year,
    count() as factor_count,
    avg(co2_tons_per_mwh) as avg_co2_factor,
    avg(ch4_tons_per_mwh) as avg_ch4_factor,
    avg(n2o_tons_per_mwh) as avg_n2o_factor,
    avg(co2e_tons_per_mwh) as avg_co2e_factor,
    avg(confidence_level) as avg_confidence,
    groupArray(calculation_method) as calculation_methods
FROM ghg_emissions_factors
GROUP BY resource_id, ba_id, factor_year;

-- Carbon program summary by jurisdiction
CREATE MATERIALIZED VIEW IF NOT EXISTS ghg_carbon_programs_summary
ENGINE = SummingMergeTree()
ORDER BY (jurisdiction, program_type, compliance_year)
AS SELECT
    jurisdiction,
    program_type,
    toYear(compliance_period_start) as compliance_year,
    count() as program_count,
    avg(allowance_price_usd_per_ton) as avg_allowance_price,
    sum(allowance_cap_tons) as total_allowance_cap,
    avg(benchmark_factor) as avg_benchmark_factor,
    sum(banking_allowed) as programs_with_banking,
    sum(borrowing_allowed) as programs_with_borrowing,
    avg(offset_usage_percent) as avg_offset_usage
FROM ghg_carbon_programs
WHERE active = true
GROUP BY jurisdiction, program_type, compliance_year;

-- Abatement potential by jurisdiction and sector
CREATE MATERIALIZED VIEW IF NOT EXISTS ghg_abatement_potential_summary
ENGINE = SummingMergeTree()
ORDER BY (jurisdiction, sector, analysis_method)
AS SELECT
    jurisdiction,
    sector,
    analysis_method,
    count() as curve_count,
    sum(total_abatement_potential_tons) as total_abatement_potential,
    avg(marginal_abatement_cost_usd_per_ton) as avg_marginal_cost,
    avg(average_abatement_cost_usd_per_ton) as avg_average_cost,
    min(valid_from) as earliest_valid_from,
    max(valid_to) as latest_valid_to
FROM ghg_abatement_curves
GROUP BY jurisdiction, sector, analysis_method;

-- Import attribution summary by jurisdiction
CREATE MATERIALIZED VIEW IF NOT EXISTS ghg_import_attribution_summary
ENGINE = SummingMergeTree()
ORDER BY (export_jurisdiction, import_node_id, attribution_year)
AS SELECT
    export_jurisdiction,
    import_node_id,
    toYear(import_period_start) as attribution_year,
    count() as attribution_count,
    sum(import_volume_mwh) as total_import_volume_mwh,
    sum(attributed_emissions_tons) as total_attributed_emissions,
    avg(attribution_factor) as avg_attribution_factor,
    groupArray(attribution_method) as attribution_methods
FROM ghg_import_attribution
GROUP BY export_jurisdiction, import_node_id, attribution_year;
