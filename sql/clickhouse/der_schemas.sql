-- DER program and economics schemas

-- DER programs table
CREATE TABLE IF NOT EXISTS der_programs (
    program_id String,
    program_name String,
    utility_id String,
    ba_id LowCardinality(String),
    zone_id String,

    -- Program type and characteristics
    program_type LowCardinality(String),
    technology_focus LowCardinality(String),

    -- Technical potential
    technical_potential_mw Float64,
    enrolled_capacity_mw Float64,
    dispatchable_capacity_mw Float64,
    persistence_factor Float64,

    -- Dispatch characteristics
    response_time_minutes Float64,
    minimum_call_duration_minutes Float64,
    maximum_calls_per_day Float64,
    availability_hours_per_day Float64,

    -- Economics
    incentive_rate_usd_per_mw Float64,
    capacity_payment_usd_per_mw_month Float64,
    energy_payment_usd_per_mwh Float64,

    -- Cost-effectiveness tests
    trc_ratio Float64,
    pac_ratio Float64,
    ruc_ratio Float64,

    -- Customer economics
    customer_utility_split_percent Float64,
    net_present_value_usd Float64,
    payback_period_years Float64,

    -- Wholesale participation
    wholesale_eligible Bool,
    dual_use_accounting Bool,
    aggregation_threshold_mw Float64,
    minimum_bid_size_mw Float64,

    -- Participation constraints
    maximum_participation_rate Float64,
    notification_lead_time_minutes Float64,
    baseline_methodology String,

    -- Performance requirements
    performance_factor_target Float64,
    penalty_rate_usd_per_mw_shortfall Float64,

    -- Status and dates
    program_status LowCardinality(String),
    enrollment_start_date Date,
    enrollment_end_date Date,
    program_start_date Date,
    program_end_date Date,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_program program_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_utility utility_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_type program_type TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_status program_status TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (utility_id, program_type, program_id)
PARTITION BY utility_id
TTL created_at + INTERVAL 5 YEAR;

-- DER dispatch profiles table
CREATE TABLE IF NOT EXISTS der_dispatch_profiles (
    profile_id String,
    program_id String,
    hour_of_day UInt8,
    day_of_week UInt8,
    month UInt8,

    -- Dispatch characteristics
    available_capacity_mw Float64,
    response_rate_mw_per_minute Float64,
    sustained_capacity_mw Float64,

    -- Performance factors
    persistence_factor Float64,
    rebound_factor Float64,

    -- Seasonal/weather adjustments
    temperature_adjustment_factor Float64,
    seasonal_adjustment_factor Float64,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_profile profile_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_program program_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_hour hour_of_day TYPE minmax GRANULARITY 1,
    INDEX idx_month month TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (program_id, month, day_of_week, hour_of_day)
PARTITION BY program_id
TTL created_at + INTERVAL 2 YEAR;

-- DER economics table
CREATE TABLE IF NOT EXISTS der_economics (
    analysis_id String,
    program_id String,
    scenario_id String,

    -- Cost-effectiveness analysis
    total_resource_cost_usd Float64,
    participant_cost_usd Float64,
    utility_cost_usd Float64,
    societal_benefit_usd Float64,

    -- Test results
    trc_ratio Float64,
    pac_ratio Float64,
    ruc_ratio Float64,
    pct_ratio Float64,
    ratepayer_impact_usd_per_mwh Float64,

    -- Value streams
    capacity_value_usd_per_mw_year Float64,
    energy_value_usd_per_mwh Float64,
    ancillary_value_usd_per_mw_year Float64,
    carbon_value_usd_per_ton Float64,

    -- Net present value
    npv_usd Float64,
    irr_percent Float64,
    payback_period_years Float64,

    -- Risk assessment
    risk_adjusted_npv_usd Float64,
    value_at_risk_usd Float64,

    -- Sensitivity analysis (JSON)
    sensitivity_scenarios String,

    -- Metadata
    analysis_date Date,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_analysis analysis_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_program program_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scenario scenario_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_date analysis_date TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (program_id, scenario_id, analysis_date)
PARTITION BY program_id
TTL created_at + INTERVAL 3 YEAR;

-- Wholesale participation table
CREATE TABLE IF NOT EXISTS der_wholesale_participation (
    participation_id String,
    program_id String,
    market String,

    -- Participation eligibility
    eligible Bool,
    minimum_size_mw Float64,
    maximum_size_mw Float64,

    -- Market rules
    must_offer_requirement Bool,
    day_ahead_eligibility Bool,
    real_time_eligibility Bool,

    -- Performance requirements
    performance_threshold Float64,
    settlement_method LowCardinality(String),

    -- Constraints (JSON arrays)
    dual_use_restrictions String,
    baseline_requirements String,

    -- Metadata
    effective_date Date,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_participation participation_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_program program_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_market market TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_eligible eligible TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (market, program_id, participation_id)
PARTITION BY market
TTL created_at + INTERVAL 5 YEAR;

-- DER-IRP co-optimization results table
CREATE TABLE IF NOT EXISTS der_irp_co_optimization (
    case_id String,
    optimization_run_id String,

    -- DER selections (JSON)
    der_capacity_selected_mw String,
    der_energy_contribution_mwh String,

    -- Costs and impacts
    total_der_cost_usd Float64,
    transmission_impact_mw Float64,
    carbon_reduction_tons Float64,

    -- Optimization summary (JSON)
    optimization_summary String,

    -- Metadata
    created_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_case case_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_run optimization_run_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (case_id, optimization_run_id)
PARTITION BY case_id
TTL created_at + INTERVAL 2 YEAR;

-- Materialized views for common queries

-- Program summary by utility and type
CREATE MATERIALIZED VIEW IF NOT EXISTS der_programs_summary_by_utility_type
ENGINE = SummingMergeTree()
ORDER BY (utility_id, program_type, program_status)
AS SELECT
    utility_id,
    program_type,
    program_status,
    count() as program_count,
    sum(technical_potential_mw) as total_technical_potential_mw,
    sum(enrolled_capacity_mw) as total_enrolled_capacity_mw,
    sum(dispatchable_capacity_mw) as total_dispatchable_capacity_mw,
    avg(trc_ratio) as avg_trc_ratio,
    avg(pac_ratio) as avg_pac_ratio,
    avg(persistence_factor) as avg_persistence_factor,
    sum(wholesale_eligible) as wholesale_eligible_count
FROM der_programs
GROUP BY utility_id, program_type, program_status;

-- Economics summary by program
CREATE MATERIALIZED VIEW IF NOT EXISTS der_economics_summary_by_program
ENGINE = SummingMergeTree()
ORDER BY (program_id, scenario_id)
AS SELECT
    program_id,
    scenario_id,
    count() as analysis_count,
    avg(trc_ratio) as avg_trc_ratio,
    avg(pac_ratio) as avg_pac_ratio,
    avg(npv_usd) as avg_npv_usd,
    avg(irr_percent) as avg_irr_percent,
    avg(payback_period_years) as avg_payback_period_years,
    avg(risk_adjusted_npv_usd) as avg_risk_adjusted_npv,
    sum(total_resource_cost_usd) as total_resource_cost_usd,
    sum(societal_benefit_usd) as total_societal_benefit_usd
FROM der_economics
GROUP BY program_id, scenario_id;

-- Dispatch capacity by program and time
CREATE MATERIALIZED VIEW IF NOT EXISTS der_dispatch_capacity_by_program_time
ENGINE = SummingMergeTree()
ORDER BY (program_id, month, day_of_week, hour_of_day)
AS SELECT
    program_id,
    month,
    day_of_week,
    hour_of_day,
    count() as profile_count,
    avg(available_capacity_mw) as avg_available_capacity_mw,
    avg(response_rate_mw_per_minute) as avg_response_rate,
    avg(sustained_capacity_mw) as avg_sustained_capacity_mw,
    avg(persistence_factor) as avg_persistence_factor
FROM der_dispatch_profiles
GROUP BY program_id, month, day_of_week, hour_of_day;

-- Wholesale participation summary by market
CREATE MATERIALIZED VIEW IF NOT EXISTS der_wholesale_participation_summary
ENGINE = SummingMergeTree()
ORDER BY (market, program_type)
AS SELECT
    market,
    program_type,
    count() as participation_count,
    sum(eligible) as eligible_count,
    avg(minimum_size_mw) as avg_minimum_size_mw,
    avg(maximum_size_mw) as avg_maximum_size_mw,
    avg(performance_threshold) as avg_performance_threshold,
    sum(day_ahead_eligibility) as day_ahead_eligible_count,
    sum(real_time_eligibility) as real_time_eligible_count
FROM der_programs p
LEFT JOIN der_wholesale_participation wp ON p.program_id = wp.program_id
GROUP BY market, program_type;
