-- Migration 015: Asset registry schemas for unified resource catalog

-- Thermal resources table
CREATE TABLE IF NOT EXISTS assets.resource_registry_thermal (
    resource_id String,
    plant_code String,
    unit_code String,
    resource_name String,
    ba_id LowCardinality(String),
    zone_id String,
    node_id String,
    technology_type LowCardinality(String),
    fuel_type LowCardinality(String),

    -- Capacity (MW)
    nameplate_capacity_mw Float64,
    summer_capacity_mw Float64,
    winter_capacity_mw Float64,

    -- Operating parameters
    heat_rate_mmbtu_per_mwh Float64,
    variable_cost_usd_per_mwh Float64,
    startup_cost_usd Float64,
    min_up_time_hours Float64,
    min_down_time_hours Float64,
    ramp_rate_mw_per_min Float64,

    -- Reliability
    eford_percent Float64,

    -- Emissions
    co2_tons_per_mwh Float64,
    nox_lbs_per_mwh Float64,
    so2_lbs_per_mwh Float64,

    -- Status and dates
    interconnection_status LowCardinality(String),
    commercial_operation_date Date,
    retirement_date Date,

    -- ELCC
    elcc_class String,
    elcc_factor Float64,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_plant plant_code TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_zone zone_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, zone_id, technology_type, resource_id)
PARTITION BY ba_id
TTL created_at + INTERVAL 5 YEAR;

-- Renewable resources table
CREATE TABLE IF NOT EXISTS assets.resource_registry_renewable (
    resource_id String,
    resource_name String,
    ba_id LowCardinality(String),
    zone_id String,
    node_id String,
    technology_type LowCardinality(String),

    -- Capacity (MW)
    nameplate_capacity_mw Float64,

    -- Performance characteristics
    capacity_factor_percent Float64,
    intermittency_factor Float64,
    curtailment_risk_percent Float64,

    -- Status and dates
    interconnection_status LowCardinality(String),
    commercial_operation_date Date,
    ppa_expiry_date Date,

    -- ELCC
    elcc_class String,
    elcc_factor Float64,

    -- Risk assessment
    congestion_risk_category LowCardinality(String),

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_zone zone_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_tech technology_type TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, zone_id, technology_type, resource_id)
PARTITION BY ba_id
TTL created_at + INTERVAL 5 YEAR;

-- Storage resources table
CREATE TABLE IF NOT EXISTS assets.resource_registry_storage (
    resource_id String,
    resource_name String,
    ba_id LowCardinality(String),
    zone_id String,
    node_id String,
    technology_type LowCardinality(String),

    -- Capacity (MW and MWh)
    nameplate_capacity_mw Float64,
    energy_capacity_mwh Float64,

    -- Operating parameters
    round_trip_efficiency_percent Float64,
    max_charge_rate_mw Float64,
    max_discharge_rate_mw Float64,
    min_state_of_charge_percent Float64,
    max_state_of_charge_percent Float64,
    cycle_limit_per_day Float64,
    degradation_rate_percent_per_cycle Float64,

    -- Status and dates
    interconnection_status LowCardinality(String),
    commercial_operation_date Date,

    -- ELCC
    elcc_class String,
    elcc_factor Float64,

    -- Market participation
    ecrs_eligible Bool,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_zone zone_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_tech technology_type TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, zone_id, technology_type, resource_id)
PARTITION BY ba_id
TTL created_at + INTERVAL 5 YEAR;

-- DER programs table
CREATE TABLE IF NOT EXISTS assets.resource_registry_der_programs (
    program_id String,
    program_name String,
    ba_id LowCardinality(String),
    zone_id String,
    program_type LowCardinality(String),

    -- Capacity (MW)
    potential_capacity_mw Float64,
    enrolled_capacity_mw Float64,
    dispatchable_mw Float64,

    -- Performance characteristics
    persistence_factor Float64,
    response_time_minutes Float64,
    availability_hours_per_day Float64,

    -- Economics
    cost_usd_per_mw Float64,
    trc_ratio Float64,
    pac_ratio Float64,
    customer_utility_split_percent Float64,

    -- Market participation
    wholesale_participation_eligible Bool,
    dual_use_accounting Bool,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_program program_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_zone zone_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_type program_type TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, zone_id, program_type, program_id)
PARTITION BY ba_id
TTL created_at + INTERVAL 3 YEAR;

-- Hydro resources table
CREATE TABLE IF NOT EXISTS assets.resource_registry_hydro (
    resource_id String,
    resource_name String,
    ba_id LowCardinality(String),
    zone_id String,
    node_id String,
    cascade_id String,
    reservoir_name String,
    river_system String,

    -- Capacity (MW and AF)
    nameplate_capacity_mw Float64,
    max_storage_volume_af Float64,
    min_storage_volume_af Float64,

    -- Operating parameters
    head_feet Float64,
    efficiency_percent Float64,
    energy_limited Bool,

    -- Seasonal rule curve (AF by month)
    jan_af Float64,
    feb_af Float64,
    mar_af Float64,
    apr_af Float64,
    may_af Float64,
    jun_af Float64,
    jul_af Float64,
    aug_af Float64,
    sep_af Float64,
    oct_af Float64,
    nov_af Float64,
    dec_af Float64,

    -- Water rights
    water_rights_priority String,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_cascade cascade_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_zone zone_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, cascade_id, river_system, resource_id)
PARTITION BY ba_id
TTL created_at + INTERVAL 5 YEAR;

-- Materialized views for common queries

-- Resource summary by BA and type
CREATE MATERIALIZED VIEW IF NOT EXISTS assets.resource_summary_by_ba_type
ENGINE = SummingMergeTree()
ORDER BY (ba_id, resource_type, technology_type)
AS SELECT
    ba_id,
    'thermal' as resource_type,
    technology_type,
    count() as resource_count,
    sum(nameplate_capacity_mw) as total_capacity_mw,
    avg(heat_rate_mmbtu_per_mwh) as avg_heat_rate,
    avg(eford_percent) as avg_eford,
    sum(co2_tons_per_mwh * nameplate_capacity_mw) as total_co2_emissions_factor
FROM assets.resource_registry_thermal
GROUP BY ba_id, technology_type

UNION ALL

SELECT
    ba_id,
    'renewable' as resource_type,
    technology_type,
    count() as resource_count,
    sum(nameplate_capacity_mw) as total_capacity_mw,
    avg(capacity_factor_percent) as avg_capacity_factor,
    avg(elcc_factor) as avg_elcc_factor,
    0 as total_co2_emissions_factor
FROM assets.resource_registry_renewable
GROUP BY ba_id, technology_type

UNION ALL

SELECT
    ba_id,
    'storage' as resource_type,
    technology_type,
    count() as resource_count,
    sum(nameplate_capacity_mw) as total_capacity_mw,
    avg(round_trip_efficiency_percent) as avg_efficiency,
    avg(elcc_factor) as avg_elcc_factor,
    0 as total_co2_emissions_factor
FROM assets.resource_registry_storage
GROUP BY ba_id, technology_type

UNION ALL

SELECT
    ba_id,
    'der' as resource_type,
    program_type as technology_type,
    count() as resource_count,
    sum(potential_capacity_mw) as total_capacity_mw,
    avg(persistence_factor) as avg_persistence,
    avg(trc_ratio) as avg_trc_ratio,
    0 as total_co2_emissions_factor
FROM assets.resource_registry_der_programs
GROUP BY ba_id, program_type;

-- Capacity by zone and technology
CREATE MATERIALIZED VIEW IF NOT EXISTS assets.capacity_by_zone_tech
ENGINE = SummingMergeTree()
ORDER BY (zone_id, technology_type, resource_type)
AS SELECT
    zone_id,
    technology_type,
    'thermal' as resource_type,
    sum(nameplate_capacity_mw) as total_capacity_mw,
    count() as resource_count
FROM assets.resource_registry_thermal
GROUP BY zone_id, technology_type

UNION ALL

SELECT
    zone_id,
    technology_type,
    'renewable' as resource_type,
    sum(nameplate_capacity_mw) as total_capacity_mw,
    count() as resource_count
FROM assets.resource_registry_renewable
GROUP BY zone_id, technology_type

UNION ALL

SELECT
    zone_id,
    technology_type,
    'storage' as resource_type,
    sum(nameplate_capacity_mw) as total_capacity_mw,
    count() as resource_count
FROM assets.resource_registry_storage
GROUP BY zone_id, technology_type

UNION ALL

SELECT
    zone_id,
    program_type as technology_type,
    'der' as resource_type,
    sum(potential_capacity_mw) as total_capacity_mw,
    count() as resource_count
FROM assets.resource_registry_der_programs
GROUP BY zone_id, program_type;
