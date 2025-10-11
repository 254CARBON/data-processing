-- WECC IRP topology schemas for ClickHouse

-- =====================================================================
-- WECC IRP topology tables
-- =====================================================================

-- WECC Balancing Authorities for IRP
CREATE TABLE IF NOT EXISTS markets.wecc_topology_ba (
    ba_id String,
    ba_name String,
    ba_type LowCardinality(String),
    iso_rto Nullable(String),
    timezone String,
    dst_rules String,  -- JSON
    load_forecast_zone Nullable(String),
    tags String,  -- JSON array
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ba_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- WECC Load Zones for IRP
CREATE TABLE IF NOT EXISTS markets.wecc_topology_zones (
    zone_id String,
    zone_name String,
    ba_id String,
    zone_type LowCardinality(String),
    load_serving_entity Nullable(String),
    weather_zone Nullable(String),
    peak_demand_mw Nullable(Float64),
    planning_area Nullable(String),
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (zone_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- WECC Nodes for IRP
CREATE TABLE IF NOT EXISTS markets.wecc_topology_nodes (
    node_id String,
    node_name String,
    zone_id String,
    node_type LowCardinality(String),
    voltage_level_kv Nullable(Float64),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    capacity_mw Nullable(Float64),
    resource_potential String,  -- JSON
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (node_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- WECC Transmission Paths for IRP
CREATE TABLE IF NOT EXISTS markets.wecc_topology_paths (
    path_id String,
    path_name String,
    from_node String,
    to_node String,
    path_type LowCardinality(String),
    capacity_mw Float64,
    losses_factor Float64,
    hurdle_rate_usd_per_mwh Nullable(Float64),
    wheel_rate_usd_per_mwh Nullable(Float64),
    wecc_path_number Nullable(String),
    operating_limit_mw Nullable(Float64),
    emergency_limit_mw Nullable(Float64),
    contingency_limit_mw Nullable(Float64),
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (path_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- WECC Interties for IRP
CREATE TABLE IF NOT EXISTS markets.wecc_topology_interties (
    intertie_id String,
    intertie_name String,
    from_ba String,
    to_ba String,
    capacity_mw Float64,
    atc_ttc_ntc String,  -- JSON
    deliverability_flags String,  -- JSON array
    edam_eligible Bool,
    hurdle_rates String,  -- JSON
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (intertie_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- WECC Hubs for IRP
CREATE TABLE IF NOT EXISTS markets.wecc_topology_hubs (
    hub_id String,
    hub_name String,
    zone_id String,
    hub_type LowCardinality(String),
    lmp_weighted_nodes String,  -- JSON array
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (hub_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- =====================================================================
-- WECC Interfaces for IRP
-- =====================================================================

-- WECC Interface Definitions
CREATE TABLE IF NOT EXISTS markets.wecc_interfaces (
    interface_id String,
    interface_name String,
    interface_type LowCardinality(String),
    from_entity String,
    to_entity String,
    directionality LowCardinality(String),
    from_zone Nullable(String),
    to_zone Nullable(String),
    transfer_capabilities String,  -- JSON
    loss_factors String,  -- JSON
    hurdle_rates String,  -- JSON
    seasonal_variations String,  -- JSON array
    flowgate_ids String,  -- JSON array
    operational_constraints String,  -- JSON
    regulatory_constraints String,  -- JSON
    edam_participation String,  -- JSON
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (interface_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- WECC Path Definitions
CREATE TABLE IF NOT EXISTS markets.wecc_path_definitions (
    path_number String,
    path_name String,
    path_type Nullable(String),
    from_location String,
    to_location String,
    rated_mw Float64,
    operating_mw Float64,
    emergency_mw Float64,
    wecc_interfaces String,  -- JSON array
    monitored_elements String,  -- JSON array
    controlling_entities String,  -- JSON array
    notes Nullable(String),
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (path_number, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- Contract Path Definitions
CREATE TABLE IF NOT EXISTS markets.contract_path_definitions (
    contract_path_id String,
    contract_path_name String,
    source String,
    sink String,
    contract_capacity_mw Float64,
    transmission_owner String,
    contract_type LowCardinality(String),
    service_type LowCardinality(String),
    rate_schedule Nullable(String),
    wheeling_rate_usd_per_mwh Nullable(Float64),
    loss_factor Nullable(Float64),
    effective_date Nullable(Date),
    termination_date Nullable(Date),
    priority_rights String,  -- JSON array
    notes Nullable(String),
    metadata String,  -- JSON blob
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (contract_path_id, created_at)
TTL created_at + INTERVAL 10 YEAR;

-- =====================================================================
-- Materialized views for efficient querying
-- =====================================================================

-- WECC Nodes by zone
CREATE MATERIALIZED VIEW IF NOT EXISTS markets.wecc_nodes_by_zone
ENGINE = MergeTree()
ORDER BY (zone_id, node_id)
AS SELECT
    zone_id,
    node_id,
    node_name,
    node_type,
    voltage_level_kv,
    latitude,
    longitude,
    capacity_mw,
    resource_potential,
    created_at,
    updated_at
FROM markets.wecc_topology_nodes;

-- WECC Paths by zone
CREATE MATERIALIZED VIEW IF NOT EXISTS markets.wecc_paths_by_zone
ENGINE = MergeTree()
ORDER BY (zone_id, path_id)
AS SELECT
    p.path_id,
    p.path_name,
    p.from_node,
    p.to_node,
    p.path_type,
    p.capacity_mw,
    p.losses_factor,
    p.hurdle_rate_usd_per_mwh,
    p.wheel_rate_usd_per_mwh,
    p.wecc_path_number,
    p.created_at,
    p.updated_at,
    n1.zone_id as from_zone_id,
    n2.zone_id as to_zone_id
FROM markets.wecc_topology_paths p
LEFT JOIN markets.wecc_topology_nodes n1 ON p.from_node = n1.node_id
LEFT JOIN markets.wecc_topology_nodes n2 ON p.to_node = n2.node_id;

-- WECC Interties by BA
CREATE MATERIALIZED VIEW IF NOT EXISTS markets.wecc_interties_by_ba
ENGINE = MergeTree()
ORDER BY (from_ba, to_ba, intertie_id)
AS SELECT
    intertie_id,
    intertie_name,
    from_ba,
    to_ba,
    capacity_mw,
    atc_ttc_ntc,
    deliverability_flags,
    edam_eligible,
    created_at,
    updated_at
FROM markets.wecc_topology_interties;

-- =====================================================================
-- IRP Scenarios table
-- =====================================================================

-- IRP Scenarios for production-cost modeling
CREATE TABLE IF NOT EXISTS irp_scenarios (
    scenario_id String,
    scenario_name String,
    market LowCardinality(String),
    fuel_price_scenario LowCardinality(String),
    carbon_price_scenario LowCardinality(String),
    edam_rules String,  -- JSON
    hydro_scenario LowCardinality(String),
    load_scenario LowCardinality(String),
    der_scenario LowCardinality(String),
    transmission_scenario LowCardinality(String),
    created_by String,
    created_at DateTime,
    version LowCardinality(String),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (market, scenario_id, created_at)
TTL created_at + INTERVAL 10 YEAR;
