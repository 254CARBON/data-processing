-- Program metrics schemas for IRP / RA / RPS / GHG MVP

CREATE DATABASE IF NOT EXISTS markets;

-- =====================================================================
-- Core fact tables
-- =====================================================================

CREATE TABLE IF NOT EXISTS markets.load_demand (
    timestamp DateTime,
    market LowCardinality(String),
    ba LowCardinality(String),
    zone LowCardinality(String),
    demand_mw Float64,
    data_source LowCardinality(Nullable(String)),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (market, ba, zone, timestamp)
TTL toDate(timestamp) + INTERVAL 10 YEAR;

CREATE TABLE IF NOT EXISTS markets.generation_actual (
    timestamp DateTime,
    market LowCardinality(String),
    ba LowCardinality(String),
    zone LowCardinality(String),
    resource_id String,
    resource_name Nullable(String),
    resource_type LowCardinality(String),
    fuel LowCardinality(String),
    output_mw Float64,
    output_mwh Float64,
    data_source LowCardinality(Nullable(String)),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (market, resource_id, timestamp)
TTL toDate(timestamp) + INTERVAL 10 YEAR;

CREATE TABLE IF NOT EXISTS markets.generation_capacity (
    effective_date Date,
    market LowCardinality(String),
    ba LowCardinality(Nullable(String)),
    zone LowCardinality(Nullable(String)),
    resource_id String,
    resource_name Nullable(String),
    resource_type LowCardinality(String),
    fuel LowCardinality(Nullable(String)),
    nameplate_mw Float64,
    ucap_factor Float32,
    ucap_mw Float64,
    availability_factor Nullable(Float32),
    cost_curve Nullable(String),
    data_source LowCardinality(Nullable(String)),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (market, resource_id, effective_date)
TTL effective_date + INTERVAL 15 YEAR;

CREATE TABLE IF NOT EXISTS markets.rec_ledger (
    vintage_year UInt16,
    market LowCardinality(String),
    lse String,
    certificate_id String,
    resource_id Nullable(String),
    mwh Float64,
    status Enum('available', 'retired', 'banked'),
    retired_year Nullable(UInt16),
    data_source LowCardinality(Nullable(String)),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (market, lse, vintage_year, certificate_id)
TTL toDate(concat(toString(vintage_year), '-01-01')) + INTERVAL 15 YEAR;

CREATE TABLE IF NOT EXISTS markets.emission_factors (
    fuel LowCardinality(String),
    scope Enum('scope1', 'scope2'),
    kg_co2e_per_mwh Float64,
    source String,
    effective_date Date,
    expires_at Nullable(Date),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (fuel, scope, effective_date)
TTL ifNull(expires_at, effective_date + INTERVAL 25 YEAR);

-- =====================================================================
-- Helper views
-- =====================================================================

CREATE VIEW IF NOT EXISTS markets.current_generation_capacity AS
SELECT
    market,
    resource_id,
    argMax(resource_name, effective_date) AS resource_name,
    argMax(resource_type, effective_date) AS resource_type,
    argMax(fuel, effective_date) AS fuel,
    argMax(ba, effective_date) AS ba,
    argMax(zone, effective_date) AS zone,
    argMax(nameplate_mw, effective_date) AS nameplate_mw,
    argMax(ucap_factor, effective_date) AS ucap_factor,
    argMax(ucap_mw, effective_date) AS ucap_mw,
    argMax(availability_factor, effective_date) AS availability_factor,
    max(effective_date) AS latest_effective_date
FROM markets.generation_capacity
GROUP BY market, resource_id;

CREATE VIEW IF NOT EXISTS markets.ra_capacity_by_zone AS
SELECT
    market,
    coalesce(ba, '') AS ba,
    coalesce(zone, '') AS zone,
    sum(ucap_mw) AS total_ucap_mw
FROM markets.current_generation_capacity
GROUP BY market, ba, zone;

CREATE VIEW IF NOT EXISTS markets.ra_hourly AS
SELECT
    ld.timestamp,
    ld.market,
    ld.ba,
    ld.zone,
    ld.demand_mw,
    cap.total_ucap_mw,
    if(ld.demand_mw = 0, NULL, (cap.total_ucap_mw - ld.demand_mw) / ld.demand_mw * 100.0) AS prm_percent
FROM markets.load_demand ld
LEFT JOIN markets.ra_capacity_by_zone cap
    ON ld.market = cap.market
   AND coalesce(ld.ba, '') = cap.ba
   AND coalesce(ld.zone, '') = cap.zone;

CREATE VIEW IF NOT EXISTS markets.ghg_inventory_hourly AS
SELECT
    ga.timestamp,
    ga.market,
    ga.ba,
    ga.zone,
    ga.resource_id,
    ga.resource_type,
    ga.fuel,
    ga.output_mwh,
    ef.scope,
    ga.output_mwh * ef.kg_co2e_per_mwh AS emissions_kg_co2e
FROM markets.generation_actual ga
LEFT JOIN markets.emission_factors ef
    ON ga.fuel = ef.fuel
   AND ef.scope = 'scope1'
   AND ga.timestamp >= toDateTime(ef.effective_date)
   AND (ef.expires_at IS NULL OR ga.timestamp < toDateTime(ef.expires_at));

CREATE VIEW IF NOT EXISTS markets.rps_compliance_annual AS
SELECT
    market,
    toYear(timestamp) AS compliance_year,
    sumIf(output_mwh, resource_type IN ('solar', 'wind', 'hydro', 'geothermal', 'biomass')) AS renewable_mwh,
    sum(output_mwh) AS total_generation_mwh
FROM markets.generation_actual
GROUP BY market, compliance_year;

-- =====================================================================
-- IRP Inputs and Outputs tables
-- =====================================================================

-- IRP Load Inputs
CREATE TABLE IF NOT EXISTS markets.irp_inputs_load (
    case_id String,
    scenario_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(String),
    timestamp DateTime,
    load_mw Nullable(Float64),
    load_forecast_mw Nullable(Float64),
    net_load_mw Nullable(Float64),
    weather_scenario LowCardinality(Nullable(String)),
    forecast_method LowCardinality(Nullable(String)),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, zone_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP Hydro Cascades Inputs
CREATE TABLE IF NOT EXISTS markets.irp_inputs_hydro_cascades (
    case_id String,
    scenario_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    cascade_id String,
    cascade_name Nullable(String),
    reservoir_id String,
    reservoir_name Nullable(String),
    timestamp DateTime,
    inflow_cfs Nullable(Float64),
    storage_af Nullable(Float64),
    elevation_feet Nullable(Float64),
    release_cfs Nullable(Float64),
    generation_mw Nullable(Float64),
    head_feet Nullable(Float64),
    efficiency_percent Nullable(Float64),
    weather_scenario LowCardinality(Nullable(String)),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, cascade_id, reservoir_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP Renewables Profiles Inputs
CREATE TABLE IF NOT EXISTS markets.irp_inputs_renewables_profiles (
    case_id String,
    scenario_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(Nullable(String)),
    resource_id String,
    resource_name Nullable(String),
    resource_type LowCardinality(Nullable(String)),
    technology LowCardinality(Nullable(String)),
    timestamp DateTime,
    capacity_mw Nullable(Float64),
    generation_mw Nullable(Float64),
    curtailment_mw Nullable(Float64),
    availability_factor Nullable(Float64),
    weather_scenario LowCardinality(Nullable(String)),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, resource_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP DER Shapes Inputs
CREATE TABLE IF NOT EXISTS markets.irp_inputs_der_shapes (
    case_id String,
    scenario_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(Nullable(String)),
    der_program_id String,
    program_name Nullable(String),
    program_type LowCardinality(Nullable(String)),
    customer_sector LowCardinality(Nullable(String)),
    timestamp DateTime,
    load_reduction_mw Nullable(Float64),
    generation_mw Nullable(Float64),
    net_load_mw Nullable(Float64),
    participation_rate Nullable(Float64),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, der_program_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP Outages Inputs
CREATE TABLE IF NOT EXISTS markets.irp_inputs_outages (
    case_id String,
    scenario_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(Nullable(String)),
    resource_id String,
    resource_name Nullable(String),
    resource_type LowCardinality(Nullable(String)),
    fuel LowCardinality(Nullable(String)),
    start_timestamp DateTime,
    end_timestamp DateTime,
    outage_type LowCardinality(String),
    outage_cause LowCardinality(Nullable(String)),
    capacity_reduction_mw Nullable(Float64),
    derated_capacity_mw Nullable(Float64),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, resource_id, start_timestamp)
PARTITION BY case_id
TTL end_timestamp + INTERVAL 7 YEAR;

-- IRP Dispatch Outputs
CREATE TABLE IF NOT EXISTS markets.irp_outputs_dispatch (
    case_id String,
    run_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(Nullable(String)),
    resource_id String,
    resource_name Nullable(String),
    resource_type LowCardinality(Nullable(String)),
    fuel LowCardinality(Nullable(String)),
    timestamp DateTime,
    dispatch_mw Float64,
    commitment_status Bool,
    startup_cost_usd Nullable(Float64),
    fuel_cost_usd Nullable(Float64),
    variable_om_cost_usd Nullable(Float64),
    emissions_tons Nullable(Float64),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, resource_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP LMP and AS Outputs
CREATE TABLE IF NOT EXISTS markets.irp_outputs_prices (
    case_id String,
    run_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(Nullable(String)),
    node_id String,
    node_name Nullable(String),
    timestamp DateTime,
    lmp_usd_per_mwh Float64,
    energy_price_usd_per_mwh Nullable(Float64),
    congestion_price_usd_per_mwh Nullable(Float64),
    losses_price_usd_per_mwh Nullable(Float64),
    regulation_up_price_usd_per_mwh Nullable(Float64),
    regulation_down_price_usd_per_mwh Nullable(Float64),
    spinning_reserve_price_usd_per_mwh Nullable(Float64),
    non_spinning_reserve_price_usd_per_mwh Nullable(Float64),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, node_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP Emissions Outputs
CREATE TABLE IF NOT EXISTS markets.irp_outputs_emissions (
    case_id String,
    run_id Nullable(String),
    market LowCardinality(String),
    ba_id LowCardinality(Nullable(String)),
    zone_id LowCardinality(Nullable(String)),
    resource_id String,
    resource_type LowCardinality(Nullable(String)),
    fuel LowCardinality(Nullable(String)),
    timestamp DateTime,
    co2_tons Nullable(Float64),
    nox_lbs Nullable(Float64),
    so2_lbs Nullable(Float64),
    pm25_lbs Nullable(Float64),
    ch4_tons Nullable(Float64),
    n2o_tons Nullable(Float64),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, resource_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- IRP Flows Outputs
CREATE TABLE IF NOT EXISTS markets.irp_outputs_flows (
    case_id String,
    run_id Nullable(String),
    market LowCardinality(String),
    path_id String,
    path_name Nullable(String),
    from_zone LowCardinality(Nullable(String)),
    to_zone LowCardinality(Nullable(String)),
    timestamp DateTime,
    flow_mw Nullable(Float64),
    congestion_cost_usd Nullable(Float64),
    losses_mw Nullable(Float64),
    shadow_price_usd_per_mw Nullable(Float64),
    curtailment_mw Nullable(Float64),
    data_source LowCardinality(Nullable(String)),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (case_id, path_id, timestamp)
PARTITION BY case_id
TTL toDate(timestamp) + INTERVAL 7 YEAR;

-- =====================================================================
-- IRP Comparison Views
-- =====================================================================

CREATE OR REPLACE VIEW markets.irp_case_compare_load AS
SELECT
    case_id,
    any(market) AS market,
    any(ba_id) AS ba_id,
    zone_id,
    min(timestamp) AS first_timestamp,
    max(timestamp) AS last_timestamp,
    avg(load_mw) AS avg_load_mw,
    max(load_mw) AS peak_load_mw,
    min(load_mw) AS min_load_mw,
    sum(load_mw) AS total_load_mwh,
    sum(load_forecast_mw) AS total_forecast_mwh,
    sum(net_load_mw) AS total_net_load_mwh,
    count() AS hours_count
FROM markets.irp_inputs_load
GROUP BY case_id, zone_id;

CREATE OR REPLACE VIEW markets.irp_case_compare_dispatch AS
SELECT
    case_id,
    resource_id,
    any(market) AS market,
    any(resource_type) AS resource_type,
    any(fuel) AS fuel,
    any(ba_id) AS ba_id,
    any(zone_id) AS zone_id,
    avg(dispatch_mw) AS avg_dispatch_mw,
    max(dispatch_mw) AS max_dispatch_mw,
    sum(fuel_cost_usd) AS total_fuel_cost_usd,
    sum(startup_cost_usd) AS total_startup_cost_usd,
    sum(variable_om_cost_usd) AS total_variable_om_cost_usd,
    sum(emissions_tons) AS total_emissions_tons,
    sum(dispatch_mw) AS total_dispatch_mwh,
    count() AS hours_count
FROM markets.irp_outputs_dispatch
GROUP BY case_id, resource_id;

CREATE OR REPLACE VIEW markets.irp_case_compare_prices AS
SELECT
    case_id,
    node_id,
    any(market) AS market,
    any(ba_id) AS ba_id,
    any(zone_id) AS zone_id,
    avg(lmp_usd_per_mwh) AS avg_lmp_usd_per_mwh,
    max(lmp_usd_per_mwh) AS max_lmp_usd_per_mwh,
    min(lmp_usd_per_mwh) AS min_lmp_usd_per_mwh,
    stddevPop(lmp_usd_per_mwh) AS lmp_stddev_usd_per_mwh,
    avg(energy_price_usd_per_mwh) AS avg_energy_price_usd_per_mwh,
    avg(congestion_price_usd_per_mwh) AS avg_congestion_price_usd_per_mwh,
    avg(losses_price_usd_per_mwh) AS avg_losses_price_usd_per_mwh,
    count() AS hours_count
FROM markets.irp_outputs_prices
GROUP BY case_id, node_id;

CREATE OR REPLACE VIEW markets.irp_case_compare_emissions AS
SELECT
    case_id,
    any(market) AS market,
    sum(co2_tons) AS total_co2_tons,
    avg(co2_tons) AS avg_co2_tons_per_interval,
    sum(nox_lbs) AS total_nox_lbs,
    sum(so2_lbs) AS total_so2_lbs,
    sum(pm25_lbs) AS total_pm25_lbs,
    sum(ch4_tons) AS total_ch4_tons,
    sum(n2o_tons) AS total_n2o_tons,
    count() AS hours_count
FROM markets.irp_outputs_emissions
GROUP BY case_id;

CREATE OR REPLACE VIEW markets.irp_case_compare_flows AS
SELECT
    case_id,
    path_id,
    any(market) AS market,
    any(from_zone) AS from_zone,
    any(to_zone) AS to_zone,
    avg(flow_mw) AS avg_flow_mw,
    sum(congestion_cost_usd) AS total_congestion_cost_usd,
    sum(losses_mw) AS total_losses_mw,
    avg(shadow_price_usd_per_mw) AS avg_shadow_price_usd_per_mw,
    sum(curtailment_mw) AS total_curtailment_mw,
    max(flow_mw) AS max_flow_mw,
    min(flow_mw) AS min_flow_mw,
    count() AS hours_count
FROM markets.irp_outputs_flows
GROUP BY case_id, path_id;
