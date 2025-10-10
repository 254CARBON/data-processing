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
