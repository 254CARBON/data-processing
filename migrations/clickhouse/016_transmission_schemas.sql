-- Migration 016: Transmission and EDAM schemas for WECC paths, losses, wheeling, and EDAM flags

-- WECC transmission paths table
CREATE TABLE IF NOT EXISTS markets.wecc_transmission_paths (
    path_id String,
    path_name String,
    wecc_path_number String,
    source_ba LowCardinality(String),
    sink_ba LowCardinality(String),
    source_zone String,
    sink_zone String,

    -- Transfer capabilities (MW)
    ttc_mw Float64,
    ntc_mw Float64,
    atc_mw Float64,

    -- Loss factors
    loss_factor_percent Float64,
    loss_factor_direction LowCardinality(String),

    -- Wheeling rates
    wheeling_rate_usd_per_mwh Float64,

    -- EDAM participation
    edam_eligible Bool DEFAULT false,
    edam_participant Bool DEFAULT false,
    edam_hurdle_rate_usd_per_mwh Float64,
    edam_transfer_limit_mw UInt32,

    -- Scheduling priority
    scheduling_priority UInt32,

    -- Status
    operational_status LowCardinality(String),
    outage_start DateTime,
    outage_end DateTime,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_path path_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_source_ba source_ba TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_sink_ba sink_ba TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_wecc_path wecc_path_number TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (source_ba, sink_ba, path_id)
PARTITION BY source_ba
TTL created_at + INTERVAL 3 YEAR;

-- Interties table
CREATE TABLE IF NOT EXISTS markets.wecc_interties (
    intertie_id String,
    intertie_name String,
    source_ba LowCardinality(String),
    sink_ba LowCardinality(String),

    -- Transfer capabilities (MW)
    ttc_mw Float64,
    ntc_mw Float64,
    atc_mw Float64,

    -- Loss factors
    loss_factor_percent Float64,

    -- Wheeling and scheduling
    wheeling_rate_usd_per_mwh Float64,
    scheduling_priority UInt32,

    -- EDAM participation
    edam_eligible Bool DEFAULT false,
    edam_participant Bool DEFAULT false,
    edam_hurdle_rate_usd_per_mwh Float64,

    -- Status
    operational_status LowCardinality(String),

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_intertie intertie_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_source_ba source_ba TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_sink_ba sink_ba TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (source_ba, sink_ba, intertie_id)
PARTITION BY source_ba
TTL created_at + INTERVAL 3 YEAR;

-- Wheeling contracts table
CREATE TABLE IF NOT EXISTS markets.wecc_wheeling_contracts (
    contract_id String,
    contract_name String,
    source_ba LowCardinality(String),
    sink_ba LowCardinality(String),
    path_id String,

    -- Contract terms
    capacity_mw Float64,
    rate_usd_per_mwh Float64,
    term_start Date,
    term_end Date,

    -- Scheduling
    priority_level UInt32,
    firm_contract Bool,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_contract contract_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_source_ba source_ba TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_sink_ba sink_ba TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_path path_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (source_ba, sink_ba, contract_id)
PARTITION BY source_ba
TTL created_at + INTERVAL 5 YEAR;

-- EDAM configurations table
CREATE TABLE IF NOT EXISTS markets.wecc_edam_configurations (
    ba_id LowCardinality(String),
    zone_id String,
    node_id String,

    -- EDAM participation
    edam_eligible Bool DEFAULT false,
    edam_participant Bool DEFAULT false,
    edam_region String,

    -- EDAM parameters
    edam_hurdle_rate_usd_per_mwh Float64,
    edam_transfer_limit_mw UInt32,
    edam_greenhouse_gas_rate_usd_per_ton Float64,

    -- EDAM scheduling
    edam_min_transfer_mw Float64,
    edam_max_transfer_mw Float64,

    -- Metadata
    effective_date Date,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_ba ba_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_zone zone_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_node node_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_region edam_region TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (ba_id, zone_id, node_id, effective_date)
PARTITION BY ba_id
TTL created_at + INTERVAL 2 YEAR;

-- Materialized views for common queries

-- Available transfer capacity by path
CREATE MATERIALIZED VIEW IF NOT EXISTS markets.wecc_atc_by_path
ENGINE = SummingMergeTree()
ORDER BY (source_ba, sink_ba, path_id, date)
AS SELECT
    source_ba,
    sink_ba,
    path_id,
    toDate(created_at) as date,
    avg(ttc_mw) as avg_ttc_mw,
    avg(ntc_mw) as avg_ntc_mw,
    avg(atc_mw) as avg_atc_mw,
    avg(loss_factor_percent) as avg_loss_factor,
    avg(wheeling_rate_usd_per_mwh) as avg_wheeling_rate,
    sum(edam_eligible) as edam_eligible_count,
    sum(edam_participant) as edam_participant_count,
    avg(edam_hurdle_rate_usd_per_mwh) as avg_edam_hurdle_rate,
    max(edam_transfer_limit_mw) as max_edam_transfer_limit
FROM markets.wecc_transmission_paths
GROUP BY source_ba, sink_ba, path_id, date;

-- EDAM participation summary by BA
CREATE MATERIALIZED VIEW IF NOT EXISTS markets.wecc_edam_participation_by_ba
ENGINE = SummingMergeTree()
ORDER BY (ba_id, edam_region, date)
AS SELECT
    ba_id,
    edam_region,
    toDate(created_at) as date,
    count() as entity_count,
    sum(edam_eligible) as edam_eligible_count,
    sum(edam_participant) as edam_participant_count,
    avg(edam_hurdle_rate_usd_per_mwh) as avg_hurdle_rate,
    avg(edam_transfer_limit_mw) as avg_transfer_limit,
    avg(edam_greenhouse_gas_rate_usd_per_ton) as avg_ghg_rate
FROM markets.wecc_edam_configurations
WHERE edam_eligible = true
GROUP BY ba_id, edam_region, date;

-- Wheeling contracts summary by BA
CREATE MATERIALIZED VIEW IF NOT EXISTS markets.wecc_wheeling_contracts_summary
ENGINE = SummingMergeTree()
ORDER BY (source_ba, sink_ba, date)
AS SELECT
    source_ba,
    sink_ba,
    toDate(created_at) as date,
    count() as contract_count,
    sum(capacity_mw) as total_capacity_mw,
    avg(rate_usd_per_mwh) as avg_rate,
    sum(firm_contract) as firm_contract_count
FROM markets.wecc_wheeling_contracts
WHERE term_end > today()
GROUP BY source_ba, sink_ba, date;
