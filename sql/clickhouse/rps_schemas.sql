-- RPS compliance and REC accounting schemas

-- RPS obligations table
CREATE TABLE IF NOT EXISTS rps_obligations (
    obligation_id String,
    jurisdiction LowCardinality(String),
    utility_id String,
    year UInt16,

    -- Obligation requirements
    total_retail_sales_gwh Float64,
    rps_percentage Float64,
    rps_obligation_gwh Float64,
    rps_class_i_gwh Float64,
    rps_class_ii_gwh Float64,
    rps_class_iii_gwh Float64,

    -- Compliance deadlines
    compliance_deadline Date,
    true_up_deadline Date,

    -- Status
    compliance_status LowCardinality(String),

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_obligation obligation_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_utility utility_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_jurisdiction jurisdiction TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_year year TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (jurisdiction, utility_id, year)
PARTITION BY jurisdiction
TTL created_at + INTERVAL 10 YEAR;

-- REC accounts table
CREATE TABLE IF NOT EXISTS rps_rec_accounts (
    account_id String,
    utility_id String,
    account_type LowCardinality(String),

    -- REC balances by class
    rec_class_i_balance Float64,
    rec_class_ii_balance Float64,
    rec_class_iii_balance Float64,
    total_rec_balance Float64,

    -- Vintage tracking
    vintage_year UInt16,
    expiration_date Date,

    -- Transaction history
    procured_this_year Float64,
    banked_from_previous Float64,
    used_this_year Float64,

    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_account account_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_utility utility_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_type account_type TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_vintage vintage_year TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (utility_id, account_type, vintage_year)
PARTITION BY utility_id
TTL created_at + INTERVAL 5 YEAR;

-- REC transactions table
CREATE TABLE IF NOT EXISTS rps_rec_transactions (
    transaction_id String,
    utility_id String,
    transaction_type LowCardinality(String),

    -- Transaction details
    rec_quantity Float64,
    rec_class LowCardinality(String),
    vintage_year UInt16,

    -- Source/destination
    source_account_id String,
    destination_account_id String,
    counterparty String,

    -- Pricing
    price_usd_per_rec Float64,

    -- Timing
    transaction_date DateTime,
    settlement_date Date,

    -- Compliance
    applied_to_year UInt16,

    -- Metadata
    created_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_transaction transaction_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_utility utility_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_type transaction_type TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_date transaction_date TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (utility_id, transaction_date, transaction_id)
PARTITION BY toYYYYMM(transaction_date)
TTL transaction_date + INTERVAL 7 YEAR;

-- Portfolio REC positions table
CREATE TABLE IF NOT EXISTS rps_portfolio_rec_positions (
    position_id String,
    utility_id String,
    portfolio_id String,

    -- Position metrics
    total_rec_position Float64,
    rec_shortfall_gwh Float64,
    rec_surplus_gwh Float64,

    -- By class
    class_i_position Float64,
    class_ii_position Float64,
    class_iii_position Float64,

    -- Risk metrics
    congestion_risk_factor Float64,
    curtailment_risk_factor Float64,
    price_risk_factor Float64,

    -- Economics
    weighted_avg_cost_usd_per_rec Float64,
    replacement_cost_usd_per_rec Float64,

    -- Vintage distribution (JSON)
    vintage_distribution String,

    -- Metadata
    as_of_date Date,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Partitioning and indexing
    INDEX idx_position position_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_utility utility_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_portfolio portfolio_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_date as_of_date TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (utility_id, portfolio_id, as_of_date)
PARTITION BY utility_id
TTL as_of_date + INTERVAL 3 YEAR;

-- Siting pre-screen results table
CREATE TABLE IF NOT EXISTS rps_siting_pre_screens (
    screen_id String,
    utility_id String,
    technology_type LowCardinality(String),

    -- Location (JSON)
    location String,

    -- Siting factors
    resource_potential_mw Float64,
    interconnection_capacity_mw Float64,
    transmission_distance_km Float64,

    -- Conflicts and constraints (JSON arrays)
    land_use_conflicts String,
    environmental_constraints String,

    -- Economics
    estimated_capex_usd_per_mw Float64,
    estimated_opex_usd_per_mw_year Float64,
    estimated_lcoe_usd_per_mwh Float64,

    -- Risk assessment
    curtailment_risk Float64,
    congestion_risk Float64,
    permitting_risk Float64,

    -- Scoring
    overall_score Float64,
    viability_rating LowCardinality(String),

    -- Metadata
    created_at DateTime DEFAULT now(),
    expires_at DateTime,

    -- Partitioning and indexing
    INDEX idx_screen screen_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_utility utility_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_technology technology_type TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_rating viability_rating TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (utility_id, technology_type, created_at)
PARTITION BY utility_id
TTL created_at + INTERVAL 2 YEAR;

-- Materialized views for common queries

-- Compliance status summary by utility and year
CREATE MATERIALIZED VIEW IF NOT EXISTS rps_compliance_summary_by_utility_year
ENGINE = SummingMergeTree()
ORDER BY (utility_id, year, compliance_status)
AS SELECT
    utility_id,
    year,
    compliance_status,
    count() as obligation_count,
    sum(rps_obligation_gwh) as total_obligation_gwh,
    sum(total_retail_sales_gwh) as total_retail_sales_gwh,
    avg(rps_percentage) as avg_rps_percentage
FROM rps_obligations
GROUP BY utility_id, year, compliance_status;

-- REC account balances by utility and vintage
CREATE MATERIALIZED VIEW IF NOT EXISTS rps_rec_balances_by_utility_vintage
ENGINE = SummingMergeTree()
ORDER BY (utility_id, vintage_year, account_type)
AS SELECT
    utility_id,
    vintage_year,
    account_type,
    count() as account_count,
    sum(rec_class_i_balance) as total_class_i_balance,
    sum(rec_class_ii_balance) as total_class_ii_balance,
    sum(rec_class_iii_balance) as total_class_iii_balance,
    sum(total_rec_balance) as total_rec_balance,
    sum(procured_this_year) as total_procured_this_year,
    sum(banked_from_previous) as total_banked_from_previous,
    sum(used_this_year) as total_used_this_year
FROM rps_rec_accounts
GROUP BY utility_id, vintage_year, account_type;

-- Portfolio position summary by utility
CREATE MATERIALIZED VIEW IF NOT EXISTS rps_portfolio_position_summary
ENGINE = SummingMergeTree()
ORDER BY (utility_id, portfolio_id, as_of_date)
AS SELECT
    utility_id,
    portfolio_id,
    as_of_date,
    count() as position_count,
    sum(total_rec_position) as total_rec_position,
    sum(rec_shortfall_gwh) as total_shortfall_gwh,
    sum(rec_surplus_gwh) as total_surplus_gwh,
    sum(class_i_position) as total_class_i_position,
    sum(class_ii_position) as total_class_ii_position,
    sum(class_iii_position) as total_class_iii_position,
    avg(congestion_risk_factor) as avg_congestion_risk,
    avg(curtailment_risk_factor) as avg_curtailment_risk,
    avg(price_risk_factor) as avg_price_risk,
    avg(weighted_avg_cost_usd_per_rec) as avg_weighted_cost,
    avg(replacement_cost_usd_per_rec) as avg_replacement_cost
FROM rps_portfolio_rec_positions
GROUP BY utility_id, portfolio_id, as_of_date;

-- Siting viability summary by technology
CREATE MATERIALIZED VIEW IF NOT EXISTS rps_siting_viability_summary
ENGINE = SummingMergeTree()
ORDER BY (technology_type, viability_rating)
AS SELECT
    technology_type,
    viability_rating,
    count() as screen_count,
    avg(overall_score) as avg_score,
    sum(resource_potential_mw) as total_resource_potential_mw,
    sum(interconnection_capacity_mw) as total_interconnection_capacity_mw,
    avg(transmission_distance_km) as avg_transmission_distance_km,
    avg(estimated_capex_usd_per_mw) as avg_capex,
    avg(estimated_lcoe_usd_per_mwh) as avg_lcoe,
    avg(curtailment_risk) as avg_curtailment_risk,
    avg(congestion_risk) as avg_congestion_risk,
    avg(permitting_risk) as avg_permitting_risk
FROM rps_siting_pre_screens
GROUP BY technology_type, viability_rating;
