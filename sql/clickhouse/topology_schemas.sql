-- Topology data schemas for ClickHouse

-- Balancing Authorities table
CREATE TABLE IF NOT EXISTS topology_ba (
    ba_id String,
    ba_name String,
    iso_rto Nullable(String),
    timezone String,
    dst_rules String,  -- JSON
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (ba_id, created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Zones table
CREATE TABLE IF NOT EXISTS topology_zones (
    zone_id String,
    zone_name String,
    ba_id String,
    zone_type String,
    load_serving_entity Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (zone_id, created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Nodes table
CREATE TABLE IF NOT EXISTS topology_nodes (
    node_id String,
    node_name String,
    zone_id String,
    node_type String,
    voltage_level_kv Nullable(Float64),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    capacity_mw Nullable(Float64),
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (node_id, created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Paths table
CREATE TABLE IF NOT EXISTS topology_paths (
    path_id String,
    path_name String,
    from_node String,
    to_node String,
    path_type String,
    capacity_mw Float64,
    losses_factor Float64,
    hurdle_rate_usd_per_mwh Nullable(Float64),
    wheel_rate_usd_per_mwh Nullable(Float64),
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (path_id, created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Interties table
CREATE TABLE IF NOT EXISTS topology_interties (
    intertie_id String,
    intertie_name String,
    from_market String,
    to_market String,
    capacity_mw Float64,
    atc_ttc_ntc String,  -- JSON
    deliverability_flags String,  -- JSON array
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (intertie_id, created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Materialized views for efficient querying

-- Nodes by zone
CREATE MATERIALIZED VIEW IF NOT EXISTS topology_nodes_by_zone
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
    created_at,
    updated_at
FROM topology_nodes;

-- Paths by zone
CREATE MATERIALIZED VIEW IF NOT EXISTS topology_paths_by_zone
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
    p.created_at,
    p.updated_at,
    n1.zone_id as from_zone_id,
    n2.zone_id as to_zone_id
FROM topology_paths p
LEFT JOIN topology_nodes n1 ON p.from_node = n1.node_id
LEFT JOIN topology_nodes n2 ON p.to_node = n2.node_id;

-- Interties by market
CREATE MATERIALIZED VIEW IF NOT EXISTS topology_interties_by_market
ENGINE = MergeTree()
ORDER BY (from_market, to_market, intertie_id)
AS SELECT
    intertie_id,
    intertie_name,
    from_market,
    to_market,
    capacity_mw,
    atc_ttc_ntc,
    deliverability_flags,
    created_at,
    updated_at
FROM topology_interties;
