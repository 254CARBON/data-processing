-- ClickHouse migration: Audit events table for immutable trail
-- Created: 2025-10-06
-- Description: Creates audit logging table used by AuditLogger

CREATE TABLE IF NOT EXISTS audit_events (
    event_id String,
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64('UTC'),
    service_name LowCardinality(String),
    environment LowCardinality(String),
    event_type LowCardinality(String),
    action String,
    actor_type LowCardinality(String),
    actor_id String,
    tenant_id String,
    resource_type String,
    resource_id String,
    correlation_id String,
    request_id String,
    ip_address String,
    metadata Map(String, String),
    details String,
    event_hash FixedString(64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, service_name, event_time)
TTL event_time + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_audit_event_hash
ON audit_events (event_hash)
TYPE set(50)
GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_audit_resource
ON audit_events (tenant_id, resource_type, resource_id)
TYPE set(50)
GRANULARITY 1;


