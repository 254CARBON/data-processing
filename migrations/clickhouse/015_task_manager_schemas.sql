-- Migration 015: Task Manager ClickHouse schemas
-- Add tables for RFTP intake, task proposals, approvals, and billing workflows

-- RFTP Requests table
CREATE TABLE IF NOT EXISTS gold.task_manager.rftp_requests (
    rftp_id String,
    title String,
    description String,
    task_type LowCardinality(String),
    jurisdiction LowCardinality(String),
    estimated_hours UInt32,
    budget_ceiling Float64,
    requested_by String,
    priority LowCardinality(String),
    due_date Nullable(Date),
    attachments String, -- JSON array
    status LowCardinality(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    INDEX idx_task_type task_type TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_jurisdiction jurisdiction TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_status status TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_requested_by requested_by TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (created_at, task_type, jurisdiction, rftp_id)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Task Proposals table
CREATE TABLE IF NOT EXISTS gold.task_manager.task_proposals (
    proposal_id String,
    rftp_id String,
    proposed_hours UInt32,
    proposed_budget Float64,
    proposed_deliverables String, -- JSON array
    proposed_timeline String, -- JSON object
    technical_approach String,
    assumptions String, -- JSON array
    risks String, -- JSON array
    created_by String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    INDEX idx_rftp_id rftp_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_created_by created_by TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (created_at, proposal_id)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Task Approvals table
CREATE TABLE IF NOT EXISTS gold.task_manager.task_approvals (
    approval_id String,
    task_id String,
    approved_by String,
    approved_at DateTime,
    approved_budget Float64,
    approved_hours UInt32,
    conditions String, -- JSON array
    notes Nullable(String),
    approval_deadline Nullable(DateTime),
    created_at DateTime DEFAULT now(),
    INDEX idx_task_id task_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_approved_by approved_by TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (approved_at, task_id, approval_id)
PARTITION BY toYYYYMM(approved_at)
TTL approved_at + INTERVAL 7 YEAR;

-- Tasks table
CREATE TABLE IF NOT EXISTS gold.task_manager.tasks (
    task_id String,
    proposal_id String,
    title String,
    description String,
    task_type LowCardinality(String),
    jurisdiction LowCardinality(String),
    status LowCardinality(String),
    assigned_to Nullable(String),
    budget Float64,
    hours UInt32,
    deliverables String, -- JSON array
    timeline String, -- JSON object
    created_by String,
    created_at DateTime DEFAULT now(),
    approved_at Nullable(DateTime),
    started_at Nullable(DateTime),
    completed_at Nullable(DateTime),
    progress_percentage UInt8 DEFAULT 0,
    spent_hours Float64 DEFAULT 0,
    spent_budget Float64 DEFAULT 0,
    artifacts String, -- JSON array
    monthly_reports String, -- JSON array
    updated_at DateTime DEFAULT now(),
    INDEX idx_task_type task_type TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_jurisdiction jurisdiction TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_status status TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_assigned_to assigned_to TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_created_by created_by TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (created_at, task_type, status, task_id)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Billing Records table
CREATE TABLE IF NOT EXISTS gold.task_manager.billing_records (
    invoice_id String,
    task_id String,
    billing_period_start Date,
    billing_period_end Date,
    hours_billed Float64,
    rate_per_hour Float64,
    total_amount Float64,
    status LowCardinality(String),
    due_date Date,
    paid_date Nullable(Date),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    INDEX idx_task_id task_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_status status TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_due_date due_date TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (created_at, status, invoice_id)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Task Progress History table
CREATE TABLE IF NOT EXISTS gold.task_manager.task_progress_history (
    task_id String,
    progress_percentage UInt8,
    spent_hours Float64,
    spent_budget Float64,
    status LowCardinality(String),
    updated_by String,
    updated_at DateTime DEFAULT now(),
    notes Nullable(String),
    INDEX idx_task_id task_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_status status TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (updated_at, task_id)
PARTITION BY toYYYYMM(updated_at)
TTL updated_at + INTERVAL 7 YEAR;

-- Monthly Status Reports table
CREATE TABLE IF NOT EXISTS gold.task_manager.monthly_reports (
    report_id String,
    task_id String,
    report_month Date,
    status_summary String,
    progress_update String,
    hours_spent Float64,
    budget_spent Float64,
    deliverables_completed String, -- JSON array
    risks_issues String, -- JSON array
    next_month_plan String,
    created_by String,
    created_at DateTime DEFAULT now(),
    INDEX idx_task_id task_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_report_month report_month TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (report_month, task_id, report_id)
PARTITION BY toYYYYMM(report_month)
TTL report_month + INTERVAL 7 YEAR;

-- Task Summary table
CREATE TABLE IF NOT EXISTS gold.task_manager.task_summary (
    task_id String,
    title String,
    task_type LowCardinality(String),
    jurisdiction LowCardinality(String),
    status LowCardinality(String),
    assigned_to Nullable(String),
    budget Float64,
    hours UInt32,
    spent_budget Float64,
    spent_hours Float64,
    progress_percentage UInt8,
    budget_utilization_percent Float64,
    hours_utilization_percent Float64,
    created_at DateTime,
    started_at Nullable(DateTime),
    completed_at Nullable(DateTime),
    duration_days Nullable(UInt32),
    INDEX idx_task_type task_type TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_status status TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_assigned_to assigned_to TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = SummingMergeTree()
ORDER BY (task_id)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 7 YEAR;

-- Task Summary Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.task_manager.task_summary_mv
TO gold.task_manager.task_summary
AS SELECT
    task_id,
    title,
    task_type,
    jurisdiction,
    status,
    assigned_to,
    budget,
    hours,
    spent_budget,
    spent_hours,
    progress_percentage,
    CASE 
        WHEN budget > 0 THEN spent_budget / budget * 100
        ELSE 0
    END as budget_utilization_percent,
    CASE 
        WHEN hours > 0 THEN spent_hours / hours * 100
        ELSE 0
    END as hours_utilization_percent,
    created_at,
    started_at,
    completed_at,
    CASE 
        WHEN completed_at IS NOT NULL AND started_at IS NOT NULL 
        THEN dateDiff('day', started_at, completed_at)
        ELSE NULL
    END as duration_days
FROM gold.task_manager.tasks;

-- Budget Alerts table
CREATE TABLE IF NOT EXISTS gold.task_manager.budget_alerts (
    task_id String,
    title String,
    task_type LowCardinality(String),
    jurisdiction LowCardinality(String),
    budget Float64,
    spent_budget Float64,
    budget_utilization_percent Float64,
    status LowCardinality(String),
    assigned_to Nullable(String),
    created_at DateTime,
    alert_generated_at DateTime,
    INDEX idx_task_id task_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_status status TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE = MergeTree()
ORDER BY (alert_generated_at, task_id)
PARTITION BY toYYYYMM(alert_generated_at)
TTL alert_generated_at + INTERVAL 1 YEAR;

-- Budget Alerts Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.task_manager.budget_alerts_mv
TO gold.task_manager.budget_alerts
AS SELECT
    task_id,
    title,
    task_type,
    jurisdiction,
    budget,
    spent_budget,
    CASE 
        WHEN budget > 0 THEN spent_budget / budget * 100
        ELSE 0
    END as budget_utilization_percent,
    status,
    assigned_to,
    created_at,
    now() as alert_generated_at
FROM gold.task_manager.tasks
WHERE 
    status IN ('in_progress', 'approved') 
    AND budget > 0 
    AND spent_budget / budget >= 0.7;
