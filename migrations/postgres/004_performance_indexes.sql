-- Performance optimization indexes for PostgreSQL
-- Optimizes taxonomy and metadata lookups

-- ==================================================================
-- INSTRUMENTS TABLE INDEXES
-- ==================================================================

-- Index for fast instrument lookups by ID
CREATE INDEX IF NOT EXISTS idx_instruments_instrument_id 
ON instruments(instrument_id);

-- Index for market filtering
CREATE INDEX IF NOT EXISTS idx_instruments_market 
ON instruments(market);

-- Index for tenant isolation
CREATE INDEX IF NOT EXISTS idx_instruments_tenant_id 
ON instruments(tenant_id);

-- Composite index for tenant + market queries
CREATE INDEX IF NOT EXISTS idx_instruments_tenant_market 
ON instruments(tenant_id, market);

-- Index for name-based searches (case-insensitive)
CREATE INDEX IF NOT EXISTS idx_instruments_name_lower 
ON instruments(LOWER(name));

-- Index for active instruments
CREATE INDEX IF NOT EXISTS idx_instruments_is_active 
ON instruments(is_active) 
WHERE is_active = true;

-- Partial index for recently updated instruments
CREATE INDEX IF NOT EXISTS idx_instruments_updated_recent 
ON instruments(updated_at) 
WHERE updated_at > NOW() - INTERVAL '30 days';

-- ==================================================================
-- TAXONOMY TABLE INDEXES
-- ==================================================================

-- Index for commodity tier lookups
CREATE INDEX IF NOT EXISTS idx_taxonomy_commodity 
ON taxonomy(commodity_tier);

-- Index for region lookups
CREATE INDEX IF NOT EXISTS idx_taxonomy_region 
ON taxonomy(region);

-- Index for product tier lookups
CREATE INDEX IF NOT EXISTS idx_taxonomy_product 
ON taxonomy(product_tier);

-- Composite index for full taxonomy classification
CREATE INDEX IF NOT EXISTS idx_taxonomy_classification 
ON taxonomy(commodity_tier, region, product_tier);

-- Index for pattern matching on taxonomy codes
CREATE INDEX IF NOT EXISTS idx_taxonomy_code_pattern 
ON taxonomy(taxonomy_code) 
WHERE taxonomy_code IS NOT NULL;

-- GIN index for full-text search on taxonomy descriptions
CREATE INDEX IF NOT EXISTS idx_taxonomy_description_fts 
ON taxonomy USING gin(to_tsvector('english', description));

-- Index for parent-child hierarchy queries
CREATE INDEX IF NOT EXISTS idx_taxonomy_parent_id 
ON taxonomy(parent_id) 
WHERE parent_id IS NOT NULL;

-- ==================================================================
-- METADATA TABLE INDEXES
-- ==================================================================

-- Index for instrument metadata lookups
CREATE INDEX IF NOT EXISTS idx_metadata_instrument_id 
ON metadata(instrument_id);

-- Index for metadata key lookups
CREATE INDEX IF NOT EXISTS idx_metadata_key 
ON metadata(key);

-- Composite index for instrument + key
CREATE INDEX IF NOT EXISTS idx_metadata_instrument_key 
ON metadata(instrument_id, key);

-- GIN index for JSONB metadata values
CREATE INDEX IF NOT EXISTS idx_metadata_value_jsonb 
ON metadata USING gin(value) 
WHERE value IS NOT NULL;

-- Index for metadata source
CREATE INDEX IF NOT EXISTS idx_metadata_source 
ON metadata(source);

-- ==================================================================
-- API KEYS TABLE INDEXES
-- ==================================================================

-- Hash index for fast API key lookups
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash 
ON api_keys(api_key_hash);

-- Index for tenant lookups
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id 
ON api_keys(tenant_id);

-- Partial index for active keys only
CREATE INDEX IF NOT EXISTS idx_api_keys_active 
ON api_keys(api_key_hash, tenant_id) 
WHERE is_active = true AND (expires_at IS NULL OR expires_at > NOW());

-- Index for key expiration management
CREATE INDEX IF NOT EXISTS idx_api_keys_expires_at 
ON api_keys(expires_at) 
WHERE expires_at IS NOT NULL;

-- Index for rate limit tracking
CREATE INDEX IF NOT EXISTS idx_api_keys_last_used 
ON api_keys(last_used_at) 
WHERE last_used_at IS NOT NULL;

-- ==================================================================
-- ADD MISSING CONSTRAINTS FOR DATA INTEGRITY
-- ==================================================================

-- Ensure instrument IDs are unique per tenant
ALTER TABLE instruments 
ADD CONSTRAINT IF NOT EXISTS unique_instrument_per_tenant 
UNIQUE (tenant_id, instrument_id);

-- Ensure taxonomy codes are unique
ALTER TABLE taxonomy 
ADD CONSTRAINT IF NOT EXISTS unique_taxonomy_code 
UNIQUE (taxonomy_code);

-- Ensure metadata keys are unique per instrument
ALTER TABLE metadata 
ADD CONSTRAINT IF NOT EXISTS unique_metadata_key_per_instrument 
UNIQUE (instrument_id, key);

-- ==================================================================
-- OPTIMIZE QUERIES WITH PREPARED STATEMENTS
-- ==================================================================

-- Prepare commonly used queries
PREPARE get_instrument_by_id (text, text) AS
SELECT * FROM instruments 
WHERE tenant_id = $1 AND instrument_id = $2 
LIMIT 1;

PREPARE get_taxonomy_by_classification (text, text, text) AS
SELECT * FROM taxonomy 
WHERE commodity_tier = $1 AND region = $2 AND product_tier = $3 
LIMIT 1;

PREPARE get_metadata_for_instrument (text) AS
SELECT key, value FROM metadata 
WHERE instrument_id = $1;

PREPARE validate_api_key (text) AS
SELECT tenant_id, permissions, metadata 
FROM api_keys 
WHERE api_key_hash = $1 
AND is_active = true 
AND (expires_at IS NULL OR expires_at > NOW()) 
LIMIT 1;

-- ==================================================================
-- CREATE MATERIALIZED VIEW FOR TAXONOMY HIERARCHY
-- ==================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_taxonomy_hierarchy AS
WITH RECURSIVE taxonomy_tree AS (
    -- Base case: root level taxonomies
    SELECT 
        taxonomy_id,
        taxonomy_code,
        commodity_tier,
        region,
        product_tier,
        parent_id,
        description,
        1 AS level,
        ARRAY[taxonomy_id] AS path
    FROM taxonomy
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive case: child taxonomies
    SELECT 
        t.taxonomy_id,
        t.taxonomy_code,
        t.commodity_tier,
        t.region,
        t.product_tier,
        t.parent_id,
        t.description,
        tt.level + 1,
        tt.path || t.taxonomy_id
    FROM taxonomy t
    INNER JOIN taxonomy_tree tt ON t.parent_id = tt.taxonomy_id
)
SELECT * FROM taxonomy_tree;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_taxonomy_hierarchy_id 
ON mv_taxonomy_hierarchy(taxonomy_id);

-- ==================================================================
-- VACUUM AND ANALYZE
-- ==================================================================

-- Update statistics for query planner
ANALYZE instruments;
ANALYZE taxonomy;
ANALYZE metadata;
ANALYZE api_keys;

-- Refresh materialized view
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_taxonomy_hierarchy;

-- ==================================================================
-- VERIFY INDEXES
-- ==================================================================

SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
AND tablename IN ('instruments', 'taxonomy', 'metadata', 'api_keys')
ORDER BY tablename, indexname;
