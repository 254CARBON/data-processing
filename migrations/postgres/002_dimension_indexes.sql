-- PostgreSQL migration: Create additional indexes and views
-- Created: 2025-01-27
-- Description: Creates additional indexes and views for performance optimization

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_commodity_region 
ON taxonomy_instruments (commodity, region);

CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_tenant_active 
ON taxonomy_instruments (tenant_id, active);

CREATE INDEX IF NOT EXISTS idx_taxonomy_regions_tenant_active 
ON taxonomy_regions (tenant_id, active);

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodities_tenant_active 
ON taxonomy_commodities (tenant_id, active);

CREATE INDEX IF NOT EXISTS idx_curve_metadata_commodity_region 
ON curve_metadata (commodity, region);

CREATE INDEX IF NOT EXISTS idx_curve_metadata_tenant_active 
ON curve_metadata (tenant_id, active);

-- Create partial indexes for active records
CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_active_instruments 
ON taxonomy_instruments (instrument_id) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_taxonomy_regions_active_regions 
ON taxonomy_regions (region_code) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodities_active_commodities 
ON taxonomy_commodities (commodity_code) WHERE active = TRUE;

-- Create views for common queries
CREATE OR REPLACE VIEW v_active_instruments AS
SELECT 
    i.instrument_id,
    i.commodity,
    i.region,
    i.product_tier,
    i.unit,
    i.contract_size,
    i.tick_size,
    i.tenant_id,
    i.metadata,
    c.commodity_name,
    r.region_name,
    r.country,
    r.timezone
FROM taxonomy_instruments i
LEFT JOIN taxonomy_commodities c ON i.commodity = c.commodity_code
LEFT JOIN taxonomy_regions r ON i.region = r.region_code
WHERE i.active = TRUE
  AND c.active = TRUE
  AND r.active = TRUE;

-- Create view for instrument lookup with aliases
CREATE OR REPLACE VIEW v_instrument_lookup AS
SELECT 
    i.instrument_id,
    i.commodity,
    i.region,
    i.product_tier,
    i.unit,
    i.contract_size,
    i.tick_size,
    i.tenant_id,
    i.metadata,
    c.commodity_name,
    r.region_name,
    r.country,
    r.timezone
FROM taxonomy_instruments i
LEFT JOIN taxonomy_commodities c ON i.commodity = c.commodity_code
LEFT JOIN taxonomy_regions r ON i.region = r.region_code
WHERE i.active = TRUE
  AND c.active = TRUE
  AND r.active = TRUE

UNION ALL

SELECT 
    i.instrument_id,
    i.commodity,
    i.region,
    i.product_tier,
    i.unit,
    i.contract_size,
    i.tick_size,
    i.tenant_id,
    i.metadata,
    c.commodity_name,
    r.region_name,
    r.country,
    r.timezone
FROM instrument_aliases a
JOIN taxonomy_instruments i ON a.instrument_id = i.instrument_id
LEFT JOIN taxonomy_commodities c ON i.commodity = c.commodity_code
LEFT JOIN taxonomy_regions r ON i.region = r.region_code
WHERE a.active = TRUE
  AND i.active = TRUE
  AND c.active = TRUE
  AND r.active = TRUE;

-- Create view for curve metadata with commodity and region info
CREATE OR REPLACE VIEW v_curve_metadata AS
SELECT 
    cm.curve_id,
    cm.curve_name,
    cm.commodity,
    cm.region,
    cm.contract_months,
    cm.interpolation_method,
    cm.tenant_id,
    cm.metadata,
    c.commodity_name,
    r.region_name,
    r.country,
    r.timezone
FROM curve_metadata cm
LEFT JOIN taxonomy_commodities c ON cm.commodity = c.commodity_code
LEFT JOIN taxonomy_regions r ON cm.region = r.region_code
WHERE cm.active = TRUE
  AND c.active = TRUE
  AND r.active = TRUE;

-- Create function to get instrument metadata by ID
CREATE OR REPLACE FUNCTION get_instrument_metadata(p_instrument_id VARCHAR(100), p_tenant_id VARCHAR(50) DEFAULT 'default')
RETURNS TABLE (
    instrument_id VARCHAR(100),
    commodity VARCHAR(50),
    region VARCHAR(50),
    product_tier VARCHAR(20),
    unit VARCHAR(20),
    contract_size DECIMAL(15,6),
    tick_size DECIMAL(15,6),
    tenant_id VARCHAR(50),
    metadata JSONB,
    commodity_name VARCHAR(100),
    region_name VARCHAR(100),
    country VARCHAR(50),
    timezone VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        i.instrument_id,
        i.commodity,
        i.region,
        i.product_tier,
        i.unit,
        i.contract_size,
        i.tick_size,
        i.tenant_id,
        i.metadata,
        c.commodity_name,
        r.region_name,
        r.country,
        r.timezone
    FROM taxonomy_instruments i
    LEFT JOIN taxonomy_commodities c ON i.commodity = c.commodity_code
    LEFT JOIN taxonomy_regions r ON i.region = r.region_code
    WHERE i.instrument_id = p_instrument_id
      AND i.tenant_id = p_tenant_id
      AND i.active = TRUE
      AND c.active = TRUE
      AND r.active = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Create function to search instruments by commodity and region
CREATE OR REPLACE FUNCTION search_instruments(
    p_commodity VARCHAR(50) DEFAULT NULL,
    p_region VARCHAR(50) DEFAULT NULL,
    p_tenant_id VARCHAR(50) DEFAULT 'default'
)
RETURNS TABLE (
    instrument_id VARCHAR(100),
    commodity VARCHAR(50),
    region VARCHAR(50),
    product_tier VARCHAR(20),
    unit VARCHAR(20),
    contract_size DECIMAL(15,6),
    tick_size DECIMAL(15,6),
    tenant_id VARCHAR(50),
    metadata JSONB,
    commodity_name VARCHAR(100),
    region_name VARCHAR(100),
    country VARCHAR(50),
    timezone VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        i.instrument_id,
        i.commodity,
        i.region,
        i.product_tier,
        i.unit,
        i.contract_size,
        i.tick_size,
        i.tenant_id,
        i.metadata,
        c.commodity_name,
        r.region_name,
        r.country,
        r.timezone
    FROM taxonomy_instruments i
    LEFT JOIN taxonomy_commodities c ON i.commodity = c.commodity_code
    LEFT JOIN taxonomy_regions r ON i.region = r.region_code
    WHERE i.tenant_id = p_tenant_id
      AND i.active = TRUE
      AND c.active = TRUE
      AND r.active = TRUE
      AND (p_commodity IS NULL OR i.commodity = p_commodity)
      AND (p_region IS NULL OR i.region = p_region);
END;
$$ LANGUAGE plpgsql;

-- Create function to get curve metadata by ID
CREATE OR REPLACE FUNCTION get_curve_metadata(p_curve_id VARCHAR(100), p_tenant_id VARCHAR(50) DEFAULT 'default')
RETURNS TABLE (
    curve_id VARCHAR(100),
    curve_name VARCHAR(200),
    commodity VARCHAR(50),
    region VARCHAR(50),
    contract_months TEXT,
    interpolation_method VARCHAR(50),
    tenant_id VARCHAR(50),
    metadata JSONB,
    commodity_name VARCHAR(100),
    region_name VARCHAR(100),
    country VARCHAR(50),
    timezone VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cm.curve_id,
        cm.curve_name,
        cm.commodity,
        cm.region,
        cm.contract_months,
        cm.interpolation_method,
        cm.tenant_id,
        cm.metadata,
        c.commodity_name,
        r.region_name,
        r.country,
        r.timezone
    FROM curve_metadata cm
    LEFT JOIN taxonomy_commodities c ON cm.commodity = c.commodity_code
    LEFT JOIN taxonomy_regions r ON cm.region = r.region_code
    WHERE cm.curve_id = p_curve_id
      AND cm.tenant_id = p_tenant_id
      AND cm.active = TRUE
      AND c.active = TRUE
      AND r.active = TRUE;
END;
$$ LANGUAGE plpgsql;

