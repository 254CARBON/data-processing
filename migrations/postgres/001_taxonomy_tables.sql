-- PostgreSQL migration: Create taxonomy tables
-- Created: 2025-01-27
-- Description: Creates taxonomy reference tables for enrichment service

-- Create database if it doesn't exist
-- CREATE DATABASE data_proc;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Taxonomy instruments table
CREATE TABLE IF NOT EXISTS taxonomy_instruments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id VARCHAR(100) NOT NULL UNIQUE,
    commodity VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    product_tier VARCHAR(20) NOT NULL DEFAULT 'standard',
    unit VARCHAR(20) NOT NULL,
    contract_size DECIMAL(15,6) NOT NULL DEFAULT 1.0,
    tick_size DECIMAL(15,6) NOT NULL DEFAULT 0.01,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Taxonomy regions table
CREATE TABLE IF NOT EXISTS taxonomy_regions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region_code VARCHAR(10) NOT NULL UNIQUE,
    region_name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    timezone VARCHAR(50),
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Taxonomy commodities table
CREATE TABLE IF NOT EXISTS taxonomy_commodities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    commodity_code VARCHAR(20) NOT NULL UNIQUE,
    commodity_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit VARCHAR(20) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Taxonomy commodity rules table
CREATE TABLE IF NOT EXISTS taxonomy_commodity_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    commodity VARCHAR(50) NOT NULL,
    keywords TEXT NOT NULL, -- Comma-separated keywords
    priority INTEGER DEFAULT 1,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Taxonomy region rules table
CREATE TABLE IF NOT EXISTS taxonomy_region_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region VARCHAR(50) NOT NULL,
    keywords TEXT NOT NULL, -- Comma-separated keywords
    priority INTEGER DEFAULT 1,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Instrument aliases table
CREATE TABLE IF NOT EXISTS instrument_aliases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alias VARCHAR(100) NOT NULL,
    instrument_id VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Curve metadata table
CREATE TABLE IF NOT EXISTS curve_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    curve_id VARCHAR(100) NOT NULL UNIQUE,
    curve_name VARCHAR(200) NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    contract_months TEXT NOT NULL, -- Comma-separated contract months
    interpolation_method VARCHAR(50) DEFAULT 'linear',
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_instrument_id 
ON taxonomy_instruments (instrument_id);

CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_commodity 
ON taxonomy_instruments (commodity);

CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_region 
ON taxonomy_instruments (region);

CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_tenant 
ON taxonomy_instruments (tenant_id);

CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_active 
ON taxonomy_instruments (active);

CREATE INDEX IF NOT EXISTS idx_taxonomy_regions_region_code 
ON taxonomy_regions (region_code);

CREATE INDEX IF NOT EXISTS idx_taxonomy_regions_tenant 
ON taxonomy_regions (tenant_id);

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodities_commodity_code 
ON taxonomy_commodities (commodity_code);

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodities_tenant 
ON taxonomy_commodities (tenant_id);

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodity_rules_commodity 
ON taxonomy_commodity_rules (commodity);

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodity_rules_active 
ON taxonomy_commodity_rules (active);

CREATE INDEX IF NOT EXISTS idx_taxonomy_region_rules_region 
ON taxonomy_region_rules (region);

CREATE INDEX IF NOT EXISTS idx_taxonomy_region_rules_active 
ON taxonomy_region_rules (active);

CREATE INDEX IF NOT EXISTS idx_instrument_aliases_alias 
ON instrument_aliases (alias);

CREATE INDEX IF NOT EXISTS idx_instrument_aliases_instrument_id 
ON instrument_aliases (instrument_id);

CREATE INDEX IF NOT EXISTS idx_curve_metadata_curve_id 
ON curve_metadata (curve_id);

CREATE INDEX IF NOT EXISTS idx_curve_metadata_commodity 
ON curve_metadata (commodity);

CREATE INDEX IF NOT EXISTS idx_curve_metadata_region 
ON curve_metadata (region);

-- Create full-text search indexes
CREATE INDEX IF NOT EXISTS idx_taxonomy_instruments_search 
ON taxonomy_instruments USING gin (instrument_id gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_taxonomy_regions_search 
ON taxonomy_regions USING gin (region_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_taxonomy_commodities_search 
ON taxonomy_commodities USING gin (commodity_name gin_trgm_ops);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_taxonomy_instruments_updated_at 
    BEFORE UPDATE ON taxonomy_instruments 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_taxonomy_regions_updated_at 
    BEFORE UPDATE ON taxonomy_regions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_taxonomy_commodities_updated_at 
    BEFORE UPDATE ON taxonomy_commodities 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_taxonomy_commodity_rules_updated_at 
    BEFORE UPDATE ON taxonomy_commodity_rules 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_taxonomy_region_rules_updated_at 
    BEFORE UPDATE ON taxonomy_region_rules 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_instrument_aliases_updated_at 
    BEFORE UPDATE ON instrument_aliases 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_curve_metadata_updated_at 
    BEFORE UPDATE ON curve_metadata 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

