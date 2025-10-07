-- Migration: Create API keys table
-- Description: Creates table for API key management with tenant isolation

CREATE TABLE IF NOT EXISTS api_keys (
    key_id VARCHAR(32) PRIMARY KEY,
    key_hash VARCHAR(64) UNIQUE NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    permissions TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    
    CONSTRAINT valid_status CHECK (status IN ('active', 'inactive', 'revoked', 'expired'))
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys(tenant_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_status ON api_keys(status);
CREATE INDEX IF NOT EXISTS idx_api_keys_expires_at ON api_keys(expires_at);
CREATE INDEX IF NOT EXISTS idx_api_keys_created_at ON api_keys(created_at);

-- Create composite index for common queries
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_status ON api_keys(tenant_id, status);

-- Add comments
COMMENT ON TABLE api_keys IS 'API keys for service authentication';
COMMENT ON COLUMN api_keys.key_id IS 'Unique identifier for the API key';
COMMENT ON COLUMN api_keys.key_hash IS 'SHA256 hash of the API key value';
COMMENT ON COLUMN api_keys.tenant_id IS 'Tenant identifier for multi-tenancy';
COMMENT ON COLUMN api_keys.name IS 'Human-readable name for the API key';
COMMENT ON COLUMN api_keys.status IS 'Current status of the API key';
COMMENT ON COLUMN api_keys.created_at IS 'When the API key was created';
COMMENT ON COLUMN api_keys.expires_at IS 'When the API key expires (NULL for no expiration)';
COMMENT ON COLUMN api_keys.last_used_at IS 'When the API key was last used';
COMMENT ON COLUMN api_keys.permissions IS 'Array of permissions granted to this API key';
COMMENT ON COLUMN api_keys.metadata IS 'Additional metadata for the API key';

-- Create function to clean up expired keys
CREATE OR REPLACE FUNCTION cleanup_expired_api_keys()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    UPDATE api_keys 
    SET status = 'expired' 
    WHERE expires_at IS NOT NULL 
    AND expires_at < CURRENT_TIMESTAMP 
    AND status = 'active';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Create function to get API key statistics
CREATE OR REPLACE FUNCTION get_api_key_stats(tenant_id_param VARCHAR(64))
RETURNS TABLE (
    total_keys BIGINT,
    active_keys BIGINT,
    expired_keys BIGINT,
    revoked_keys BIGINT,
    last_used TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_keys,
        COUNT(*) FILTER (WHERE status = 'active') as active_keys,
        COUNT(*) FILTER (WHERE status = 'expired') as expired_keys,
        COUNT(*) FILTER (WHERE status = 'revoked') as revoked_keys,
        MAX(last_used_at) as last_used
    FROM api_keys 
    WHERE api_keys.tenant_id = tenant_id_param;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON api_keys TO data_processing_user;
-- GRANT USAGE ON SEQUENCE api_keys_id_seq TO data_processing_user;
