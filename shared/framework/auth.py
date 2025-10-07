"""Authentication framework for API key management."""

import secrets
import hashlib
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class APIKeyStatus(str, Enum):
    """API key status enumeration."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    REVOKED = "revoked"
    EXPIRED = "expired"


@dataclass
class APIKey:
    """API key data structure."""
    key_id: str
    key_hash: str
    tenant_id: str
    name: str
    status: APIKeyStatus
    created_at: datetime
    expires_at: Optional[datetime]
    last_used_at: Optional[datetime]
    permissions: List[str]
    metadata: Dict[str, Any]


class APIKeyManager:
    """Manages API keys for authentication."""
    
    def __init__(self, postgres_client, redis_client):
        self.postgres = postgres_client
        self.redis = redis_client
        self.logger = structlog.get_logger("api-key-manager")
        
    def generate_key(self, tenant_id: str, name: str, permissions: List[str] = None, 
                    expires_in_days: Optional[int] = None, metadata: Dict[str, Any] = None) -> str:
        """Generate a new API key."""
        # Generate random key
        key_value = secrets.token_urlsafe(32)
        key_hash = self._hash_key(key_value)
        key_id = secrets.token_urlsafe(16)
        
        # Set expiration
        expires_at = None
        if expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_in_days)
        
        # Create API key record
        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            tenant_id=tenant_id,
            name=name,
            status=APIKeyStatus.ACTIVE,
            created_at=datetime.utcnow(),
            expires_at=expires_at,
            last_used_at=None,
            permissions=permissions or [],
            metadata=metadata or {}
        )
        
        # Store in database
        await self._store_key(api_key)
        
        # Cache in Redis
        await self._cache_key(api_key)
        
        self.logger.info("API key generated", key_id=key_id, tenant_id=tenant_id, name=name)
        
        return key_value
    
    def validate_key(self, key_value: str) -> Optional[APIKey]:
        """Validate an API key."""
        key_hash = self._hash_key(key_value)
        
        # Try cache first
        cached_key = await self._get_cached_key(key_hash)
        if cached_key:
            await self._update_last_used(cached_key.key_id)
            return cached_key
        
        # Query database
        db_key = await self._get_key_from_db(key_hash)
        if db_key:
            await self._cache_key(db_key)
            await self._update_last_used(db_key.key_id)
            return db_key
        
        return None
    
    def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key."""
        try:
            # Update database
            await self.postgres.update(
                "api_keys",
                {"status": APIKeyStatus.REVOKED},
                "key_id = $1",
                key_id
            )
            
            # Remove from cache
            await self._remove_cached_key(key_id)
            
            self.logger.info("API key revoked", key_id=key_id)
            return True
            
        except Exception as e:
            self.logger.error("Failed to revoke API key", key_id=key_id, error=str(e))
            return False
    
    def list_keys(self, tenant_id: str) -> List[APIKey]:
        """List API keys for a tenant."""
        try:
            rows = await self.postgres.execute(
                "SELECT * FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC",
                tenant_id
            )
            
            return [self._row_to_api_key(row) for row in rows]
            
        except Exception as e:
            self.logger.error("Failed to list API keys", tenant_id=tenant_id, error=str(e))
            return []
    
    def get_key_info(self, key_id: str) -> Optional[APIKey]:
        """Get API key information."""
        try:
            row = await self.postgres.execute_one(
                "SELECT * FROM api_keys WHERE key_id = $1",
                key_id
            )
            
            return self._row_to_api_key(row) if row else None
            
        except Exception as e:
            self.logger.error("Failed to get key info", key_id=key_id, error=str(e))
            return None
    
    def _hash_key(self, key_value: str) -> str:
        """Hash API key for storage."""
        return hashlib.sha256(key_value.encode()).hexdigest()
    
    async def _store_key(self, api_key: APIKey) -> None:
        """Store API key in database."""
        await self.postgres.insert("api_keys", {
            "key_id": api_key.key_id,
            "key_hash": api_key.key_hash,
            "tenant_id": api_key.tenant_id,
            "name": api_key.name,
            "status": api_key.status.value,
            "created_at": api_key.created_at,
            "expires_at": api_key.expires_at,
            "last_used_at": api_key.last_used_at,
            "permissions": api_key.permissions,
            "metadata": api_key.metadata
        })
    
    async def _cache_key(self, api_key: APIKey) -> None:
        """Cache API key in Redis."""
        cache_key = f"api_key:{api_key.key_hash}"
        await self.redis.set_json(cache_key, {
            "key_id": api_key.key_id,
            "tenant_id": api_key.tenant_id,
            "name": api_key.name,
            "status": api_key.status.value,
            "permissions": api_key.permissions,
            "metadata": api_key.metadata
        }, ttl=3600)  # 1 hour TTL
    
    async def _get_cached_key(self, key_hash: str) -> Optional[APIKey]:
        """Get API key from cache."""
        cache_key = f"api_key:{key_hash}"
        cached_data = await self.redis.get_json(cache_key)
        
        if cached_data:
            return APIKey(
                key_id=cached_data["key_id"],
                key_hash=key_hash,
                tenant_id=cached_data["tenant_id"],
                name=cached_data["name"],
                status=APIKeyStatus(cached_data["status"]),
                created_at=datetime.utcnow(),  # Not cached
                expires_at=None,  # Not cached
                last_used_at=None,  # Not cached
                permissions=cached_data["permissions"],
                metadata=cached_data["metadata"]
            )
        
        return None
    
    async def _get_key_from_db(self, key_hash: str) -> Optional[APIKey]:
        """Get API key from database."""
        row = await self.postgres.execute_one(
            "SELECT * FROM api_keys WHERE key_hash = $1 AND status = 'active'",
            key_hash
        )
        
        return self._row_to_api_key(row) if row else None
    
    async def _update_last_used(self, key_id: str) -> None:
        """Update last used timestamp."""
        await self.postgres.update(
            "api_keys",
            {"last_used_at": datetime.utcnow()},
            "key_id = $1",
            key_id
        )
    
    async def _remove_cached_key(self, key_id: str) -> None:
        """Remove API key from cache."""
        # Get key hash first
        row = await self.postgres.execute_one(
            "SELECT key_hash FROM api_keys WHERE key_id = $1",
            key_id
        )
        
        if row:
            cache_key = f"api_key:{row['key_hash']}"
            await self.redis.delete(cache_key)
    
    def _row_to_api_key(self, row: Dict[str, Any]) -> APIKey:
        """Convert database row to APIKey object."""
        return APIKey(
            key_id=row["key_id"],
            key_hash=row["key_hash"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            status=APIKeyStatus(row["status"]),
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            last_used_at=row["last_used_at"],
            permissions=row["permissions"],
            metadata=row["metadata"]
        )


class APIKeyValidator:
    """Validates API keys and permissions."""
    
    def __init__(self, api_key_manager: APIKeyManager):
        self.api_key_manager = api_key_manager
        self.logger = structlog.get_logger("api-key-validator")
    
    async def validate_request(self, api_key: str, required_permissions: List[str] = None) -> Optional[Dict[str, Any]]:
        """Validate API key and permissions for a request."""
        # Validate API key
        key_info = await self.api_key_manager.validate_key(api_key)
        if not key_info:
            return None
        
        # Check if key is active
        if key_info.status != APIKeyStatus.ACTIVE:
            return None
        
        # Check if key is expired
        if key_info.expires_at and key_info.expires_at < datetime.utcnow():
            return None
        
        # Check permissions
        if required_permissions:
            if not all(perm in key_info.permissions for perm in required_permissions):
                return None
        
        return {
            "tenant_id": key_info.tenant_id,
            "permissions": key_info.permissions,
            "metadata": key_info.metadata
        }
