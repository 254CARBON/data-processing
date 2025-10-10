"""Redis cache helpers for Served projections."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from shared.schemas.models import ProjectionData
from shared.storage.redis import RedisClient


logger = logging.getLogger(__name__)


class ServedProjectionCache:
    """Typed helper around Redis for Served-layer projections."""

    def __init__(self, redis_client: RedisClient, default_ttl: int = 3600):
        self.redis = redis_client
        self.default_ttl = default_ttl
        self.logger = logging.getLogger("served-projection-cache")

    async def ensure_connected(self) -> None:
        """Ensure the underlying Redis client is connected."""
        if not getattr(self.redis, "is_connected", False):
            await self.redis.connect()

    async def close(self) -> None:
        """Close the underlying Redis client."""
        if getattr(self.redis, "is_connected", False):
            await self.redis.close()

    async def set_latest_price(
        self,
        projection: ProjectionData,
        ttl: Optional[int] = None,
    ) -> None:
        """Store the latest price projection."""
        await self.ensure_connected()
        key = self._latest_price_key(projection.tenant_id, projection.instrument_id)
        await self.redis.set_json(key, projection.to_dict(), ttl or self.default_ttl)
        self.logger.debug(
            "Stored latest price projection",
            key=key,
            ttl=ttl or self.default_ttl,
        )

    async def get_latest_price(
        self,
        tenant_id: str,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the latest price projection."""
        await self.ensure_connected()
        key = self._latest_price_key(tenant_id, instrument_id)
        return await self.redis.get_json(key)

    async def invalidate_latest_price(self, tenant_id: str, instrument_id: str) -> int:
        """Remove latest price projection."""
        await self.ensure_connected()
        key = self._latest_price_key(tenant_id, instrument_id)
        return await self.redis.delete(key)

    async def set_curve_snapshot(
        self,
        projection: ProjectionData,
        ttl: Optional[int] = None,
    ) -> None:
        """Store a curve snapshot projection."""
        await self.ensure_connected()
        horizon = _normalise_horizon(projection.data.get("horizon"))
        key = self._curve_snapshot_key(
            projection.tenant_id,
            projection.instrument_id,
            horizon,
        )
        payload = projection.to_dict()
        payload["data"]["horizon"] = horizon
        await self.redis.set_json(key, payload, ttl or self.default_ttl)
        self.logger.debug(
            "Stored curve snapshot projection",
            key=key,
            ttl=ttl or self.default_ttl,
        )

    async def get_curve_snapshot(
        self,
        tenant_id: str,
        instrument_id: str,
        horizon: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a curve snapshot projection."""
        await self.ensure_connected()
        key = self._curve_snapshot_key(tenant_id, instrument_id, _normalise_horizon(horizon))
        return await self.redis.get_json(key)

    async def invalidate_curve_snapshot(
        self,
        tenant_id: str,
        instrument_id: str,
        horizon: Optional[str] = None,
    ) -> int:
        """
        Invalidate stored curve snapshots.

        Returns the number of keys removed.
        """
        await self.ensure_connected()
        if horizon:
            key = self._curve_snapshot_key(tenant_id, instrument_id, _normalise_horizon(horizon))
            return await self.redis.delete(key)

        pattern = f"served:curve_snapshot:{tenant_id}:{instrument_id}:*"
        keys = await self.redis.keys(pattern)
        if not keys:
            return 0
        deleted = 0
        for key in keys:
            deleted += await self.redis.delete(key)
        return deleted

    async def set_custom_projection(
        self,
        projection: ProjectionData,
        ttl: Optional[int] = None,
    ) -> None:
        """Store a custom projection payload."""
        await self.ensure_connected()
        projection_type = projection.projection_type
        key = self._custom_projection_key(
            projection.tenant_id,
            projection_type,
            projection.instrument_id,
        )
        await self.redis.set_json(key, projection.to_dict(), ttl or self.default_ttl)
        self.logger.debug(
            "Stored custom projection",
            key=key,
            ttl=ttl or self.default_ttl,
        )

    async def get_custom_projection(
        self,
        tenant_id: str,
        projection_type: str,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a custom projection payload."""
        await self.ensure_connected()
        key = self._custom_projection_key(tenant_id, projection_type, instrument_id)
        return await self.redis.get_json(key)

    async def invalidate_custom_projection(
        self,
        tenant_id: str,
        projection_type: Optional[str] = None,
        instrument_id: Optional[str] = None,
    ) -> int:
        """Invalidate cached custom projections."""
        await self.ensure_connected()
        if projection_type and instrument_id:
            key = self._custom_projection_key(tenant_id, projection_type, instrument_id)
            return await self.redis.delete(key)

        pattern_parts = ["served:custom"]
        pattern_parts.append(projection_type or "*")
        pattern_parts.append(tenant_id or "*")
        pattern_parts.append(instrument_id or "*")
        pattern = ":".join(pattern_parts)
        keys = await self.redis.keys(pattern)
        if not keys:
            return 0
        deleted = 0
        for key in keys:
            deleted += await self.redis.delete(key)
        return deleted

    async def get_stats(self) -> Dict[str, Any]:
        """Retrieve cache statistics."""
        await self.ensure_connected()
        info = await self.redis.get_connection_info()
        return {
            "used_memory": info.get("used_memory"),
            "used_memory_human": info.get("used_memory_human"),
            "connected_clients": info.get("connected_clients"),
            "keyspace_hits": info.get("keyspace_hits"),
            "keyspace_misses": info.get("keyspace_misses"),
        }

    @staticmethod
    def _latest_price_key(tenant_id: str, instrument_id: str) -> str:
        return f"served:latest_price:{tenant_id}:{instrument_id}"

    @staticmethod
    def _curve_snapshot_key(tenant_id: str, instrument_id: str, horizon: str) -> str:
        return f"served:curve_snapshot:{tenant_id}:{instrument_id}:{horizon}"

    @staticmethod
    def _custom_projection_key(tenant_id: str, projection_type: str, instrument_id: str) -> str:
        return f"served:custom:{projection_type}:{tenant_id}:{instrument_id}"


def _normalise_horizon(horizon: Union[str, None]) -> str:
    if not horizon:
        return "unknown"
    return str(horizon).lower()
