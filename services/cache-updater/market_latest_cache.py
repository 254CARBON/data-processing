"""Write-through cache for latest market ticks with stampede protection."""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

import structlog
from redis.asyncio.lock import Lock

from shared.storage.redis import RedisClient

logger = structlog.get_logger(__name__)


class MarketLatestCache:
    """Maintain a hot cache of latest ticks with jittered TTL and soft refresh window."""

    def __init__(
        self,
        redis_client: RedisClient,
        ttl_range_seconds: Tuple[int, int] = (15, 60),
        refresh_margin_seconds: int = 5,
        lock_timeout_seconds: int = 5,
    ) -> None:
        if ttl_range_seconds[0] <= 0:
            raise ValueError("ttl_range_seconds minimum must be positive")
        if ttl_range_seconds[1] < ttl_range_seconds[0]:
            raise ValueError("ttl_range_seconds maximum must be >= minimum")
        if refresh_margin_seconds < 1:
            raise ValueError("refresh_margin_seconds must be >= 1")
        if lock_timeout_seconds < 1:
            raise ValueError("lock_timeout_seconds must be >= 1")

        self._redis = redis_client
        self._ttl_range = ttl_range_seconds
        self._refresh_margin = refresh_margin_seconds
        self._lock_timeout = lock_timeout_seconds
        self._logger = structlog.get_logger("market-latest-cache")

    async def write_through(
        self,
        *,
        symbol: str,
        market: str,
        tenant_id: str,
        payload: Dict[str, Any],
    ) -> None:
        """Persist the latest tick both to storage and Redis with jittered TTL."""
        await self._ensure_connection()

        ttl = self._pick_ttl()
        now = int(time.time())
        refresh_at = now + max(ttl - self._refresh_margin, 1)
        entry = {
            "symbol": symbol,
            "market": market,
            "tenant_id": tenant_id,
            "data": payload,
            "cache_meta": {
                "written_at": now,
                "ttl": ttl,
                "refresh_at": refresh_at,
                "expires_at": now + ttl,
            },
        }

        await self._redis.set_json(self._key(symbol), entry, ttl=ttl)
        self._logger.debug("Write-through cache updated", key=self._key(symbol), ttl=ttl)

    async def get_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Return the cached payload if present."""
        await self._ensure_connection()
        entry = await self._redis.get_json(self._key(symbol))
        if not entry:
            return None
        return entry.get("data", entry)

    async def get_or_load(
        self,
        symbol: str,
        loader: Callable[[], Awaitable[Optional[Dict[str, Any]]]],
        *,
        market: Optional[str] = None,
        tenant_id: str = "default",
        background_refresh: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Return cached payload or load it under a stampede-protected lock.

        Args:
            symbol: Logical symbol identifier (matches cache key suffix).
            loader: Async callable returning the latest tick dictionary.
            market: Optional market hint used when populating the cache.
            tenant_id: Optional tenant identifier, defaults to `default`.
            background_refresh: When True, refresh close-to-expiry entries asynchronously.
        """
        await self._ensure_connection()

        entry = await self._redis.get_json(self._key(symbol))
        now = int(time.time())
        if entry:
            data = entry.get("data", entry)
            refresh_at = int(entry.get("cache_meta", {}).get("refresh_at", 0))
            if background_refresh and now >= refresh_at:
                asyncio.create_task(
                    self._refresh(symbol, loader, market or entry.get("market"), entry.get("tenant_id", tenant_id))
                )
            return data

        return await self._refresh(symbol, loader, market, tenant_id, force=True)

    async def _refresh(
        self,
        symbol: str,
        loader: Callable[[], Awaitable[Optional[Dict[str, Any]]]],
        market: Optional[str],
        tenant_id: Optional[str],
        *,
        force: bool = False,
    ) -> Optional[Dict[str, Any]]:
        lock = await self._acquire_lock(symbol)
        if lock is None:
            if force:
                # Wait briefly to allow the winning loader to populate the cache, then retry.
                await asyncio.sleep(0.05)
                entry = await self._redis.get_json(self._key(symbol))
                if entry:
                    return entry.get("data", entry)
            return None

        try:
            result = await loader()
            if result is None:
                return None

            resolved_market = market or result.get("market") or "unknown"
            resolved_tenant = tenant_id or result.get("tenant_id") or "default"
            await self.write_through(
                symbol=symbol,
                market=resolved_market,
                tenant_id=resolved_tenant,
                payload=result,
            )
            return result
        except Exception:
            self._logger.error("Cache refresh failed", key=self._key(symbol), exc_info=True)
            raise
        finally:
            await self._release_lock(lock)

    def _key(self, symbol: str) -> str:
        return f"market:latest:{symbol}"

    def _lock_key(self, symbol: str) -> str:
        return f"{self._key(symbol)}:lock"

    def _pick_ttl(self) -> int:
        return random.randint(self._ttl_range[0], self._ttl_range[1])

    async def _ensure_connection(self) -> None:
        if not getattr(self._redis, "is_connected", False):
            await self._redis.connect()

    async def _acquire_lock(self, symbol: str) -> Optional[Lock]:
        await self._ensure_connection()
        assert self._redis.client is not None
        lock = self._redis.client.lock(self._lock_key(symbol), timeout=self._lock_timeout)
        acquired = await lock.acquire(blocking=False)
        if not acquired:
            return None
        return lock

    async def _release_lock(self, lock: Lock) -> None:
        try:
            await lock.release()
        except Exception as exc:  # pragma: no cover - best-effort cleanup
            self._logger.debug("Failed to release cache lock", error=str(exc))
