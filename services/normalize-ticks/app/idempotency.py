"""
In-memory idempotency guard used to skip duplicate events.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from typing import Dict


class IdempotencyGuard:
    """Sliding-window idempotency cache with TTL semantics."""

    def __init__(self, ttl_seconds: int, max_entries: int) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")

        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: "OrderedDict[str, float]" = OrderedDict()
        self._lock = asyncio.Lock()

    async def check_and_remember(self, key: str) -> bool:
        """
        Record the key if it has not been seen recently.

        Returns:
            True if the key was new and recorded, False if it is a duplicate.
        """
        now = time.monotonic()
        async with self._lock:
            self._evict_expired(now)

            if key in self._entries:
                return False

            self._entries[key] = now
            self._entries.move_to_end(key)
            if len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

        return True

    async def mark(self, key: str) -> None:
        """Forcefully record a key without checking for duplicates."""
        now = time.monotonic()
        async with self._lock:
            self._evict_expired(now)
            self._entries[key] = now
            self._entries.move_to_end(key)
            if len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    async def seen(self, key: str) -> bool:
        """Return True if the key has already been processed within the TTL window."""
        now = time.monotonic()
        async with self._lock:
            self._evict_expired(now)
            return key in self._entries

    def snapshot(self) -> Dict[str, float]:
        """Return a copy of the cache for metrics/debugging."""
        return dict(self._entries)

    def size(self) -> int:
        """Return the current number of tracked keys."""
        return len(self._entries)

    def _evict_expired(self, current_time: float) -> None:
        """Remove keys that have passed their TTL."""
        expiry = current_time - self._ttl_seconds
        while self._entries:
            key, timestamp = next(iter(self._entries.items()))
            if timestamp >= expiry:
                break
            self._entries.popitem(last=False)
