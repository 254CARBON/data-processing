from datetime import datetime, timezone
from typing import Any, Dict, List
import fnmatch
import os
import sys

import pytest
import pytest_asyncio

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SERVICE_PROJECTION_PATH = os.path.join(PROJECT_ROOT, "service-projection")
if SERVICE_PROJECTION_PATH not in sys.path:
    sys.path.append(SERVICE_PROJECTION_PATH)

from app.output.served_cache import ServedProjectionCache, _normalise_horizon
from shared.framework.cache import CacheKey
from shared.schemas.models import ProjectionData


class StubRedisClient:
    """Minimal async Redis client used for unit tests."""

    def __init__(self):
        self.store: Dict[str, Dict[str, Any]] = {}
        self.is_connected = False

    async def connect(self) -> None:
        self.is_connected = True

    async def close(self) -> None:
        self.is_connected = False

    async def set_json(self, key: str, value: Dict[str, Any], ttl: int | None = None) -> None:
        self.store[key] = value

    async def get_json(self, key: str) -> Dict[str, Any] | None:
        value = self.store.get(key)
        if value is None:
            return None
        # Return a shallow copy to avoid accidental mutation in tests
        return dict(value)

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0

    async def keys(self, pattern: str) -> List[str]:
        return [key for key in self.store if fnmatch.fnmatch(key, pattern)]

    async def get_connection_info(self) -> Dict[str, Any]:
        return {
            "used_memory": len(self.store),
            "used_memory_human": f"{len(self.store)} keys",
            "connected_clients": 1 if self.is_connected else 0,
            "keyspace_hits": 0,
            "keyspace_misses": 0,
            "status": "connected" if self.is_connected else "disconnected",
        }


@pytest.fixture
def stub_redis():
    return StubRedisClient()


@pytest_asyncio.fixture
async def served_cache(stub_redis):
    cache = ServedProjectionCache(stub_redis, default_ttl=60)
    await cache.ensure_connected()
    yield cache
    await cache.close()


def test_cache_key_generation_sanitises_parts():
    key = CacheKey.generate_for_price("tenant:1", "INST:1", "latest")
    assert key == "price:tenant_1:INST_1:latest"


@pytest.mark.asyncio
async def test_latest_price_roundtrip(served_cache: ServedProjectionCache):
    projection = ProjectionData(
        projection_type="latest_price",
        instrument_id="INST-1",
        data={"price": 42.5, "volume": 100, "timestamp": datetime.now(timezone.utc).isoformat()},
        last_updated=datetime.now(timezone.utc),
        tenant_id="tenant-1",
    )

    await served_cache.set_latest_price(projection)
    stored = await served_cache.get_latest_price("tenant-1", "INST-1")

    assert stored is not None
    assert stored["instrument_id"] == "INST-1"
    assert stored["data"]["price"] == 42.5

    deleted = await served_cache.invalidate_latest_price("tenant-1", "INST-1")
    assert deleted == 1
    assert await served_cache.get_latest_price("tenant-1", "INST-1") is None


@pytest.mark.asyncio
async def test_curve_snapshot_horizon_normalised(served_cache: ServedProjectionCache):
    projection = ProjectionData(
        projection_type="curve_snapshot",
        instrument_id="INST-2",
        data={
            "horizon": "1M",
            "curve_points": [{"contract_month": "2024-03", "price": 55.1, "volume": 10}],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        last_updated=datetime.now(timezone.utc),
        tenant_id="tenant-1",
    )

    await served_cache.set_curve_snapshot(projection)
    stored = await served_cache.get_curve_snapshot("tenant-1", "INST-2", "1M")
    assert stored is not None
    assert stored["data"]["horizon"] == "1m"

    # Invalidate all horizons for the instrument
    removed = await served_cache.invalidate_curve_snapshot("tenant-1", "INST-2")
    assert removed == 1


@pytest.mark.asyncio
async def test_custom_projection_roundtrip(served_cache: ServedProjectionCache):
    projection = ProjectionData(
        projection_type="spread",
        instrument_id="INST-3",
        data={"spread_value": 2.15, "timestamp": datetime.now(timezone.utc).isoformat()},
        last_updated=datetime.now(timezone.utc),
        tenant_id="tenant-1",
    )

    await served_cache.set_custom_projection(projection)
    stored = await served_cache.get_custom_projection("tenant-1", "spread", "INST-3")
    assert stored is not None
    assert stored["data"]["spread_value"] == 2.15

    removed = await served_cache.invalidate_custom_projection("tenant-1", "spread", "INST-3")
    assert removed == 1


def test_normalise_horizon_helper():
    assert _normalise_horizon("1M") == "1m"
    assert _normalise_horizon(None) == "unknown"
