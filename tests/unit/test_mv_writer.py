"""Tests for the MaterializedViewWriter projection outputs."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from shared.schemas.models import ProjectionData

# Dynamically load the module because the package path contains a hyphen.
MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "service-projection"
    / "app"
    / "output"
    / "mv_writer.py"
)
SPEC = importlib.util.spec_from_file_location(
    "service_projection.app.output.mv_writer", MODULE_PATH
)
mv_writer_module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader is not None
SPEC.loader.exec_module(mv_writer_module)  # type: ignore[assignment]
sys.modules["service_projection.app.output.mv_writer"] = mv_writer_module

MaterializedViewWriter = mv_writer_module.MaterializedViewWriter


class DummyClickHouseClient:
    """Test double for ClickHouseClient."""

    def __init__(self, *_args, **_kwargs):
        self.insert = AsyncMock()
        self.connect = AsyncMock()
        self.close = AsyncMock()


@pytest.fixture
def dummy_config():
    """Provide a minimal configuration object for the writer."""

    return SimpleNamespace(
        clickhouse_host="localhost",
        clickhouse_port=8123,
        clickhouse_database="served",
        clickhouse_user="default",
        clickhouse_password="",
        processing_timeout=30,
        clickhouse_pool_max=5,
    )


@pytest.mark.asyncio
async def test_write_latest_price_projection(monkeypatch, dummy_config):
    """Latest price projections are serialised correctly for ClickHouse."""
    dummy_client = DummyClickHouseClient()
    monkeypatch.setattr(
        mv_writer_module,
        "ClickHouseClient",
        lambda *_args, **_kwargs: dummy_client,
    )

    writer = MaterializedViewWriter(dummy_config)
    await writer.start()

    observed_at = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    projection = ProjectionData(
        projection_type="latest_price",
        instrument_id="INST-1",
        data={
            "price": 42.5,
            "volume": 1000,
            "timestamp": observed_at.isoformat(),
            "quality_flags": ["valid"],
            "metadata": {"foo": "bar"},
        },
        last_updated=datetime(2025, 1, 1, 12, 5, tzinfo=timezone.utc),
        tenant_id="tenant-123",
        metadata={"source": "aggregation", "foo": "original"},
    )

    await writer.write_latest_price(projection)

    dummy_client.insert.assert_awaited_once()
    table, rows = dummy_client.insert.await_args.args
    assert table == "served_latest"
    assert len(rows) == 1
    row = rows[0]

    assert row["tenant_id"] == "tenant-123"
    assert row["instrument_id"] == "INST-1"
    assert row["price"] == 42.5
    assert row["volume"] == 1000
    assert row["quality_flags"] == ["valid"]
    assert row["source"] == "aggregation"
    assert row["snapshot_at"] == "2025-01-01 12:00:00.000000"
    assert row["projection_type"] == "latest_price"
    assert json.loads(row["metadata"]) == {
        "foo": "bar",
        "source": "aggregation",
    }


@pytest.mark.asyncio
async def test_write_curve_snapshot_projection(monkeypatch, dummy_config):
    """Curve snapshot projections are serialised correctly for ClickHouse."""
    dummy_client = DummyClickHouseClient()
    monkeypatch.setattr(
        mv_writer_module,
        "ClickHouseClient",
        lambda *_args, **_kwargs: dummy_client,
    )

    writer = MaterializedViewWriter(dummy_config)
    await writer.start()

    observed_at = datetime(2025, 3, 15, 8, 30, tzinfo=timezone.utc)
    curve_points = [{"contract_month": "2025-06", "price": 12.3, "volume": 5.0}]
    projection = ProjectionData(
        projection_type="curve_snapshot",
        instrument_id="INST-2",
        data={
            "horizon": "3m",
            "curve_points": curve_points,
            "timestamp": observed_at.isoformat(),
            "quality_flags": ["valid", "late_arrival"],
            "interpolation_method": "cubic",
            "metadata": {"curve": "base"},
        },
        last_updated=datetime(2025, 3, 15, 8, 35, tzinfo=timezone.utc),
        tenant_id="tenant-xyz",
        metadata={},
    )

    await writer.write_curve_snapshot(projection)

    dummy_client.insert.assert_awaited_once()
    table, rows = dummy_client.insert.await_args.args
    assert table == "served_curve_snapshots"
    row = rows[0]
    assert row["tenant_id"] == "tenant-xyz"
    assert row["instrument_id"] == "INST-2"
    assert row["horizon"] == "3m"
    assert json.loads(row["curve_points"]) == curve_points
    assert row["interpolation_method"] == "cubic"
    assert row["quality_flags"] == ["valid", "late_arrival"]
    assert row["snapshot_at"] == "2025-03-15 08:30:00.000000"
    assert row["projection_type"] == "curve_snapshot"
    assert json.loads(row["metadata"]) == {"curve": "base"}
