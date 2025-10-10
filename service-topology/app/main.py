"""
Topology service for North American power markets.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiohttp import web

from shared.framework.config import ServiceConfig
from shared.framework.service import AsyncService
from shared.storage.clickhouse import ClickHouseClient
from shared.storage.redis import RedisClient

from .models import (
    NodeType,
    PathType,
    TopologyBA,
    TopologyGraph,
    TopologyIntertie,
    TopologyNode,
    TopologyPath,
    TopologyZone,
    ZoneType,
)
from .ercot_topology import ERCOT_TOPOLOGY_DATA
from .miso_topology import MISO_TOPOLOGY_DATA
from .wecc_topology import WECC_TOPOLOGY_DATA


class TopologyConfig(ServiceConfig):
    """Typed configuration for the topology service."""

    def __init__(self) -> None:
        super().__init__(service_name="topology")
        self.topology_cache_ttl = int(
            os.getenv("TOPOLOGY_CACHE_TTL", os.getenv("DATA_PROC_TOPOLOGY_CACHE_TTL", "3600"))
        )
        refresh_hours = float(
            os.getenv(
                "TOPOLOGY_REFRESH_INTERVAL_HOURS",
                os.getenv("DATA_PROC_TOPOLOGY_REFRESH_INTERVAL_HOURS", "24"),
            )
        )
        self.topology_refresh_interval_seconds = int(refresh_hours * 3600)


class TopologyService(AsyncService):
    """Topology service for managing power system topology graphs."""

    def __init__(self) -> None:
        self.config = TopologyConfig()
        super().__init__(self.config)

        self.clickhouse = ClickHouseClient(self.config.clickhouse_url)
        self.redis = RedisClient(self.config.redis_url)

        self.cache_ttl = self.config.topology_cache_ttl
        self.refresh_interval = self.config.topology_refresh_interval_seconds

        self.market_topologies: Dict[str, Dict[str, Any]] = {
            "wecc": WECC_TOPOLOGY_DATA,
            "miso": MISO_TOPOLOGY_DATA,
            "ercot": ERCOT_TOPOLOGY_DATA,
        }

        self._graphs: Dict[str, TopologyGraph] = {}
        self._refresh_task: Optional[asyncio.Task] = None

    def _setup_service_routes(self) -> None:
        """Register HTTP routes for topology queries."""
        if not self.app:
            return

        self.app.router.add_get("/topology/{market}", self._handle_get_topology)
        self.app.router.add_get("/topology/{market}/nodes/{node_id}", self._handle_get_node)
        self.app.router.add_get("/topology/{market}/paths/{path_id}", self._handle_get_path)

    async def _startup_hook(self) -> None:
        """Initialize storage connections and load topology data."""
        await self.clickhouse.connect()
        await self._ensure_schema()
        await self.redis.connect()
        await self._load_topology_data()

        if self.refresh_interval > 0:
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def _shutdown_hook(self) -> None:
        """Shutdown background tasks and close storage connections."""
        if self._refresh_task:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task

        await self.redis.disconnect()
        await self.clickhouse.disconnect()

    async def _ensure_schema(self) -> None:
        """Create ClickHouse tables and materialized views if they do not exist."""
        schema_path = Path(__file__).resolve().parents[2] / "sql" / "clickhouse" / "topology_schemas.sql"
        if not schema_path.exists():
            self.logger.warning("Topology schema file not found", path=str(schema_path))
            return

        statements: List[str] = []
        buffer: List[str] = []
        with schema_path.open("r", encoding="utf-8") as schema_file:
            for raw_line in schema_file:
                line = raw_line.strip()
                if not line or line.startswith("--"):
                    continue

                buffer.append(raw_line.rstrip("\n"))
                if line.endswith(";"):
                    statement = "\n".join(buffer).rstrip(";").strip()
                    buffer = []
                    if statement:
                        statements.append(statement)

        if buffer:
            statement = "\n".join(buffer).rstrip(";").strip()
            if statement:
                statements.append(statement)

        for statement in statements:
            try:
                await self.clickhouse.execute(statement)
            except Exception as exc:  # pragma: no cover - schema failures logged
                self.logger.error("Failed to ensure topology schema", error=str(exc), statement=statement)

    async def _refresh_loop(self) -> None:
        """Periodically refresh topology data from static sources."""
        while True:
            try:
                await asyncio.sleep(self.refresh_interval)
                await self._load_topology_data()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pragma: no cover - background logging
                self.logger.error("Topology refresh failed", error=str(exc))

    async def _load_topology_data(self, markets: Optional[List[str]] = None) -> None:
        """Load topology data for selected markets."""
        markets_to_process = markets or list(self.market_topologies.keys())

        for market in markets_to_process:
            topology_data = self.market_topologies.get(market)
            if not topology_data:
                self.logger.warning("No topology data found", market=market)
                continue

            try:
                topology_graph = self._parse_topology_data(market, topology_data)
                await self._store_topology_graph(topology_graph)
                await self._cache_topology_graph(topology_graph)
                self._graphs[market] = topology_graph

                self.logger.info(
                    "Loaded topology",
                    market=market,
                    ba_count=topology_graph.ba_count,
                    zone_count=topology_graph.zone_count,
                    node_count=topology_graph.node_count,
                    path_count=topology_graph.path_count,
                )
            except Exception as exc:  # pragma: no cover - logging only
                self.logger.error("Failed to load topology", market=market, error=str(exc))

    def _parse_topology_data(self, market: str, data: Dict[str, Any]) -> TopologyGraph:
        """Parse topology data into model objects."""
        balancing_authorities = [
            TopologyBA(
                ba_id=ba_data["ba_id"],
                ba_name=ba_data["ba_name"],
                iso_rto=ba_data.get("iso_rto"),
                timezone=ba_data["timezone"],
                dst_rules=ba_data.get("dst_rules", {}),
            )
            for ba_data in data.get("balancing_authorities", [])
        ]

        zones = [
            TopologyZone(
                zone_id=zone_data["zone_id"],
                zone_name=zone_data["zone_name"],
                ba_id=zone_data["ba_id"],
                zone_type=ZoneType(zone_data["zone_type"]),
                load_serving_entity=zone_data.get("load_serving_entity"),
            )
            for zone_data in data.get("zones", [])
        ]

        nodes = [
            TopologyNode(
                node_id=node_data["node_id"],
                node_name=node_data["node_name"],
                zone_id=node_data["zone_id"],
                node_type=NodeType(node_data["node_type"]),
                voltage_level_kv=node_data.get("voltage_level_kv"),
                latitude=node_data.get("latitude"),
                longitude=node_data.get("longitude"),
                capacity_mw=node_data.get("capacity_mw"),
            )
            for node_data in data.get("nodes", [])
        ]

        paths = [
            TopologyPath(
                path_id=path_data["path_id"],
                path_name=path_data["path_name"],
                from_node=path_data["from_node"],
                to_node=path_data["to_node"],
                path_type=PathType(path_data["path_type"]),
                capacity_mw=path_data["capacity_mw"],
                losses_factor=path_data["losses_factor"],
                hurdle_rate_usd_per_mwh=path_data.get("hurdle_rate_usd_per_mwh"),
                wheel_rate_usd_per_mwh=path_data.get("wheel_rate_usd_per_mwh"),
            )
            for path_data in data.get("paths", [])
        ]

        interties = [
            TopologyIntertie(
                intertie_id=intertie_data["intertie_id"],
                intertie_name=intertie_data["intertie_name"],
                from_market=intertie_data["from_market"],
                to_market=intertie_data["to_market"],
                capacity_mw=intertie_data["capacity_mw"],
                atc_ttc_ntc=intertie_data.get("atc_ttc_ntc", {}),
                deliverability_flags=intertie_data.get("deliverability_flags", []),
            )
            for intertie_data in data.get("interties", [])
        ]

        return TopologyGraph(
            market=market,
            version="1.0.0",
            generated_at=datetime.now(timezone.utc),
            balancing_authorities=balancing_authorities,
            zones=zones,
            nodes=nodes,
            paths=paths,
            interties=interties,
        )

    async def _store_topology_graph(self, topology_graph: TopologyGraph) -> None:
        """Store topology graph in ClickHouse."""
        try:
            if topology_graph.balancing_authorities:
                await self.clickhouse.insert(
                    "topology_ba",
                    [
                        {
                            "ba_id": ba.ba_id,
                            "ba_name": ba.ba_name,
                            "iso_rto": ba.iso_rto,
                            "timezone": ba.timezone,
                            "dst_rules": json.dumps(ba.dst_rules),
                            "created_at": ba.created_at,
                            "updated_at": ba.updated_at,
                        }
                        for ba in topology_graph.balancing_authorities
                    ],
                )

            if topology_graph.zones:
                await self.clickhouse.insert(
                    "topology_zones",
                    [
                        {
                            "zone_id": zone.zone_id,
                            "zone_name": zone.zone_name,
                            "ba_id": zone.ba_id,
                            "zone_type": zone.zone_type.value,
                            "load_serving_entity": zone.load_serving_entity,
                            "created_at": zone.created_at,
                            "updated_at": zone.updated_at,
                        }
                        for zone in topology_graph.zones
                    ],
                )

            if topology_graph.nodes:
                await self.clickhouse.insert(
                    "topology_nodes",
                    [
                        {
                            "node_id": node.node_id,
                            "node_name": node.node_name,
                            "zone_id": node.zone_id,
                            "node_type": node.node_type.value,
                            "voltage_level_kv": node.voltage_level_kv,
                            "latitude": node.latitude,
                            "longitude": node.longitude,
                            "capacity_mw": node.capacity_mw,
                            "created_at": node.created_at,
                            "updated_at": node.updated_at,
                        }
                        for node in topology_graph.nodes
                    ],
                )

            if topology_graph.paths:
                await self.clickhouse.insert(
                    "topology_paths",
                    [
                        {
                            "path_id": path.path_id,
                            "path_name": path.path_name,
                            "from_node": path.from_node,
                            "to_node": path.to_node,
                            "path_type": path.path_type.value,
                            "capacity_mw": path.capacity_mw,
                            "losses_factor": path.losses_factor,
                            "hurdle_rate_usd_per_mwh": path.hurdle_rate_usd_per_mwh,
                            "wheel_rate_usd_per_mwh": path.wheel_rate_usd_per_mwh,
                            "created_at": path.created_at,
                            "updated_at": path.updated_at,
                        }
                        for path in topology_graph.paths
                    ],
                )

            if topology_graph.interties:
                await self.clickhouse.insert(
                    "topology_interties",
                    [
                        {
                            "intertie_id": intertie.intertie_id,
                            "intertie_name": intertie.intertie_name,
                            "from_market": intertie.from_market,
                            "to_market": intertie.to_market,
                            "capacity_mw": intertie.capacity_mw,
                            "atc_ttc_ntc": json.dumps(intertie.atc_ttc_ntc),
                            "deliverability_flags": json.dumps(intertie.deliverability_flags),
                            "created_at": intertie.created_at,
                            "updated_at": intertie.updated_at,
                        }
                        for intertie in topology_graph.interties
                    ],
                )
        except Exception as exc:  # pragma: no cover - storage failures surfaced via logs
            self.logger.error("Failed to persist topology graph", market=topology_graph.market, error=str(exc))

    async def _cache_topology_graph(self, topology_graph: TopologyGraph) -> None:
        """Cache topology graph in Redis."""
        cache_key = self._cache_key(topology_graph.market, topology_graph.version)
        payload = self._serialize_topology_graph(topology_graph)

        try:
            await self.redis.set(cache_key, payload, ttl=self.cache_ttl)
        except Exception as exc:  # pragma: no cover - caching failures surfaced via logs
            self.logger.error("Failed to cache topology graph", market=topology_graph.market, error=str(exc))

    async def get_topology_graph(self, market: str) -> Optional[TopologyGraph]:
        """Get topology graph for a market."""
        normalized_market = market.lower()
        if normalized_market in self._graphs:
            return self._graphs[normalized_market]

        cache_key = self._cache_key(normalized_market, "1.0.0")

        try:
            cached_data = await self.redis.get(cache_key)
        except Exception as exc:
            self.logger.error("Failed to fetch topology from cache", market=normalized_market, error=str(exc))
            cached_data = None

        if cached_data:
            if isinstance(cached_data, str):
                cached_data = json.loads(cached_data)
            topology_graph = self._deserialize_topology_graph(cached_data)
            self._graphs[normalized_market] = topology_graph
            return topology_graph

        return None

    async def get_node_info(self, market: str, node_id: str) -> Optional[TopologyNode]:
        """Get information for a specific node."""
        topology = await self.get_topology_graph(market)
        if not topology:
            return None

        return next((node for node in topology.nodes if node.node_id == node_id), None)

    async def get_path_losses(self, market: str, path_id: str) -> Optional[float]:
        """Get losses factor for a specific path."""
        topology = await self.get_topology_graph(market)
        if not topology:
            return None

        path = next((item for item in topology.paths if item.path_id == path_id), None)
        return path.losses_factor if path else None

    async def get_hurdle_rate(self, market: str, path_id: str) -> Optional[float]:
        """Get hurdle rate for a specific path."""
        topology = await self.get_topology_graph(market)
        if not topology:
            return None

        path = next((item for item in topology.paths if item.path_id == path_id), None)
        return path.hurdle_rate_usd_per_mwh if path else None

    async def get_intertie_capacity(self, market: str, intertie_id: str) -> Optional[float]:
        """Get capacity for a specific intertie."""
        topology = await self.get_topology_graph(market)
        if not topology:
            return None

        intertie = next((item for item in topology.interties if item.intertie_id == intertie_id), None)
        return intertie.capacity_mw if intertie else None

    async def refresh_topology(self, market: Optional[str] = None) -> None:
        """Refresh topology data for one or all markets."""
        markets_to_refresh = [market.lower()] if market else list(self.market_topologies.keys())
        await self._load_topology_data(markets_to_refresh)

    def _serialize_topology_graph(self, topology_graph: TopologyGraph) -> Dict[str, Any]:
        """Serialize TopologyGraph to primitive types for caching or responses."""
        return {
            "metadata": {
                "market": topology_graph.market,
                "version": topology_graph.version,
                "generated_at": topology_graph.generated_at.isoformat(),
                "ba_count": topology_graph.ba_count,
                "zone_count": topology_graph.zone_count,
                "node_count": topology_graph.node_count,
                "path_count": topology_graph.path_count,
                "intertie_count": topology_graph.intertie_count,
            },
            "balancing_authorities": [
                {
                    "ba_id": ba.ba_id,
                    "ba_name": ba.ba_name,
                    "iso_rto": ba.iso_rto,
                    "timezone": ba.timezone,
                    "dst_rules": ba.dst_rules,
                }
                for ba in topology_graph.balancing_authorities
            ],
            "zones": [
                {
                    "zone_id": zone.zone_id,
                    "zone_name": zone.zone_name,
                    "ba_id": zone.ba_id,
                    "zone_type": zone.zone_type.value,
                    "load_serving_entity": zone.load_serving_entity,
                }
                for zone in topology_graph.zones
            ],
            "nodes": [
                {
                    "node_id": node.node_id,
                    "node_name": node.node_name,
                    "zone_id": node.zone_id,
                    "node_type": node.node_type.value,
                    "voltage_level_kv": node.voltage_level_kv,
                    "latitude": node.latitude,
                    "longitude": node.longitude,
                    "capacity_mw": node.capacity_mw,
                }
                for node in topology_graph.nodes
            ],
            "paths": [
                {
                    "path_id": path.path_id,
                    "path_name": path.path_name,
                    "from_node": path.from_node,
                    "to_node": path.to_node,
                    "path_type": path.path_type.value,
                    "capacity_mw": path.capacity_mw,
                    "losses_factor": path.losses_factor,
                    "hurdle_rate_usd_per_mwh": path.hurdle_rate_usd_per_mwh,
                    "wheel_rate_usd_per_mwh": path.wheel_rate_usd_per_mwh,
                }
                for path in topology_graph.paths
            ],
            "interties": [
                {
                    "intertie_id": intertie.intertie_id,
                    "intertie_name": intertie.intertie_name,
                    "from_market": intertie.from_market,
                    "to_market": intertie.to_market,
                    "capacity_mw": intertie.capacity_mw,
                    "atc_ttc_ntc": intertie.atc_ttc_ntc,
                    "deliverability_flags": intertie.deliverability_flags,
                }
                for intertie in topology_graph.interties
            ],
        }

    def _deserialize_topology_graph(self, data: Dict[str, Any]) -> TopologyGraph:
        """Reconstruct TopologyGraph from cached data."""
        metadata = data.get("metadata", {})
        generated_at_raw = metadata.get("generated_at", datetime.now(timezone.utc).isoformat())
        generated_at = datetime.fromisoformat(generated_at_raw)

        graph = TopologyGraph(
            market=metadata.get("market", "unknown"),
            version=metadata.get("version", "1.0.0"),
            generated_at=generated_at,
            balancing_authorities=[
                TopologyBA(
                    ba_id=ba["ba_id"],
                    ba_name=ba["ba_name"],
                    iso_rto=ba.get("iso_rto"),
                    timezone=ba["timezone"],
                    dst_rules=ba.get("dst_rules", {}),
                )
                for ba in data.get("balancing_authorities", [])
            ],
            zones=[
                TopologyZone(
                    zone_id=zone["zone_id"],
                    zone_name=zone["zone_name"],
                    ba_id=zone["ba_id"],
                    zone_type=ZoneType(zone["zone_type"]),
                    load_serving_entity=zone.get("load_serving_entity"),
                )
                for zone in data.get("zones", [])
            ],
            nodes=[
                TopologyNode(
                    node_id=node["node_id"],
                    node_name=node["node_name"],
                    zone_id=node["zone_id"],
                    node_type=NodeType(node["node_type"]),
                    voltage_level_kv=node.get("voltage_level_kv"),
                    latitude=node.get("latitude"),
                    longitude=node.get("longitude"),
                    capacity_mw=node.get("capacity_mw"),
                )
                for node in data.get("nodes", [])
            ],
            paths=[
                TopologyPath(
                    path_id=path["path_id"],
                    path_name=path["path_name"],
                    from_node=path["from_node"],
                    to_node=path["to_node"],
                    path_type=PathType(path["path_type"]),
                    capacity_mw=path["capacity_mw"],
                    losses_factor=path["losses_factor"],
                    hurdle_rate_usd_per_mwh=path.get("hurdle_rate_usd_per_mwh"),
                    wheel_rate_usd_per_mwh=path.get("wheel_rate_usd_per_mwh"),
                )
                for path in data.get("paths", [])
            ],
            interties=[
                TopologyIntertie(
                    intertie_id=intertie["intertie_id"],
                    intertie_name=intertie["intertie_name"],
                    from_market=intertie["from_market"],
                    to_market=intertie["to_market"],
                    capacity_mw=intertie["capacity_mw"],
                    atc_ttc_ntc=intertie.get("atc_ttc_ntc", {}),
                    deliverability_flags=intertie.get("deliverability_flags", []),
                )
                for intertie in data.get("interties", [])
            ],
        )

        return graph

    def _cache_key(self, market: str, version: str) -> str:
        """Generate cache key for stored topology."""
        return f"topology:{market}:v{version}"

    async def _handle_get_topology(self, request: web.Request) -> web.Response:
        """HTTP handler for fetching a topology graph."""
        market = request.match_info["market"].lower()
        graph = await self.get_topology_graph(market)
        if not graph:
            return web.json_response({"error": "market_not_found", "market": market}, status=404)

        return web.json_response(self._serialize_topology_graph(graph))

    async def _handle_get_node(self, request: web.Request) -> web.Response:
        """HTTP handler for fetching node information."""
        market = request.match_info["market"].lower()
        node_id = request.match_info["node_id"]

        node = await self.get_node_info(market, node_id)
        if not node:
            return web.json_response({"error": "node_not_found", "market": market, "node_id": node_id}, status=404)

        payload = {
            "node_id": node.node_id,
            "node_name": node.node_name,
            "zone_id": node.zone_id,
            "node_type": node.node_type.value,
            "voltage_level_kv": node.voltage_level_kv,
            "latitude": node.latitude,
            "longitude": node.longitude,
            "capacity_mw": node.capacity_mw,
        }
        return web.json_response(payload)

    async def _handle_get_path(self, request: web.Request) -> web.Response:
        """HTTP handler for fetching path information."""
        market = request.match_info["market"].lower()
        path_id = request.match_info["path_id"]

        topology = await self.get_topology_graph(market)
        if not topology:
            return web.json_response({"error": "market_not_found", "market": market}, status=404)

        path = next((item for item in topology.paths if item.path_id == path_id), None)
        if not path:
            return web.json_response({"error": "path_not_found", "market": market, "path_id": path_id}, status=404)

        payload = {
            "path_id": path.path_id,
            "path_name": path.path_name,
            "from_node": path.from_node,
            "to_node": path.to_node,
            "path_type": path.path_type.value,
            "capacity_mw": path.capacity_mw,
            "losses_factor": path.losses_factor,
            "hurdle_rate_usd_per_mwh": path.hurdle_rate_usd_per_mwh,
            "wheel_rate_usd_per_mwh": path.wheel_rate_usd_per_mwh,
        }
        return web.json_response(payload)


async def main() -> None:
    """Entrypoint for running the topology service."""
    service = TopologyService()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
