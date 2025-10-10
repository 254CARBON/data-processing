"""Internal query API for projections."""

from typing import Optional

import structlog
from aiohttp import web

from shared.storage.redis import RedisClient, RedisConfig

from ..output.served_cache import ServedProjectionCache


logger = structlog.get_logger(__name__)


class InternalAPI:
    """Internal query API for projections backed by Redis cache."""
    
    def __init__(self, config):
        self.config = config
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self.redis = RedisClient(
            RedisConfig(
                url=self._build_redis_url(),
                max_connections=config.redis_pool_max,
                timeout=config.processing_timeout,
            )
        )
        self.served_cache = ServedProjectionCache(
            self.redis,
            default_ttl=getattr(self.config, "cache_ttl_seconds", 3600),
        )
        self.port = int(getattr(config, "internal_api_port", 8085))
        
    async def start(self) -> None:
        """Start the internal API."""
        await self.served_cache.ensure_connected()
        
        self.app = web.Application()
        self.app.router.add_get("/projections/latest_price", self.get_latest_price)
        self.app.router.add_get("/projections/curve_snapshot", self.get_curve_snapshot)
        self.app.router.add_get("/projections/custom", self.get_custom_projection)
        
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        bind_port = self.port
        try:
            self.site = web.TCPSite(self.runner, "0.0.0.0", bind_port)
            await self.site.start()
        except OSError as exc:
            if bind_port == 0:
                raise
            logger.warning(
                "Internal API port unavailable, retrying with ephemeral port",
                requested_port=bind_port,
                error=str(exc),
            )
            self.site = web.TCPSite(self.runner, "0.0.0.0", 0)
            await self.site.start()
            sock = self.site._server.sockets[0]
            bind_port = sock.getsockname()[1]
            self.port = bind_port
        else:
            if self.site and self.site._server and self.site._server.sockets:
                bind_port = self.site._server.sockets[0].getsockname()[1]
                self.port = bind_port

        logger.info("Internal API started", port=bind_port)
        
    async def stop(self) -> None:
        """Stop the internal API."""
        if self.runner:
            await self.runner.cleanup()
            self.site = None
        await self.served_cache.close()
        logger.info("Internal API stopped")
        
    async def get_latest_price(self, request: web.Request) -> web.Response:
        """Get latest price projection from Redis."""
        instrument_id = request.query.get("instrument_id")
        tenant_id = request.query.get("tenant_id")
        
        if not instrument_id or not tenant_id:
            return web.json_response(
                {"error": "Missing instrument_id or tenant_id"},
                status=400,
            )
        
        projection = await self.served_cache.get_latest_price(tenant_id, instrument_id)
        
        if projection is None:
            return web.json_response(
                {"error": "Projection not found"},
                status=404,
            )
        
        return web.json_response(projection)
            
    async def get_curve_snapshot(self, request: web.Request) -> web.Response:
        """Get curve snapshot projection from Redis."""
        instrument_id = request.query.get("instrument_id")
        tenant_id = request.query.get("tenant_id")
        horizon = request.query.get("horizon")
        
        if not all([instrument_id, tenant_id, horizon]):
            return web.json_response(
                {"error": "Missing instrument_id, tenant_id, or horizon"},
                status=400,
            )
        
        projection = await self.served_cache.get_curve_snapshot(tenant_id, instrument_id, horizon)
        
        if projection is None:
            return web.json_response(
                {"error": "Projection not found"},
                status=404,
            )
        
        return web.json_response(projection)
            
    async def get_custom_projection(self, request: web.Request) -> web.Response:
        """Get custom projection from Redis."""
        instrument_id = request.query.get("instrument_id")
        tenant_id = request.query.get("tenant_id")
        projection_type = request.query.get("projection_type")
        
        if not all([instrument_id, tenant_id, projection_type]):
            return web.json_response(
                {"error": "Missing instrument_id, tenant_id, or projection_type"},
                status=400,
            )
        
        projection = await self.served_cache.get_custom_projection(tenant_id, projection_type, instrument_id)
        
        if projection is None:
            return web.json_response(
                {"error": "Projection not found"},
                status=404,
            )
        
        return web.json_response(projection)
    
    def _build_redis_url(self) -> str:
        """Construct Redis connection URL."""
        credentials = ""
        if getattr(self.config, "redis_password", ""):
            credentials = f":{self.config.redis_password}@"
        return f"redis://{credentials}{self.config.redis_host}:{self.config.redis_port}/{self.config.redis_database}"
