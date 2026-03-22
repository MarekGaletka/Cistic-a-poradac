"""FastAPI web application for GOD MODE Media Library."""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .api import router as api_router

logger = logging.getLogger(__name__)

# Rate limiting state
_rate_limit_hits: dict[str, list[float]] = defaultdict(list)
_RATE_LIMIT_WINDOW = 60.0  # seconds
_RATE_LIMIT_MAX = 600  # requests per window (generous for local UI)


def create_app(catalog_path: Path | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        catalog_path: Path to SQLite catalog database.
                     If None, uses default (~/.config/gml/catalog.db).
    """
    from ..catalog import default_catalog_path

    api_token = os.environ.get("GML_API_TOKEN", "")

    app = FastAPI(
        title="GOD MODE Media Library",
        version="0.1.0",
        description="Media organizer with metadata-first safety",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.state.catalog_path = catalog_path or default_catalog_path()

    # CORS — allow configurable origins (default: localhost only)
    origins = os.environ.get("GML_CORS_ORIGINS", "").split(",")
    origins = [o.strip() for o in origins if o.strip()]
    if not origins:
        origins = ["http://localhost:*", "http://127.0.0.1:*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )

    # Token-based API authentication (when GML_API_TOKEN is set)
    @app.middleware("http")
    async def auth_middleware(request: Request, call_next):
        if not api_token:
            return await call_next(request)

        path = request.url.path
        # Allow static files, docs, and openapi schema without auth
        if not path.startswith("/api/"):
            return await call_next(request)
        # Allow WebSocket upgrade (auth checked in WS handler if needed)
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await call_next(request)

        # Check Bearer token or X-API-Token header
        auth_header = request.headers.get("authorization", "")
        token_header = request.headers.get("x-api-token", "")
        token_param = request.query_params.get("token", "")

        provided = ""
        if auth_header.startswith("Bearer "):
            provided = auth_header[7:]
        elif token_header:
            provided = token_header
        elif token_param:
            provided = token_param

        if provided != api_token:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API token"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        return await call_next(request)

    # Rate limiting middleware — only active when GML_RATE_LIMIT is set
    # (disabled by default for local/desktop use)
    rate_limit_max = int(os.environ.get("GML_RATE_LIMIT", "0"))

    if rate_limit_max > 0:

        @app.middleware("http")
        async def rate_limit_middleware(request: Request, call_next):
            if not request.url.path.startswith("/api/"):
                return await call_next(request)

            client_ip = request.client.host if request.client else "unknown"
            now = time.monotonic()

            hits = _rate_limit_hits[client_ip]
            cutoff = now - _RATE_LIMIT_WINDOW
            _rate_limit_hits[client_ip] = [t for t in hits if t > cutoff]

            if len(_rate_limit_hits[client_ip]) >= rate_limit_max:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded. Try again later."},
                    headers={"Retry-After": str(int(_RATE_LIMIT_WINDOW))},
                )

            _rate_limit_hits[client_ip].append(now)
            return await call_next(request)

    # Security headers + no-cache for static assets (development)
    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        # Prevent browser caching of JS/CSS so changes are picked up immediately
        path = request.url.path
        if path.endswith((".js", ".css", ".html")):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response

    app.include_router(api_router, prefix="/api")

    # Serve static frontend files
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app
