"""FastAPI web application for GOD MODE Media Library."""

from __future__ import annotations

import hashlib
import hmac
import logging
import mimetypes
import os
import time
from collections import defaultdict
from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
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
    from ..catalog import Catalog, default_catalog_path

    api_token = os.environ.get("GML_API_TOKEN", "")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from .api import _capture_event_loop
        _capture_event_loop()
        yield

    app = FastAPI(
        title="GOD MODE Media Library",
        version="0.1.0",
        description="Media organizer with metadata-first safety",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
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
        # Allow public share routes without auth
        if path.startswith("/shared/"):
            return await call_next(request)
        # Allow static files, docs, and openapi schema without auth
        if not path.startswith("/api/"):
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

        if not hmac.compare_digest(provided, api_token):
            # For WebSocket upgrades, reject with 403 (WS doesn't support 401)
            if request.headers.get("upgrade", "").lower() == "websocket":
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Invalid or missing API token"},
                )
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

    # ── Public share endpoints (bypass auth) ────────────────────────

    def _open_share_catalog() -> Catalog:
        cat = Catalog(app.state.catalog_path)
        cat.open()
        return cat

    @app.get("/shared/{token}/info")
    async def shared_file_info(token: str):
        """Return metadata about a shared file without downloading."""
        cat = _open_share_catalog()
        try:
            share = cat.get_share_by_token(token)
            if share is None:
                return JSONResponse(status_code=404, content={"detail": "Share not found"})
            if share.get("expired"):
                return JSONResponse(status_code=410, content={"detail": "Share expired"})
            if share.get("max_downloads_reached"):
                return JSONResponse(status_code=410, content={"detail": "Max downloads reached"})

            file_path = Path(share["path"])
            if not file_path.exists():
                return JSONResponse(status_code=404, content={"detail": "File no longer exists"})

            mime, _ = mimetypes.guess_type(str(file_path))
            return {
                "name": file_path.name,
                "size": file_path.stat().st_size,
                "type": mime or "application/octet-stream",
                "has_password": share["has_password"],
                "label": share.get("label", ""),
            }
        finally:
            cat.close()

    @app.get("/shared/{token}")
    async def shared_file_download(
        token: str,
        password: str = Query(default=None),
        request: Request = None,
    ):
        """Download a shared file."""
        cat = _open_share_catalog()
        try:
            share = cat.get_share_by_token(token)
            if share is None:
                return JSONResponse(status_code=404, content={"detail": "Share not found"})
            if share.get("expired"):
                return JSONResponse(status_code=410, content={"detail": "Share expired"})
            if share.get("max_downloads_reached"):
                return JSONResponse(status_code=410, content={"detail": "Max downloads reached"})

            # Password check
            if share["has_password"]:
                provided_pw = password
                if not provided_pw and request:
                    provided_pw = request.headers.get("x-share-password", "")
                if not provided_pw:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Password required"},
                    )
                pw_hash = hashlib.sha256(provided_pw.encode("utf-8")).hexdigest()
                if pw_hash != share["password_hash"]:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "Invalid password"},
                    )

            file_path = Path(share["path"])
            if not file_path.exists():
                return JSONResponse(status_code=404, content={"detail": "File no longer exists"})

            # Increment download count
            cat.increment_download(share["id"])

            mime, _ = mimetypes.guess_type(str(file_path))
            content_type = mime or "application/octet-stream"

            def file_streamer():
                with open(file_path, "rb") as f:
                    while chunk := f.read(1024 * 1024):
                        yield chunk

            return StreamingResponse(
                file_streamer(),
                media_type=content_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{file_path.name}"',
                    "Content-Length": str(file_path.stat().st_size),
                },
            )
        finally:
            cat.close()

    app.include_router(api_router, prefix="/api")

    # Serve static frontend files
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app
