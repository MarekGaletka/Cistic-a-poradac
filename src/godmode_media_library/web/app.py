"""FastAPI web application for GOD MODE Media Library."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .api import router as api_router


def create_app(catalog_path: Path | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        catalog_path: Path to SQLite catalog database.
                     If None, uses default (~/.config/gml/catalog.db).
    """
    from ..catalog import default_catalog_path

    app = FastAPI(
        title="GOD MODE Media Library",
        version="0.1.0",
        description="Media organizer with metadata-first safety",
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
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # Security headers
    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

    app.include_router(api_router, prefix="/api")

    # Serve static frontend files
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app
