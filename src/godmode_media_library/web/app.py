"""FastAPI web application for GOD MODE Media Library."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
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

    app.include_router(api_router, prefix="/api")

    # Serve static frontend files
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app
