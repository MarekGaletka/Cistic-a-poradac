"""FastAPI web application for GOD MODE Media Library."""

from __future__ import annotations

import hashlib
import hmac
import logging
import mimetypes
import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote as _url_quote

from fastapi import FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .api import router as api_router
from .shared import _close_catalog_pool, _return_catalog

logger = logging.getLogger(__name__)

# Rate limiting state
_rate_limit_hits: dict[str, list[float]] = defaultdict(list)
_RATE_LIMIT_WINDOW = 60.0  # seconds
_RATE_LIMIT_MAX = 120  # requests per window (~2/sec, suitable for single-user)
_RATE_LIMIT_MAX_IPS = 10_000  # max unique IPs tracked

# Auth failure rate limiting — track failed auth attempts per IP
_auth_failures: dict[str, list[float]] = defaultdict(list)
_AUTH_FAILURE_WINDOW = 60.0  # seconds
_AUTH_FAILURE_MAX = 10  # max failures per IP per window
_AUTH_FAILURE_MAX_IPS = 10_000  # max unique IPs tracked

# Share password failure rate limiting — track failed share password attempts per IP
_share_pw_failures: dict[str, list[float]] = defaultdict(list)
_SHARE_PW_FAILURE_WINDOW = 300.0  # 5 minutes
_SHARE_PW_FAILURE_MAX = 10  # max failures per IP per window
_SHARE_PW_FAILURE_MAX_IPS = 10_000


def _prune_rate_dict(d: dict[str, list[float]], window: float, max_ips: int) -> None:
    """Evict entries older than *window* seconds; cap dict to *max_ips*."""
    now = time.monotonic()
    cutoff = now - window
    # Remove stale entries
    stale = [ip for ip, hits in d.items() if not hits or hits[-1] < cutoff]
    for ip in stale:
        del d[ip]
    # Hard cap: if still too many IPs, drop the oldest
    if len(d) > max_ips:
        by_latest = sorted(d.items(), key=lambda kv: kv[1][-1] if kv[1] else 0)
        for ip, _ in by_latest[: len(d) - max_ips]:
            del d[ip]


def create_app(catalog_path: Path | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        catalog_path: Path to SQLite catalog database.
                     If None, uses default (~/.config/gml/catalog.db).
    """
    from ..catalog import Catalog, default_catalog_path

    api_token = os.environ.get("GML_API_TOKEN", "")
    if not api_token:
        logger.warning("GML_API_TOKEN is not set — API authentication is disabled. Set GML_API_TOKEN to secure the API.")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from .api import _capture_event_loop

        _capture_event_loop()
        yield
        _close_catalog_pool()

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

    # CORS — allow configurable origins.
    # In debug/dev mode (GML_DEBUG=1), allow all origins for convenience.
    # In production (default), restrict to localhost only.
    debug_mode = os.environ.get("GML_DEBUG", "").strip() in ("1", "true", "yes")
    origins = os.environ.get("GML_CORS_ORIGINS", "").split(",")
    origins = [o.strip() for o in origins if o.strip()]
    if not origins:
        origins = ["*"] if debug_mode else ["http://localhost:*", "http://127.0.0.1:*"]
    if "*" in origins:
        logger.warning("CORS allow_origins='*' is enabled (GML_DEBUG mode). Do not use in production.")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )

    # CSRF protection: when no API token is set, check Origin header on mutating requests
    # to prevent cross-site request forgery from malicious websites.
    if not api_token:

        @app.middleware("http")
        async def csrf_middleware(request: Request, call_next):
            if request.method in ("POST", "PUT", "DELETE", "PATCH"):
                origin = request.headers.get("origin", "")
                if origin:
                    # Allow localhost origins (any port)
                    from urllib.parse import urlparse

                    parsed = urlparse(origin)
                    if parsed.hostname not in ("localhost", "127.0.0.1", "::1", "[::1]"):
                        return JSONResponse(
                            status_code=403,
                            content={"detail": "CSRF check failed: origin not allowed"},
                        )
            return await call_next(request)

    # Token-based API authentication (when GML_API_TOKEN is set)
    # Localhost IPs are trusted — token auth only enforced for remote clients
    _LOCALHOST_IPS = {"127.0.0.1", "::1", "localhost"}

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

        client_ip = request.client.host if request.client else "unknown"

        # Trust localhost — no token needed for local desktop use
        if client_ip in _LOCALHOST_IPS:
            return await call_next(request)

        # Periodically evict stale entries to bound memory
        _prune_rate_dict(_auth_failures, _AUTH_FAILURE_WINDOW, _AUTH_FAILURE_MAX_IPS)

        # Check if this IP is rate-limited due to too many auth failures
        now = time.monotonic()
        failures = _auth_failures[client_ip]
        cutoff = now - _AUTH_FAILURE_WINDOW
        filtered_failures = [t for t in failures if t > cutoff]
        if len(filtered_failures) > _AUTH_FAILURE_MAX * 2:
            filtered_failures = filtered_failures[-_AUTH_FAILURE_MAX * 2 :]
        _auth_failures[client_ip] = filtered_failures
        if len(_auth_failures[client_ip]) >= _AUTH_FAILURE_MAX:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many authentication failures. Try again later."},
                headers={"Retry-After": str(int(_AUTH_FAILURE_WINDOW))},
            )

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
            logger.warning("API token passed via query parameter — use Authorization header instead for security")

        if not hmac.compare_digest(provided, api_token):
            # Record this auth failure for rate limiting
            _auth_failures[client_ip].append(now)

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

            # Periodically evict stale entries to bound memory
            _prune_rate_dict(_rate_limit_hits, _RATE_LIMIT_WINDOW, _RATE_LIMIT_MAX_IPS)

            hits = _rate_limit_hits[client_ip]
            cutoff = now - _RATE_LIMIT_WINDOW
            # Filter to window and cap list size to prevent memory abuse
            filtered = [t for t in hits if t > cutoff]
            _PER_IP_LIST_CAP = rate_limit_max * 2  # hard cap: 2x rate limit
            if len(filtered) > _PER_IP_LIST_CAP:
                filtered = filtered[-_PER_IP_LIST_CAP:]
            _rate_limit_hits[client_ip] = filtered

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
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' data: blob: https://*.tile.openstreetmap.org; "
            "connect-src 'self' ws: wss:"
        )
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
            _return_catalog(cat)

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
                # Rate limit share password attempts per IP
                client_ip = request.client.host if request and request.client else "unknown"
                now_pw = time.monotonic()
                _prune_rate_dict(_share_pw_failures, _SHARE_PW_FAILURE_WINDOW, _SHARE_PW_FAILURE_MAX_IPS)
                pw_attempts = _share_pw_failures[client_ip]
                pw_cutoff = now_pw - _SHARE_PW_FAILURE_WINDOW
                _share_pw_failures[client_ip] = [t for t in pw_attempts if t > pw_cutoff]
                if len(_share_pw_failures[client_ip]) >= _SHARE_PW_FAILURE_MAX:
                    return JSONResponse(
                        status_code=429,
                        content={"detail": "Too many password attempts. Try again later."},
                        headers={"Retry-After": str(int(_SHARE_PW_FAILURE_WINDOW))},
                    )

                # Prefer X-Share-Password header or POST body over query param
                provided_pw = None
                if request:
                    provided_pw = request.headers.get("x-share-password", "") or None
                if not provided_pw and password:
                    provided_pw = password
                    logger.warning("Share password passed via query parameter — use X-Share-Password header instead for security")
                if not provided_pw:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Password required"},
                    )
                stored = share["password_hash"]
                if stored.count(":") == 2:
                    # New PBKDF2 format: hex(salt):iterations:hex(dk)
                    salt_hex, iter_str, dk_hex = stored.split(":", 2)
                    salt = bytes.fromhex(salt_hex)
                    iterations = int(iter_str)
                    expected_dk = bytes.fromhex(dk_hex)
                    provided_dk = hashlib.pbkdf2_hmac(
                        "sha256",
                        provided_pw.encode("utf-8"),
                        salt,
                        iterations,
                    )
                    pw_ok = hmac.compare_digest(provided_dk, expected_dk)
                elif ":" in stored:
                    # Legacy PBKDF2 format: hex(salt):hex(dk) with 100K iterations
                    salt_hex, dk_hex = stored.split(":", 1)
                    salt = bytes.fromhex(salt_hex)
                    expected_dk = bytes.fromhex(dk_hex)
                    provided_dk = hashlib.pbkdf2_hmac(
                        "sha256",
                        provided_pw.encode("utf-8"),
                        salt,
                        100_000,
                    )
                    pw_ok = hmac.compare_digest(provided_dk, expected_dk)
                else:
                    # Legacy SHA-256 format (pre-migration shares)
                    pw_hash = hashlib.sha256(provided_pw.encode("utf-8")).hexdigest()
                    pw_ok = hmac.compare_digest(pw_hash, stored)
                if not pw_ok:
                    _share_pw_failures[client_ip].append(now_pw)
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
                    "Content-Disposition": "attachment; filename*=UTF-8''" + _url_quote(file_path.name, safe=""),
                    "Content-Length": str(file_path.stat().st_size),
                },
            )
        finally:
            _return_catalog(cat)

    app.include_router(api_router, prefix="/api")

    # Serve static frontend files — inject API token into index.html
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        index_html = static_dir / "index.html"

        @app.get("/{full_path:path}")
        async def serve_spa(request: Request, full_path: str):
            # Serve specific static files directly
            file_path = static_dir / full_path
            if full_path and file_path.exists() and file_path.is_file():
                content_type, _ = mimetypes.guess_type(str(file_path))
                return Response(
                    content=file_path.read_bytes(),
                    media_type=content_type or "application/octet-stream",
                )
            # SPA fallback — serve index.html with injected token
            if index_html.exists():
                html = index_html.read_text(encoding="utf-8")
                if api_token:
                    # Inject token meta tag so frontend can authenticate API calls
                    html = html.replace(
                        "</head>",
                        f'  <meta name="gml-api-token" content="{api_token}">\n</head>',
                    )
                return Response(content=html, media_type="text/html")
            return JSONResponse(status_code=404, content={"detail": "Not found"})

    return app
