"""REST API endpoints for GOD MODE Media Library.

Thin aggregator that assembles sub-routers from the ``routes/`` package.
All shared state, helpers, and Pydantic models live in ``shared.py``.
"""

from __future__ import annotations

from fastapi import APIRouter

from .routes.backup import router as backup_router
from .routes.cloud import _remotes_cache  # noqa: F401 — re-export for tests
from .routes.cloud import router as cloud_router
from .routes.consolidation import router as consolidation_router
from .routes.duplicates import router as duplicates_router
from .routes.faces import router as faces_router
from .routes.files import router as files_router
from .routes.gallery import router as gallery_router
from .routes.recovery import router as recovery_router
from .routes.reorganize import router as reorganize_router
from .routes.scenarios import router as scenarios_router
from .routes.shares import router as shares_router
from .routes.system import router as system_router
from .routes.tags import router as tags_router

# Re-export shared symbols for backward compatibility (tests, app.py)
from .shared import (  # noqa: F401
    _BLOCKED_PREFIXES,
    _DEFAULT_QUARANTINE_ROOT,
    _DEFAULT_SHARE_EXPIRY_HOURS,
    _MAX_COMPLETED_TASKS,
    _TASK_TTL_SECONDS,
    CreateShareRequest,
    CreateTagRequest,
    DedupRulesRequest,
    DeleteRequest,
    DuplicateKeepRequest,
    FavoriteRequest,
    MoveRequest,
    NoteRequest,
    QuarantineRequest,
    RatingRequest,
    RemoveRootRequest,
    RenameItem,
    RenameRequest,
    ReorganizeConfigRequest,
    ReorganizeExecuteRequest,
    RestoreRequest,
    RootsRequest,
    ScanConfig,
    TagFilesRequest,
    TaskStatus,
    _capture_event_loop,
    _check_path_within_roots,
    _create_task,
    _evict_old_plans,
    _evict_old_tasks,
    _finish_task,
    _get_bookmarks,
    _get_configured_roots,
    _get_favorites_list,
    _get_favorites_set,
    _is_path_allowed,
    _notify_ws,
    _open_catalog,
    _reorganize_plans,
    _reorganize_plans_lock,
    _row_to_dict,
    _sanitize_path,
    _set_configured_roots,
    _set_favorites,
    _task_to_msg,
    _tasks,
    _tasks_lock,
    _thumb_cache_dir,
    _thumb_cache_get,
    _thumb_cache_key,
    _thumb_cache_put,
    _update_progress,
    _ws_connections,
    _ws_lock,
)

router = APIRouter()

# Include all sub-routers (no prefix — routes already carry full paths)
router.include_router(system_router)
router.include_router(files_router)
router.include_router(duplicates_router)
router.include_router(tags_router)
router.include_router(shares_router)
router.include_router(gallery_router)
router.include_router(scenarios_router)
router.include_router(reorganize_router)
router.include_router(recovery_router)
router.include_router(backup_router)
router.include_router(cloud_router)
router.include_router(faces_router)
router.include_router(consolidation_router)
