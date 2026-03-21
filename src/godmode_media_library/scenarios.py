"""Scenario engine for GOD MODE Media Library.

A scenario is a saved, reusable workflow — a sequence of steps that can be
executed with one click. Examples:

  - "Připojil jsem 4TB disk" → deep scan → integrity check → reorganize
  - "iPhone záloha"          → scan iPhone DCIM → dedup → copy to NAS
  - "Týdenní údržba"         → integrity check → dedup resolve → cleanup quarantine

Scenarios are stored as JSON in ~/.config/gml/scenarios.json.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

_SCENARIOS_PATH = Path.home() / ".config" / "gml" / "scenarios.json"

# ---------------------------------------------------------------------------
# Step types — each maps to an action the engine can execute
# ---------------------------------------------------------------------------

STEP_TYPES = {
    "deep_scan": {
        "label_key": "scenario.step_deep_scan",
        "icon": "\U0001f50d",
        "config_fields": [],
    },
    "app_mine": {
        "label_key": "scenario.step_app_mine",
        "icon": "\U0001f4f1",
        "config_fields": ["app_ids"],
    },
    "integrity_check": {
        "label_key": "scenario.step_integrity",
        "icon": "\U0001f6e1\ufe0f",
        "config_fields": [],
    },
    "scan": {
        "label_key": "scenario.step_scan",
        "icon": "\U0001f4f7",
        "config_fields": ["roots", "workers"],
    },
    "reorganize": {
        "label_key": "scenario.step_reorganize",
        "icon": "\U0001f4e6",
        "config_fields": ["sources", "destination", "structure_pattern", "deduplicate", "merge_metadata", "delete_originals"],
    },
    "dedup_resolve": {
        "label_key": "scenario.step_dedup",
        "icon": "\U0001f4cb",
        "config_fields": ["strategy"],
    },
    "quarantine_cleanup": {
        "label_key": "scenario.step_quarantine_cleanup",
        "icon": "\U0001f5d1\ufe0f",
        "config_fields": ["older_than_days"],
    },
    "photorec": {
        "label_key": "scenario.step_photorec",
        "icon": "\U0001f4be",
        "config_fields": ["source", "output_dir"],
    },
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ScenarioStep:
    """A single step in a scenario."""
    type: str  # one of STEP_TYPES keys
    config: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class ScenarioTrigger:
    """Optional trigger condition for auto-running a scenario."""
    type: str = "manual"  # manual | volume_mount | schedule
    volume_name: str = ""  # for volume_mount trigger
    schedule_cron: str = ""  # for schedule trigger (future)


@dataclass
class Scenario:
    """A complete reusable scenario."""
    id: str = ""
    name: str = ""
    description: str = ""
    icon: str = "\U0001f3ac"
    color: str = "#58a6ff"
    steps: list[ScenarioStep] = field(default_factory=list)
    trigger: ScenarioTrigger = field(default_factory=ScenarioTrigger)
    created_at: float = 0.0
    last_run_at: float | None = None
    run_count: int = 0

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]
        if not self.created_at:
            self.created_at = time.time()


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _load_scenarios() -> list[Scenario]:
    """Load all scenarios from disk."""
    if not _SCENARIOS_PATH.exists():
        return []
    try:
        data = json.loads(_SCENARIOS_PATH.read_text())
        scenarios = []
        for item in data:
            steps = [ScenarioStep(**s) for s in item.get("steps", [])]
            trigger_data = item.get("trigger", {})
            trigger = ScenarioTrigger(**trigger_data) if trigger_data else ScenarioTrigger()
            sc = Scenario(
                id=item.get("id", ""),
                name=item.get("name", ""),
                description=item.get("description", ""),
                icon=item.get("icon", "\U0001f3ac"),
                color=item.get("color", "#58a6ff"),
                steps=steps,
                trigger=trigger,
                created_at=item.get("created_at", 0),
                last_run_at=item.get("last_run_at"),
                run_count=item.get("run_count", 0),
            )
            scenarios.append(sc)
        return scenarios
    except Exception as e:
        logger.error("Failed to load scenarios: %s", e)
        return []


def _save_scenarios(scenarios: list[Scenario]) -> None:
    """Persist scenarios to disk."""
    _SCENARIOS_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = []
    for sc in scenarios:
        d = {
            "id": sc.id,
            "name": sc.name,
            "description": sc.description,
            "icon": sc.icon,
            "color": sc.color,
            "steps": [asdict(s) for s in sc.steps],
            "trigger": asdict(sc.trigger),
            "created_at": sc.created_at,
            "last_run_at": sc.last_run_at,
            "run_count": sc.run_count,
        }
        data.append(d)
    _SCENARIOS_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def list_scenarios() -> list[dict]:
    """Return all scenarios as dicts."""
    return [_scenario_to_dict(sc) for sc in _load_scenarios()]


def get_scenario(scenario_id: str) -> dict | None:
    """Get a single scenario by ID."""
    for sc in _load_scenarios():
        if sc.id == scenario_id:
            return _scenario_to_dict(sc)
    return None


def create_scenario(data: dict) -> dict:
    """Create a new scenario."""
    scenarios = _load_scenarios()
    steps = [ScenarioStep(**s) for s in data.get("steps", [])]
    trigger_data = data.get("trigger", {})
    trigger = ScenarioTrigger(**trigger_data) if trigger_data else ScenarioTrigger()
    sc = Scenario(
        name=data.get("name", "Nový scénář"),
        description=data.get("description", ""),
        icon=data.get("icon", "\U0001f3ac"),
        color=data.get("color", "#58a6ff"),
        steps=steps,
        trigger=trigger,
    )
    scenarios.append(sc)
    _save_scenarios(scenarios)
    return _scenario_to_dict(sc)


def update_scenario(scenario_id: str, data: dict) -> dict | None:
    """Update an existing scenario."""
    scenarios = _load_scenarios()
    for sc in scenarios:
        if sc.id == scenario_id:
            sc.name = data.get("name", sc.name)
            sc.description = data.get("description", sc.description)
            sc.icon = data.get("icon", sc.icon)
            sc.color = data.get("color", sc.color)
            if "steps" in data:
                sc.steps = [ScenarioStep(**s) for s in data["steps"]]
            if "trigger" in data:
                sc.trigger = ScenarioTrigger(**data["trigger"])
            _save_scenarios(scenarios)
            return _scenario_to_dict(sc)
    return None


def delete_scenario(scenario_id: str) -> bool:
    """Delete a scenario. Returns True if found and deleted."""
    scenarios = _load_scenarios()
    new_list = [sc for sc in scenarios if sc.id != scenario_id]
    if len(new_list) == len(scenarios):
        return False
    _save_scenarios(new_list)
    return True


def duplicate_scenario(scenario_id: str) -> dict | None:
    """Duplicate a scenario with a new ID."""
    scenarios = _load_scenarios()
    for sc in scenarios:
        if sc.id == scenario_id:
            new_sc = Scenario(
                name=sc.name + " (kopie)",
                description=sc.description,
                icon=sc.icon,
                color=sc.color,
                steps=[ScenarioStep(type=s.type, config=dict(s.config), enabled=s.enabled) for s in sc.steps],
                trigger=ScenarioTrigger(),  # Reset trigger for copy
            )
            scenarios.append(new_sc)
            _save_scenarios(scenarios)
            return _scenario_to_dict(new_sc)
    return None


def mark_scenario_run(scenario_id: str) -> None:
    """Update last_run_at and run_count after execution."""
    scenarios = _load_scenarios()
    for sc in scenarios:
        if sc.id == scenario_id:
            sc.last_run_at = time.time()
            sc.run_count += 1
            break
    _save_scenarios(scenarios)


def get_templates() -> list[dict]:
    """Return built-in scenario templates."""
    return [
        {
            "id": "tpl_full_disk",
            "name": "Kompletní zpracování disku",
            "description": "Recovery → kontrola integrity → sken → deduplikace → reorganizace",
            "icon": "\U0001f4bd",
            "color": "#58a6ff",
            "steps": [
                {"type": "deep_scan", "config": {}, "enabled": True},
                {"type": "integrity_check", "config": {}, "enabled": True},
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
                {"type": "reorganize", "config": {"structure_pattern": "year_month", "deduplicate": True}, "enabled": True},
            ],
        },
        {
            "id": "tpl_iphone_backup",
            "name": "Záloha z iPhonu",
            "description": "Naskenuje iPhone/iPad DCIM, deduplikuje a zkopíruje na cíl",
            "icon": "\U0001f4f1",
            "color": "#3fb950",
            "steps": [
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
                {"type": "reorganize", "config": {"structure_pattern": "year_month", "deduplicate": True}, "enabled": True},
            ],
        },
        {
            "id": "tpl_weekly_maintenance",
            "name": "Týdenní údržba",
            "description": "Kontrola integrity, vyřešení duplicit, úklid karantény",
            "icon": "\U0001f527",
            "color": "#d29922",
            "steps": [
                {"type": "integrity_check", "config": {}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
                {"type": "quarantine_cleanup", "config": {"older_than_days": 30}, "enabled": True},
            ],
        },
        {
            "id": "tpl_rescue",
            "name": "Záchrana dat z disku",
            "description": "PhotoRec recovery + hloubkový sken + kontrola integrity",
            "icon": "\U0001f6d1",
            "color": "#f85149",
            "steps": [
                {"type": "photorec", "config": {}, "enabled": True},
                {"type": "deep_scan", "config": {}, "enabled": True},
                {"type": "integrity_check", "config": {}, "enabled": True},
            ],
        },
        {
            "id": "tpl_quick_dedup",
            "name": "Rychlá deduplikace",
            "description": "Sken + okamžité vyřešení duplicit",
            "icon": "\u26a1",
            "color": "#a371f7",
            "steps": [
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
            ],
        },
    ]


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------

def execute_scenario(
    scenario_id: str,
    catalog_path: str,
    progress_fn: Callable | None = None,
) -> dict:
    """Execute all enabled steps in a scenario sequentially.

    Returns a summary dict with per-step results.
    """
    sc_data = get_scenario(scenario_id)
    if not sc_data:
        return {"error": f"Scénář {scenario_id} nenalezen"}

    steps = [s for s in sc_data["steps"] if s.get("enabled", True)]
    total_steps = len(steps)
    results: list[dict] = []

    for idx, step in enumerate(steps):
        step_type = step["type"]
        step_config = step.get("config", {})
        step_info = STEP_TYPES.get(step_type, {})

        if progress_fn:
            progress_fn({
                "phase": "step",
                "step_index": idx,
                "total_steps": total_steps,
                "step_type": step_type,
                "step_icon": step_info.get("icon", ""),
                "progress_pct": int((idx / max(total_steps, 1)) * 100),
            })

        try:
            result = _execute_step(step_type, step_config, catalog_path, progress_fn)
            results.append({
                "step_type": step_type,
                "status": "completed",
                "result": result,
            })
        except Exception as e:
            logger.exception("Step %s failed", step_type)
            results.append({
                "step_type": step_type,
                "status": "failed",
                "error": str(e),
            })
            # Continue with next step even if one fails

    # Mark the scenario as run
    mark_scenario_run(scenario_id)

    completed = sum(1 for r in results if r["status"] == "completed")
    failed = sum(1 for r in results if r["status"] == "failed")

    if progress_fn:
        progress_fn({
            "phase": "complete",
            "progress_pct": 100,
            "completed_steps": completed,
            "failed_steps": failed,
        })

    return {
        "scenario_id": scenario_id,
        "scenario_name": sc_data["name"],
        "total_steps": total_steps,
        "completed": completed,
        "failed": failed,
        "step_results": results,
    }


def _execute_step(step_type: str, config: dict, catalog_path: str, progress_fn: Callable | None) -> dict:
    """Execute a single step."""

    if step_type == "deep_scan":
        from .recovery import deep_scan
        result = deep_scan()
        return {"files_found": result.files_found, "total_size": result.total_size}

    if step_type == "app_mine":
        from .recovery import mine_app_media
        app_ids = config.get("app_ids")
        results = mine_app_media(app_ids=app_ids)
        total_files = sum(r.files_found for r in results)
        total_size = sum(r.total_size for r in results)
        apps_with_files = sum(1 for r in results if r.files_found > 0)
        return {"total_files": total_files, "total_size": total_size, "apps_with_media": apps_with_files}

    if step_type == "integrity_check":
        from .recovery import check_integrity
        result = check_integrity(catalog_path=catalog_path)
        return {"total_checked": result.total_checked, "corrupted": result.corrupted, "healthy": result.healthy}

    if step_type == "scan":
        from .scanner import incremental_scan
        from .config import GMLConfig
        cfg = GMLConfig.load()
        roots = config.get("roots") or [str(r) for r in cfg.roots]
        workers = config.get("workers", 4)
        stats = incremental_scan(
            roots=[Path(r) for r in roots] if roots else [],
            catalog_path=catalog_path,
            workers=workers,
        )
        return {"scanned": getattr(stats, "total_files", 0) if stats else 0}

    if step_type == "reorganize":
        # Reorganize requires manual destination — just return a note
        return {"note": "Reorganizace vyžaduje ruční konfiguraci zdrojů a cíle"}

    if step_type == "dedup_resolve":
        from .catalog import Catalog
        cat = Catalog(catalog_path)
        cat.open()
        try:
            groups = cat.query_duplicates(limit=10000)
            resolved = 0
            for g in groups.get("groups", []):
                if len(g.get("files", [])) > 1:
                    resolved += 1
            return {"groups_found": len(groups.get("groups", [])), "note": "Duplicity připraveny k řešení"}
        finally:
            cat.close()

    if step_type == "quarantine_cleanup":
        from .recovery import list_quarantine, delete_from_quarantine
        import time as _time
        older_than_days = config.get("older_than_days", 30)
        entries = list_quarantine()
        old_paths = []
        cutoff = _time.time() - (older_than_days * 86400)
        for e in entries:
            # If we have quarantine_date, check age
            if e.quarantine_date:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(e.quarantine_date)
                    if dt.timestamp() < cutoff:
                        old_paths.append(e.path)
                except Exception:
                    pass
            else:
                # No date — check file mtime
                try:
                    from pathlib import Path as _P
                    mtime = _P(e.path).stat().st_mtime
                    if mtime < cutoff:
                        old_paths.append(e.path)
                except Exception:
                    pass

        if old_paths:
            result = delete_from_quarantine(old_paths)
            return {"cleaned": result.get("deleted", 0)}
        return {"cleaned": 0, "note": "Žádné staré soubory v karanténě"}

    if step_type == "photorec":
        return {"note": "PhotoRec vyžaduje ruční výběr disku"}

    return {"note": f"Neznámý typ kroku: {step_type}"}


# ---------------------------------------------------------------------------
# Volume mount detection (macOS)
# ---------------------------------------------------------------------------

def check_volume_triggers() -> list[dict]:
    """Check if any mounted volumes match scenario triggers.

    Returns list of scenarios that should be triggered.
    """
    scenarios = _load_scenarios()
    triggered = []

    mounted_volumes = set()
    volumes_dir = Path("/Volumes")
    if volumes_dir.exists():
        for vol in volumes_dir.iterdir():
            if vol.is_dir() and not vol.name.startswith("."):
                mounted_volumes.add(vol.name)

    for sc in scenarios:
        if sc.trigger.type == "volume_mount" and sc.trigger.volume_name:
            if sc.trigger.volume_name in mounted_volumes:
                triggered.append(_scenario_to_dict(sc))

    return triggered


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scenario_to_dict(sc: Scenario) -> dict:
    return {
        "id": sc.id,
        "name": sc.name,
        "description": sc.description,
        "icon": sc.icon,
        "color": sc.color,
        "steps": [asdict(s) for s in sc.steps],
        "trigger": asdict(sc.trigger),
        "created_at": sc.created_at,
        "last_run_at": sc.last_run_at,
        "run_count": sc.run_count,
    }
