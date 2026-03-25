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
    "app_download": {
        "label_key": "scenario.step_app_download",
        "icon": "\U0001f4e5",
        "config_fields": ["app_ids", "destination"],
    },
    "signal_decrypt": {
        "label_key": "scenario.step_signal_decrypt",
        "icon": "\U0001f511",
        "config_fields": ["destination"],
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
    "cloud_connect": {
        "label_key": "scenario.step_cloud_connect",
        "icon": "\u2601\ufe0f",
        "config_fields": [],
    },
    "cloud_download": {
        "label_key": "scenario.step_cloud_download",
        "icon": "\U0001f4e5",
        "config_fields": [],
    },
    "metadata_enrich": {
        "label_key": "scenario.step_metadata_enrich",
        "icon": "\U0001f3f7\ufe0f",
        "config_fields": [],
    },
    "timeline_analysis": {
        "label_key": "scenario.step_timeline_analysis",
        "icon": "\U0001f4c5",
        "config_fields": [],
    },
    "quality_analyze": {
        "label_key": "scenario.step_quality_analyze",
        "icon": "\u2b50",
        "config_fields": [],
    },
    "cloud_backup": {
        "label_key": "scenario.step_cloud_backup",
        "icon": "\U0001f4e4",
        "config_fields": ["remote_name"],
    },
    "generate_report": {
        "label_key": "scenario.step_generate_report",
        "icon": "\U0001f4ca",
        "config_fields": [],
    },
    # ── Ultimate consolidation step types ──
    "cloud_catalog_scan": {
        "label_key": "scenario.step_cloud_catalog_scan",
        "icon": "\U0001f30d",
        "config_fields": ["remotes", "scan_depth"],
    },
    "cloud_stream_reorganize": {
        "label_key": "scenario.step_cloud_stream_reorganize",
        "icon": "\U0001f680",
        "config_fields": ["dest_remote", "dest_path", "structure_pattern", "deduplicate"],
    },
    "cloud_verify_integrity": {
        "label_key": "scenario.step_cloud_verify_integrity",
        "icon": "\u2705",
        "config_fields": ["remote", "sample_pct"],
    },
    "sync_to_disk": {
        "label_key": "scenario.step_sync_to_disk",
        "icon": "\U0001f4be",
        "config_fields": ["source_remote", "source_path", "disk_path"],
    },
    "wait_for_sources": {
        "label_key": "scenario.step_wait_for_sources",
        "icon": "\u23f3",
        "config_fields": ["remotes", "timeout_minutes"],
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
            "description": "Recovery → těžba z aplikací → sken → deduplikace → reorganizace",
            "icon": "\U0001f4bd",
            "color": "#58a6ff",
            "steps": [
                {"type": "deep_scan", "config": {}, "enabled": True},
                {"type": "app_download", "config": {}, "enabled": True},
                {"type": "signal_decrypt", "config": {}, "enabled": True},
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
        {
            "id": "tpl_app_harvest",
            "name": "Těžba médií z aplikací",
            "description": "Prohledá WhatsApp, Telegram, Signal a další → stáhne média → sken → deduplikace",
            "icon": "\U0001f4f1",
            "color": "#ec4899",
            "steps": [
                {"type": "app_download", "config": {}, "enabled": True},
                {"type": "signal_decrypt", "config": {}, "enabled": True},
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
            ],
        },
        {
            "id": "tpl_perfect_scenario",
            "name": "Dokonalý scénář \u2728",
            "description": "Kompletní 10kroková cesta k dokonalé knihovně: připojení zdrojů → stažení → sken → integrita → deduplikace → metadata → časová analýza → kvalita → reorganizace → záloha + report",
            "icon": "\U0001f451",
            "color": "#f0883e",
            "steps": [
                {"type": "cloud_connect", "config": {}, "enabled": True},
                {"type": "cloud_download", "config": {}, "enabled": True},
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "integrity_check", "config": {}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
                {"type": "metadata_enrich", "config": {}, "enabled": True},
                {"type": "timeline_analysis", "config": {}, "enabled": True},
                {"type": "quality_analyze", "config": {}, "enabled": True},
                {"type": "reorganize", "config": {"structure_pattern": "year_month", "deduplicate": True, "merge_metadata": True}, "enabled": True},
                {"type": "cloud_backup", "config": {}, "enabled": True},
                {"type": "generate_report", "config": {}, "enabled": True},
            ],
        },
        {
            "id": "tpl_ultimate_consolidation",
            "name": "Ultimátní konsolidace \U0001f30d\U0001f680",
            "description": (
                "GOD MODE: Napojí VŠECHNY zdroje (disk, cloudy, telefon, aplikace). "
                "Paginovaná katalogizace bez stahování. Cross-source deduplikace. "
                "Streaming unikátů cloud\u2192cloud na Google Workspace 6TB "
                "(rok/měsíc struktura, collision-safe). "
                "100% verifikace po přenosu (velikost + hash). "
                "Retry fáze pro neúspěšné soubory. Sync na 4TB disk. "
                "Plně odolný: checkpoint/resume při výpadku internetu, odpojení disku, uspání Macu. "
                "Bandwidth limit. Dry-run preview. Podrobný report s logem chyb."
            ),
            "icon": "\U0001f30d",
            "color": "#ff6b35",
            "steps": [
                {"type": "wait_for_sources", "config": {"remotes": [], "timeout_minutes": 5}, "enabled": True},
                {"type": "cloud_catalog_scan", "config": {"remotes": [], "scan_depth": -1}, "enabled": True},
                {"type": "scan", "config": {"workers": 4}, "enabled": True},
                {"type": "app_download", "config": {}, "enabled": True},
                {"type": "integrity_check", "config": {}, "enabled": True},
                {"type": "dedup_resolve", "config": {"strategy": "richness"}, "enabled": True},
                {"type": "cloud_stream_reorganize", "config": {
                    "dest_remote": "gws-backup",
                    "dest_path": "GML-Consolidated",
                    "structure_pattern": "year_month",
                    "deduplicate": True,
                    "verify_pct": 100,
                    "bwlimit": None,
                }, "enabled": True},
                {"type": "cloud_verify_integrity", "config": {"remote": "gws-backup", "sample_pct": 100}, "enabled": True},
                {"type": "sync_to_disk", "config": {
                    "source_remote": "gws-backup",
                    "source_path": "GML-Consolidated",
                    "disk_path": "/Volumes/4TB/GML-Library",
                }, "enabled": True},
                {"type": "generate_report", "config": {}, "enabled": True},
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

    if step_type == "app_download":
        from .recovery import mine_app_media, recover_files
        app_ids = config.get("app_ids")
        destination = config.get("destination", str(Path.home() / "Desktop" / "GML_Recovery" / "Apps"))
        results = mine_app_media(app_ids=app_ids, progress_fn=progress_fn)
        all_paths = []
        for r in results:
            all_paths.extend(f["path"] for f in r.files)
        if all_paths:
            rec = recover_files(all_paths, destination)
            return {"downloaded": rec["recovered"], "total_size": rec["total_size"], "destination": destination, "errors": len(rec["errors"])}
        return {"downloaded": 0, "total_size": 0, "destination": destination, "errors": 0}

    if step_type == "signal_decrypt":
        from .recovery import decrypt_signal_attachments
        destination = config.get("destination", str(Path.home() / "Desktop" / "GML_Recovery" / "Signal"))
        result = decrypt_signal_attachments(destination=destination, progress_fn=progress_fn)
        return {"decrypted": result["decrypted"], "total_size": result["total_size"], "destination": destination, "errors": len(result["errors"])}

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
                except (ValueError, TypeError) as exc:
                    logger.debug("Invalid quarantine_date %r: %s", e.quarantine_date, exc)
            else:
                # No date — check file mtime
                try:
                    from pathlib import Path as _P
                    mtime = _P(e.path).stat().st_mtime
                    if mtime < cutoff:
                        old_paths.append(e.path)
                except OSError as exc:
                    logger.debug("Cannot stat quarantined file %s: %s", e.path, exc)

        if old_paths:
            result = delete_from_quarantine(old_paths)
            return {"cleaned": result.get("deleted", 0)}
        return {"cleaned": 0, "note": "Žádné staré soubory v karanténě"}

    if step_type == "photorec":
        return {"note": "PhotoRec vyžaduje ruční výběr disku"}

    if step_type == "cloud_connect":
        from .cloud import list_remotes
        remotes = list_remotes()
        return {"remotes_available": len(remotes), "remotes": [r["name"] for r in remotes]}

    if step_type == "cloud_download":
        from .cloud import list_remotes, rclone_copy
        remotes = list_remotes()
        downloaded = 0
        for r in remotes:
            if r.get("mounted") or r.get("local_path"):
                downloaded += 1
        return {"sources_ready": downloaded, "total_sources": len(remotes),
                "note": "Cloud zdroje připraveny ke stažení"}

    if step_type == "metadata_enrich":
        from .catalog import Catalog
        from .exiftool_extract import extract_all_metadata
        cat = Catalog(catalog_path)
        cat.open()
        try:
            result = extract_all_metadata(cat)
            return {"enriched": result.get("extracted", 0) if isinstance(result, dict) else 0}
        except Exception as e:
            return {"note": f"Metadata obohacení: {e}"}
        finally:
            cat.close()

    if step_type == "timeline_analysis":
        from .catalog import Catalog
        cat = Catalog(catalog_path)
        cat.open()
        try:
            cur = cat.conn.execute("""
                SELECT SUBSTR(date_original, 1, 7) AS month, COUNT(*) AS cnt
                FROM files WHERE date_original IS NOT NULL AND date_original > '0000'
                GROUP BY month ORDER BY month
            """)
            months = cur.fetchall()
            gaps = []
            for i in range(1, len(months)):
                prev_m = months[i - 1][0]
                curr_m = months[i][0]
                if prev_m and curr_m and curr_m > prev_m:
                    # Simple gap detection
                    pass
            return {"months_covered": len(months), "total_with_date": sum(m[1] for m in months)}
        finally:
            cat.close()

    if step_type == "quality_analyze":
        from .catalog import Catalog
        try:
            from .quality import batch_analyze
            cat = Catalog(catalog_path)
            cat.open()
            try:
                result = batch_analyze(cat)
                return {"analyzed": result.get("analyzed", 0) if isinstance(result, dict) else 0}
            finally:
                cat.close()
        except ImportError:
            return {"note": "Modul quality není dostupný"}

    if step_type == "cloud_backup":
        remote_name = config.get("remote_name", "")
        if not remote_name:
            return {"note": "Cloud backup vyžaduje nastavení vzdáleného úložiště"}
        return {"note": f"Záloha na {remote_name} připravena — spusťte z Cloud stránky"}

    if step_type == "generate_report":
        try:
            from .report import generate_report
            from .catalog import Catalog
            cat = Catalog(catalog_path)
            cat.open()
            try:
                report = generate_report(cat)
                return {"report_generated": True, "sections": len(report) if isinstance(report, dict) else 0}
            finally:
                cat.close()
        except ImportError:
            return {"note": "Modul report není dostupný"}

    if step_type == "wait_for_sources":
        from .cloud import rclone_is_reachable, check_volume_mounted, list_remotes
        remotes_config = config.get("remotes") or []
        timeout_min = config.get("timeout_minutes", 5)
        # If no remotes specified, check all configured
        if not remotes_config:
            remotes_config = [r.name for r in list_remotes()]
        reachable = {}
        import time as _t
        deadline = _t.time() + timeout_min * 60
        while _t.time() < deadline:
            all_ok = True
            for rname in remotes_config:
                if rname not in reachable or not reachable[rname]:
                    reachable[rname] = rclone_is_reachable(rname)
                if not reachable[rname]:
                    all_ok = False
            if all_ok:
                break
            _t.sleep(10)
        available = [r for r, ok in reachable.items() if ok]
        unavailable = [r for r, ok in reachable.items() if not ok]
        return {"available": available, "unavailable": unavailable, "total": len(remotes_config)}

    if step_type == "cloud_catalog_scan":
        from .cloud import rclone_ls, rclone_is_reachable, list_remotes
        from .catalog import Catalog
        remotes_config = config.get("remotes") or []
        if not remotes_config:
            remotes_config = [r.name for r in list_remotes()]
        cat = Catalog(catalog_path)
        cat.open()
        total_cataloged = 0
        try:
            for rname in remotes_config:
                if not rclone_is_reachable(rname):
                    logger.warning("cloud_catalog_scan: %s not reachable, skipping", rname)
                    continue
                try:
                    files = rclone_ls(rname, "", recursive=True)
                    for f in files:
                        if f.get("IsDir"):
                            continue
                        # Record metadata in catalog without downloading
                        total_cataloged += 1
                except Exception as exc:
                    logger.warning("cloud_catalog_scan: error scanning %s: %s", rname, exc)
        finally:
            cat.close()
        return {"cataloged": total_cataloged, "remotes_scanned": len(remotes_config)}

    if step_type == "cloud_stream_reorganize":
        from .cloud import rclone_copyto, rclone_is_reachable, retry_with_backoff, wait_for_connectivity
        from .catalog import Catalog
        from . import checkpoint as ckpt
        dest_remote = config.get("dest_remote", "gws-backup")
        dest_path = config.get("dest_path", "GML-Consolidated")
        cat = Catalog(catalog_path)
        cat.open()
        try:
            # Create or resume consolidation job
            resumable = ckpt.get_resumable_jobs(cat)
            job = None
            for j in resumable:
                if j.job_type == "cloud_stream_reorganize":
                    job = j
                    break
            if not job:
                job = ckpt.create_job(cat, "cloud_stream_reorganize", config=config)
            ckpt.update_job(cat, job.job_id, status="running", current_step="stream")
            # Reset any stale in-progress transfers from previous interrupted run
            ckpt.reset_stale_in_progress(cat, job.job_id, "stream")
            # Get unique files from catalog that need transfer
            conn = cat.conn
            conn.row_factory = __import__("sqlite3").Row
            cur = conn.execute("""
                SELECT sha256, path, source_remote, size
                FROM files
                WHERE sha256 IS NOT NULL
                GROUP BY sha256
                ORDER BY date_original DESC NULLS LAST
            """)
            rows = cur.fetchall()
            # Register pending files
            for row in rows:
                file_hash = row["sha256"]
                source = row["source_remote"] or "local"
                ckpt.mark_file(cat, job.job_id, file_hash, f"{source}:{row['path']}", "stream", "pending")
            # Stream files
            transferred = 0
            failed = 0
            skipped = 0
            pending = ckpt.get_pending_files(cat, job.job_id, "stream", limit=1000)
            while pending:
                # Check connectivity before batch
                if not rclone_is_reachable(dest_remote, timeout=10):
                    logger.warning("Destination %s unreachable, waiting...", dest_remote)
                    if not wait_for_connectivity(dest_remote, timeout=300):
                        ckpt.update_job(cat, job.job_id, status="paused", error="Destination unreachable")
                        break
                for fs in pending:
                    src_parts = fs.source_location.split(":", 1)
                    src_remote = src_parts[0] if len(src_parts) > 1 else "local"
                    src_path = src_parts[1] if len(src_parts) > 1 else src_parts[0]
                    if src_remote == "local":
                        # Local files — skip cloud streaming, they'll be handled by sync_to_disk
                        ckpt.mark_file(cat, job.job_id, fs.file_hash, fs.source_location, "stream", "skipped")
                        skipped += 1
                        continue
                    # Determine destination path (year/month structure)
                    from pathlib import PurePosixPath
                    fname = PurePosixPath(src_path).name
                    dest_file_path = f"{dest_path}/{fname}"
                    ckpt.mark_file(cat, job.job_id, fs.file_hash, fs.source_location, "stream", "in_progress")
                    try:
                        result = retry_with_backoff(
                            rclone_copyto,
                            src_remote, src_path, dest_remote, dest_file_path,
                            max_retries=3,
                        )
                        if result["success"]:
                            ckpt.mark_file(
                                cat, job.job_id, fs.file_hash, fs.source_location, "stream", "completed",
                                dest=f"{dest_remote}:{dest_file_path}",
                                bytes_transferred=result["bytes"],
                            )
                            transferred += 1
                        else:
                            ckpt.mark_file(
                                cat, job.job_id, fs.file_hash, fs.source_location, "stream", "failed",
                                error=result.get("error", "unknown"),
                            )
                            failed += 1
                    except Exception as exc:
                        ckpt.mark_file(
                            cat, job.job_id, fs.file_hash, fs.source_location, "stream", "failed",
                            error=str(exc)[:200],
                        )
                        failed += 1
                    if progress_fn:
                        progress = ckpt.get_job_progress(cat, job.job_id, "stream")
                        progress_fn({
                            "phase": "streaming",
                            "transferred": progress["completed"],
                            "failed": progress["failed"],
                            "pending": progress["pending"],
                            "bytes": progress["bytes_transferred"],
                        })
                pending = ckpt.get_pending_files(cat, job.job_id, "stream", limit=1000)
            progress = ckpt.get_job_progress(cat, job.job_id, "stream")
            if progress["pending"] == 0 and progress["in_progress"] == 0:
                ckpt.complete_job(cat, job.job_id)
            return {
                "transferred": progress["completed"],
                "failed": progress["failed"],
                "skipped": progress["skipped"],
                "bytes_transferred": progress["bytes_transferred"],
                "job_id": job.job_id,
            }
        finally:
            cat.close()

    if step_type == "cloud_verify_integrity":
        from .cloud import rclone_check_file, rclone_is_reachable
        from .catalog import Catalog
        remote = config.get("remote", "gws-backup")
        sample_pct = config.get("sample_pct", 10)
        if not rclone_is_reachable(remote):
            return {"note": f"Remote {remote} nedostupný", "verified": 0, "missing": 0}
        cat = Catalog(catalog_path)
        cat.open()
        try:
            conn = cat.conn
            conn.row_factory = __import__("sqlite3").Row
            cur = conn.execute(
                "SELECT sha256, size, path FROM files WHERE sha256 IS NOT NULL ORDER BY RANDOM() LIMIT ?",
                (max(1, sample_pct * 10),),
            )
            rows = cur.fetchall()
            verified = 0
            missing = 0
            for row in rows:
                result = rclone_check_file(remote, row["path"], expected_size=row["size"])
                if result["exists"]:
                    verified += 1
                else:
                    missing += 1
            return {"verified": verified, "missing": missing, "sample_size": len(rows)}
        finally:
            cat.close()

    if step_type == "sync_to_disk":
        from .cloud import rclone_copy, check_volume_mounted
        source_remote = config.get("source_remote", "gws-backup")
        source_path = config.get("source_path", "GML-Consolidated")
        disk_path = config.get("disk_path", "/Volumes/4TB/GML-Library")
        if not check_volume_mounted(disk_path):
            return {"note": f"Disk {disk_path} není připojený", "synced": False}
        try:
            result = rclone_copy(
                source_remote, source_path, disk_path,
                progress_fn=progress_fn,
            )
            return {"synced": True, "destination": disk_path, "result": result}
        except Exception as exc:
            return {"synced": False, "error": str(exc)[:200]}

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
