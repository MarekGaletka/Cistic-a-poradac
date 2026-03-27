from __future__ import annotations

import sys
from dataclasses import dataclass, field, fields
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ImportError:
        tomllib = None  # type: ignore[assignment]


@dataclass
class GMLConfig:
    """Resolved configuration with CLI > project-local > global precedence."""

    min_size_kb: int = 500
    large_file_threshold_mb: int = 500
    protect_asset_components: bool = True
    prefer_earliest_origin_time: bool = True
    prefer_richer_metadata: bool = True
    prefer_roots: list[str] = field(default_factory=list)
    exiftool_bin: str = "exiftool"
    person_prefix: str = "Person"
    geocode_min_delay_seconds: float = 1.1
    max_dimension: int = 1600
    eps: float = 0.5
    min_samples: int = 2
    scan_workers: int = 4

    # Cache
    thumbnail_cache: bool = True  # Persist generated thumbnails on disk
    thumbnail_cache_dir: str = ""  # Custom path (empty = ~/.config/gml/cache/thumbnails)

    # Web / rate limiting
    rate_limit_per_minute: int = 120  # API rate limit (requests/min); 0 = disabled

    # Deduplication rules
    dedup_strategy: str = "richness"  # richness | newest | largest | manual
    dedup_similarity_threshold: int = 10  # Hamming distance for perceptual hash similarity
    dedup_auto_resolve: bool = False  # Auto-resolve duplicates without confirmation
    dedup_merge_metadata: bool = True  # Merge metadata from all duplicates into survivor
    dedup_quarantine_path: str = ""  # Custom quarantine path (empty = default ~/.config/gml/quarantine)
    dedup_exclude_extensions: list[str] = field(default_factory=list)  # Extensions to skip
    dedup_exclude_paths: list[str] = field(default_factory=list)  # Path patterns to skip
    dedup_min_file_size_kb: int = 0  # Skip files smaller than this


def _global_config_path() -> Path:
    return Path.home() / ".config" / "gml" / "config.toml"


def _project_config_path() -> Path:
    return Path.cwd() / "gml.toml"


def _load_toml(path: Path) -> dict:
    if tomllib is None:
        return {}
    if not path.is_file():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def load_config(
    cli_overrides: dict | None = None,
    global_path: Path | None = None,
    project_path: Path | None = None,
) -> GMLConfig:
    """Load configuration with precedence: CLI args > project-local > global > defaults.

    Args:
        cli_overrides: Dict of CLI-provided values (only non-None entries are used).
        global_path: Override path for global config (default: ~/.config/gml/config.toml).
        project_path: Override path for project config (default: ./gml.toml).
    """
    global_path = global_path or _global_config_path()
    project_path = project_path or _project_config_path()

    global_conf = _load_toml(global_path)
    project_conf = _load_toml(project_path)

    merged: dict = {}
    merged.update(global_conf)
    merged.update(project_conf)
    if cli_overrides:
        merged.update({k: v for k, v in cli_overrides.items() if v is not None})

    config = GMLConfig()
    type_errors: list[str] = []
    for f in fields(config):
        if f.name in merged:
            value = merged[f.name]
            # Strict type validation: reject wrong types instead of silently coercing.
            # TOML natively produces correct types; mismatches indicate a config error.
            if f.type == "bool":
                if not isinstance(value, bool):
                    type_errors.append(
                        f"{f.name}: expected bool, got {type(value).__name__} ({value!r})"
                    )
                    continue
            elif f.type == "int":
                # Allow int but reject bool (bool is a subclass of int in Python)
                if isinstance(value, bool) or not isinstance(value, int):
                    type_errors.append(
                        f"{f.name}: expected int, got {type(value).__name__} ({value!r})"
                    )
                    continue
            elif f.type == "float":
                # Accept int or float (TOML may produce int for "1" vs "1.0")
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    type_errors.append(
                        f"{f.name}: expected float, got {type(value).__name__} ({value!r})"
                    )
                    continue
                value = float(value)
            elif f.type == "str":
                if not isinstance(value, str):
                    type_errors.append(
                        f"{f.name}: expected str, got {type(value).__name__} ({value!r})"
                    )
                    continue
            elif f.type == "list[str]" and (not isinstance(value, list) or (
                value and not all(isinstance(v, str) for v in value)
            )):
                type_errors.append(
                    f"{f.name}: expected list of strings, got {type(value).__name__} ({value!r})"
                )
                continue
            object.__setattr__(config, f.name, value)

    if type_errors:
        raise ConfigValidationError(
            "Config type mismatch (check your TOML values):\n"
            + "\n".join(f"  - {e}" for e in type_errors)
        )

    validate_config(config)

    # Validate dedup_quarantine_path is not a system directory
    if config.dedup_quarantine_path:
        resolved_qpath = Path(config.dedup_quarantine_path).resolve()
        _FORBIDDEN_ROOTS = {"/", "/bin", "/sbin", "/usr", "/etc", "/var", "/System", "/Library", "/tmp"}
        if str(resolved_qpath) in _FORBIDDEN_ROOTS:
            raise ConfigValidationError(
                f"dedup_quarantine_path must not be a system directory, got: {resolved_qpath}"
            )
        object.__setattr__(config, "dedup_quarantine_path", str(resolved_qpath))

    return config


class ConfigValidationError(ValueError):
    """Raised when a configuration value is out of its allowed range."""


def validate_config(config: GMLConfig) -> None:
    """Validate configuration values are within acceptable ranges.

    Raises:
        ConfigValidationError: If any value is out of range.
    """
    errors: list[str] = []

    if not (1 <= config.scan_workers <= 32):
        errors.append(f"scan_workers must be 1-32, got {config.scan_workers}")

    if not (0.01 <= config.eps <= 2.0):
        errors.append(f"eps must be 0.01-2.0, got {config.eps}")

    if not (1 <= config.min_samples <= 100):
        errors.append(f"min_samples must be 1-100, got {config.min_samples}")

    if config.dedup_strategy not in ("richness", "newest", "largest", "manual"):
        errors.append(f"dedup_strategy must be one of richness/newest/largest/manual, got {config.dedup_strategy}")
    if not (1 <= config.dedup_similarity_threshold <= 64):
        errors.append(f"dedup_similarity_threshold must be 1-64, got {config.dedup_similarity_threshold}")

    if errors:
        raise ConfigValidationError("Invalid configuration:\n" + "\n".join(f"  - {e}" for e in errors))


def format_config(config: GMLConfig) -> str:
    """Format config as TOML string for display."""
    lines = ["# GOD MODE Media Library — resolved configuration", ""]
    for f in fields(config):
        value = getattr(config, f.name)
        if isinstance(value, bool):
            lines.append(f"{f.name} = {'true' if value else 'false'}")
        elif isinstance(value, str):
            lines.append(f'{f.name} = "{value}"')
        elif isinstance(value, list):
            items = ", ".join(f'"{v}"' for v in value)
            lines.append(f"{f.name} = [{items}]")
        else:
            lines.append(f"{f.name} = {value}")
    return "\n".join(lines) + "\n"
