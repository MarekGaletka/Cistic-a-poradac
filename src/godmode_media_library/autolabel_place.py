from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .asset_sets import build_asset_membership
from .audit import collect_file_records
from .labels import load_labels_table, merge_label_updates, write_labels_table
from .utils import ensure_dir, write_tsv

PLACE_EXTS = {
    "jpg",
    "jpeg",
    "png",
    "heic",
    "heif",
    "gif",
    "tif",
    "tiff",
    "bmp",
    "webp",
    "mov",
    "mp4",
    "m4v",
    "avi",
    "mkv",
    "mts",
    "3gp",
    "raw",
    "dng",
    "cr2",
    "cr3",
    "nef",
    "arw",
    "orf",
    "rw2",
}

_LAT_KEYS = (
    "GPSLatitude",
    "Composite:GPSLatitude",
    "EXIF:GPSLatitude",
    "XMP:GPSLatitude",
)
_LON_KEYS = (
    "GPSLongitude",
    "Composite:GPSLongitude",
    "EXIF:GPSLongitude",
    "XMP:GPSLongitude",
)
_COORD_KEYS = (
    "QuickTime:GPSCoordinates",
    "Composite:GPSPosition",
    "GPSPosition",
    "GPSCoordinates",
)


@dataclass(frozen=True)
class AutoPlaceResult:
    labels_out: Path
    report_path: Path
    missing_path: Path
    scanned_files: int
    candidate_files: int
    gps_files: int
    reverse_geocoded: int
    touched_labels: int
    changed_labels: int
    unresolved_candidates: int
    exiftool_used: str


def _chunks(items: list[Path], size: int) -> list[list[Path]]:
    out: list[list[Path]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _to_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _parse_coord_string(text: str) -> tuple[float, float] | None:
    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", text)
    if len(nums) < 2:
        return None

    lat = float(nums[0])
    lon = float(nums[1])
    upper = text.upper()
    if "S" in upper and lat > 0:
        lat = -lat
    if "W" in upper and lon > 0:
        lon = -lon
    return _normalize_lat_lon(lat, lon)


def _normalize_lat_lon(lat: float, lon: float) -> tuple[float, float] | None:
    if lat < -90 or lat > 90:
        return None
    if lon < -180 or lon > 180:
        return None
    return (lat, lon)


def _coords_from_exif(entry: dict[str, object]) -> tuple[float, float] | None:
    lat: float | None = None
    lon: float | None = None

    for key in _LAT_KEYS:
        if key in entry:
            lat = _to_float(entry[key])
            if lat is not None:
                break

    for key in _LON_KEYS:
        if key in entry:
            lon = _to_float(entry[key])
            if lon is not None:
                break

    if lat is not None and lon is not None:
        return _normalize_lat_lon(lat, lon)

    for key in _COORD_KEYS:
        value = entry.get(key)
        if not isinstance(value, str):
            continue
        parsed = _parse_coord_string(value)
        if parsed is not None:
            return parsed

    return None


def extract_gps_with_exiftool(paths: list[Path], exiftool_bin: str = "exiftool") -> tuple[dict[Path, tuple[float, float]], str]:
    binary = shutil.which(exiftool_bin) if "/" not in exiftool_bin else exiftool_bin
    if not binary:
        raise RuntimeError(
            "ExifTool is not available. Install ExifTool to enable auto-place GPS extraction."
        )

    mapping: dict[Path, tuple[float, float]] = {}

    for chunk in _chunks(paths, 200):
        cmd = [
            binary,
            "-j",
            "-n",
            "-q",
            "-q",
            "-api",
            "LargeFileSupport=1",
            "-GPSLatitude",
            "-GPSLongitude",
            "-Composite:GPSLatitude",
            "-Composite:GPSLongitude",
            "-EXIF:GPSLatitude",
            "-EXIF:GPSLongitude",
            "-XMP:GPSLatitude",
            "-XMP:GPSLongitude",
            "-QuickTime:GPSCoordinates",
            "-GPSPosition",
            "-Composite:GPSPosition",
        ]
        cmd.extend(str(p) for p in chunk)
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode not in (0, 1):
            raise RuntimeError(f"ExifTool failed: {proc.stderr.strip()}")
        if not proc.stdout.strip():
            continue

        try:
            rows = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse ExifTool JSON output: {exc}") from exc

        for row in rows:
            if not isinstance(row, dict):
                continue
            src = row.get("SourceFile")
            if not isinstance(src, str):
                continue
            path = Path(src).expanduser().resolve()
            coords = _coords_from_exif(row)
            if coords is not None:
                mapping[path] = coords

    return mapping, str(binary)


def _coord_key(lat: float, lon: float, decimals: int = 5) -> str:
    return f"{lat:.{decimals}f},{lon:.{decimals}f}"


def _coord_label(lat: float, lon: float) -> str:
    return f"GPS {_coord_key(lat, lon, decimals=5)}"


def _load_cache(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in data.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def _save_cache(path: Path, cache: dict[str, str]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(cache, ensure_ascii=True, indent=2), encoding="utf-8")


def _format_geocode_label(address: dict[str, str]) -> str:
    locality = (
        address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("municipality")
        or address.get("hamlet")
    )
    county = address.get("county")
    state = address.get("state")
    country = address.get("country")

    if locality and country:
        return f"{locality}, {country}"
    if county and country:
        return f"{county}, {country}"
    if state and country:
        return f"{state}, {country}"
    if country:
        return country
    return ""


def reverse_geocode_coords(
    coords: set[tuple[float, float]],
    *,
    cache_path: Path,
    user_agent: str = "godmode-media-library",
    min_delay_seconds: float = 1.1,
) -> tuple[dict[tuple[float, float], str], int]:
    try:
        from geopy.extra.rate_limiter import RateLimiter
        from geopy.geocoders import Nominatim
    except Exception as exc:  # pragma: no cover - optional dependency path
        raise RuntimeError(
            "Reverse geocoding requires geopy. Install with: pip install geopy"
        ) from exc

    cache = _load_cache(cache_path)
    geolocator = Nominatim(user_agent=user_agent, timeout=10)
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=min_delay_seconds)

    results: dict[tuple[float, float], str] = {}
    api_calls = 0

    for lat, lon in sorted(coords):
        key = _coord_key(lat, lon)
        cached = cache.get(key)
        if cached:
            results[(lat, lon)] = cached
            continue

        label = ""
        try:
            location = reverse((lat, lon), language="en")
            if location and isinstance(location.raw, dict):
                address = location.raw.get("address")
                if isinstance(address, dict):
                    normalized = {
                        str(k): str(v)
                        for k, v in address.items()
                        if isinstance(k, str) and isinstance(v, str)
                    }
                    label = _format_geocode_label(normalized)
        except Exception:
            label = ""

        api_calls += 1
        if not label:
            label = _coord_label(lat, lon)

        cache[key] = label
        results[(lat, lon)] = label

    _save_cache(cache_path, cache)
    return results, api_calls


def auto_place_labels(
    *,
    roots: list[Path],
    labels_in: Path | None,
    labels_out: Path,
    report_dir: Path,
    exiftool_bin: str = "exiftool",
    reverse_geocode: bool = False,
    geocode_cache_path: Path | None = None,
    geocode_min_delay_seconds: float = 1.1,
    overwrite_place: bool = False,
) -> AutoPlaceResult:
    records = collect_file_records(roots)
    candidate_paths = sorted({rec.path.resolve() for rec in records if rec.ext.lower() in PLACE_EXTS})

    gps_map: dict[Path, tuple[float, float]] = {}
    exiftool_used = ""
    if candidate_paths:
        gps_map, exiftool_used = extract_gps_with_exiftool(candidate_paths, exiftool_bin=exiftool_bin)

    path_to_key, _, key_to_exts = build_asset_membership(candidate_paths)
    key_to_paths: dict[str, list[Path]] = {k: [] for k in key_to_exts}
    for p, key in path_to_key.items():
        key_to_paths.setdefault(key, []).append(p)

    place_by_unit: dict[str, tuple[float, float]] = {}
    for path, coords in gps_map.items():
        key = path_to_key.get(path)
        uid = f"asset::{key}" if key else f"path::{path}"
        place_by_unit.setdefault(uid, coords)

    all_updates: dict[Path, dict[str, str]] = {}
    unresolved: list[Path] = []

    geocoded: dict[tuple[float, float], str] = {}
    reverse_calls = 0
    if reverse_geocode and place_by_unit:
        cache_path = geocode_cache_path or (report_dir / "geocode_cache.json")
        geocoded, reverse_calls = reverse_geocode_coords(
            set(place_by_unit.values()),
            cache_path=cache_path,
            min_delay_seconds=geocode_min_delay_seconds,
        )

    for path in candidate_paths:
        key = path_to_key.get(path)
        uid = f"asset::{key}" if key else f"path::{path}"
        coords = place_by_unit.get(uid)
        if coords is None:
            unresolved.append(path)
            continue

        lat, lon = coords
        if reverse_geocode:
            place_label = geocoded.get(coords, _coord_label(lat, lon))
        else:
            place_label = _coord_label(lat, lon)

        if key:
            members = key_to_paths.get(key, [path])
            for m in members:
                all_updates[m.resolve()] = {"place": place_label}
        else:
            all_updates[path.resolve()] = {"place": place_label}

    header, table = load_labels_table(labels_in)
    touched, changed = merge_label_updates(
        table,
        all_updates,
        overwrite_place=overwrite_place,
        overwrite_people=False,
    )
    write_labels_table(labels_out, header, table)

    ensure_dir(report_dir)
    missing_path = report_dir / "auto_place_missing.tsv"
    write_tsv(missing_path, ["path"], ((str(p),) for p in sorted(set(unresolved), key=str)))

    report_path = report_dir / "auto_place_report.json"
    report = {
        "roots": [str(r) for r in roots],
        "labels_in": str(labels_in) if labels_in else "",
        "labels_out": str(labels_out),
        "scanned_files": len(records),
        "candidate_files": len(candidate_paths),
        "gps_files": len(gps_map),
        "asset_or_file_units_with_gps": len(place_by_unit),
        "reverse_geocode_enabled": reverse_geocode,
        "reverse_geocode_api_calls": reverse_calls,
        "touched_labels": touched,
        "changed_labels": changed,
        "unresolved_candidates": len(set(unresolved)),
        "exiftool_used": exiftool_used,
        "missing_path": str(missing_path),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

    return AutoPlaceResult(
        labels_out=labels_out,
        report_path=report_path,
        missing_path=missing_path,
        scanned_files=len(records),
        candidate_files=len(candidate_paths),
        gps_files=len(gps_map),
        reverse_geocoded=reverse_calls,
        touched_labels=touched,
        changed_labels=changed,
        unresolved_candidates=len(set(unresolved)),
        exiftool_used=exiftool_used,
    )
