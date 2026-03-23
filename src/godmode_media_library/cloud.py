"""Cloud storage support via rclone + native platform detection.

Supports: MEGA, pCloud, Google Drive, Google Photos, iCloud, OneDrive,
Dropbox, S3/GCS, and any rclone-compatible backend.

Two access modes:
  1. Mount mode: rclone mount creates a virtual filesystem, GML scans it like local
  2. Sync mode: rclone copy/sync downloads files to a local cache directory
"""

from __future__ import annotations

import contextlib
import json
import logging
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# Known cloud provider configs for rclone
PROVIDERS = {
    "mega": {
        "label": "MEGA",
        "rclone_type": "mega",
        "setup": "rclone config create mega mega user YOUR_EMAIL pass YOUR_PASSWORD",
        "media_paths": ["", "Camera Uploads", "Photos"],
        "icon": "\U0001f4e6",
    },
    "pcloud": {
        "label": "pCloud",
        "rclone_type": "pcloud",
        "setup": "rclone config create pcloud pcloud  # (otevře OAuth prohlížeč)",
        "media_paths": ["", "My Pictures", "My Videos"],
        "icon": "\u2601\ufe0f",
    },
    "drive": {
        "label": "Google Drive",
        "rclone_type": "drive",
        "setup": "rclone config create gdrive drive  # (otevře OAuth prohlížeč)",
        "media_paths": ["", "Photos", "My Drive/Photos"],
        "icon": "\U0001f4be",
    },
    "google photos": {
        "label": "Google Photos",
        "rclone_type": "google photos",
        "setup": "rclone config create gphotos \"google photos\"  # (OAuth + read-only)",
        "media_paths": ["media/all", "media/by-year"],
        "icon": "\U0001f4f7",
    },
    "onedrive": {
        "label": "OneDrive",
        "rclone_type": "onedrive",
        "setup": "rclone config create onedrive onedrive  # (OAuth prohlížeč)",
        "media_paths": ["", "Pictures", "Photos"],
        "icon": "\U0001f4c1",
    },
    "dropbox": {
        "label": "Dropbox",
        "rclone_type": "dropbox",
        "setup": "rclone config create dropbox dropbox  # (OAuth prohlížeč)",
        "media_paths": ["", "Camera Uploads", "Photos"],
        "icon": "\U0001f4e5",
    },
    "s3": {
        "label": "Amazon S3",
        "rclone_type": "s3",
        "setup": "rclone config create s3 s3 provider AWS access_key_id KEY secret_access_key SECRET",
        "media_paths": [""],
        "icon": "\u2601\ufe0f",
    },
}


@dataclass
class RcloneRemote:
    name: str
    type: str
    provider_label: str = ""
    icon: str = "\u2601\ufe0f"

    def __post_init__(self):
        for ptype, info in PROVIDERS.items():
            if self.type == info["rclone_type"] or self.type == ptype:
                self.provider_label = info["label"]
                self.icon = info["icon"]
                break
        if not self.provider_label:
            self.provider_label = self.type.title()


@dataclass
class CloudSource:
    """A cloud storage source with status information."""
    name: str
    provider: str
    remote_type: str
    mounted: bool = False
    mount_path: str = ""
    synced: bool = False
    sync_path: str = ""
    icon: str = "\u2601\ufe0f"
    available: bool = False
    file_count: int = 0
    total_size: int = 0


@dataclass
class SyncResult:
    """Result of a cloud sync/copy operation."""
    remote: str
    remote_path: str
    local_path: str
    files_transferred: int = 0
    bytes_transferred: int = 0
    errors: int = 0
    elapsed_seconds: float = 0


def check_rclone() -> bool:
    """Return True if rclone is available on PATH."""
    return shutil.which("rclone") is not None


def rclone_version() -> str | None:
    """Return rclone version string or None."""
    if not check_rclone():
        return None
    try:
        # Just get version from first line
        result2 = subprocess.run(
            ["rclone", "version"],
            capture_output=True, text=True, timeout=5,
        )
        first_line = result2.stdout.strip().splitlines()[0] if result2.stdout else ""
        return first_line.replace("rclone ", "").strip()
    except (subprocess.TimeoutExpired, OSError, IndexError):
        return None


def list_remotes() -> list[RcloneRemote]:
    """List configured rclone remotes."""
    if not check_rclone():
        return []
    try:
        result = subprocess.run(
            ["rclone", "listremotes", "--long"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return []
        remotes = []
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) >= 2:
                name = parts[0].rstrip(":")
                rtype = parts[1]
                remotes.append(RcloneRemote(name=name, type=rtype))
        return remotes
    except (subprocess.TimeoutExpired, OSError):
        return []


def rclone_ls(remote: str, path: str = "", recursive: bool = False) -> list[dict]:
    """List files/dirs at a remote path using rclone lsjson."""
    if not check_rclone():
        raise RuntimeError("rclone is not installed")

    target = f"{remote}:{path}" if path else f"{remote}:"
    cmd = ["rclone", "lsjson", target]
    if recursive:
        cmd.append("--recursive")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"rclone lsjson failed: {result.stderr.strip()}")
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("rclone lsjson timed out") from e


def rclone_size(remote: str, path: str = "") -> dict:
    """Get total size and file count for a remote path."""
    if not check_rclone():
        raise RuntimeError("rclone is not installed")

    target = f"{remote}:{path}" if path else f"{remote}:"
    try:
        result = subprocess.run(
            ["rclone", "size", target, "--json"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            return {"count": 0, "bytes": 0}
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return {"count": 0, "bytes": 0}


def rclone_about(remote: str) -> dict:
    """Get storage usage info (total, used, free) for a remote."""
    if not check_rclone():
        return {}
    try:
        result = subprocess.run(
            ["rclone", "about", f"{remote}:", "--json"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return {}
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return {}


def rclone_copy(
    remote: str,
    remote_path: str,
    local_path: str,
    *,
    include_pattern: str = "",
    dry_run: bool = False,
    progress_fn=None,
) -> SyncResult:
    """Copy files from remote to local directory.

    Args:
        remote: rclone remote name
        remote_path: path within remote
        local_path: local destination directory
        include_pattern: glob pattern for files to include (e.g. "*.{jpg,png,mp4}")
        dry_run: if True, only show what would be copied
        progress_fn: callback(stats_dict) for progress updates
    """
    if not check_rclone():
        raise RuntimeError("rclone is not installed")

    import time
    start = time.monotonic()

    source = f"{remote}:{remote_path}" if remote_path else f"{remote}:"
    Path(local_path).mkdir(parents=True, exist_ok=True)

    cmd = [
        "rclone", "copy", source, local_path,
        "--stats-one-line",
        "--stats", "2s",
        "-v",
    ]
    if include_pattern:
        cmd.extend(["--include", include_pattern])
    if dry_run:
        cmd.append("--dry-run")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600,
        )
        elapsed = time.monotonic() - start

        # Parse stats from stderr
        files_transferred = 0
        bytes_transferred = 0
        errors = 0
        for line in (result.stderr or "").splitlines():
            if "Transferred:" in line and "Bytes" not in line:
                # e.g. "Transferred:      42 / 42, 100%"
                parts = line.split(":")
                if len(parts) >= 2:
                    with contextlib.suppress(ValueError, IndexError):
                        files_transferred = int(parts[1].strip().split("/")[0].strip().split(",")[0].strip())
            if "Errors:" in line:
                with contextlib.suppress(ValueError, IndexError):
                    errors = int(line.split(":")[1].strip())

        return SyncResult(
            remote=remote,
            remote_path=remote_path,
            local_path=local_path,
            files_transferred=files_transferred,
            bytes_transferred=bytes_transferred,
            errors=errors,
            elapsed_seconds=elapsed,
        )
    except subprocess.TimeoutExpired:
        return SyncResult(
            remote=remote, remote_path=remote_path, local_path=local_path,
            errors=1, elapsed_seconds=time.monotonic() - start,
        )


def rclone_mount(remote: str, mount_point: str | None = None) -> tuple[str, bool]:
    """Mount a remote as a local filesystem.

    Returns (mount_path, success).
    """
    if not check_rclone():
        raise RuntimeError("rclone is not installed")

    if mount_point is None:
        mount_point = str(Path.home() / "mnt" / remote)

    Path(mount_point).mkdir(parents=True, exist_ok=True)

    # Check if already mounted
    if _is_mount_active(mount_point):
        return mount_point, True

    cmd = [
        "rclone", "mount", f"{remote}:", mount_point,
        "--daemon",
        "--vfs-cache-mode", "full",
        "--vfs-cache-max-age", "24h",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            logger.info("Mounted %s at %s", remote, mount_point)
            return mount_point, True
        logger.warning("Failed to mount %s: %s", remote, result.stderr)
        return mount_point, False
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.warning("Mount error for %s: %s", remote, exc)
        return mount_point, False


def rclone_unmount(mount_point: str) -> bool:
    """Unmount a FUSE mount. Returns True on success."""
    try:
        # macOS uses umount, Linux uses fusermount
        cmd = ["umount", mount_point] if platform.system() == "Darwin" else ["fusermount", "-u", mount_point]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _is_mount_active(mount_point: str) -> bool:
    """Check if a path is an active mount point."""
    try:
        result = subprocess.run(
            ["mount"], capture_output=True, text=True, timeout=5,
        )
        return mount_point in result.stdout
    except (subprocess.TimeoutExpired, OSError):
        return False


def resolve_root(root_spec: str) -> Path:
    """Resolve a root specification to a local path.

    Supports:
    - Regular paths: /Users/me/Photos -> Path("/Users/me/Photos")
    - Home-relative: ~/Photos -> expanded
    - rclone remote: gdrive:Photos -> checks if mounted, returns mount path
    """
    # Regular or home-relative path
    if ":" not in root_spec or (len(root_spec) >= 2 and root_spec[1] == ":"):
        expanded = Path(root_spec).expanduser()
        if not expanded.exists():
            raise ValueError(f"Path does not exist: {expanded}")
        return expanded

    # rclone remote syntax: remote:path
    remote, _, remote_path = root_spec.partition(":")

    # Check common mount points
    mount_candidates = [
        Path.home() / "mnt" / remote,
        Path.home() / remote,
        Path(f"/mnt/{remote}"),
        Path(f"/Volumes/{remote}"),
    ]

    for mount in mount_candidates:
        full = mount / remote_path if remote_path else mount
        if full.exists():
            return full

    raise ValueError(
        f"Remote '{root_spec}' is not mounted locally. "
        f"Mount it first with: rclone mount {remote}: ~/mnt/{remote} --daemon"
    )


def mount_command(remote: str, mount_point: str | None = None) -> str:
    """Generate rclone mount command string."""
    if mount_point is None:
        mount_point = f"~/mnt/{remote}"
    return f"rclone mount {remote}: {mount_point} --daemon --vfs-cache-mode full"


# ── Native platform paths ──────────────────────────────────────


def detect_icloud_paths() -> list[dict]:
    """Detect iCloud photo/media paths on macOS."""
    if platform.system() != "Darwin":
        return []

    paths = []

    # iCloud Drive
    icloud_drive = Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs"
    if icloud_drive.exists():
        paths.append({
            "name": "iCloud Drive",
            "path": str(icloud_drive),
            "type": "icloud_drive",
            "icon": "\U0001f34e",
        })

    # iCloud Photos (managed by Photos.app)
    photos_lib = Path.home() / "Pictures" / "Photos Library.photoslibrary"
    if photos_lib.exists():
        masters = photos_lib / "originals"
        if not masters.exists():
            masters = photos_lib / "Masters"
        if masters.exists():
            paths.append({
                "name": "iCloud Photos (lokální kopie)",
                "path": str(masters),
                "type": "icloud_photos",
                "icon": "\U0001f4f7",
            })

    # iCloud app data — grouped into a single entry with sub-paths
    icloud_apps = Path.home() / "Library" / "Mobile Documents"
    if icloud_apps.exists():
        sub_paths = []
        for app_dir in sorted(icloud_apps.iterdir()):
            if not app_dir.is_dir():
                continue
            docs = app_dir / "Documents"
            if docs.exists():
                app_name = app_dir.name.split("~")[-1]
                sub_paths.append({
                    "name": app_name,
                    "path": str(docs),
                })
        if sub_paths:
            paths.append({
                "name": "iCloud Apps",
                "path": str(icloud_apps),
                "type": "icloud_apps",
                "icon": "\U0001f34e",
                "app_count": len(sub_paths),
                "apps": sub_paths,
            })

    return paths


def detect_native_cloud_paths() -> list[dict]:
    """Detect cloud storage paths that are natively synced (no rclone needed)."""
    paths = []

    # iCloud
    paths.extend(detect_icloud_paths())

    # Google Drive for Desktop (macOS/Windows)
    gdrive_paths = [
        Path.home() / "Google Drive",
        Path.home() / "My Drive",
        Path("/Volumes/GoogleDrive"),
        Path.home() / "Library" / "CloudStorage" / "GoogleDrive",
    ]
    # macOS CloudStorage symlinks
    cloud_storage = Path.home() / "Library" / "CloudStorage"
    if cloud_storage.exists():
        for d in cloud_storage.iterdir():
            if d.is_dir() or d.is_symlink():
                name = d.name
                icon = "\u2601\ufe0f"
                if "Google" in name or "gdrive" in name.lower():
                    icon = "\U0001f4be"
                elif "OneDrive" in name:
                    icon = "\U0001f4c1"
                elif "Dropbox" in name:
                    icon = "\U0001f4e5"
                elif "pCloud" in name:
                    icon = "\u2601\ufe0f"
                elif "MEGA" in name or "mega" in name.lower():
                    icon = "\U0001f4e6"
                paths.append({
                    "name": name,
                    "path": str(d),
                    "type": "native_sync",
                    "icon": icon,
                })

    for gp in gdrive_paths:
        if gp.exists() and not any(p["path"] == str(gp) for p in paths):
            paths.append({
                "name": "Google Drive",
                "path": str(gp),
                "type": "native_sync",
                "icon": "\U0001f4be",
            })

    # Dropbox
    dropbox = Path.home() / "Dropbox"
    if dropbox.exists() and not any(p["path"] == str(dropbox) for p in paths):
        paths.append({
            "name": "Dropbox",
            "path": str(dropbox),
            "type": "native_sync",
            "icon": "\U0001f4e5",
        })

    # MEGA Desktop
    mega = Path.home() / "MEGA"
    if mega.exists() and not any(p["path"] == str(mega) for p in paths):
        paths.append({
            "name": "MEGA",
            "path": str(mega),
            "type": "native_sync",
            "icon": "\U0001f4e6",
        })

    # pCloud Drive
    pcloud_paths = [
        Path.home() / "pCloudDrive",
        Path("/Volumes/pCloud"),
    ]
    for pp in pcloud_paths:
        if pp.exists() and not any(p["path"] == str(pp) for p in paths):
            paths.append({
                "name": "pCloud",
                "path": str(pp),
                "type": "native_sync",
                "icon": "\u2601\ufe0f",
            })

    return paths


# ── Default sync directory ──


def default_sync_dir() -> Path:
    """Default directory for cloud sync downloads."""
    return Path.home() / ".config" / "gml" / "cloud"


# ── Provider guide ──


def provider_setup_guide(provider_key: str) -> dict:
    """Return setup instructions for a specific provider."""
    info = PROVIDERS.get(provider_key)
    if not info:
        return {"error": f"Unknown provider: {provider_key}"}

    return {
        "provider": info["label"],
        "icon": info["icon"],
        "rclone_type": info["rclone_type"],
        "steps": [
            {"step": 1, "title": "Nainstaluj rclone", "command": "brew install rclone"},
            {"step": 2, "title": "Nastav remote", "command": info["setup"]},
            {
                "step": 3,
                "title": "Připoj (mount)",
                "command": (
                    f"mkdir -p ~/mnt/{provider_key} && rclone mount"
                    f" {provider_key}: ~/mnt/{provider_key} --daemon --vfs-cache-mode full"
                ),
            },
            {
                "step": 4,
                "title": "Nebo stáhni (sync)",
                "command": f"rclone copy {provider_key}: ~/.config/gml/cloud/{provider_key} --progress",
            },
        ],
        "media_paths": info["media_paths"],
    }


def get_cloud_status() -> dict:
    """Get comprehensive cloud storage status."""
    rclone_ok = check_rclone()
    version = rclone_version() if rclone_ok else None
    remotes = list_remotes() if rclone_ok else []
    native_paths = detect_native_cloud_paths()

    sources = []

    # Add rclone remotes
    for r in remotes:
        # Check if mounted
        mount_path = str(Path.home() / "mnt" / r.name)
        mounted = Path(mount_path).exists() and _is_mount_active(mount_path)

        # Check if synced
        sync_path = str(default_sync_dir() / r.name)
        synced = Path(sync_path).exists() and any(Path(sync_path).iterdir()) if Path(sync_path).exists() else False

        sources.append({
            "name": r.name,
            "provider": r.provider_label,
            "remote_type": r.type,
            "source_type": "rclone",
            "mounted": mounted,
            "mount_path": mount_path if mounted else "",
            "synced": synced,
            "sync_path": sync_path if synced else "",
            "icon": r.icon,
            "available": mounted or synced,
        })

    # Add native paths (skip grouped entries like icloud_apps — shown in /cloud/native)
    for np in native_paths:
        if np.get("type") == "icloud_apps":
            continue
        sources.append({
            "name": np["name"],
            "provider": np["name"],
            "remote_type": np["type"],
            "source_type": "native",
            "mounted": True,
            "mount_path": np["path"],
            "synced": False,
            "sync_path": "",
            "icon": np["icon"],
            "available": True,
        })

    return {
        "rclone_installed": rclone_ok,
        "rclone_version": version,
        "sources": sources,
        "providers": {k: {"label": v["label"], "icon": v["icon"]} for k, v in PROVIDERS.items()},
    }


def format_cloud_guide() -> str:
    """Return a formatted guide for setting up cloud storage."""
    lines = [
        "Cloud Storage Setup Guide",
        "=" * 40,
        "",
        "1. Install rclone:",
        "   macOS:   brew install rclone",
        "   Linux:   curl https://rclone.org/install.sh | sudo bash",
        "   Windows: winget install Rclone.Rclone",
        "",
        "2. Configure a remote:",
        "   rclone config",
        "",
        "3. Mount the remote:",
        "   mkdir -p ~/mnt/myremote",
        "   rclone mount myremote: ~/mnt/myremote --daemon --vfs-cache-mode full",
        "",
        "4. Scan with GML:",
        "   gml scan --roots ~/mnt/myremote/Photos",
        "",
        "Supported providers:",
        "  MEGA:          rclone config → 'mega'",
        "  pCloud:        rclone config → 'pcloud'",
        "  Google Drive:  rclone config → 'drive'",
        "  Google Photos: rclone config → 'google photos'  (read-only)",
        "  iCloud:        macOS native: ~/Library/Mobile Documents/",
        "  OneDrive:      rclone config → 'onedrive'",
        "  Dropbox:       rclone config → 'dropbox'",
        "  S3/GCS:        rclone config → 's3'",
    ]
    return "\n".join(lines)
