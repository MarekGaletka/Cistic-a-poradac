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


def _rclone_bin() -> str:
    """Return the best rclone binary path.

    On macOS, prefer /usr/local/bin/rclone (official binary with FUSE support)
    over Homebrew's version which cannot do rclone mount.
    """
    if platform.system() == "Darwin":
        official = "/usr/local/bin/rclone"
        if Path(official).is_file():
            return official
    return shutil.which("rclone") or "rclone"


# Known cloud provider configs for rclone
PROVIDERS = {
    "mega": {
        "label": "MEGA",
        "rclone_type": "mega",
        "setup": "rclone config create mega mega user YOUR_EMAIL pass YOUR_PASSWORD",
        "media_paths": ["", "Camera Uploads", "Photos"],
        "icon": "\U0001f4e6",
        "auth": "credentials",
        "fields": [
            {"key": "user", "label": "E-mail", "type": "email", "required": True},
            {"key": "pass", "label": "Heslo", "type": "password", "required": True},
        ],
    },
    "pcloud": {
        "label": "pCloud",
        "rclone_type": "pcloud",
        "setup": "rclone config create pcloud pcloud  # (otevře OAuth prohlížeč)",
        "media_paths": ["", "My Pictures", "My Videos"],
        "icon": "\u2601\ufe0f",
        "auth": "oauth",
        "fields": [],
    },
    "drive": {
        "label": "Google Drive",
        "rclone_type": "drive",
        "setup": "rclone config create gdrive drive  # (otevře OAuth prohlížeč)",
        "media_paths": ["", "Photos", "My Drive/Photos"],
        "icon": "\U0001f4be",
        "auth": "oauth",
        "fields": [],
    },
    "google photos": {
        "label": "Google Photos",
        "rclone_type": "google photos",
        "setup": "rclone config create gphotos \"google photos\"  # (OAuth + read-only)",
        "media_paths": ["media/all", "media/by-year"],
        "icon": "\U0001f4f7",
        "auth": "oauth",
        "fields": [],
    },
    "onedrive": {
        "label": "OneDrive",
        "rclone_type": "onedrive",
        "setup": "rclone config create onedrive onedrive  # (OAuth prohlížeč)",
        "media_paths": ["", "Pictures", "Photos"],
        "icon": "\U0001f4c1",
        "auth": "oauth",
        "fields": [],
    },
    "dropbox": {
        "label": "Dropbox",
        "rclone_type": "dropbox",
        "setup": "rclone config create dropbox dropbox  # (OAuth prohlížeč)",
        "media_paths": ["", "Camera Uploads", "Photos"],
        "icon": "\U0001f4e5",
        "auth": "oauth",
        "fields": [],
    },
    "s3": {
        "label": "Amazon S3",
        "rclone_type": "s3",
        "setup": "rclone config create s3 s3 provider AWS access_key_id KEY secret_access_key SECRET",
        "media_paths": [""],
        "icon": "\u2601\ufe0f",
        "auth": "credentials",
        "fields": [
            {"key": "provider", "label": "Provider", "type": "select", "required": True,
             "options": ["AWS", "Cloudflare", "DigitalOcean", "Wasabi", "Other"]},
            {"key": "access_key_id", "label": "Access Key ID", "type": "text", "required": True},
            {"key": "secret_access_key", "label": "Secret Access Key", "type": "password", "required": True},
            {"key": "region", "label": "Region", "type": "text", "required": False},
            {"key": "endpoint", "label": "Endpoint URL", "type": "text", "required": False},
        ],
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


# ── Active OAuth processes ──
_oauth_processes: dict[str, subprocess.Popen] = {}


def create_remote(
    provider_key: str,
    name: str,
    credentials: dict[str, str] | None = None,
) -> dict:
    """Create an rclone remote programmatically.

    For credential-based providers (MEGA, S3): pass credentials dict.
    For OAuth providers: this starts the OAuth flow (opens browser).
    Returns {"success": True/False, "message": str, "oauth": bool}.
    """
    if not check_rclone():
        return {"success": False, "message": "rclone is not installed"}

    info = PROVIDERS.get(provider_key)
    if not info:
        return {"success": False, "message": f"Unknown provider: {provider_key}"}

    rclone_type = info["rclone_type"]

    # Check if remote already exists
    existing = [r.name for r in list_remotes()]
    if name in existing:
        return {"success": False, "message": f"Remote '{name}' already exists"}

    if info.get("auth") == "oauth":
        return _start_oauth_flow(provider_key, name, rclone_type)

    # Credential-based: build rclone config create command
    cmd = [_rclone_bin(), "config", "create", name, rclone_type]
    for field in info.get("fields", []):
        key = field["key"]
        value = (credentials or {}).get(key, "")
        if value:
            cmd.extend([key, value])
        elif field.get("required"):
            return {"success": False, "message": f"Missing required field: {field['label']}"}

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return {"success": False, "message": result.stderr.strip() or "Config creation failed"}
        logger.info("Created remote '%s' (type=%s)", name, rclone_type)
        return {"success": True, "message": f"Remote '{name}' created", "oauth": False}
    except (subprocess.TimeoutExpired, OSError) as exc:
        return {"success": False, "message": str(exc)}


def _start_oauth_flow(provider_key: str, name: str, rclone_type: str) -> dict:
    """Start OAuth authorization flow for a provider.

    Runs `rclone authorize <type>` which opens a browser for the user to log in.
    The process runs in the background; check status with `get_oauth_status()`.
    """
    global _oauth_processes

    # Kill any existing OAuth process for this name
    if name in _oauth_processes:
        with contextlib.suppress(Exception):
            _oauth_processes[name].kill()

    try:
        proc = subprocess.Popen(
            [_rclone_bin(), "authorize", rclone_type],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _oauth_processes[name] = proc
        logger.info("Started OAuth flow for '%s' (type=%s, pid=%d)", name, rclone_type, proc.pid)
        return {
            "success": True,
            "message": "OAuth flow started — complete authorization in the browser window",
            "oauth": True,
            "provider_key": provider_key,
            "remote_name": name,
        }
    except OSError as exc:
        return {"success": False, "message": str(exc)}


def get_oauth_status(name: str) -> dict:
    """Check if an OAuth flow has completed. Returns token if done."""
    proc = _oauth_processes.get(name)
    if not proc:
        return {"status": "not_found"}

    poll = proc.poll()
    if poll is None:
        return {"status": "pending"}

    # Process finished — capture output
    stdout, stderr = proc.communicate(timeout=1)
    del _oauth_processes[name]

    if poll != 0:
        return {"status": "error", "message": stderr.strip() or "OAuth failed"}

    # Extract token JSON from stdout — rclone prints it between braces
    token = _extract_oauth_token(stdout)
    if not token:
        return {"status": "error", "message": "Could not extract OAuth token"}

    return {"status": "completed", "token": token}


def _extract_oauth_token(output: str) -> str | None:
    """Extract the OAuth token JSON from rclone authorize output."""
    # rclone prints: Paste the following into your remote machine --->
    # {"access_token":"...","token_type":"Bearer",...}
    # <---End paste
    lines = output.strip().splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("{") and "access_token" in stripped:
            return stripped
        # Sometimes it's on the line after "--->""
        if "--->" in line and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if next_line.startswith("{"):
                return next_line
    return None


def finalize_oauth(
    provider_key: str,
    name: str,
    token: str,
) -> dict:
    """Create an rclone remote using an OAuth token."""
    if not check_rclone():
        return {"success": False, "message": "rclone is not installed"}

    info = PROVIDERS.get(provider_key)
    if not info:
        return {"success": False, "message": f"Unknown provider: {provider_key}"}

    rclone_type = info["rclone_type"]

    cmd = [_rclone_bin(), "config", "create", name, rclone_type, f"token={token}"]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return {"success": False, "message": result.stderr.strip() or "Config creation failed"}
        logger.info("Created OAuth remote '%s' (type=%s)", name, rclone_type)
        return {"success": True, "message": f"Remote '{name}' connected"}
    except (subprocess.TimeoutExpired, OSError) as exc:
        return {"success": False, "message": str(exc)}


def delete_remote(name: str) -> dict:
    """Delete an rclone remote configuration."""
    if not check_rclone():
        return {"success": False, "message": "rclone is not installed"}
    try:
        result = subprocess.run(
            [_rclone_bin(), "config", "delete", name],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return {"success": False, "message": result.stderr.strip()}
        logger.info("Deleted remote '%s'", name)
        return {"success": True, "message": f"Remote '{name}' removed"}
    except (subprocess.TimeoutExpired, OSError) as exc:
        return {"success": False, "message": str(exc)}


def test_remote(name: str) -> dict:
    """Test if a remote is accessible by listing its root."""
    if not check_rclone():
        return {"success": False, "message": "rclone is not installed"}
    try:
        result = subprocess.run(
            [_rclone_bin(), "lsd", f"{name}:", "--max-depth", "1"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return {"success": True, "message": "Connection OK"}
        return {"success": False, "message": result.stderr.strip() or "Connection failed"}
    except subprocess.TimeoutExpired:
        return {"success": False, "message": "Connection timed out"}
    except OSError as exc:
        return {"success": False, "message": str(exc)}


def check_rclone() -> bool:
    """Return True if rclone is available."""
    return Path(_rclone_bin()).is_file() or shutil.which("rclone") is not None


def rclone_version() -> str | None:
    """Return rclone version string or None."""
    if not check_rclone():
        return None
    try:
        # Just get version from first line
        result2 = subprocess.run(
            [_rclone_bin(), "version"],
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
            [_rclone_bin(), "listremotes", "--long"],
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
    cmd = [_rclone_bin(), "lsjson", target]
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
            [_rclone_bin(), "size", target, "--json"],
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
            [_rclone_bin(), "about", f"{remote}:", "--json"],
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
        _rclone_bin(), "copy", source, local_path,
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


def rclone_upload(
    local_path: str,
    remote: str,
    remote_path: str = "",
    *,
    include_pattern: str = "",
    dry_run: bool = False,
) -> SyncResult:
    """Upload (copy) local files to a remote.

    Args:
        local_path: local source directory
        remote: rclone remote name
        remote_path: destination path within remote (default: root)
        include_pattern: glob pattern for files to include
        dry_run: if True, only show what would be uploaded
    """
    if not check_rclone():
        raise RuntimeError("rclone is not installed")

    import time
    start = time.monotonic()

    destination = f"{remote}:{remote_path}" if remote_path else f"{remote}:"

    cmd = [
        _rclone_bin(), "copy", local_path, destination,
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
            cmd, capture_output=True, text=True, timeout=7200,
        )
        elapsed = time.monotonic() - start

        files_transferred = 0
        bytes_transferred = 0
        errors = 0
        for line in (result.stderr or "").splitlines():
            if "Transferred:" in line and "Bytes" not in line:
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

    # Pre-check: is FUSE available on macOS?
    if platform.system() == "Darwin":
        fuse_available = (
            Path("/Library/Filesystems/macfuse.fs").exists()
            or Path("/Library/Filesystems/osxfuse.fs").exists()
        )
        if not fuse_available:
            raise RuntimeError(
                "macFUSE není nainstalovaný. Nainstalujte: brew install macfuse "
                "nebo stáhněte z https://osxfuse.github.io/. "
                "Alternativně použijte 'Stáhnout lokálně'."
            )

    cmd = [
        _rclone_bin(), "mount", f"{remote}:", mount_point,
        "--daemon",
        "--vfs-cache-mode", "full",
        "--vfs-cache-max-age", "24h",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            logger.info("Mounted %s at %s", remote, mount_point)
            return mount_point, True
        stderr = result.stderr or ""
        logger.warning("Failed to mount %s: %s", remote, stderr)
        # Detect FUSE-related errors (various rclone error messages)
        fuse_keywords = ("cannot find FUSE", "not supported on MacOS when rclone is installed via Homebrew")
        if any(kw in stderr for kw in fuse_keywords):
            raise RuntimeError(
                "rclone mount vyžaduje oficiální rclone binárku (ne z Homebrew). "
                "Stáhněte z https://rclone.org/downloads/ a nainstalujte do /usr/local/bin/rclone. "
                "Alternativně použijte 'Stáhnout lokálně'."
            )
        if "daemon timed out" in stderr.lower() or "daemon exited" in stderr.lower():
            raise RuntimeError(
                "macFUSE není nainstalovaný nebo kext není načtený. "
                "Zkuste: sudo kextload /Library/Filesystems/macfuse.fs/Contents/Extensions/26/macfuse.kext"
            )
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


# ── Cloud-to-cloud copy, verification, and retry utilities ──


def rclone_copyto(
    src_remote: str, src_path: str,
    dst_remote: str, dst_path: str,
    *,
    timeout: int = 600,
) -> dict:
    """Copy a single file between remotes. Streams through local RAM, no disk write.

    Returns {"success": bool, "bytes": int, "elapsed": float, "error": str|None}
    """
    if not check_rclone():
        return {"success": False, "bytes": 0, "elapsed": 0.0, "error": "rclone is not installed"}

    import re
    import time
    start = time.monotonic()

    cmd = [
        _rclone_bin(), "copyto",
        f"{src_remote}:{src_path}",
        f"{dst_remote}:{dst_path}",
        "--retries", "3",
        "--low-level-retries", "10",
        "--stats-one-line",
        "-v",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        elapsed = time.monotonic() - start

        if result.returncode != 0:
            error_msg = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "copyto failed"
            return {"success": False, "bytes": 0, "elapsed": elapsed, "error": error_msg}

        # Parse bytes transferred from stderr stats line
        bytes_transferred = 0
        for line in (result.stderr or "").splitlines():
            # e.g. "Transferred:   1.234 MiB / 1.234 MiB, 100%, 500 KiB/s, ETA 0s"
            m = re.search(r"Transferred:\s+([\d.]+)\s*(\w+)", line)
            if m:
                value = float(m.group(1))
                unit = m.group(2).lower()
                multipliers = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4}
                bytes_transferred = int(value * multipliers.get(unit, 1))

        return {"success": True, "bytes": bytes_transferred, "elapsed": elapsed, "error": None}

    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        logger.warning("rclone copyto timed out after %.1fs: %s:%s -> %s:%s",
                        elapsed, src_remote, src_path, dst_remote, dst_path)
        return {"success": False, "bytes": 0, "elapsed": elapsed, "error": f"Timed out after {timeout}s"}
    except OSError as exc:
        elapsed = time.monotonic() - start
        logger.error("rclone copyto OS error: %s", exc)
        return {"success": False, "bytes": 0, "elapsed": elapsed, "error": str(exc)}


def rclone_check_file(
    remote: str, path: str,
    expected_size: int | None = None,
) -> dict:
    """Check if a file exists on remote, optionally verify size.

    Returns {"exists": bool, "size": int|None, "size_match": bool|None}
    """
    if not check_rclone():
        return {"exists": False, "size": None, "size_match": None}

    cmd = [_rclone_bin(), "lsjson", f"{remote}:{path}"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return {"exists": False, "size": None, "size_match": None}

        items = json.loads(result.stdout)
        if not items:
            return {"exists": False, "size": None, "size_match": None}

        # lsjson on a single file returns a list with one entry
        item = items[0]
        size = item.get("Size")
        size_match = None
        if expected_size is not None and size is not None:
            size_match = size == expected_size

        return {"exists": True, "size": size, "size_match": size_match}

    except subprocess.TimeoutExpired:
        logger.warning("rclone lsjson timed out for %s:%s", remote, path)
        return {"exists": False, "size": None, "size_match": None}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("rclone check_file error for %s:%s: %s", remote, path, exc)
        return {"exists": False, "size": None, "size_match": None}


def rclone_hashsum(remote: str, path: str, hash_type: str = "sha256") -> str | None:
    """Get hash of a remote file without downloading. Returns hex string or None."""
    if not check_rclone():
        return None

    cmd = [_rclone_bin(), "hashsum", hash_type, f"{remote}:{path}"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            logger.warning("rclone hashsum failed for %s:%s: %s", remote, path, result.stderr.strip())
            return None

        # Output format: "<hash>  <filename>"
        first_line = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
        if first_line:
            return first_line.split()[0]
        return None

    except subprocess.TimeoutExpired:
        logger.warning("rclone hashsum timed out for %s:%s", remote, path)
        return None
    except (OSError, IndexError) as exc:
        logger.warning("rclone hashsum error for %s:%s: %s", remote, path, exc)
        return None


def rclone_is_reachable(remote: str, timeout: int = 10) -> bool:
    """Quick check if remote is accessible."""
    if not check_rclone():
        return False

    cmd = [_rclone_bin(), "lsd", f"{remote}:", "--max-depth", "0"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def retry_with_backoff(
    fn,
    *args,
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple = (RuntimeError, subprocess.TimeoutExpired, OSError),
    **kwargs,
):
    """Execute fn with exponential backoff retry.

    Args:
        fn: callable to execute
        *args: positional arguments for fn
        max_retries: maximum number of retry attempts
        base_delay: initial delay in seconds (doubled each retry)
        max_delay: maximum delay cap in seconds
        retryable_exceptions: tuple of exception types that trigger a retry
        **kwargs: keyword arguments for fn

    Returns:
        The return value of fn on success.

    Raises:
        The last exception if all retries are exhausted.
    """
    import random
    import time

    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except retryable_exceptions as exc:
            last_exc = exc
            if attempt == max_retries:
                logger.error("retry_with_backoff: all %d attempts failed for %s: %s",
                             max_retries + 1, fn.__name__ if hasattr(fn, '__name__') else fn, exc)
                raise
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            logger.warning("retry_with_backoff: attempt %d/%d failed for %s (%s), retrying in %.1fs",
                           attempt + 1, max_retries + 1,
                           fn.__name__ if hasattr(fn, '__name__') else fn,
                           exc, delay)
            time.sleep(delay)

    # Should not reach here, but satisfy type checkers
    raise last_exc  # type: ignore[misc]


def check_volume_mounted(path: str) -> bool:
    """Check if a path on a mounted volume is accessible.

    For /Volumes/ paths, checks that the volume mount point exists.
    For other paths, simply checks path existence.
    """
    p = Path(path)
    if path.startswith("/Volumes/"):
        parts = path.split("/")
        if len(parts) >= 3:
            volume_root = Path("/") / parts[1] / parts[2]
            return volume_root.exists()
        return False
    return p.exists()


def wait_for_connectivity(
    remote: str,
    timeout: int = 300,
    poll_interval: int = 10,
    progress_fn=None,
) -> bool:
    """Block until remote is reachable or timeout. Returns True if connected.

    Args:
        remote: rclone remote name
        timeout: maximum wait time in seconds
        poll_interval: seconds between connectivity checks
        progress_fn: optional callback(elapsed_seconds, timeout) for progress updates
    """
    import time

    start = time.monotonic()
    while True:
        if rclone_is_reachable(remote, timeout=min(poll_interval, 10)):
            logger.info("wait_for_connectivity: %s is reachable after %.1fs",
                        remote, time.monotonic() - start)
            return True

        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            logger.warning("wait_for_connectivity: %s not reachable after %.1fs (timeout=%ds)",
                           remote, elapsed, timeout)
            return False

        if progress_fn is not None:
            with contextlib.suppress(Exception):
                progress_fn(elapsed, timeout)

        remaining = timeout - elapsed
        time.sleep(min(poll_interval, remaining))
