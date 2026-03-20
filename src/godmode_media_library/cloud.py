"""Cloud storage support via rclone.

Provides helpers for detecting rclone, listing remotes, and resolving
cloud-backed paths (e.g. `gdrive:Photos`) into local mount points.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RcloneRemote:
    name: str
    type: str


def check_rclone() -> bool:
    """Return True if rclone is available on PATH."""
    return shutil.which("rclone") is not None


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


def rclone_ls(remote: str, path: str = "") -> list[dict]:
    """List files/dirs at a remote path using rclone lsjson.

    Returns list of dicts with keys: Path, Name, Size, MimeType, IsDir, ModTime.
    """
    if not check_rclone():
        raise RuntimeError("rclone is not installed")

    target = f"{remote}:{path}" if path else f"{remote}:"
    try:
        result = subprocess.run(
            ["rclone", "lsjson", target],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(f"rclone lsjson failed: {result.stderr.strip()}")
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("rclone lsjson timed out") from e


def resolve_root(root_spec: str) -> Path:
    """Resolve a root specification to a local path.

    Supports:
    - Regular paths: /Users/me/Photos → Path("/Users/me/Photos")
    - Home-relative: ~/Photos → expanded
    - rclone remote: gdrive:Photos → checks if mounted, returns mount path

    Raises ValueError if the path doesn't exist or isn't mounted.
    """
    # Regular or home-relative path
    if ":" not in root_spec or (len(root_spec) >= 2 and root_spec[1] == ":"):
        # Windows drive letter (C:) or no colon = regular path
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
    """Generate rclone mount command for a remote.

    Returns the shell command string (not executed).
    """
    if mount_point is None:
        mount_point = f"~/mnt/{remote}"
    return f"rclone mount {remote}: {mount_point} --daemon --vfs-cache-mode full"


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
        "Common providers:",
        "  Google Drive:  rclone config → 'drive'",
        "  Google Photos: rclone config → 'google photos'",
        "  iCloud:        macOS native: ~/Library/Mobile Documents/",
        "  S3/GCS:        rclone config → 's3' / 'google cloud storage'",
        "  OneDrive:      rclone config → 'onedrive'",
        "  Dropbox:       rclone config → 'dropbox'",
    ]
    return "\n".join(lines)
