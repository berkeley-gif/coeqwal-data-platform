"""rclone subprocess wrappers for Google Drive access, plus the rclone
preflight checks.

Three operations are exposed to the rest of the pipeline:

- `rclone_lsjson` list a Drive folder, return parsed JSON
- `rclone_copy_file` download a single file (with live progress on stdout)
- `rclone_cat` read a small file into memory (used for the trend CSV)

Two preflight helpers (`_preflight_rclone_installed`,
`_preflight_rclone_remote`) live here rather than in `preflight.py` because
they share the `_RCLONE_CONFIG_ERROR_MARKERS` logic with the wrappers and
share the same actionable error messages.
"""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Dict, List, Optional

from .config import RCLONE_REMOTE
from .errors import PreflightError

log = logging.getLogger("gdrive_bulk_download")


# rclone stderr substrings that indicate a config-level problem (no remote,
# bad config file, expired/revoked OAuth token). When we see these, the run
# should abort with an actionable message rather than swallow the error and
# mark every row as "MISSING_ZIP" through a folder-not-found-shaped path.
_RCLONE_CONFIG_ERROR_MARKERS = (
    "didn't find section",
    "not found in config file",
    "couldn't find section",
    "Failed to create file system",
    "couldn't decrypt",
    "401 Unauthorized",
    "invalid_grant",
    "Token has been expired or revoked",
)


def _is_rclone_config_error(stderr: str) -> bool:
    s = stderr.lower()
    return any(m.lower() in s for m in _RCLONE_CONFIG_ERROR_MARKERS)


def rclone_lsjson(folder_id: str, subpath: str = "",
                  dirs_only: bool = False,
                  rclone_remote: Optional[str] = None) -> List[Dict]:
    """List contents of a Drive folder via rclone lsjson.

    Two failure shapes:
      - Config-level error (no remote, expired token, etc.): raise
        PreflightError so the whole run aborts with the same kind of
        message as the up-front pre-flight checks. We never want a stale
        rclone config to silently mark every scenario as MISSING_ZIP.
      - Folder-not-found / per-row issue: log a warning and return [].
        The caller surfaces this through the audit (MISSING_ZIP, etc.).
    """
    remote = rclone_remote or RCLONE_REMOTE
    target = f"{remote}:{subpath}"
    cmd = ["rclone", "lsjson", target]
    if folder_id:
        cmd.append(f"--drive-root-folder-id={folder_id}")
    if dirs_only:
        cmd.append("--dirs-only")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if _is_rclone_config_error(stderr):
            raise PreflightError(
                f"\n[rclone] Config-level error talking to '{remote}:': {stderr}\n"
                f"This kills the run because the same error would repeat for every "
                f"scenario. Fix the rclone config and retry:\n"
                f"  rclone listremotes\n"
                f"  rclone config reconnect {remote}:    # if the OAuth token is stale\n"
                f"See etl/README.md (Cloud9 setup -> rclone) for the full walkthrough.\n"
            )
        log.warning("rclone lsjson failed: %s", stderr)
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        log.warning("rclone lsjson returned invalid JSON: %s",
                    result.stdout[:200])
        return []


def rclone_copy_file(folder_id: str, remote_path: str,
                     local_dest_dir: str,
                     rclone_remote: Optional[str] = None) -> bool:
    """Download a single file from Drive to a local directory.

    When `folder_id` is non-empty, `remote_path` is interpreted as a subpath
    under that folder (Drive ID-rooted). When `folder_id` is empty,
    `remote_path` is a full path from the rclone remote root (path-mode,
    used for spreadsheet rows that have a `drive_folder_name`/`dv_root` but
    no folder URL).
    """
    remote = rclone_remote or RCLONE_REMOTE
    target = f"{remote}:{remote_path}"
    cmd = ["rclone", "copy", target, local_dest_dir]
    if folder_id:
        cmd.append(f"--drive-root-folder-id={folder_id}")
    cmd.append("--progress")
    try:
        result = subprocess.run(cmd, timeout=3600)
    except subprocess.TimeoutExpired:
        log.error("rclone copy timed out after 3600s for %s", remote_path)
        return False
    if result.returncode != 0:
        log.error("rclone copy failed (exit %d) -- see rclone output above",
                  result.returncode)
        return False
    return True


def rclone_cat(folder_id: str, remote_path: str,
               rclone_remote: Optional[str] = None) -> Optional[bytes]:
    """Read a small file from Drive directly into memory.

    Used for the trend report CSV (typically a few hundred KB), which is
    hashed and uploaded to S3 from the same in-memory buffer. The model
    run ZIP goes through `rclone_copy_file` instead, because it's too
    large to safely fit in RAM.

    When `folder_id` is non-empty, `remote_path` is interpreted as a subpath
    under that folder. When `folder_id` is empty, `remote_path` is a full
    path from the rclone remote root (path-mode).
    """
    remote = rclone_remote or RCLONE_REMOTE
    target = f"{remote}:{remote_path}"
    cmd = ["rclone", "cat", target]
    if folder_id:
        cmd.append(f"--drive-root-folder-id={folder_id}")
    result = subprocess.run(cmd, capture_output=True, timeout=300)
    if result.returncode != 0:
        log.error("rclone cat failed: %s", result.stderr.decode().strip())
        return None
    return result.stdout


def _preflight_rclone_installed() -> None:
    """Confirm `rclone` is on PATH."""
    try:
        result = subprocess.run(
            ["rclone", "version"], capture_output=True, text=True, timeout=10,
        )
    except FileNotFoundError:
        raise PreflightError(
            "\n[preflight] rclone is not installed (or not on PATH).\n"
            "Install it on Cloud9 with:\n"
            "  curl https://rclone.org/install.sh | sudo bash\n"
        )
    if result.returncode != 0:
        raise PreflightError(
            f"\n[preflight] `rclone version` failed with exit code {result.returncode}:\n"
            f"{(result.stderr or result.stdout).strip()}\n"
        )


def _preflight_rclone_remote(remote: str) -> None:
    """Confirm the configured rclone remote is registered."""
    try:
        result = subprocess.run(
            ["rclone", "listremotes"], capture_output=True, text=True, timeout=10,
        )
    except FileNotFoundError:
        # _preflight_rclone_installed runs first, so this is unreachable in
        # normal flow. Re-raise the same actionable message here for safety.
        raise PreflightError(
            "\n[preflight] rclone is not installed (or not on PATH).\n"
            "Install it on Cloud9 with:\n"
            "  curl https://rclone.org/install.sh | sudo bash\n"
        )
    if result.returncode != 0:
        raise PreflightError(
            f"\n[preflight] `rclone listremotes` failed with exit code {result.returncode}:\n"
            f"{(result.stderr or result.stdout).strip()}\n"
        )
    remotes = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    expected = f"{remote}:"
    if expected not in remotes:
        raise PreflightError(
            f"\n[preflight] rclone remote '{expected}' is not configured.\n"
            f"See etl/README.md (Cloud9 setup -> rclone) for the walkthrough.\n"
        )


def _preflight_rclone_auth(remote: str) -> None:
    """Confirm the rclone remote's OAuth token still works.

    `_preflight_rclone_remote` only proves the local config has an entry
    for the remote. This call proves the OAuth refresh token inside that
    entry can still mint an access token (i.e. has not been revoked or
    expired).
    """
    cmd = [
        "rclone", "lsjson", f"{remote}:",
        "--max-depth", "1", "--dirs-only",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except FileNotFoundError:
        # Unreachable in normal flow (`_preflight_rclone_installed` runs first).
        raise PreflightError(
            "\n[preflight] rclone is not installed (or not on PATH).\n"
            "Install it on Cloud9 with:\n"
            "  curl https://rclone.org/install.sh | sudo bash\n"
        )

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if _is_rclone_config_error(stderr):
            raise PreflightError(
                f"\n[preflight] rclone remote '{remote}:' auth failed: {stderr}\n"
                f"The OAuth refresh token is likely expired or revoked.\n"
              f"See etl/README.md (Cloud9 setup -> rclone) for walkthrough.\n"
            )
        raise PreflightError(
            f"\n[preflight] rclone could not reach remote '{remote}:': {stderr}\n"
            f"Possible causes: network outage, Drive API quota, or the\n"
            f"remote is misconfigured at the rclone-internal level. Try:\n"
            f"  rclone lsd {remote}:    # full listing for diagnostics\n"
        )
