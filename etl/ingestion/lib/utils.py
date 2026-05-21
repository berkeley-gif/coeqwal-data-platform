"""Small utility helpers used across the ingestion pipeline.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

from .config import EXCLUDED_SUBFOLDERS


def _norm_for_match(path: str) -> str:
    """Normalize a path for substring matching: forward slashes, lowercased, with
    leading/trailing `/`."""
    norm = path.replace("\\", "/").lstrip("./").lower()
    return f"/{norm}/"


def _in_excluded_subfolder(path: str) -> bool:
    """True when any path segment matches an entry in `EXCLUDED_SUBFOLDERS`.
    Used to skip `archive/`, `discard/`, `old/`, `backup/` gdrive folders."""
    parts = path.replace("\\", "/").lower().split("/")
    return any(part in EXCLUDED_SUBFOLDERS for part in parts)


def _basename_of(path: str) -> str:
    """Basename, treating both forward and back slashes as separators."""
    if not path:
        return ""
    return path.replace("\\", "/").rsplit("/", 1)[-1]


def _sha256_of_file(path: str) -> str:
    """SHA-256 of a file's bytes, streamed in 1 MiB chunks for large ZIPs."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_of_zip_entry(zf: zipfile.ZipFile, name: str) -> Tuple[str, int]:
    """SHA-256 + uncompressed byte count for a ZIP member, streamed."""
    h = hashlib.sha256()
    size = 0
    with zf.open(name) as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _sha256_of_bytes(data: bytes) -> str:
    """SHA-256 of an in-memory bytes object."""
    return hashlib.sha256(data).hexdigest()


def _sha256_of_row(row: Dict[str, Any]) -> str:
    """Canonical hash of a spreadsheet row's payload, for the sidecar provenance."""
    canonical = json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now_iso_utc() -> str:
    """Current UTC timestamp in `YYYY-MM-DDTHH:MM:SSZ` form (sidecar fields)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _operator_tag() -> str:
    """`user@host` tag for sidecar provenance. Falls back to `unknown` when
    the environment is missing the usual variables."""
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
    try:
        host = socket.gethostname()
    except OSError:
        host = "unknown"
    return f"{user}@{host}"
