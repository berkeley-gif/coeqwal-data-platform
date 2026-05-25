"""PostgreSQL connection helper for the COEQWAL ETL.
"""

from __future__ import annotations

import logging
import os
from typing import Optional
from urllib.parse import urlparse

log = logging.getLogger(__name__)

DATABASE_URL_ENV = "DATABASE_URL"


class DatabaseUrlMissing(RuntimeError, ValueError):
    """Raised when DATABASE_URL is required but not set in the environment.

    Inherits from both `RuntimeError` (the natural classification for an
    unset environment variable at runtime) and `ValueError` (so the many
    pre-existing `except ValueError:` blocks in stats modules that
    previously raised `ValueError("DATABASE_URL not set")` themselves
    still catch it after migrating to this helper).
    """


def get_database_url(required: bool = True) -> Optional[str]:
    """Return the DATABASE_URL from the environment.

    If `required` and the variable is unset, raise `DatabaseUrlMissing` with
    an actionable message. If not required, return None silently.
    """
    url = os.environ.get(DATABASE_URL_ENV)
    if url:
        return url
    if required:
        raise DatabaseUrlMissing(
            f"{DATABASE_URL_ENV} is not set. Export it in your shell, e.g.:\n"
            f"  export {DATABASE_URL_ENV}='postgresql://USER:PASS@HOST:5432/coeqwal_scenario'\n"
            f"Cloud9 typically sets this in ~/.bashrc. Local dev uses Docker Compose:\n"
            f"  export {DATABASE_URL_ENV}='postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario'"
        )
    return None


def _summarize_url_for_log(url: str) -> str:
    """Render a postgres URL as `user@host:port/dbname` for log lines.

    The password is never read. Reads only `username`, `hostname`, `port`,
    and `path` from the parsed URL. Returns `<unparseable>` if the URL
    is not in URI form (e.g. libpq key=value DSN strings).
    """
    try:
        parsed = urlparse(url)
        if not parsed.scheme:
            return "<unparseable>"
        host = parsed.hostname or "?"
        port = parsed.port if parsed.port is not None else "?"
        dbname = (parsed.path or "/").strip("/") or "?"
        user = parsed.username or "?"
        return f"{user}@{host}:{port}/{dbname}"
    except Exception:
        return "<unparseable>"


def get_db_connection(required: bool = True, db_url: Optional[str] = None):
    """Open a psycopg2 connection.

    If `db_url` is given, use it directly (caller-provided overrides win).
    Otherwise read from the `DATABASE_URL` environment variable via
    `get_database_url`.

    Lazy import of psycopg2 so scripts that only need constants do not pay
    the import cost. Returns None when `required=False` and neither
    `db_url` nor the env var is available (callers can then run in
    CSV-only / dry-run mode).

    Logs two INFO lines per call: one before connect identifying the URL
    source, one after connect identifying the target as `user@host:port/db`.
    Credentials are never logged.
    """
    url = db_url or get_database_url(required=required)
    if url is None:
        return None

    source = "db_url parameter" if db_url else f"{DATABASE_URL_ENV} env var"
    log.info("Opening DB connection (URL source: %s)", source)

    try:
        import psycopg2  # noqa: F401 (imported for the side effect of raising)
        import psycopg2.extras  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2 is not installed. Install with:\n"
            "  pip install psycopg2-binary"
        ) from exc

    import psycopg2

    conn = psycopg2.connect(url)

    try:
        log.info("Connected to %s", _summarize_url_for_log(url))
    except Exception:
        # Logging failures must never break the connection itself
        pass

    return conn
