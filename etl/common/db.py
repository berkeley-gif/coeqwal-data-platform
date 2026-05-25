"""PostgreSQL connection helper for the COEQWAL ETL.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

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


def get_db_connection(required: bool = True, db_url: Optional[str] = None):
    """Open a psycopg2 connection.

    If `db_url` is given, use it directly (caller-provided overrides win).
    Otherwise read from the `DATABASE_URL` environment variable via
    `get_database_url`.

    Lazy import of psycopg2 so scripts that only need constants do not pay
    the import cost. Returns None when `required=False` and neither
    `db_url` nor the env var is available (callers can then run in
    CSV-only / dry-run mode).
    """
    url = db_url or get_database_url(required=required)
    if url is None:
        return None

    try:
        import psycopg2  # noqa: F401 (imported for the side effect of raising)
        import psycopg2.extras  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2 is not installed. Install with:\n"
            "  pip install psycopg2-binary"
        ) from exc

    import psycopg2

    return psycopg2.connect(url)
