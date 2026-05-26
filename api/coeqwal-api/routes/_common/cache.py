"""Shared TTL-cache helper.

One parameter (`API_CACHE_TTL_SECONDS`) controls in-process caching across every
route. Default is 5 minutes. Set the env var to `0` to disable caching
entirely. Intended to support local development so an ETL refresh or a code edit
takes effect on the next request. Once in production, we can raise this.

`make_ttl_cache(name, maxsize)` returns either a real `cachetools.TTLCache`
or a tiny no-op stand-in that always misses. Callers use the same `in` /
`[]` / `[k] =` interface in either case, so disabling the cache requires
zero per-call branching.

`api_cache_max_age()` returns the same TTL value (in seconds) so route
handlers can set `Cache-Control: public, max-age=...` headers in step with
the in-process cache.
"""

from __future__ import annotations

import os
from typing import Any, Iterator, MutableMapping

from cachetools import TTLCache

_DEFAULT_TTL_SECONDS = 300

try:
    _TTL_SECONDS = max(0, int(os.environ.get("API_CACHE_TTL_SECONDS", _DEFAULT_TTL_SECONDS)))
except ValueError:
    _TTL_SECONDS = _DEFAULT_TTL_SECONDS


class _NoopCache(MutableMapping[str, Any]):
    """Cache stand-in that never stores anything.

    Implements the read/write subset of MutableMapping that every route's
    cache use site relies on. Reads always miss. Writes are silently dropped.
    """

    def __contains__(self, key: object) -> bool:
        return False

    def __getitem__(self, key: str) -> Any:
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        return None

    def __delitem__(self, key: str) -> None:
        return None

    def __iter__(self) -> Iterator[str]:
        return iter(())

    def __len__(self) -> int:
        return 0


def make_ttl_cache(name: str, maxsize: int) -> MutableMapping[str, Any]:
    """Return a TTL cache for the given route module.

    `name` is used by debug tooling and log lines. It does not affect
    behavior. `maxsize` is forwarded to `cachetools.TTLCache`. Pick a value
    that comfortably exceeds the realistic key count for the route.

    When `API_CACHE_TTL_SECONDS=0` is set in the environment, this returns
    a no-op cache that always misses (so handlers fall through to fresh DB
    queries every call, which is what local dev usually wants).
    """
    if _TTL_SECONDS <= 0:
        return _NoopCache()
    return TTLCache(maxsize=maxsize, ttl=_TTL_SECONDS)


def api_cache_ttl_seconds() -> int:
    """Effective in-process TTL in seconds (0 when caching is disabled)."""
    return _TTL_SECONDS


def api_cache_max_age() -> int:
    """Cache-Control max-age value to send on cached responses.

    Falls back to a small positive number when caching is disabled, so
    browsers still get a brief reuse window for the duration of a single
    page load.
    """
    if _TTL_SECONDS > 0:
        return _TTL_SECONDS
    return 30


__all__ = [
    "make_ttl_cache",
    "api_cache_ttl_seconds",
    "api_cache_max_age",
]
