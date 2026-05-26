"""Shared helpers used by multiple route modules."""

from .cache import api_cache_max_age, api_cache_ttl_seconds, make_ttl_cache
from .null_handling import safe_float, safe_int, safe_str

__all__ = [
    "safe_float",
    "safe_int",
    "safe_str",
    "make_ttl_cache",
    "api_cache_ttl_seconds",
    "api_cache_max_age",
]
