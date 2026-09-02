"""Shared Flask extensions (initialized in app/__init__.py)."""
import os

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_login import current_user
from flask_wtf.csrf import CSRFProtect


def _rate_limit_key():
    """
    Per-user rate-limit key when signed in, IP otherwise.

    Why not just IP? Strangers behind the same NAT (corporate proxy,
    family Wi-Fi, conference network) shouldn't share a budget for
    expensive endpoints like AI Coach generation. Once the user logs in
    we know the real principal — keying off ``user:<id>`` makes
    per-account caps work even when 10 testers share one IP.

    Why not user only? On anonymous endpoints (e.g. /login, /signup)
    there is no current_user, so we still need a fallback that prevents
    a script from creating thousands of accounts from one host.
    """
    try:
        if current_user.is_authenticated:
            return f"user:{current_user.id}"
    except Exception:
        pass
    return get_remote_address()


csrf = CSRFProtect()


def _rate_limit_storage_uri():
    """Shared Redis when we have it, so a 30/day cap is 30/day across workers.

    ``RATELIMIT_STORAGE_URI`` wins when set. Otherwise reuse the query-cache
    Redis (``QUERY_CACHE_REDIS_URL``) so production does not need a second
    env var. ``memory://`` is the last resort (tests / a laptop with no Redis).
    """
    explicit = (os.environ.get("RATELIMIT_STORAGE_URI") or "").strip()
    if explicit:
        return explicit
    redis_url = (os.environ.get("QUERY_CACHE_REDIS_URL") or "").strip()
    if redis_url:
        return redis_url
    return "memory://"


limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=[],
    storage_uri=_rate_limit_storage_uri(),
)
