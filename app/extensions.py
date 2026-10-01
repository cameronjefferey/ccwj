"""Shared Flask extensions (initialized in app/__init__.py)."""
import os

from flask_limiter import Limiter
from flask_login import current_user
from flask_wtf.csrf import CSRFProtect

from app.client_ip import real_client_ip


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
    return real_client_ip()


csrf = CSRFProtect()


def _rate_limit_storage_uri():
    """Shared Redis when we have it, so a 30/day cap is 30/day across workers.

    ``RATELIMIT_STORAGE_URI`` wins when set. Otherwise ``REDIS_URL`` (the
    store the demo caps use), then the query-cache Redis
    (``QUERY_CACHE_REDIS_URL``) so a deploy that has not added ``REDIS_URL``
    keeps today's shared limiter. ``memory://`` is the last resort and logs
    nothing here — demo caps log their own warning when they fall back.
    """
    explicit = (os.environ.get("RATELIMIT_STORAGE_URI") or "").strip()
    if explicit:
        return explicit
    redis_url = (os.environ.get("REDIS_URL") or "").strip()
    if redis_url:
        return redis_url
    cache_url = (os.environ.get("QUERY_CACHE_REDIS_URL") or "").strip()
    if cache_url:
        return cache_url
    return "memory://"


def _default_limit_exempt() -> bool:
    """Health checks and static files stay outside the 300/hour default."""
    try:
        from flask import request
        path = request.path or ""
    except Exception:
        return False
    if path.startswith("/static/") or path.startswith("/healthz"):
        return True
    return path in ("/favicon.ico", "/sw.js")


limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=["300 per hour"],
    default_limits_exempt_when=_default_limit_exempt,
    storage_uri=_rate_limit_storage_uri(),
)
