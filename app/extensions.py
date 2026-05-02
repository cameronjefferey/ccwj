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
# storage_uri='memory://': counters live in-process. With Gunicorn's
# multi-worker setup that means each worker has its own counter, so a
# 30/day cap is effectively (30 * num_workers)/day. That's still a real
# ceiling for closed-beta sharing — moving to Redis is the right answer
# once the user count or worker count grows enough that the multiplier
# matters. Set RATELIMIT_STORAGE_URI=redis://host:port/0 in env to flip.
limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=[],
    storage_uri=os.environ.get("RATELIMIT_STORAGE_URI", "memory://"),
)
