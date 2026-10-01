"""Real client IP behind Cloudflare and Render.

Cloudflare sets ``CF-Connecting-IP`` (and ``CF-Ray``) on proxied requests.
That header is what we use once the site is orange-clouded. Direct hits
that did not come through Cloudflare keep ``request.remote_addr``, which
``ProxyFix`` has already rewritten from ``X-Forwarded-For`` using
``PROXY_X_FOR`` (default 1, Render's single proxy hop).
"""
from __future__ import annotations

import os


def proxy_x_for_hops(raw: str | None = None) -> int:
    """How many ``X-Forwarded-For`` hops to trust. Invalid values fall back to 1."""
    if raw is None:
        raw = os.environ.get("PROXY_X_FOR", "1")
    text = str(raw if raw is not None else "1").strip()
    if not text:
        return 1
    try:
        return max(0, int(text))
    except ValueError:
        return 1


def real_client_ip() -> str:
    """Client address for rate limits and demo caps."""
    from flask import request

    cf_ip = (request.headers.get("CF-Connecting-IP") or "").strip()
    # CF-Ray is added by Cloudflare's edge. A bare CF-Connecting-IP without
    # it is not treated as proof the request came through Cloudflare.
    via_cloudflare = bool((request.headers.get("CF-Ray") or "").strip())
    if cf_ip and via_cloudflare:
        return cf_ip
    return (request.remote_addr or "").strip() or "unknown"
