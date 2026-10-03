"""Response headers that do not depend on a new vendor being configured.

The enforcing policy is only ``frame-ancestors`` (plus ``X-Frame-Options``).
The rest of the Content-Security-Policy is report-only. ``script-src`` and
``style-src`` still allow ``'unsafe-inline'`` because templates ship inline
scripts and styles (theme boot, Chart.js defaults, the landing-page YouTube
loader, per-page chart setup). A nonce pass is the follow-up that lets this
policy drop ``unsafe-inline`` and become enforcing. Report-only cannot break
Stripe Checkout, the SnapTrade portal, YouTube embeds, or charts.
"""
from __future__ import annotations

# Origins templates and the browser actually request. Keep this list in
# sync when a new CDN, embed, or checkout host shows up in app/templates.
_CSP_REPORT_ONLY = "; ".join([
    "default-src 'self'",
    "base-uri 'self'",
    "object-src 'none'",
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net "
    "https://challenges.cloudflare.com https://www.redditstatic.com https://js.stripe.com",
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com",
    "font-src 'self' https://fonts.gstatic.com data:",
    "img-src 'self' data: blob: https://i.ytimg.com https://img.youtube.com "
    "https://*.stripe.com https://alb.reddit.com https://pixel.reddit.com "
    "https://www.redditstatic.com",
    "frame-src 'self' https://www.youtube-nocookie.com https://www.youtube.com "
    "https://challenges.cloudflare.com https://js.stripe.com https://hooks.stripe.com "
    "https://checkout.stripe.com https://billing.stripe.com https://*.snaptrade.com",
    "connect-src 'self' https://challenges.cloudflare.com https://api.stripe.com "
    "https://checkout.stripe.com https://www.youtube-nocookie.com "
    "https://www.redditstatic.com https://pixel.reddit.com https://alb.reddit.com "
    "https://pixel-config.reddit.com",
    "media-src 'self' blob:",
    "worker-src 'self'",
    "manifest-src 'self'",
    "form-action 'self' https://checkout.stripe.com https://billing.stripe.com "
    "https://*.snaptrade.com",
    "frame-ancestors 'self'",
])


def apply_security_headers(response):
    """Site-wide headers. Safe when every new env var is unset."""
    from flask import request
    from app.utils import is_demo_user

    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault(
        "Referrer-Policy", "strict-origin-when-cross-origin"
    )
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    # HSTS is host-scoped. Sending it on the public HTTPS site is the point;
    # local HTTP still gets the header so a misconfigured deploy cannot omit it.
    response.headers.setdefault(
        "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
    )
    response.headers.setdefault(
        "Content-Security-Policy", "frame-ancestors 'self'"
    )
    response.headers.setdefault(
        "Content-Security-Policy-Report-Only", _CSP_REPORT_ONLY
    )

    path = request.path or ""
    if (
        path.startswith("/demo/")
        or path == "/demo"
        or path.startswith("/go/")
        or is_demo_user()
    ):
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
    # Flask's test client and a None SEND_FILE_MAX_AGE_DEFAULT both emit
    # no-cache. Public assets should stay cacheable in production too.
    if path.startswith("/static/"):
        if path.endswith((".webp", ".png", ".jpg", ".jpeg", ".svg", ".ico", ".woff2")):
            response.headers["Cache-Control"] = "public, max-age=604800"
        elif path.endswith((".js", ".css")):
            response.headers["Cache-Control"] = "public, max-age=3600"
    if path == "/favicon.ico" and "Cache-Control" not in response.headers:
        response.headers["Cache-Control"] = "public, max-age=86400"
    return response


def redirect_onrender_origin():
    """301 the Render hostname to the public site. Health checks stay put.

    Render's deploy probe hits ``/healthz`` on ``*.onrender.com``. Redirecting
    that path fails the deploy. Every other path, including the query string,
    goes to https://happytrader.me.
    """
    from flask import redirect, request

    path = request.path or ""
    if path == "/healthz" or path.startswith("/healthz/") or path == "/version":
        return None
    host = (request.host or "").split(":")[0].strip().lower()
    if host != "onrender.com" and not host.endswith(".onrender.com"):
        return None
    target = "https://happytrader.me" + (request.full_path or "/")
    if target.endswith("?") and not request.query_string:
        target = target[:-1]
    return redirect(target, code=301)
