"""Public homepage videos.

The walkthrough and Shorts are not on YouTube yet. Leave ``youtube_id``
and ``mp4_url`` empty until a real source exists, then drop in either:

- ``youtube_id``: the 11-character id only (not a full URL)
- ``mp4_url``: an https URL or a site-root path such as ``/static/...``

``poster`` is optional. A site-relative static path, a root path, or an
https URL. When it is empty and ``youtube_id`` is set, the page uses the
YouTube thumbnail. When both are empty, the poster is a CSS frame.

The template never emits an iframe. ``landing.html`` builds a
youtube-nocookie embed only after a click.
"""

from __future__ import annotations

# step, title, caption, youtube_id or mp4_url, poster.
# ``step`` is "hero" or 1..n. ``duration_label`` is display-only.
HERO_VIDEO = {
    "step": "hero",
    "title": "How HappyTrader works",
    "caption": (
        "About two and a half minutes. Then connect a brokerage and "
        "open the same pages on your history."
    ),
    "duration_label": "2:30",
    "youtube_id": "",
    "mp4_url": "",
    "poster": "",
}

STORY_STEPS = [
    {
        "step": 1,
        "title": "Connect your brokerage",
        "caption": (
            "Read-only, through SnapTrade. Schwab, Fidelity, Vanguard, "
            "Robinhood, and others. HappyTrader never stores your broker password."
        ),
        "duration_label": "Short",
        "youtube_id": "",
        "mp4_url": "",
        "poster": "",
        "links_learn": False,
    },
    {
        "step": 2,
        "title": "See every trade and position",
        "caption": (
            "Each symbol opens to the fills, the open lots, and a chart "
            "of equity, options, and dividends."
        ),
        "duration_label": "Short",
        "youtube_id": "",
        "mp4_url": "",
        "poster": "",
        "links_learn": False,
    },
    {
        "step": 3,
        "title": "If held to expiration",
        "caption": (
            "On an early close, the P&L you took sits next to the P&L if "
            "that contract had been held to expiration. The gap is what "
            "closing early cost or saved. The line after the close is an estimate."
        ),
        "duration_label": "Short",
        "youtube_id": "",
        "mp4_url": "",
        "poster": "",
        "links_learn": False,
    },
    {
        "step": 4,
        "title": "Covered-call runs",
        "caption": (
            "A share lot and the calls written on it, from the buy through "
            "the sale, the assignment, or today. Premium, the share result, "
            "and one net. Broker fees stay out of that math."
        ),
        "duration_label": "Short",
        "youtube_id": "",
        "mp4_url": "",
        "poster": "",
        "links_learn": False,
    },
    {
        "step": 5,
        "title": "Privacy mode and share cards",
        "caption": (
            "Privacy mode masks account names and balances on your screen. "
            "A share card is a picture of one closed trade — symbol, strategy, "
            "dates, and realized P&L — and it leaves out the account, the broker, "
            "and the balance."
        ),
        "duration_label": "Short",
        "youtube_id": "",
        "mp4_url": "",
        "poster": "",
        "links_learn": False,
    },
    {
        "step": 6,
        "title": "Learn options",
        "caption": (
            "Options 101 is a short series on the words these pages use, "
            "including strike, expiration, and covered call."
        ),
        "duration_label": "Short",
        "youtube_id": "",
        "mp4_url": "",
        "poster": "",
        "links_learn": True,
    },
]


def resolve_learn_url():
    """``/learn`` when that exact route exists, otherwise ``None``.

    A future ``/learn/<slug>`` page is not a substitute: linking the
    homepage at a path that 404s is worse than no link.
    """
    from flask import current_app, url_for

    for rule in current_app.url_map.iter_rules():
        if rule.rule.rstrip("/") != "/learn":
            continue
        if "GET" not in (rule.methods or set()):
            continue
        if rule.arguments:
            continue
        try:
            return url_for(rule.endpoint)
        except Exception:
            return "/learn"
    return None


def _poster_url(item):
    poster = (item.get("poster") or "").strip()
    youtube_id = (item.get("youtube_id") or "").strip()
    if poster:
        if poster.startswith(("http://", "https://", "/")):
            return poster
        from flask import url_for
        return url_for("static", filename=poster)
    if youtube_id:
        return f"https://i.ytimg.com/vi/{youtube_id}/hqdefault.jpg"
    return ""


def present_video(item):
    """Copy a config row and attach the resolved poster URL."""
    out = dict(item)
    out["youtube_id"] = (item.get("youtube_id") or "").strip()
    out["mp4_url"] = (item.get("mp4_url") or "").strip()
    out["poster"] = (item.get("poster") or "").strip()
    out["poster_url"] = _poster_url(out)
    out["links_learn"] = bool(item.get("links_learn"))
    return out


def hero_video():
    return present_video(HERO_VIDEO)


def story_steps():
    return [present_video(step) for step in STORY_STEPS]
