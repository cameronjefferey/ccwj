"""Public homepage videos.

``youtube_id`` is the 11-character id only (not a full URL). ``mp4_url``
is an optional https URL or a site-root path such as ``/static/...``.

``poster`` is optional. A site-relative static path, a root path, or an
https URL. When it is empty and ``youtube_id`` is set, the page uses the
YouTube ``maxresdefault`` thumbnail on i.ytimg.com. When both are empty,
the poster is a CSS frame. ``poster_srcset`` is optional pairs of
``(path, "1280w")`` for a responsive poster. The hero uses that so the
walkthrough does not show the YouTube still.

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
    "youtube_id": "NpU79Lwkdn4",
    "mp4_url": "",
    # Custom still. The YouTube maxres frame shows account-wide totals.
    "poster": "marketing/walkthrough_poster_1280.webp",
    "poster_srcset": (
        ("marketing/walkthrough_poster_1280.webp", "1280w"),
        ("marketing/walkthrough_poster.webp", "1920w"),
    ),
    "poster_sizes": "(min-width: 960px) 920px, 100vw",
}

STORY_STEPS = [
    {
        "step": 1,
        "title": "Which strategies work",
        "caption": (
            "Covered calls by days to expiry. The label is read from the "
            "fills and from the shares held when the option was written."
        ),
        "duration_label": "Short",
        "youtube_id": "GZ3mPiagkLo",
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
        "youtube_id": "uAmHW-4RtaA",
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
        "youtube_id": "sCZVeeY_6SA",
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
        "youtube_id": "u_YWl5fKEjo",
        "mp4_url": "",
        "poster": "",
        "links_learn": False,
    },
    {
        "step": 5,
        "title": "The fit matrix",
        "caption": (
            "Win rate and return by strategy and sector. Opening a cell "
            "shows the trades behind that pair. Cells with no trades stay empty."
        ),
        "duration_label": "Short",
        "youtube_id": "jKMUBsGDETc",
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
        "youtube_id": "BwVHe9MmA9c",
        "mp4_url": "",
        "poster": "",
        "links_learn": True,
    },
]

# Full-length trade stories. The matching Shorts are sCZVeeY_6SA (ONON,
# also the if-held phone above) and IFay1hJfpSc (RKLB).
TRADE_STORIES = [
    {
        "step": "onon",
        "title": "ONON calls, closed early",
        "caption": (
            "The close was -$3,333. Holding to expiration would have been "
            "about +$11,933. The line after the close is an estimate."
        ),
        "duration_label": "",
        "youtube_id": "VssdUIrHcjs",
        "mp4_url": "",
        "poster": "",
    },
    {
        "step": "rklb",
        "title": "RKLB covered calls",
        "caption": (
            "Five covered calls expired. The sixth was assigned, and the "
            "shares were called away."
        ),
        "duration_label": "",
        "youtube_id": "kqxo9BPDMcs",
        "mp4_url": "",
        "poster": "",
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


def _static_poster(path):
    path = (path or "").strip()
    if not path:
        return ""
    if path.startswith(("http://", "https://", "/")):
        return path
    from flask import url_for
    return url_for("static", filename=path)


def _poster_url(item):
    poster = (item.get("poster") or "").strip()
    youtube_id = (item.get("youtube_id") or "").strip()
    if poster:
        return _static_poster(poster)
    if youtube_id:
        return f"https://i.ytimg.com/vi/{youtube_id}/maxresdefault.jpg"
    return ""


def _poster_srcset(item):
    parts = []
    for path, width in item.get("poster_srcset") or ():
        url = _static_poster(path)
        if url:
            parts.append(f"{url} {width}")
    return ", ".join(parts)


def present_video(item):
    """Copy a config row and attach the resolved poster URL."""
    out = dict(item)
    out["youtube_id"] = (item.get("youtube_id") or "").strip()
    out["mp4_url"] = (item.get("mp4_url") or "").strip()
    out["poster"] = (item.get("poster") or "").strip()
    out["poster_url"] = _poster_url(out)
    out["poster_srcset"] = _poster_srcset(out)
    out["poster_sizes"] = (item.get("poster_sizes") or "").strip()
    out["links_learn"] = bool(item.get("links_learn"))
    return out


def hero_video():
    return present_video(HERO_VIDEO)


def story_steps():
    return [present_video(step) for step in STORY_STEPS]


def trade_stories():
    return [present_video(story) for story in TRADE_STORIES]
