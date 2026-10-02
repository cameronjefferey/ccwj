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

import os
import re

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
        # Same still as the strategy-cards band. Dollars on that still are
        # the published marketing crop.
        "feature_image": "marketing/strategies.webp",
        "feature_width": 1400,
        "feature_height": 748,
        "feature_alt": "Strategy cards from the demo account, with return, win rate, and a six-month strip.",
        "feature_caption": "",
        "feature_note": "",
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
        "feature_image": "marketing/pnl_real.webp",
        "feature_width": 1600,
        "feature_height": 804,
        "feature_alt": (
            "Cumulative P&L on BE trades, April to September 2026, "
            "with trade-day markers"
        ),
        "feature_caption": "Real account · BE",
        "feature_note": (
            "Every trade day on one line. The run-up, the drawdown and "
            "the recovery, with options and shares split out."
        ),
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
        "feature_image": "marketing/msft-if-held.webp",
        "feature_width": 1400,
        "feature_height": 315,
        "feature_alt": "If held to expiration summary for MSFT in the demo account.",
        "feature_caption": "Demo account",
        "feature_note": "",
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
        # Masked strategy cards. The covered-call card is the subject.
        # Dollar totals on this still are already hidden.
        "feature_image": "marketing/catch/win-rate.webp",
        "feature_width": 1280,
        "feature_height": 720,
        "feature_alt": (
            "Strategy cards for covered calls, a 74% win rate across 662 "
            "trades, and long calls, a 44% win rate across 218 trades. "
            "Dollar totals on the screen are masked."
        ),
        "feature_caption": "",
        "feature_note": "",
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
        "feature_image": "marketing/fit-matrix.webp",
        "feature_width": 1400,
        "feature_height": 641,
        "feature_alt": "Strategy fit matrix from the demo account, return by strategy and sector.",
        "feature_caption": "Where your edge is",
        "feature_note": "",
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
        # No app still for the series. The Short poster is the frame.
        "feature_image": "",
        "feature_width": 0,
        "feature_height": 0,
        "feature_alt": "",
        "feature_caption": "",
        "feature_note": "",
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
    out["feature_image"] = (item.get("feature_image") or "").strip()
    out["feature_image_url"] = (
        _static_poster(out["feature_image"]) if out["feature_image"] else ""
    )
    out["feature_width"] = int(item.get("feature_width") or 0)
    out["feature_height"] = int(item.get("feature_height") or 0)
    out["feature_alt"] = item.get("feature_alt") or ""
    out["feature_caption"] = item.get("feature_caption") or ""
    out["feature_note"] = item.get("feature_note") or ""
    return out


def hero_video():
    return present_video(HERO_VIDEO)


def story_steps():
    return [present_video(step) for step in STORY_STEPS]


def trade_stories():
    return [present_video(story) for story in TRADE_STORIES]


# Homepage "Here's what you'd catch" stills. These five are the owner's
# Real Trade Stories edits. The ids stay here, but the template only
# renders "Watch the story" when CATCH_STORY_VIDEOS_LIVE=1 (set that on
# Render after the cuts are public). Unset, or any other value, hides
# the links. A blank or non-11-character id stays hidden either way.
_YOUTUBE_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")


def catch_story_videos_live() -> bool:
    """True only when the Real Trade Stories watch links should be public."""
    return (os.environ.get("CATCH_STORY_VIDEOS_LIVE") or "").strip() == "1"

CATCH_STORY_VIDEOS = {
    "onon": "abjZ4_UFhP4",
    "rklb": "JB-Zvno6hYQ",
    "be-close": "L1Wmhww0Xrk",
    "be-swing": "B7KZ9ZMelMc",
    "win-rate": "Sf-SOuSnw30",
}

CATCH_STORIES = [
    {
        "id": "onon",
        "tab": "ONON",
        "caption": (
            "Selling these ONON calls the morning after results locked in "
            "−$3,333 realized, while holding to expiration would have "
            "finished about +$11,933."
        ),
        "alt": (
            "ONON daily close from August 5 to September 13, ending at "
            "$48.63, with the $41 strike drawn across the chart. The $41 "
            "call was bought for $2.85 and sold the next morning. Realized "
            "result −$3,333; about +$11,933 if held to expiration."
        ),
        "image": "marketing/catch/onon.webp",
        "image_sm": "marketing/catch/onon-800.webp",
        "width": 1280,
        "height": 720,
    },
    {
        "id": "rklb",
        "tab": "RKLB",
        "caption": (
            "Five RKLB covered calls expired for +$931, but call six had a "
            "strike below my $69 cost, so assignment locked in −$600 on the "
            "shares. Two weeks later the stock was at $84.80."
        ),
        "alt": (
            "RKLB daily close from February 20 to April 17, ending at "
            "$84.80. The $69 cost and the $63 strike are marked. Five "
            "covered calls expired for +$931. Call six, after fees, was "
            "+$75.34. Assignment at $63 locked in −$600 on the shares. "
            "Net of the run +$406."
        ),
        "image": "marketing/catch/rklb.webp",
        "image_sm": "marketing/catch/rklb-800.webp",
        "width": 1280,
        "height": 720,
    },
    {
        "id": "be-close",
        "tab": "BE close",
        "caption": (
            "I bought back this BE covered call for a −$2,357 loss. It "
            "expired worthless two days later, and the early close gave up "
            "$6,265 versus holding."
        ),
        "alt": (
            "A BE covered call bought back on June 26. The loss on the "
            "contract was −$2,357. The contract expired worthless, and the "
            "early close gave up $6,265 versus holding."
        ),
        "image": "marketing/catch/be-close.webp",
        "image_sm": "marketing/catch/be-close-800.webp",
        "width": 1280,
        "height": 720,
    },
    {
        "id": "be-swing",
        "tab": "BE swing",
        "caption": (
            "One BE position swung from about +$24k to about −$15k and "
            "back to +$11k, while its covered calls kept collecting premium "
            "all the way through."
        ),
        "alt": (
            "Cumulative P&L on one BE position from April to September "
            "2026, shares and options by day. The line peaks in mid-June, "
            "falls through late July, and recovers into late September."
        ),
        "image": "marketing/catch/be-swing.webp",
        "image_sm": "marketing/catch/be-swing-800.webp",
        "width": 1280,
        "height": 720,
    },
    {
        "id": "win-rate",
        "tab": "Win rate",
        "caption": (
            "Covered calls win 74% of the time, but long calls, at a 44% "
            "win rate, made about nine times as much per trade."
        ),
        "alt": (
            "Strategy cards for covered calls, a 74% win rate across 662 "
            "trades, and long calls, a 44% win rate across 218 trades. "
            "Dollar totals on the screen are masked."
        ),
        "image": "marketing/catch/win-rate.webp",
        "image_sm": "marketing/catch/win-rate-800.webp",
        "width": 1280,
        "height": 720,
    },
]


def catch_stories():
    """Stills, plus a watch link only when the cuts are live and the id is real."""
    live = catch_story_videos_live()
    out = []
    for row in CATCH_STORIES:
        item = dict(row)
        vid = (CATCH_STORY_VIDEOS.get(row["id"]) or "").strip()
        item["youtube_id"] = vid if live and _YOUTUBE_ID.fullmatch(vid) else ""
        out.append(item)
    return out
