"""Focused ad landings, one per message test.

``/go/<slug>`` is noindex. The slug and the optional ``?v=`` headline
variant stick on the first-party touch cookie so later funnel events
and the signup record carry the campaign.

Learn and Mistakes are one promise: a headline, one subhead, one primary
button, a trust line, then only the proof that promise needs. Signup is
the primary button. The live demo is a text link.

``/go/real-pnl`` is the long Monday ad page (``go_real_pnl.html``):
side-by-side Create free account and Explore live demo, then the chart,
ONON/RKLB, the position-story video, fit matrix, how it works, compact
pricing, and FAQ.
"""
from __future__ import annotations

from flask import abort, render_template, request, url_for

from app import app
from app.extensions import limiter

TRIAL_LINE = (
    "Learning and paper trading are free. "
    "Your 30-day trial starts when you connect a real brokerage."
)

TRUST_LINE = (
    "Free, no card. Read-only connection through SnapTrade. "
    "Works with Schwab, Fidelity, Vanguard, Robinhood, and others."
)

# Hero, the proof that matches the ad, then objections and a close.
# Learn and Mistakes use wide stills only. /go/real-pnl places Shorts
# in a glowing 9:16 frame (no phone bezel) beside the copy.
LANDINGS = {
    "learn": {
        "title": "Learn options free",
        "headline": "Free options lessons, then paper trading",
        "lead": "Ten short lessons and a paper account. Free forever.",
        "variants": {
            "past": "Learn from real past trades, then practice with paper money",
            "free": "Options lessons are free. So is paper money.",
        },
        "cta_label": "Create free account",
        "cta_base": "start-learning",
        "signup_route": "learn",
        "kicker": "Free forever · no card",
        "shot": None,
        "second": None,
        "points_label": "What is free",
        "points": (
            {
                "heading": "Ten short lessons",
                "body": (
                    "Calls, puts, covered calls, the wheel, and spreads, "
                    "in plain English."
                ),
                "link_label": "Open the series",
                "link_to": "learn",
                "cta": "learn-series",
            },
            {
                "heading": "Practice with paper money",
                "body": (
                    "Place one call or one put. Simulated fills, real prices. "
                    "Paper trading stays free."
                ),
                "link_label": "",
                "link_to": "",
                "cta": "",
            },
            {
                "heading": "Replay a trade, free",
                "body": (
                    "One decision at a time, on a path with a known ending. "
                    "No account required to watch."
                ),
                "link_label": "Replay a trade",
                "link_to": "learn_replays",
                "cta": "replay",
            },
        ),
    },
    "real-pnl": {
        "title": "Real P&L across brokers",
        "headline": "What did your covered calls really make?",
        "lead": (
            "Rolls, covered-call runs, and early exits, "
            "on the trades themselves."
        ),
        # Existing ad URLs still pass ?v=rolls and ?v=runs. Those keys
        # stay so the funnel records the variant. The approved headline
        # is the only copy on the page.
        "variants": {
            "rolls": "What did your covered calls really make?",
            "runs": "What did your covered calls really make?",
        },
        "cta_label": "Create free account",
        "cta_base": "create-account",
        "signup_route": "real-pnl",
        "kicker": "30 days · no card",
        "shot": None,
        "second": None,
        "points_label": "",
        "points": (),
    },
    "mistakes": {
        "title": "Which early closes cost you",
        "headline": "See which early closes cost you",
        "lead": (
            "A BE covered call was bought back for −$2,357. "
            "It expired worthless."
        ),
        "variants": {
            "early": "An early close, and what holding would have made",
            "premium": "The close you took, next to holding through expiration",
        },
        "cta_label": "Create free account",
        "cta_base": "create-account",
        "signup_route": "mistakes",
        "kicker": "Free to start · no card",
        "shot": {
            "file": "marketing/catch/be-close.webp",
            "srcset": (
                ("marketing/catch/be-close-800.webp", "800w"),
                ("marketing/catch/be-close.webp", "1280w"),
            ),
            "sizes": "(max-width: 960px) 92vw, 640px",
            "width": 1280,
            "height": 720,
            "alt": (
                "A BE covered call bought back for a −$2,357 loss. "
                "The contract expired worthless, and the early close "
                "gave up $6,265 versus holding."
            ),
            "caption": (
                "Founder's account · BE. Bought back for −$2,357. "
                "Expired worthless two days later. The early close gave "
                "up $6,265 versus holding."
            ),
        },
        "second": {
            "heading": "Closed the morning after results",
            "body": (
                "ONON calls sold the morning after results locked in "
                "−$3,333. Holding to expiration would have finished "
                "about +$11,933. The line after the close is an estimate."
            ),
            "file": "marketing/catch/onon.webp",
            "srcset": (
                ("marketing/catch/onon-800.webp", "800w"),
                ("marketing/catch/onon.webp", "1280w"),
            ),
            "sizes": "(max-width: 960px) 92vw, 720px",
            "width": 1280,
            "height": 720,
            "alt": (
                "ONON calls sold the morning after results. "
                "Realized result −$3,333. About +$11,933 if held to expiration."
            ),
            "caption": "Founder's account · ONON",
        },
        "points_label": "",
        "points": (),
    },
}

LANDING_SLUGS = tuple(LANDINGS)


def headline_for(slug: str, variant: str) -> str:
    page = LANDINGS[slug]
    return page["variants"].get(variant) or page["headline"]


def allowed_variant(slug: str, raw) -> str:
    text = (raw or "").strip()
    if text in LANDINGS.get(slug, {}).get("variants", {}):
        return text
    return ""


def page_for_route(route: str) -> dict | None:
    for page in LANDINGS.values():
        if page.get("signup_route") == route:
            return page
    return None


def signup_headline(route: str) -> str:
    page = page_for_route(route)
    return page["headline"] if page else ""


def signup_subhead(route: str) -> str:
    page = page_for_route(route)
    return page["lead"] if page else ""


def _signup_url(page: dict) -> str:
    route = page.get("signup_route") or ""
    if route:
        return url_for("signup", route=route)
    return url_for("signup")


def _point_href(link_to: str) -> str:
    if link_to == "learn":
        from app.marketing_videos import resolve_learn_url
        return resolve_learn_url() or url_for("learn_index")
    if link_to == "learn_replays":
        return url_for("learn_index") + "#replays"
    return ""


@app.route("/go/<slug>")
@limiter.limit("120 per minute; 2000 per hour")
def go_landing(slug):
    page = LANDINGS.get(slug)
    if page is None:
        abort(404)
    variant = allowed_variant(slug, request.args.get("v"))
    headline = headline_for(slug, variant)
    points = []
    for point in page.get("points") or ():
        row = dict(point)
        row["href"] = _point_href(point.get("link_to") or "")
        points.append(row)
    extra = {}
    template = "go_landing.html"
    if slug == "real-pnl":
        from app.marketing_videos import catch_stories, story_steps, trade_stories

        steps = story_steps()
        # sCZVeeY_6SA is the if-held Short. TRADE_STORIES documents
        # VssdUIrHcjs (ONON calls, closed early) as that Short's full-length
        # upload. The catch-story id abjZ4_UFhP4 stays gated and is not used.
        # The hero loop is a static Lottie of the covered-call runs screen
        # (marketing/covered-call-scroll.json), not a YouTube Short.
        position_video = next(
            row for row in trade_stories() if row.get("step") == "onon"
        )
        template = "go_real_pnl.html"
        extra = {
            "story_steps": steps,
            "catch_stories": [
                row for row in catch_stories() if row["id"] in ("onon", "rklb")
            ],
            "position_video": position_video,
            "minimal_nav": True,
            "demo_pair": True,
            "catch_quiet": True,
            "pricing_compact": True,
        }
    return render_template(
        template,
        title=page["title"],
        slug=slug,
        page=page,
        headline=headline,
        variant=variant,
        trial_line=TRIAL_LINE,
        trust_line=TRUST_LINE,
        points=points,
        og_title=headline,
        og_description=page["lead"],
        signup_url=_signup_url(page),
        cta_label=page["cta_label"],
        cta_base=page["cta_base"],
        campaign=True,
        marketing_chrome=True,
        **extra,
    )
