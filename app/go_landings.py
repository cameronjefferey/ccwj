"""Focused ad landings, one per message test.

``/go/<slug>`` is noindex. The slug and the optional ``?v=`` headline
variant stick on the first-party touch cookie so later funnel events
and the signup record carry the campaign.

Each page is the homepage, with a campaign hero and the matching
section pulled up under it. Signup is the primary button. The live
demo is a text link.
"""
from __future__ import annotations

from flask import abort, render_template, request, url_for

from app import app
from app.extensions import limiter

TRIAL_LINE = (
    "Learning and paper trading are free. "
    "Your 30-day trial starts when you connect a real brokerage."
)

# Shared product sections. The slug's tuple is the order under the hero.
# The first one is the ad's promise.
_AFTER = (
    "fit",
    "fitshot",
    "profile",
    "how",
    "proof",
    "pricing",
    "faq",
    "close",
)

LANDINGS = {
    "learn": {
        "title": "Learn options free",
        "headline": "Learn options free, then practice with paper money",
        "lead": "Free lessons, trade replays, and a paper account.",
        "variants": {
            "past": "Learn from real past trades, then practice with paper money",
            "free": "Options lessons are free. So is paper money.",
        },
        "cta_label": "Start learning free",
        "cta_base": "start-learning",
        "signup_route": "learn",
        "sections": (
            "learn",
            "practice",
            "replays",
            "mid",
            "ifheld",
            "covered",
            "story",
            "strategies",
            "catch",
            "trades",
            "pnl",
            *_AFTER,
        ),
    },
    "real-pnl": {
        "title": "What the trades really made",
        "headline": (
            "Do you know what your covered calls really made after all the rolls?"
        ),
        "lead": "Rolls, covered-call runs, and early exits, on the trades themselves.",
        "variants": {
            "rolls": "What did the rolls add, after every covered-call run?",
            "runs": "What the covered-call run actually made",
        },
        "cta_label": "Create free account",
        "cta_base": "create-account",
        "signup_route": "",
        "sections": (
            "covered",
            "pnl",
            "trades",
            "mid",
            "strategies",
            "ifheld",
            "catch",
            "story",
            "learn",
            "practice",
            "replays",
            *_AFTER,
        ),
    },
    "mistakes": {
        "title": "Bought back too early",
        "headline": "Bought it back early. Then it expired worthless.",
        "lead": "The broker shows the close. The page keeps what holding would have made.",
        "variants": {
            "early": "Early exits your broker never grades",
            "premium": "The premium you kept, and the premium you gave back",
        },
        "cta_label": "Create free account",
        "cta_base": "create-account",
        "signup_route": "",
        "sections": (
            "ifheld",
            "catch",
            "trades",
            "mid",
            "covered",
            "pnl",
            "strategies",
            "story",
            "learn",
            "practice",
            "replays",
            *_AFTER,
        ),
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


def _signup_url(page: dict) -> str:
    route = page.get("signup_route") or ""
    if route:
        return url_for("signup", route=route)
    return url_for("signup")


@app.route("/go/<slug>")
@limiter.limit("120 per minute; 2000 per hour")
def go_landing(slug):
    page = LANDINGS.get(slug)
    if page is None:
        abort(404)
    from app.marketing_videos import (
        catch_stories,
        resolve_learn_url,
        story_steps,
        trade_stories,
    )

    variant = allowed_variant(slug, request.args.get("v"))
    return render_template(
        "go_landing.html",
        title=page["title"],
        slug=slug,
        page=page,
        headline=headline_for(slug, variant),
        variant=variant,
        trial_line=TRIAL_LINE,
        sections=page["sections"],
        signup_url=_signup_url(page),
        cta_label=page["cta_label"],
        cta_base=page["cta_base"],
        campaign=True,
        marketing_chrome=True,
        story_steps=story_steps(),
        trade_stories=trade_stories(),
        catch_stories=catch_stories(),
        learn_url=resolve_learn_url(),
    )
