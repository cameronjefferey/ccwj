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

# Hero, three angle sections, then proof, a short FAQ, and the close.
_TAIL = ("proof", "faq_short", "close")

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
        "kicker": "Free forever · no card",
        "hero_short_step": 6,
        "sections": ("learn", "practice", "replays", *_TAIL),
    },
    "real-pnl": {
        "title": "Real P&L across brokers",
        "headline": "See your real P&L across every broker",
        "lead": "Schwab, Fidelity, Vanguard, Robinhood, and others. Read-only.",
        "variants": {
            "rolls": "One P&L for every account you connect",
            "runs": "Your trades, marked across every brokerage",
        },
        "cta_label": "Create free account",
        "cta_base": "create-account",
        "signup_route": "real-pnl",
        "kicker": "30 days · no card",
        "hero_short_step": 2,
        "sections": ("overview", "pnl", "trades", *_TAIL),
    },
    "mistakes": {
        "title": "Bought back too early",
        "headline": "Bought it back early. Then it expired worthless.",
        "lead": (
            "The founder's covered call was bought back at a loss. "
            "It expired worthless two days later."
        ),
        "variants": {
            "early": "An early close, and what holding would have made",
            "premium": "The close you took, next to holding through expiration",
        },
        "cta_label": "Create free account",
        "cta_base": "create-account",
        "signup_route": "mistakes",
        "kicker": "30 days · no card",
        "hero_short_step": 3,
        "sections": ("mistake", "early", "counterfactual", *_TAIL),
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
    headline = headline_for(slug, variant)
    steps = story_steps()
    hero_short = next(
        (step for step in steps if step["step"] == page.get("hero_short_step")),
        None,
    )
    return render_template(
        "go_landing.html",
        title=page["title"],
        slug=slug,
        page=page,
        headline=headline,
        variant=variant,
        trial_line=TRIAL_LINE,
        og_title=headline,
        og_description=page["lead"],
        hero_short=hero_short,
        sections=page["sections"],
        signup_url=_signup_url(page),
        cta_label=page["cta_label"],
        cta_base=page["cta_base"],
        campaign=True,
        marketing_chrome=True,
        story_steps=steps,
        trade_stories=trade_stories(),
        catch_stories=catch_stories(),
        learn_url=resolve_learn_url(),
    )
