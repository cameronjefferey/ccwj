"""Focused ad landings, one per message test.

``/go/<slug>`` is noindex. The slug and the optional ``?v=`` headline
variant stick on the first-party touch cookie so later funnel events
and the signup record carry the campaign.
"""
from __future__ import annotations

from flask import abort, render_template, request

from app import app
from app.extensions import limiter

TRIAL_LINE = (
    "Learning and paper trading are free. "
    "Your 30-day trial starts when you connect a real brokerage."
)

# Headline variants are an allow-list. An unknown ``?v=`` keeps the
# default headline and is not stored.
LANDINGS = {
    "learn": {
        "title": "Learn options free",
        "headline": "Learn options free, with real past trades and paper money.",
        "variants": {
            "past": "Learn from real past trades, then practice with paper money.",
            "free": "Options lessons are free. So is paper money.",
        },
        "image": "marketing/msft-if-held.webp",
        "image_width": 1400,
        "image_height": 315,
        "alt": (
            "If held to expiration for one MSFT covered-call position. "
            "Closing early cost money versus holding, on a single position."
        ),
        "primary_label": "Start learning free",
        "primary_endpoint": "signup",
        "primary_kwargs": {"route": "learn"},
        "primary_cta": "start-learning",
        "secondary_label": "",
        "secondary_endpoint": "",
        "secondary_kwargs": {},
        "secondary_cta": "",
    },
    "real-pnl": {
        "title": "What the trades really made",
        "headline": (
            "See what your options trades really made "
            "(rolls, covered-call runs, early exits)."
        ),
        "variants": {
            "rolls": "Rolls, covered-call runs, and early exits, on one page.",
            "runs": "What the covered-call run actually made.",
        },
        "image": "marketing/catch/be-close.webp",
        "image_width": 1280,
        "image_height": 720,
        "alt": (
            "One BE covered call bought back early. The contract later "
            "expired, and the chart is that single position."
        ),
        "primary_label": "Try the live demo",
        "primary_endpoint": "demo_start",
        "primary_kwargs": {},
        "primary_cta": "try-demo",
        "secondary_label": "Start free trial",
        "secondary_endpoint": "signup",
        "secondary_kwargs": {},
        "secondary_cta": "start-trial",
    },
    "mistakes": {
        "title": "Mistakes your broker won't show",
        "headline": (
            "Catch the mistakes your broker won't show you "
            "(early exits, assignments, missed premium)."
        ),
        "variants": {
            "early": "Early exits your broker never grades.",
            "premium": "The premium you kept, and the premium you gave back.",
        },
        "image": "marketing/catch/be-swing.webp",
        "image_width": 1280,
        "image_height": 720,
        "alt": (
            "Cumulative P&L on one BE position from April to September 2026. "
            "The line rises, drops, and recovers."
        ),
        "primary_label": "Connect your broker, free 30 days",
        "primary_endpoint": "signup",
        "primary_kwargs": {},
        "primary_cta": "connect-broker",
        "secondary_label": "",
        "secondary_endpoint": "",
        "secondary_kwargs": {},
        "secondary_cta": "",
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


@app.route("/go/<slug>")
@limiter.limit("120 per minute; 2000 per hour")
def go_landing(slug):
    page = LANDINGS.get(slug)
    if page is None:
        abort(404)
    variant = allowed_variant(slug, request.args.get("v"))
    return render_template(
        "go_landing.html",
        slug=slug,
        page=page,
        headline=headline_for(slug, variant),
        variant=variant,
        trial_line=TRIAL_LINE,
    )
