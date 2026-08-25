"""CI guards for Jinja footguns that 500 outside a view's try/except.

The /story crash after PR #45 was `this_week.items` resolving to
dict.items the method. Pytest was green because nothing rendered the
template. These checks run in the existing CI pytest job — no extra
workflow needed.

Never name a Jinja-consumed dict key `items`, `values`, or `keys`.
Call the method when you mean it: `all_features.items()`.
"""

from pathlib import Path
from types import SimpleNamespace
import re

_TEMPLATES = Path(__file__).resolve().parents[1] / "app" / "templates"

# Jinja print/statement/comment tags only — CSS `align-items` is out of scope.
_JINJA_TAG = re.compile(r"\{[{%#].*?[}%#]\}", re.DOTALL)
# Attribute access, not a call. `foo.items()` is fine; `foo.items` is not.
_DICT_METHOD_ATTR = re.compile(r"\.(items|values|keys)(?!\s*\()")


def _jinja_dict_method_hits(source: str):
    hits = []
    for tag in _JINJA_TAG.findall(source):
        for match in _DICT_METHOD_ATTR.finditer(tag):
            hits.append((match.group(1), tag.strip()))
    return hits


def _render_trader_story(novel):
    from app import app
    with app.test_request_context("/story"):
        return app.jinja_env.get_template("trader_story.html").render(
            current_user=SimpleNamespace(is_authenticated=False, username="t"),
            title="Trader Profile",
            novel=novel,
            accounts=[],
            selected_account="",
            error=None,
        )


def test_templates_do_not_use_dict_method_attributes():
    offenders = []
    for path in sorted(_TEMPLATES.rglob("*.html")):
        rel = path.relative_to(_TEMPLATES)
        for method, tag in _jinja_dict_method_hits(path.read_text()):
            offenders.append(f"{rel}: .{method} in {tag}")
    assert not offenders, (
        "Jinja prefers attributes over keys, so `.items` / `.values` / "
        "`.keys` is the dict method (not a list key) and 500s the page. "
        "Rename the key, or call the method with ().\n" + "\n".join(offenders)
    )


def test_guard_flags_the_story_500_pattern():
    bad = "{% for w in novel.loop.this_week.items %}"
    assert _jinja_dict_method_hits(bad) == [("items", bad)]
    assert not _jinja_dict_method_hits("{% for s, f in all_features.items() %}")
    assert not _jinja_dict_method_hits(".ts-finding { align-items: baseline; }")


def test_trader_story_template_renders_loop_from_builders():
    """Render the real /story template with a builder-built loop.

    The view's try/except wraps query + compose, then render_template
    runs outside it — a Jinja TypeError becomes the generic 500 page.
    This is the check that would have failed on PR #45.
    """
    import pandas as pd

    from app.story_loop import compose_story_loop
    from app.trader_story import build_book, compose_novel
    from tests.test_story_loop import (
        _TODAY, _habit_rolls, _history_with_last_week, _open,
    )
    from tests.test_trader_story import _BOOK_SUMMARY, _BOOK_TRADES

    novel = compose_novel(build_book(_BOOK_TRADES, None, None, _BOOK_SUMMARY),
                          _BOOK_TRADES)
    assert novel is not None
    open_df = pd.DataFrame([
        _open(trade_symbol="VICR  260828P00210000", option_strike=210.0,
              direction="Sold"),
        _open(trade_symbol="VICR  260828P00190000", option_strike=190.0,
              direction="Bought", premium_received=0.0, premium_paid=-300.0,
              current_unrealized_pnl=-80.0),
    ])
    novel["loop"] = compose_story_loop(
        _history_with_last_week(last_fills=4, typical_fills=4, weeks=5),
        open_df,
        _habit_rolls(),
        today=_TODAY,
    )
    assert novel["loop"]["this_week"]["watches"]

    html = _render_trader_story(novel)
    assert "This week" in html
    assert "Last week" in html
    assert "VICR" in html
    assert "roll it, or let this one expire?" in html
    assert "Put Spread" in html
    assert "+$520" in html
    assert "Profile summary" in html


def test_trader_story_template_renders_empty_loop_card():
    """Empty watches must still render — the old `{% if this_week.items %}`
    was always true and then 500'd the for-loop."""
    from app.story_loop import build_last_week, build_this_week
    from tests.test_story_loop import _TODAY

    this_week = build_this_week(None, today=_TODAY)
    last_week = build_last_week(None, today=_TODAY)
    novel = {
        "hero_counts": {"stories": 1, "chapters": 1, "since": None,
                        "open_stories": 0},
        "profile": {"headline": "A quiet book.", "facts": []},
        "execution": None,
        "standouts": [],
        "scoreboard": [],
        "eras": [],
        "open_stories": 0,
        "loop": {"this_week": this_week, "last_week": last_week},
    }

    html = _render_trader_story(novel)
    assert "Nothing on the clock" in html
    assert "This week" in html
    assert "A quiet book." in html


def test_trader_story_template_renders_leftover_sentence():
    import pandas as pd

    from app.story_loop import build_this_week
    from datetime import timedelta

    from tests.test_story_loop import _TODAY, _open, _otm_rolls_at_dte

    this_week = build_this_week(
        pd.DataFrame([
            _open(symbol="NVDA", option_type="C", option_strike=230.0,
                  option_expiry=_TODAY + timedelta(days=3),
                  current_unrealized_pnl=450.0),
        ]),
        _otm_rolls_at_dte(),
        today=_TODAY,
    )
    novel = {
        "hero_counts": {"stories": 1, "chapters": 1, "since": None,
                        "open_stories": 0},
        "profile": {"headline": "A quiet book.", "facts": []},
        "execution": None,
        "standouts": [],
        "scoreboard": [],
        "eras": [],
        "open_stories": 0,
        "loop": {"this_week": this_week,
                 "last_week": {"label": "Aug 17–23", "headline": "Quiet.",
                               "facts": []}},
    }
    html = _render_trader_story(novel)
    assert "+$450" in html
    assert "Currently +$450 with 3 days left" in html
    assert "costs you 15% of the credit" in html
    assert "instead of expiry" in html
