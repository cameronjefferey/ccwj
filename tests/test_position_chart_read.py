"""Equity line vs options line on the position chart."""
import inspect
from types import SimpleNamespace

from app.position_chart_read import (
    _SYSTEM,
    _too_long,
    chart_path_facts,
    chart_read_sentences,
    review_brief,
    visible_chart_read,
)


def _series(eq, opt):
    return {
        "dates": [f"2026-01-{i+1:02d}" for i in range(len(eq))],
        "equity": eq,
        "options": opt,
    }


def test_rising_stock_and_option_losses_on_declines():
    # Shares climb, options drip up, then both fall and options fall harder.
    eq = [0, 200, 400, 700, 1000, 1400, 1800, 2200, 2600, 3000, 2400, 1800, 1000, 400]
    opt = [0, 20, 40, 80, 100, 140, 180, 200, 220, 250, 100, -80, -400, -900]
    markers = [{"d": f"2026-01-{i+1:02d}"} for i in (2, 4, 6, 8)]
    facts = chart_path_facts(_series(eq, opt), markers)
    assert facts is not None
    assert facts["window_option_loss"]
    lead, rest = chart_read_sentences(facts)
    assert "way up" in lead
    assert "shares fell" in lead
    assert "should" not in (lead + rest).lower()
    assert "100%" not in lead + rest
    assert "take advantage" not in (lead + rest).lower()


def test_bounce_days_do_not_hide_the_drop():
    # Option losses land on bounce days inside the decline, so a day-by-day
    # sum says options gained on falling sessions. The high-to-low window
    # still shows the giveback.
    eq = [i * 500 for i in range(12)] + [5400, 5600, 2000]
    opt = [i * 30 for i in range(12)] + [1130, -800, -1200]
    markers = [{"d": f"2026-01-{i+1:02d}"} for i in (2, 4, 6, 8, 10)]
    facts = chart_path_facts(_series(eq, opt), markers)
    assert facts is not None
    assert facts["window_option_loss"]
    assert not facts["options_worse_on_declines"]
    lead, _rest = chart_read_sentences(facts)
    assert "way up" in lead


def test_review_brief_is_the_lines_and_the_prompt_picks_no_lesson():
    markers = [
        {"d": "2026-04-23", "t": ["Started the stock position: 200 shares at $238.19 ($47,639)."]},
        {"d": "2026-05-11", "t": ["The $290 call expired worthless — you kept the full $3,003 premium."]},
        {"d": "2026-05-13", "t": ["Bought back the $285 call for $3,121 — a net $733 loss on the contract."]},
        {"d": "2026-06-01", "t": ["Sold the shares."]},
    ]
    brief = review_brief(markers)
    assert [row["line"] for row in brief["review_lines"]] == [m["t"][0] for m in markers]
    assert "closing early" not in _SYSTEM.lower()
    assert "covered call" not in _SYSTEM.lower()
    assert review_brief(markers[:2]) is None


def test_short_or_flat_chart_stays_quiet():
    eq = [0, 10, 20]
    opt = [0, 1, 2]
    assert chart_path_facts(_series(eq, opt), []) is None

    flat_eq = [100] * 20
    moving_opt = [i * 40 for i in range(20)]
    assert chart_path_facts(_series(flat_eq, moving_opt), []) is None


def test_locked_chart_read_never_exposes_paid_remainder():
    text = (
        "You opened the position in April. "
        "The option exits cost $430 compared with expiry. "
        "The lesson from this chart is that those exits cost $430."
    )

    lead, locked_rest = visible_chart_read(text, unlocked=False)
    assert lead == "You opened the position in April."
    assert locked_rest == ""

    paid_lead, paid_rest = visible_chart_read(text, unlocked=True)
    assert paid_lead == lead
    assert "option exits" in paid_rest
    assert paid_rest.endswith("$430.")


def test_locked_one_sentence_chart_read_fails_closed():
    text = "The lesson from this chart is that the exits cost $430."

    lead, rest = visible_chart_read(text, unlocked=False)

    assert lead == "A chart read is ready."
    assert rest == ""
    assert "$430" not in lead
    assert _too_long(text) is not None


def test_locked_chart_read_endpoint_redacts_cached_body(monkeypatch):
    from app import app
    import app.llm_access as llm_access
    import app.position_chart_read as chart_read_module
    import app.position_detail as position_detail_module

    full = (
        "You opened the position in April. "
        "The protected comparison is $430. "
        "The lesson from this chart is that the exits cost $430."
    )
    monkeypatch.setattr(
        chart_read_module,
        "load_chart_read",
        lambda *_args: {"body": full, "brief": "{}"},
    )
    monkeypatch.setattr(llm_access, "user_can_use_paid_llm", lambda _user_id: False)
    monkeypatch.setattr(
        position_detail_module, "current_user", SimpleNamespace(id=17)
    )

    view = inspect.unwrap(app.view_functions["position_chart_read"])
    with app.test_request_context(
        "/position/TEST/chart-read",
        method="POST",
        data={"digest": "abc123", "scope": "tenant|1"},
    ):
        response = view("TEST")

    payload = response.get_json()
    assert payload["lead"] == "You opened the position in April."
    assert payload["body"] == ""
    assert payload["locked"] is True
    assert "protected comparison" not in response.get_data(as_text=True)
