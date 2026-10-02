"""Equity line vs options line on the position chart."""
import inspect
import time
from types import SimpleNamespace

from pathlib import Path

from app.position_chart_read import (
    _SYSTEM,
    _is_small_sum,
    _too_long,
    _ungrounded,
    _waiting_cost,
    chart_body_within,
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


def test_review_brief_never_silently_truncates_long_positions():
    markers = [
        {"d": f"2026-01-{(i % 28) + 1:02d}", "t": [f"Trade day {i}."]}
        for i in range(81)
    ]

    assert review_brief(markers) is None


def test_short_or_flat_chart_stays_quiet():
    eq = [0, 10, 20]
    opt = [0, 1, 2]
    assert chart_path_facts(_series(eq, opt), []) is None

    flat_eq = [100] * 20
    moving_opt = [i * 40 for i in range(20)]
    assert chart_path_facts(_series(flat_eq, moving_opt), []) is None


def test_small_sum_accepts_two_to_four_distinct_source_amounts():
    assert _is_small_sum(10, {1, 9})
    assert _is_small_sum(6, {1, 2, 3})
    assert _is_small_sum(10, {1, 2, 3, 4})
    assert not _is_small_sum(10, {10})


def test_small_sum_validation_is_bounded_for_long_reviews():
    amounts = {float(value) for value in range(1, 241)}

    started = time.perf_counter()
    matched = _is_small_sum(10_000, amounts)
    elapsed = time.perf_counter() - started

    assert matched is False
    assert elapsed < 1.0


def test_waiting_cost_counts_each_same_day_exit():
    lines = [{
        "date": "2026-06-19",
        "line": (
            "One contract expired worthless — that close gave up $100 vs holding. "
            "Another contract expired worthless — that close gave up $200 vs holding."
        ),
    }]

    assert _waiting_cost(lines) == 300


def test_waiting_cost_does_not_cross_same_day_event_boundaries():
    lines = [{
        "date": "2026-06-19",
        "line": (
            "One contract finished in the money — that close gave up $900 vs holding. "
            "Another contract expired worthless — that close gave up $200 vs holding."
        ),
    }]

    assert _waiting_cost(lines) == 200


def test_canonical_waiting_total_may_sum_more_than_four_exits():
    lines = [
        {
            "date": f"2026-06-{day:02d}",
            "line": (
                "The contract expired worthless — that close gave up "
                f"${amount:,} vs holding."
            ),
        }
        for day, amount in enumerate((100, 200, 300, 400, 500), start=1)
    ]
    draft = (
        "You closed five contracts before expiry. "
        "Those closes cost $1,500. "
        "The lesson from this chart is that closing cost $1,500."
    )

    assert _waiting_cost(lines) == 1_500
    assert _ungrounded(draft, lines) is None


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


def test_locked_chart_read_hides_lesson_that_model_puts_first():
    text = (
        "The lesson from this chart is that you paid $430 to exit. "
        "You opened the position in April."
    )

    lead, rest = visible_chart_read(text, unlocked=False)

    assert lead == "A chart read is ready."
    assert rest == ""
    assert "$430" not in lead
    assert _too_long(text) == (
        'the last sentence must start with "The lesson from this chart"'
    )


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
    assert payload["hide"] is False
    assert "protected comparison" not in response.get_data(as_text=True)


def _chart_read_view(monkeypatch, *, row, generate=None, execute=None, unlock=False):
    from app import app
    import app.db as db
    import app.llm_access as llm_access
    import app.position_chart_read as chart_read_module
    import app.position_detail as position_detail_module

    monkeypatch.setattr(chart_read_module, "load_chart_read", lambda *_args: row)
    monkeypatch.setattr(llm_access, "user_can_use_paid_llm", lambda _user_id: unlock)
    monkeypatch.setattr(
        position_detail_module, "current_user", SimpleNamespace(id=17)
    )
    if generate is not None:
        monkeypatch.setattr(chart_read_module, "chart_body_within", generate)
    if execute is not None:
        monkeypatch.setattr(db, "execute", execute)
    view = inspect.unwrap(app.view_functions["position_chart_read"])
    with app.test_request_context(
        "/position/BE/chart-read",
        method="POST",
        data={"digest": "abc123", "scope": "tenant|1"},
    ):
        return view("BE")


def test_chart_read_endpoint_hides_when_generation_returns_nothing(monkeypatch):
    response = _chart_read_view(
        monkeypatch,
        row={"body": "", "brief": "{}"},
        generate=lambda *_args, **_kwargs: None,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["hide"] is True
    assert payload["lead"] == ""
    assert payload["body"] == ""


def test_chart_read_endpoint_hides_when_generation_raises(monkeypatch):
    def explode(*_args, **_kwargs):
        raise RuntimeError("vendor down")

    response = _chart_read_view(
        monkeypatch,
        row={"body": "", "brief": "{}"},
        generate=explode,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["hide"] is True
    assert payload["lead"] == ""
    assert "vendor down" not in response.get_data(as_text=True)


def test_chart_read_endpoint_hides_when_the_stored_read_cannot_be_loaded(monkeypatch):
    from app import app
    import app.position_chart_read as chart_read_module
    import app.position_detail as position_detail_module

    def explode(*_args, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(chart_read_module, "load_chart_read", explode)
    monkeypatch.setattr(
        position_detail_module, "current_user", SimpleNamespace(id=17)
    )
    view = inspect.unwrap(app.view_functions["position_chart_read"])
    with app.test_request_context(
        "/position/BE/chart-read",
        method="POST",
        data={"digest": "abc123", "scope": "tenant|1"},
    ):
        response = view("BE")

    payload = response.get_json()
    assert payload["hide"] is True
    assert payload["lead"] == ""
    assert "db down" not in response.get_data(as_text=True)


def test_chart_read_endpoint_still_returns_text_when_the_cache_write_fails(monkeypatch):
    prose = (
        "You opened the position in April. "
        "The lesson from this chart is that the exits cost $430."
    )

    def refuse_write(*_args, **_kwargs):
        raise RuntimeError("socket closed")

    response = _chart_read_view(
        monkeypatch,
        row={"body": "", "brief": "{}"},
        generate=lambda *_args, **_kwargs: prose,
        execute=refuse_write,
        unlock=True,
    )

    payload = response.get_json()
    assert payload["hide"] is False
    assert payload["lead"] == "You opened the position in April."
    assert "exits cost $430" in payload["body"]


def test_chart_body_within_returns_before_a_hung_model(monkeypatch):
    import app.position_chart_read as chart_read_module

    def hang(_facts, deadline=None):
        time.sleep(5)
        return "late prose that must not be used"

    monkeypatch.setattr(chart_read_module, "generate_chart_body", hang)
    started = time.perf_counter()
    body = chart_body_within({"review_lines": []}, budget_s=0.2)
    elapsed = time.perf_counter() - started

    assert body is None
    assert elapsed < 1.5


def test_chart_read_script_clears_the_skeleton_on_failure():
    page = Path("app/templates/position_detail.html").read_text()
    script = page.split("getElementById('chartRead')", 1)[1].split("</script>", 1)[0]

    assert "AbortController" in script
    assert "10000" in script
    assert "box.hidden = true" in script
    assert "data.hide" in script
    assert ".catch(function () {})" not in script
