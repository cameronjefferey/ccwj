"""Ask AI brief + conversation helpers (no live LLM)."""
import pandas as pd

import app.insights as insights
import app.models as models


def test_saved_thread_query_keeps_id_available_for_stable_outer_order(monkeypatch):
    """The outer chronological sort needs ``id`` from the inner query.

    Without projecting it, Postgres rejects every thread read and the
    fail-closed helper makes saved Ask AI turns appear to vanish.
    """
    seen = {}

    def fake_fetch_all(sql, params):
        seen["sql"] = sql
        seen["params"] = params
        return [{"role": "user", "content": "Earlier turn"}]

    monkeypatch.setattr(models, "fetch_all", fake_fetch_all)

    rows = models.get_insight_messages(7, limit=12)

    inner = seen["sql"].split("FROM insight_messages", 1)[0]
    assert "SELECT id, role, content, model_key, created_at" in inner
    assert ") recent ORDER BY created_at ASC, id ASC" in seen["sql"]
    assert seen["params"] == (7, 12)
    assert rows[0]["content"] == "Earlier turn"


def test_ask_brief_includes_execution_and_prior_analysis():
    text = insights._ask_brief(
        "giveback 12%",
        "3 symbols",
        "WEEK 2026-08-10: 2 closed",
        execution_text="EXECUTION REVIEW\n- −$400 early exits",
        prior_text="PRIOR ANALYSIS:\nYou held past peak on CSPs.",
    )
    assert "BEHAVIORAL SIGNALS" in text
    assert "giveback 12%" in text
    assert "PORTFOLIO OVERVIEW" in text
    assert "LAST WEEK DATA" in text
    assert "EXECUTION REVIEW" in text
    assert "PRIOR ANALYSIS" in text


def test_ask_brief_empty_when_nothing():
    assert insights._ask_brief(None, None, None) is None


def test_call_coach_question_keeps_brief_in_system(monkeypatch):
    seen = {}

    def fake_call_llm(system, user, **kw):
        seen["system"] = system
        seen["user"] = user
        seen.update(kw)
        return "answer", None

    monkeypatch.setattr(insights, "call_llm", fake_call_llm)
    text, err = insights._call_coach_question(
        "BRIEF DATA",
        "Which DTE is strongest?",
        model_key="claude-haiku-4-5",
        allow_paid=False,
        history=[{"role": "user", "content": "first"}, {"role": "assistant", "content": "ok"}],
    )
    assert err is None
    assert text == "answer"
    assert "BRIEF DATA" in seen["system"]
    assert seen["user"] == "Which DTE is strongest?"
    assert seen["max_tokens"] == 1500
    assert seen["history"][0]["content"] == "first"
    assert seen["allow_paid"] is False


def test_prior_analysis_truncates(monkeypatch):
    monkeypatch.setattr(
        insights, "get_insight_for_user",
        lambda uid: {"full_analysis": "x" * 4000, "summary": "s"},
    )
    text = insights._prior_analysis_brief(1)
    assert text.startswith("PRIOR ANALYSIS (older narration")
    assert "CANONICAL EXIT TIMING" in text
    assert text.endswith("[truncated]")
    assert len(text) < 3200


def _signal_row(**overrides):
    row = {
        "strategy": "Covered Call",
        "account": "Schwab Account",
        "reliable_contracts": 9,
        "total_closed": 77,
        "total_pnl_given_back": 5441,
        "avg_giveback_pct": 0,
        "avg_days_held_past_peak": 0,
        "avg_pct_premium_captured": 0,
        "best_dte_bucket": None,
        "best_dte_trades": 0,
        "best_dte_win_rate": 0,
        "worst_dte_bucket": None,
        "worst_dte_trades": 0,
        "worst_dte_win_rate": 0,
    }
    row.update(overrides)
    return row


def test_exit_timing_rollup_merges_same_strategy_across_accounts():
    """Two accounts both labeled Covered Call are one card, not two.

    A Long Call row under the reliable-contract minimum stays off the
    card list, so it cannot be relabeled as a second Covered Call.
    """
    df = pd.DataFrame([
        _signal_row(account="A"),
        _signal_row(
            account="B", reliable_contracts=4, total_closed=101,
            total_pnl_given_back=4379, avg_giveback_pct=95,
            avg_days_held_past_peak=3,
        ),
        _signal_row(
            strategy="Long Call", account="A", reliable_contracts=2,
            total_closed=40, total_pnl_given_back=13766,
            avg_giveback_pct=5, avg_days_held_past_peak=1,
        ),
    ])
    rows = insights._rollup_exit_signals(df)
    assert [r["strategy"] for r in rows] == ["Covered Call"]
    assert rows[0]["account_count"] == 2
    assert rows[0]["trades"] == 13
    assert rows[0]["total_closed"] == 178
    assert rows[0]["pnl_given_back"] == 5441 + 4379
    # 9 contracts at 0% and 4 at 95%, weighted: 380 / 13.
    assert round(rows[0]["giveback_pct"], 1) == round(380 / 13, 1)

    headlines = insights._exit_timing_headlines(rows, 15, 410)
    brief = insights._format_exit_timing_brief(headlines, rows)
    assert f"${headlines['total_given_back']:,.0f}" in brief
    assert f"{headlines['avg_giveback']:.0f}%" in brief
    assert "15 of 410" in brief
    assert "$23,586" not in brief
    assert "Long Call" not in brief
    assert brief.count("Covered Call:") == 1
    assert "CANONICAL EXIT TIMING" in brief
    assert "do not rename" in brief.lower()


def test_coaching_brief_collapses_covered_call_across_accounts(monkeypatch):
    """The page list is the rollup, not one card per mart row.

    PR #117 tested ``_rollup_exit_signals`` and left
    ``_build_coaching_brief`` appending every (account, strategy) row.
    Live By Strategy kept two Covered Call cards ($5,441 at 0% and
    $4,379 at 95%). Both rows share the generic Schwab display name;
    the section is one card per strategy name.
    """
    df = pd.DataFrame([
        _signal_row(
            account="Schwab Account", tenant_id="snaptrade:aaaaaaaa",
        ),
        _signal_row(
            account="Schwab Account", tenant_id="snaptrade:bbbbbbbb",
            reliable_contracts=4, total_closed=101,
            total_pnl_given_back=4379, avg_giveback_pct=95,
            avg_days_held_past_peak=3,
        ),
        _signal_row(
            strategy="Long Call", account="Schwab Account",
            tenant_id="snaptrade:aaaaaaaa", reliable_contracts=2,
            total_closed=40, total_pnl_given_back=13766,
            avg_giveback_pct=5, avg_days_held_past_peak=1,
        ),
    ])

    def fake_parallel(client, specs):
        assert "coach_signals" in specs
        return {"coach_signals": df}

    monkeypatch.setattr("app.routes._bq_parallel", fake_parallel)

    from app import app as flask_app
    with flask_app.app_context():
        brief, data = insights._build_coaching_brief(
            None, ["snaptrade:aaaaaaaa", "snaptrade:bbbbbbbb"],
        )

    signals = data["signals"]
    assert [s["strategy"] for s in signals] == ["Covered Call"]
    assert signals[0]["strategy_label"] == "Covered Call"
    assert "·" not in signals[0]["strategy_label"]
    assert signals[0]["account_count"] == 2
    assert signals[0]["trades"] == 13
    assert signals[0]["total_closed"] == 178
    assert signals[0]["pnl_given_back"] == 5441 + 4379
    assert round(signals[0]["giveback_pct"], 1) == round(380 / 13, 1)
    # The 2-contract Long Call stays off the card list but in coverage.
    assert data["reliable_contracts"] == 15
    assert data["total_closed"] == 218
    assert data["exit_timing"]["reliable_contracts"] == 15
    assert data["exit_timing"]["total_closed"] == 218
    assert brief.count("Covered Call:") == 1
    assert "Covered Call ·" not in brief
    assert "2 accounts combined" in brief
    assert "Long Call" not in brief

    from jinja2 import Environment
    from pathlib import Path
    src = Path(__file__).resolve().parents[1].joinpath(
        "app/templates/insights.html"
    ).read_text()
    start = src.index("{% for s in coaching.signals %}")
    end = src.index("{% endfor %}", start) + len("{% endfor %}")
    html = Environment().from_string(src[start:end]).render(coaching=data)
    assert html.count('class="strat-name"') == 1
    assert "Covered Call" in html
    assert "Covered Call ·" not in html
    assert "13 of 178" in html
    assert "$9,820" in html
    assert "2 accounts" in html


def test_stale_analysis_coverage_disagrees_with_live_cards():
    text = "only 4 of 391 closed contracts have sufficient daily snapshot data"
    conflict = insights.analysis_coverage_conflict(text, 15, 410)
    assert conflict == {
        "cited_reliable": 4,
        "cited_total": 391,
        "live_reliable": 15,
        "live_total": 410,
    }
    assert insights.analysis_coverage_conflict(text, 4, 391) is None
    # A single example ("2 of 3 contracts") is not a coverage claim.
    assert insights.analysis_coverage_conflict("2 of 3 contracts", 15, 410) is None


def test_ask_markdown_hash_heading_is_a_compact_label():
    html = str(insights._md_to_html(
        "# DTE Performance: Your Strongest Windows\n- 0–7 DTE **wins**",
        compact_headings=True,
    ))
    assert "# DTE" not in html
    assert "<h1>" not in html and "<h2>" not in html
    assert 'class="ask-md-heading"' in html
    assert "DTE Performance: Your Strongest Windows" in html
    assert "<strong>wins</strong>" in html


def test_analysis_markdown_hash_heading_is_a_section():
    html = str(insights._md_to_html("# Summary\n## Detail"))
    assert "<h2>Summary</h2>" in html
    assert "<h2>Detail</h2>" in html
    assert "# Summary" not in html


def test_markdown_list_items_are_escaped():
    html = str(insights._md_to_html("- <script>alert(1)</script>"))
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
