"""Ask AI brief + conversation helpers (no live LLM)."""
import app.insights as insights


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
    assert text.startswith("PRIOR ANALYSIS:")
    assert text.endswith("[truncated]")
    assert len(text) < 3200
