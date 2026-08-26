"""LLM catalog, paid-model fallback, and multi-turn history shaping."""
import app.llm as llm


def test_paid_models_appear_when_vendor_key_present(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("GEMINI_API_KEY", "gem-test")
    monkeypatch.delenv("SELECTABLE_LLM_MODELS", raising=False)
    keys = {m["key"] for m in llm.selectable_models()}
    assert "claude-haiku-4-5" in keys
    assert "claude-sonnet-4-6" in keys
    assert "claude-opus-4-8" in keys
    assert "gemini-2.5-pro" in keys
    assert llm.model_is_paid("claude-sonnet-4-6") is True
    assert llm.model_is_paid("gemini-2.5-pro") is True
    assert llm.model_is_paid("claude-haiku-4-5") is False


def test_every_paid_catalog_row_is_selectable_without_allowlist(monkeypatch):
    """Operator path: add a MODEL_CATALOG row with tier=paid; it shows
    whenever the vendor key is set. The env allowlist is only for included
    models."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("GEMINI_API_KEY", "gem-test")
    monkeypatch.delenv("SELECTABLE_LLM_MODELS", raising=False)
    selectable = {m["key"]: m for m in llm.selectable_models()}
    paid = [k for k, spec in llm.MODEL_CATALOG.items() if spec["tier"] == "paid"]
    assert paid, "catalog should include at least one paid model"
    for key in paid:
        assert key in selectable
        assert selectable[key]["group"] == "addon"
        assert selectable[key]["tier"] == "paid"
    included = [m for m in selectable.values() if m["tier"] != "paid"]
    assert included
    assert all(m["group"] == "included" for m in included)


def test_resolve_paid_key_falls_back_without_allow_paid(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("GEMINI_API_KEY", "gem-test")
    monkeypatch.delenv("SELECTABLE_LLM_MODELS", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "claude")
    assert llm.resolve_model_key("claude-sonnet-4-6") == "claude-haiku-4-5"
    assert llm.resolve_model_key("gemini-2.5-pro") == "claude-haiku-4-5"
    assert llm.resolve_model_key("claude-sonnet-4-6", allow_paid=True) == "claude-sonnet-4-6"
    assert llm.resolve_model_key("gemini-2.5-pro", allow_paid=True) == "gemini-2.5-pro"


def test_default_model_prefers_unpaid(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("GEMINI_API_KEY", "gem-test")
    monkeypatch.delenv("SELECTABLE_LLM_MODELS", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "claude")
    assert llm.default_model_key() == "claude-haiku-4-5"


def test_normalized_history_drops_junk():
    assert llm._normalized_history([
        {"role": "user", "content": "hello"},
        {"role": "system", "content": "nope"},
        {"role": "assistant", "content": "  "},
        {"role": "assistant", "content": "hi"},
    ]) == [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi"},
    ]


def test_call_llm_passes_history_to_claude(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("GEMINI_API_KEY", "")
    seen = {}

    def fake_claude(model, system, user, *, kind, max_tokens, temperature, history=None):
        seen.update({
            "model": model, "system": system, "user": user,
            "history": history, "kind": kind,
        })
        return "ok", None

    monkeypatch.setattr(llm, "_call_claude", fake_claude)
    text, err = llm.call_llm(
        "sys", "follow up?",
        kind="coach.ask", max_tokens=100, temperature=0.1,
        model_key="claude-haiku-4-5",
        history=[{"role": "user", "content": "first"}, {"role": "assistant", "content": "ans"}],
    )
    assert err is None
    assert text == "ok"
    assert seen["user"] == "follow up?"
    assert seen["history"] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "ans"},
    ]


def test_insights_picker_template_has_included_and_addon_groups():
    from pathlib import Path

    html = Path("app/templates/insights.html").read_text()
    assert 'optgroup label="Included"' in html
    assert 'optgroup label="HappyTrader AI"' in html
    assert "js-llm-model-field" in html
    assert "billing_checkout_ai" in html
    assert "pricing" in html
    assert "happytrader-ai" in html


def test_pricing_page_offers_ai_addon():
    from app import app

    with app.test_client() as c:
        r = c.get("/pricing")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "HappyTrader AI" in body
    assert "9.99" in body
    assert "Gemini 2.5 Pro" in body
    assert "Claude Opus" in body
    assert "<h3>Full-access trial</h3>" in body
    assert "<h3>Pro</h3>" in body
    assert "<h3>Free</h3>" not in body
