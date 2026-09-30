"""Strategy Fit AI must use and cache the visible account scope."""

from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from app import app
import app.strategy_fit_insights as insights


def _route_body():
    """Skip login/rate-limit wrappers; this test exercises route behavior."""
    return insights.generate_strategy_fit_insights.__wrapped__.__wrapped__


def test_generation_preserves_tenant_and_group_scope(monkeypatch):
    saved = {}
    seen = {}

    monkeypatch.setattr(insights, "current_user", SimpleNamespace(id=42))
    monkeypatch.setattr(insights, "demo_block_writes", lambda _action: None)
    monkeypatch.setattr(insights, "llm_available", lambda: True)
    monkeypatch.setattr(insights, "get_bigquery_client", lambda: object())
    monkeypatch.setattr(
        insights,
        "_user_accounts",
        lambda selected: seen.setdefault("accounts", ["snaptrade:aaa"]),
    )
    monkeypatch.setattr(
        insights,
        "_build_strategy_fit_brief",
        lambda client, tenant_ids: ("tenant-scoped brief", {}),
    )
    monkeypatch.setattr(
        insights,
        "_call_strategy_fit",
        lambda brief, model_key=None: (("Summary", "Full"), None),
    )
    monkeypatch.setattr(insights, "get_user_llm_model", lambda user_id: None)
    monkeypatch.setattr(insights, "resolve_model_key", lambda key: "flash")
    monkeypatch.setattr(
        insights,
        "save_strategy_fit_insight",
        lambda user_id, **kwargs: saved.update(user_id=user_id, **kwargs),
    )

    with app.test_request_context(
        "/strategy-fit/insights/generate"
        "?tenants=snaptrade:aaa&groups=7&dim=dte",
        method="POST",
    ):
        response = _route_body()()

    assert seen["accounts"] == ["snaptrade:aaa"]
    assert saved["account_filter"] == "tenants:snaptrade:aaa"
    query = parse_qs(urlparse(response.location).query)
    assert query["view"] == ["fit"]
    assert query["tenants"] == ["snaptrade:aaa"]
    assert query["groups"] == ["7"]
    assert query["dim"] == ["dte"]
