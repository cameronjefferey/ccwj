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


def test_fit_brief_counts_a_spread_as_one_trade(monkeypatch):
    """The write-up must use the same trade the matrix counts."""
    import pandas as pd

    from app.strategy_fit_insights import _build_strategy_fit_brief

    summary = pd.DataFrame([
        {
            "tenant_id": "snaptrade:one",
            "account": "Main",
            "symbol": "SPY",
            "strategy": "Put Credit Spread",
            "sector": "Index",
            "subsector": "Index",
            "total_pnl": 80.0,
            "realized_pnl": 80.0,
            "unrealized_pnl": 0.0,
            "num_individual_trades": 2,
            "num_winners": 2,
            "num_losers": 0,
        },
    ])
    units = [{
        "tenant_id": "snaptrade:one",
        "symbol": "SPY",
        "strategy": "Put Credit Spread",
        "sector": "Index",
        "counts_as_trade": True,
        "num_winners": 1,
        "num_losers": 0,
    }]

    monkeypatch.setattr(
        "app.query_cache._to_dataframe_fast", lambda _q: summary.copy(),
    )
    monkeypatch.setattr(
        "app.grouped_trades.fetch_grouped_book",
        lambda *_a, **_k: (units, []),
    )
    brief, payload = _build_strategy_fit_brief(
        type("C", (), {"query": lambda *a, **k: None})(),
        ["snaptrade:one"],
    )
    assert "1 trade" in brief
    assert "2 trades" not in brief
    assert "2 fills" not in brief
    assert "A spread counts as one trade" in brief
    assert payload["baseline"]["total_trades"] == 1
    assert payload["baseline"]["winners"] == 1
    assert "+$-" not in brief
