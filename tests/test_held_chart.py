"""P&L if an option had been held to expiration, and the price path around it.

The dollar identity is the one ``int_option_exit_quality`` already
publishes: realized minus ``early_close_vs_expiry_delta``. These tests
pin that for long and short, calls and puts, and the chart series that
hangs off it (fill prices at the open and close, intrinsic after).
"""

import json
from datetime import date

import pandas as pd
import pytest

from app.held_chart import (
    OPTION_MARKS_QUERY,
    UNDERLYING_CLOSES_QUERY,
    build_held_charts,
    fetch_held_series,
    intrinsic_per_share,
    option_mark_per_share,
    peek_held_summary,
    pnl_if_held,
)


def _row(**kw):
    base = {
        "tenant_id": "snaptrade:abc",
        "account": "Schwab Account",
        "trade_symbol": "ONON  250912C00041000",
        "option_type": "C",
        "option_strike": 41.0,
        "option_expiry": date(2025, 9, 12),
        "direction": "Bought",
        "open_date": date(2025, 8, 12),
        "close_date": date(2025, 8, 13),
        "close_type": "Closed",
        "contracts": 25,
        "realized_pnl": -3333.0,
        "cost_to_close": 0.0,
        "proceeds_from_close": 3825.0,
        "intrinsic_at_expiry": 7.6364,
        "early_close_vs_expiry_delta": -15266.0,
        "gradeable_early_close": True,
        "underlying_close_at_expiry": 48.6364,
    }
    base.update(kw)
    return base


def _prices(pairs):
    return pd.DataFrame(
        [{"date": d, "close_price": px} for d, px in pairs]
    )


def _kinds(chart):
    return [p["kind"] for p in chart["points"]]


def test_pnl_if_held_is_realized_minus_expiry_delta():
    # Long 25-lot: bought about $2.86, sold $1.53, expiry intrinsic
    # $7.6364. Warehouse delta is closing cash minus settlement.
    assert pnl_if_held(-3333.0, -15266.0) == 11933.0
    assert pnl_if_held(120.0, -180.0) == 300.0
    assert pnl_if_held(-500.0, 1200.0) == -1700.0
    assert pnl_if_held(None, -10.0) is None
    assert pnl_if_held(10.0, None) is None


def test_intrinsic_calls_and_puts():
    assert intrinsic_per_share("C", 41, 48.5) == pytest.approx(7.5)
    assert intrinsic_per_share("Call", 41, 40) == 0
    assert intrinsic_per_share("P", 50, 40) == pytest.approx(10)
    assert intrinsic_per_share("Put", 50, 55) == 0
    assert intrinsic_per_share("C", 41, None) is None
    assert intrinsic_per_share("", 41, 50) is None


def test_long_call_chart_matches_hindsight_dollars_and_fills():
    charts = build_held_charts(pd.DataFrame([_row()]), _prices([
        (date(2025, 8, 12), 40.0),
        (date(2025, 8, 13), 39.0),
        (date(2025, 8, 14), 42.0),
        (date(2025, 9, 12), 48.6364),
    ]))
    assert len(charts) == 1
    c = charts[0]
    assert c["label"] == "$41 call"
    assert c["direction_label"] == "Bought to open"
    assert c["realized_pnl"] == -3333.0
    assert c["pnl_if_held"] == 11933.0
    assert c["difference"] == -15266.0
    # Fill prices: |opening cash| / (25 × 100), |closing cash| / (25 × 100).
    # opening = -3333 - 3825 = -7158.
    assert c["open_price"] == pytest.approx(7158 / 2500)
    assert c["close_price"] == pytest.approx(1.53)
    assert c["expiry_price"] == pytest.approx(7.6364)
    assert c["points"][0]["kind"] == "open"
    assert c["points"][0]["source"] == "fill"
    assert c["points"][0]["price"] == pytest.approx(c["open_price"])
    close = next(p for p in c["points"] if p["kind"] == "close")
    assert close["price"] == pytest.approx(1.53)
    assert close["source"] == "fill"
    after = [p for p in c["points"] if p["kind"] in ("if_held", "expiry")]
    assert after
    assert all(p["source"] == "intrinsic" for p in after)
    # Aug 14 spot 42, strike 41 → $1 intrinsic, not the close fill.
    aug14 = next(p for p in c["points"] if p["date"] == "2025-08-14")
    assert aug14["kind"] == "if_held"
    assert aug14["price"] == pytest.approx(1.0)
    expiry = c["points"][-1]
    assert expiry["kind"] == "expiry"
    assert expiry["price"] == pytest.approx(7.6364)
    # Contract dollars = price × 100 × quantity.
    assert close["total"] == pytest.approx(1.53 * 100 * 25)
    assert "Fees aren't included." in c["path_note"]
    assert "estimate" in c["path_note"].lower()
    assert c["if_held_estimated"] is True
    json.dumps(c)


def test_short_call_bought_back_then_worthless():
    # Sold 1 call at $3, bought back at $1.80, expired worthless.
    # realized 120, delta -180, if held keeps the full $300 credit.
    row = _row(
        direction="Sold",
        option_type="C",
        option_strike=100,
        contracts=1,
        trade_symbol="XYZ   250815C00100000",
        open_date=date(2025, 8, 11),
        close_date=date(2025, 8, 12),
        option_expiry=date(2025, 8, 15),
        realized_pnl=120.0,
        cost_to_close=-180.0,
        proceeds_from_close=0.0,
        intrinsic_at_expiry=0.0,
        early_close_vs_expiry_delta=-180.0,
    )
    charts = build_held_charts(pd.DataFrame([row]), _prices([
        (date(2025, 8, 11), 90.0),
        (date(2025, 8, 12), 91.0),
        (date(2025, 8, 13), 92.0),
        (date(2025, 8, 14), 93.0),
        (date(2025, 8, 15), 94.0),
    ]))
    c = charts[0]
    assert c["direction_label"] == "Sold to open"
    assert c["open_price"] == pytest.approx(3.0)
    assert c["close_price"] == pytest.approx(1.8)
    assert c["realized_pnl"] == 120.0
    assert c["pnl_if_held"] == 300.0
    assert c["difference"] == -180.0
    assert all(p["price"] >= 0 for p in c["points"])
    assert c["points"][-1]["price"] == 0
    assert c["points"][-1]["kind"] == "expiry"


def test_short_put_itm_at_expiry_and_long_put():
    # Short put: sold $2, bought back $0.50, finished $10 intrinsic.
    # settlement for the short is -$1,000. delta = -50 - (-1000) = 950.
    short = _row(
        direction="Sold",
        option_type="P",
        option_strike=50,
        contracts=1,
        trade_symbol="XYZ   250815P00050000",
        open_date=date(2025, 8, 11),
        close_date=date(2025, 8, 12),
        option_expiry=date(2025, 8, 15),
        realized_pnl=150.0,
        cost_to_close=-50.0,
        proceeds_from_close=0.0,
        intrinsic_at_expiry=10.0,
        early_close_vs_expiry_delta=950.0,
    )
    # Long put: bought $4, sold $6, same $10 intrinsic.
    # delta = 600 - 1000 = -400. if held = 200 - (-400) = 600.
    long = _row(
        direction="Bought",
        option_type="P",
        option_strike=50,
        contracts=1,
        trade_symbol="XYZ   250815P00050000",
        tenant_id="snaptrade:other",
        open_date=date(2025, 8, 11),
        close_date=date(2025, 8, 12),
        option_expiry=date(2025, 8, 15),
        realized_pnl=200.0,
        cost_to_close=0.0,
        proceeds_from_close=600.0,
        intrinsic_at_expiry=10.0,
        early_close_vs_expiry_delta=-400.0,
    )
    prices = _prices([
        (date(2025, 8, 11), 55.0),
        (date(2025, 8, 12), 54.0),
        (date(2025, 8, 13), 48.0),
        (date(2025, 8, 15), 40.0),
    ])
    charts = build_held_charts(pd.DataFrame([short, long]), prices)
    by_dir = {c["direction"]: c for c in charts}
    assert by_dir["Sold"]["label"] == "$50 put"
    assert by_dir["Sold"]["pnl_if_held"] == -800.0
    assert by_dir["Sold"]["difference"] == 950.0
    assert by_dir["Sold"]["open_price"] == pytest.approx(2.0)
    assert by_dir["Sold"]["close_price"] == pytest.approx(0.5)
    assert by_dir["Bought"]["pnl_if_held"] == 600.0
    assert by_dir["Bought"]["difference"] == -400.0
    assert by_dir["Bought"]["open_price"] == pytest.approx(4.0)
    assert by_dir["Bought"]["close_price"] == pytest.approx(6.0)
    # Aug 13 spot 48, strike 50 → $2 intrinsic on the dashed segment.
    mid = next(p for p in by_dir["Sold"]["points"] if p["date"] == "2025-08-13")
    assert mid["kind"] == "if_held"
    assert mid["price"] == pytest.approx(2.0)
    # Largest |difference| is the short (950), so it is first and expanded.
    assert charts[0]["direction"] == "Sold"
    assert charts[0]["collapsed"] is False


def test_marks_fill_the_held_segment_and_not_the_dashed_tail():
    row = _row(
        contracts=1,
        open_date=date(2025, 8, 11),
        close_date=date(2025, 8, 14),
        option_expiry=date(2025, 8, 15),
        realized_pnl=-50.0,
        cost_to_close=0.0,
        proceeds_from_close=50.0,
        intrinsic_at_expiry=0.0,
        early_close_vs_expiry_delta=50.0,
        option_strike=100,
    )
    # opening = -50 - 50 = -100 → $1.00 open. close = $0.50.
    prices = _prices([
        (date(2025, 8, 11), 90.0),
        (date(2025, 8, 12), 90.0),
        (date(2025, 8, 13), 90.0),
        (date(2025, 8, 14), 90.0),
        (date(2025, 8, 15), 90.0),
    ])
    marks = pd.DataFrame([
        {
            "tenant_id": "snaptrade:abc",
            "trade_symbol": row["trade_symbol"],
            "date": date(2025, 8, 12),
            "current_price": 4.25,
            "market_value": 425.0,
            "quantity": 1,
        },
    ])
    c = build_held_charts(pd.DataFrame([row]), prices, marks)[0]
    by_date = {p["date"]: p for p in c["points"]}
    assert by_date["2025-08-12"]["source"] == "mark"
    assert by_date["2025-08-12"]["price"] == pytest.approx(4.25)
    # Carried to the next session; the close day stays the fill.
    assert by_date["2025-08-13"]["source"] == "mark"
    assert by_date["2025-08-13"]["price"] == pytest.approx(4.25)
    assert by_date["2025-08-14"]["kind"] == "close"
    assert by_date["2025-08-14"]["source"] == "fill"
    assert by_date["2025-08-15"]["source"] == "intrinsic"
    assert c["held_from_marks"] is True
    # Spot 90 vs strike 100 is $0 intrinsic — the mark must not leak past the close.
    assert by_date["2025-08-15"]["price"] == 0


def test_mark_stored_as_contract_dollars_is_normalized_per_share():
    # current_price 285 with market value $285 on 1 contract is $2.85/share.
    assert option_mark_per_share(285, -285, -1) == pytest.approx(2.85)
    assert option_mark_per_share(2.85, -285, -1) == pytest.approx(2.85)
    assert option_mark_per_share(0, 0, -1) == 0


def test_open_and_ungraded_contracts_are_omitted():
    prices = _prices([
        (date(2025, 8, 12), 40.0),
        (date(2025, 8, 13), 40.0),
        (date(2025, 9, 12), 50.0),
    ])
    rows = [
        _row(gradeable_early_close=False),
        _row(early_close_vs_expiry_delta=None),
        # Held through expiration: close date is the expiry. No dashed tail.
        _row(close_date=date(2025, 9, 12), gradeable_early_close=True),
        _row(contracts=0),
    ]
    assert build_held_charts(pd.DataFrame(rows), prices) == []
    assert build_held_charts(pd.DataFrame(), prices) == []
    assert build_held_charts(None, prices) == []


def test_missing_daily_prices_still_marks_open_close_and_expiry():
    c = build_held_charts(pd.DataFrame([_row()]), pd.DataFrame())[0]
    assert _kinds(c) == ["open", "close", "expiry"]
    assert c["points"][-1]["price"] == pytest.approx(7.6364)
    assert c["held_estimated"] is False
    assert "fill prices" in c["path_note"]


def test_intrinsic_between_open_and_close_is_labeled_estimated():
    c = build_held_charts(pd.DataFrame([_row(
        open_date=date(2025, 8, 11),
        close_date=date(2025, 8, 13),
    )]), _prices([
        (date(2025, 8, 11), 50.0),
        (date(2025, 8, 12), 50.0),
        (date(2025, 8, 13), 50.0),
        (date(2025, 9, 12), 48.6364),
    ]))[0]
    mid = next(p for p in c["points"] if p["date"] == "2025-08-12")
    assert mid["kind"] == "held"
    assert mid["source"] == "intrinsic"
    # Spot 50, strike 41 → $9, not a made-up option premium.
    assert mid["price"] == pytest.approx(9.0)
    assert c["held_estimated"] is True
    assert c["held_legend"] == "While held (estimated)"


def test_leg_filter_keeps_only_the_selected_open_date():
    early = _row(open_date=date(2025, 8, 12), trade_symbol="A")
    later = _row(open_date=date(2025, 9, 1), close_date=date(2025, 9, 2),
                 trade_symbol="B")
    prices = _prices([
        (date(2025, 8, 12), 40),
        (date(2025, 8, 13), 40),
        (date(2025, 9, 1), 40),
        (date(2025, 9, 2), 40),
        (date(2025, 9, 12), 48),
    ])
    charts = build_held_charts(
        pd.DataFrame([early, later]),
        prices,
        keep=lambda d: d < date(2025, 8, 20),
    )
    assert [c["trade_symbol"] for c in charts] == ["A"]


def test_expiry_endpoint_prefers_warehouse_intrinsic_over_the_daily_close():
    # Daily close would imply $9 intrinsic (50 − 41). The warehouse row
    # says 7.6364 — the dot and the P&L have to agree with that.
    c = build_held_charts(pd.DataFrame([_row()]), _prices([
        (date(2025, 8, 12), 40.0),
        (date(2025, 8, 13), 40.0),
        (date(2025, 9, 12), 50.0),
    ]))[0]
    assert c["points"][-1]["price"] == pytest.approx(7.6364)


def test_same_day_round_trip_before_expiry():
    c = build_held_charts(pd.DataFrame([_row(
        open_date=date(2025, 8, 12),
        close_date=date(2025, 8, 12),
    )]), _prices([
        (date(2025, 8, 12), 40.0),
        (date(2025, 8, 13), 45.0),
        (date(2025, 9, 12), 48.6364),
    ]))[0]
    assert c["points"][0]["kind"] == "open_close"
    assert "if_held" in _kinds(c) or c["points"][-1]["kind"] == "expiry"


def test_peek_summary_is_the_largest_difference():
    small = _row(early_close_vs_expiry_delta=-20.0, realized_pnl=10.0,
                 trade_symbol="SMALL", contracts=1,
                 cost_to_close=-30.0, proceeds_from_close=0.0,
                 intrinsic_at_expiry=0.0)
    big = _row()
    charts = build_held_charts(pd.DataFrame([small, big]), _prices([
        (date(2025, 8, 12), 40),
        (date(2025, 8, 13), 40),
        (date(2025, 9, 12), 48),
    ]))
    peek = peek_held_summary(charts)
    assert peek["difference"] == -15266.0
    assert peek["pnl_if_held"] == 11933.0
    assert peek["fees_note"] == "Fees aren't included."
    assert peek["points"][0]["kind"] == "open"
    assert peek_held_summary([]) is None


def test_account_label_and_multi_account_flag():
    rows = [
        _row(tenant_id="snaptrade:a", account="Schwab Account"),
        _row(tenant_id="snaptrade:b", account="Schwab Account",
             trade_symbol="OTHER"),
    ]
    charts = build_held_charts(
        pd.DataFrame(rows),
        _prices([
            (date(2025, 8, 12), 40),
            (date(2025, 8, 13), 40),
            (date(2025, 9, 12), 48),
        ]),
        label_for=lambda tid, acct: {"snaptrade:a": "Sara"}.get(tid, acct),
    )
    labels = {c["tenant_id"]: c["account"] for c in charts}
    assert labels["snaptrade:a"] == "Sara"
    assert labels["snaptrade:b"] == "Schwab Account"
    assert all(c["show_account"] for c in charts)


def test_underlying_query_is_public_and_marks_query_is_scoped():
    assert "tenant" not in UNDERLYING_CLOSES_QUERY.lower()
    assert "ANY_VALUE" in UNDERLYING_CLOSES_QUERY
    assert "tenant_id" in OPTION_MARKS_QUERY
    assert "{tenant_filter}" in OPTION_MARKS_QUERY


def test_held_series_are_outside_the_shared_position_batch():
    """A marks or price failure must not be a key in the batch that
    blanks Position Detail when the runner raises."""
    from app.position_detail import position_detail_query_batch
    batch = position_detail_query_batch(
        "ONON", ["snaptrade:abc"], ["snaptrade:abc"])
    assert "underlying_closes" not in batch
    assert "option_marks" not in batch


def test_marks_model_and_daily_prices_project_the_chart_columns():
    """int_option_marks_daily is a real dbt model (not a name we invented).
    The chart selects these columns; stg_daily_prices supplies the rest."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    marks = (root / "dbt/models/intermediate/int_option_marks_daily.sql").read_text()
    prices = (root / "dbt/models/staging/stg_daily_prices.sql").read_text()
    for col in (
        "tenant_id", "trade_symbol", "underlying_symbol", "date",
        "current_price", "market_value", "quantity",
    ):
        assert col in marks
    assert "int_option_marks_daily" in OPTION_MARKS_QUERY
    for col in ("tenant_id", "trade_symbol", "date", "current_price",
                "market_value", "quantity", "underlying_symbol"):
        assert col in OPTION_MARKS_QUERY
    assert "cast(date as date)" in prices
    assert "as close_price" in prices
    assert "as symbol" in prices
    assert "stg_daily_prices" in UNDERLYING_CLOSES_QUERY
    for col in ("date", "close_price", "symbol"):
        assert col in UNDERLYING_CLOSES_QUERY


def test_fetch_held_series_returns_empty_frames_when_queries_raise():
    def boom(*_a, **_k):
        raise RuntimeError("unrecognized name: int_option_marks_daily")

    import app.query_cache as query_cache
    original = query_cache.cached_query_df
    query_cache.cached_query_df = boom
    try:
        closes, marks = fetch_held_series(object(), "ONON", "AND tenant_id IN ('snaptrade:abc')")
    finally:
        query_cache.cached_query_df = original
    assert closes.empty
    assert marks.empty


def test_fetch_held_series_keeps_closes_when_only_marks_raise():
    def query(_client, sql, label=None):
        if label == "option_marks":
            raise RuntimeError("marks table missing")
        return pd.DataFrame([{"date": date(2025, 8, 12), "close_price": 40.0}])

    import app.query_cache as query_cache
    original = query_cache.cached_query_df
    query_cache.cached_query_df = query
    try:
        closes, marks = fetch_held_series(object(), "ONON", "")
    finally:
        query_cache.cached_query_df = original
    assert list(closes["close_price"]) == [40.0]
    assert marks.empty


def test_template_renders_the_summary_and_the_fees_note():
    from app import app
    charts = build_held_charts(pd.DataFrame([_row()]), _prices([
        (date(2025, 8, 12), 40.0),
        (date(2025, 8, 13), 39.0),
        (date(2025, 9, 12), 48.6364),
    ]))
    with app.app_context():
        html = app.jinja_env.get_template("_held_to_expiry.html").render(
            held_charts=charts,
            story_days=[{"type": "day"}],
        )
    assert "If held to expiration" in html
    assert "+$11,933" in html
    assert "-$3,333" in html
    assert "-$15,266" in html
    assert "Fees aren" in html and "t included." in html
    assert "Per share" in html
    assert "Contract $" in html
    assert 'id="if-held"' in html
    assert "hindsight notes" in html
    # Open contracts are simply absent — an empty list renders nothing.
    with app.app_context():
        empty = app.jinja_env.get_template("_held_to_expiry.html").render(
            held_charts=[], story_days=[])
    assert empty.strip() == ""


def _logged_in_user():
    from unittest.mock import MagicMock
    user = MagicMock()
    user.is_authenticated = True
    user.is_active = True
    user.is_anonymous = False
    user.id = 42
    user.username = "acme"
    user.get_id = lambda: "42"
    return user


def _summary_frame():
    return pd.DataFrame([{
        "tenant_id": "snaptrade:abc",
        "account": "Schwab Account",
        "symbol": "ONON",
        "strategy": "Long Call",
        "status": "Closed",
        "total_pnl": -3333.0,
        "realized_pnl": -3333.0,
        "unrealized_pnl": 0.0,
        "total_premium_received": 0.0,
        "total_premium_paid": 7158.0,
        "num_trade_groups": 1,
        "num_individual_trades": 2,
        "num_winners": 0,
        "num_losers": 1,
        "win_rate": 0.0,
        "avg_pnl_per_trade": -3333.0,
        "avg_days_in_trade": 1.0,
        "total_dividend_income": 0.0,
        "dividend_count": 0,
        "total_return": -3333.0,
        "first_trade_date": date(2025, 8, 12),
        "last_trade_date": date(2025, 8, 13),
        "company_name": "On Holding AG",
    }])


def test_position_page_and_peek_render_when_held_queries_raise():
    """The marks and underlying-close queries raising must not blank
    Position Detail or 503 the peek drawer."""
    from unittest.mock import patch
    import app.position_detail as detail
    import app.query_cache as query_cache

    def boom(*_a, **_k):
        raise RuntimeError("int_option_marks_daily is not a real table")

    def page_batch(_client, queries):
        assert "underlying_closes" not in queries
        assert "option_marks" not in queries
        return {
            name: (_summary_frame() if name == "summary" else pd.DataFrame())
            for name in queries
        }

    def peek_batch(_client, queries):
        assert "underlying_closes" not in queries
        assert "option_marks" not in queries
        return {
            "summary": _summary_frame(),
            "current": pd.DataFrame([{
                "tenant_id": "snaptrade:abc",
                "account": "Schwab Account",
                "symbol": "ONON",
                "instrument_type": "Equity",
                "trade_symbol": "ONON",
                "quantity": 0.0,
                "current_price": 48.0,
                "market_value": 0.0,
                "cost_basis": 0.0,
                "unrealized_pnl": 0.0,
                "option_expiry": None,
            }]),
            "execution": pd.DataFrame(),
        }

    user = _logged_in_user()
    original = query_cache.cached_query_df
    query_cache.cached_query_df = boom
    try:
        with patch.object(detail, "current_user", user), \
             patch("flask_login.utils._get_user", lambda: user), \
             patch.object(detail, "_redirect_if_no_accounts", lambda: None), \
             patch.object(detail, "_user_account_list", lambda: ["Schwab Account"]), \
             patch.object(detail, "_user_tenant_list", lambda: ["snaptrade:abc"]), \
             patch.object(detail, "_tenants_for_scope", lambda *_a, **_k: ["snaptrade:abc"]), \
             patch.object(detail, "_tenant_label_map_for_user", lambda *_a, **_k: {}), \
             patch.object(detail, "_account_label_map", lambda *_a, **_k: {}), \
             patch.object(detail, "get_bigquery_client", lambda: object()), \
             patch.object(detail, "cached_query_df", boom), \
             patch.object(detail, "_bq_parallel", page_batch):
            page = detail.app.test_client().get("/position/ONON")
        with patch.object(detail, "current_user", user), \
             patch("flask_login.utils._get_user", lambda: user), \
             patch.object(detail, "_redirect_if_no_accounts", lambda: None), \
             patch.object(detail, "_tenants_for_scope", lambda *_a, **_k: ["snaptrade:abc"]), \
             patch.object(detail, "_tenant_label_map_for_user", lambda *_a, **_k: {}), \
             patch.object(detail, "get_bigquery_client", lambda: object()), \
             patch.object(detail, "cached_query_df", boom), \
             patch.object(detail, "_bq_parallel", peek_batch):
            peek = detail.app.test_client().get("/api/position/ONON/peek")
    finally:
        query_cache.cached_query_df = original

    assert page.status_code == 200, page.data[:800]
    html = page.data.decode()
    assert "We're having trouble loading this position" not in html
    assert "ONON" in html
    assert "Realized" in html

    assert peek.status_code == 200, peek.data[:800]
    body = peek.get_json()
    assert body["symbol"] == "ONON"
    assert body["total_pnl"] == -3333.0
    assert body.get("error") is None
    assert body.get("held") is None
