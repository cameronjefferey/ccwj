"""Positions and position-detail audit fixes (Sep 2026). No BigQuery."""

from datetime import date, timedelta

import pandas as pd
import pytest

from app.pnl_charts import _synthetic_cumulative_pnl_for_position
from app.position_detail import (
    _annotate_open_legs,
    _breakdown_footer,
    _dedupe_trade_display_rows,
    _format_share_qty,
    _option_leg_cost_proceeds,
    _option_return_pct,
    _pin_strategy_open_unrealized_to_snapshot,
    _resolve_trade_count,
    _resolve_win_loss,
    _unique_open_strategy_labels,
    _wl_from_closed_frames,
)
from app.positions_page import sort_strategy_detail_rows
from app.snaptrade_normalize import _crypto_pair_base


def test_option_cost_includes_sell_to_open_credit():
    # CFLT Feb 21 '25 $40C: buy-to-open $1,866.10, sell-to-open $1,216.78,
    # close proceeds $1,433.58, booked P&L $784.26.
    leg = {
        "direction": "Bought",
        "premium_paid": -1866.10,
        "premium_received": 1216.78,
        "cost_to_close": 0,
        "proceeds_from_close": 1433.58,
        "total_pnl": 784.26,
    }
    cost, proceeds, pnl = _option_leg_cost_proceeds(leg)
    assert cost == 1866.10
    assert proceeds == 2650.36
    assert pnl == 784.26
    assert round(proceeds - cost, 2) == pnl
    assert _option_return_pct(leg, pnl) == 42.0


def test_short_return_uses_credit_not_buyback():
    # CFLT Jan 17 '25 $30C covered call: credit $3,933.51, buy-back $40.22.
    leg = {
        "direction": "Sold",
        "premium_received": 3933.51,
        "premium_paid": 0,
        "cost_to_close": -40.22,
        "proceeds_from_close": 0,
        "total_pnl": 3893.29,
    }
    cost, proceeds, pnl = _option_leg_cost_proceeds(leg)
    assert cost == 40.22
    assert proceeds == 3933.51
    assert round(proceeds - cost, 2) == pnl
    assert _option_return_pct(leg, pnl) == 99.0


def test_expired_short_with_no_buyback_is_not_blank():
    leg = {
        "direction": "Sold",
        "premium_received": 250.0,
        "premium_paid": 0,
        "cost_to_close": 0,
        "proceeds_from_close": 0,
        "total_pnl": 250.0,
    }
    cost, proceeds, pnl = _option_leg_cost_proceeds(leg)
    assert cost == 0
    assert proceeds == 250.0
    assert _option_return_pct(leg, pnl) == 100.0


def test_option_return_blank_without_capital_basis():
    leg = {"direction": "Sold", "premium_received": 0, "premium_paid": 0}
    assert _option_return_pct(leg, 0) is None


def test_pin_iyw_sara_unrealized_to_snapshot():
    rows = [
        {
            "tenant_id": "t-401k",
            "account": "Sara 401k",
            "strategy": "Buy and Hold",
            "status": "Open",
            "realized_pnl": 0.0,
            "unrealized_pnl": 38302.28,
            "total_dividend_income": 129.65,
            "total_pnl": 38431.93,
            "total_return": 38431.93,
        },
        {
            "tenant_id": "t-sara",
            "account": "Sara Investment",
            "strategy": "Buy and Hold",
            "status": "Open",
            "realized_pnl": 0.0,
            "unrealized_pnl": 35285.00,
            "total_dividend_income": 95.39,
            "total_pnl": 35380.39,
            "total_return": 35380.39,
        },
    ]
    current = pd.DataFrame([
        {"tenant_id": "t-401k", "instrument_type": "Equity", "unrealized_pnl": 38363.73},
        {"tenant_id": "t-sara", "instrument_type": "Equity", "unrealized_pnl": 35285.00},
    ])
    _pin_strategy_open_unrealized_to_snapshot(rows, current)
    assert rows[0]["unrealized_pnl"] == 38363.73
    assert rows[0]["total_pnl"] == pytest.approx(38493.38)
    assert rows[0]["total_return"] == pytest.approx(38493.38)
    # Already matched the snapshot — leave it alone.
    assert rows[1]["unrealized_pnl"] == 35285.00
    assert rows[1]["total_pnl"] == 35380.39


def test_pin_skips_when_two_equity_strategies_share_a_tenant():
    rows = [
        {"tenant_id": "t", "strategy": "Buy and Hold", "status": "Open",
         "unrealized_pnl": 10.0, "total_pnl": 10.0, "total_return": 10.0},
        {"tenant_id": "t", "strategy": "Dividend", "status": "Open",
         "unrealized_pnl": 4.0, "total_pnl": 4.0, "total_return": 4.0},
    ]
    current = pd.DataFrame([
        {"tenant_id": "t", "instrument_type": "Equity", "unrealized_pnl": 99.0},
    ])
    _pin_strategy_open_unrealized_to_snapshot(rows, current)
    assert rows[0]["unrealized_pnl"] == 10.0
    assert rows[1]["unrealized_pnl"] == 4.0


def test_open_leg_dates_follow_the_tenant():
    positions = [
        {"tenant_id": "emmory", "account": "Emmory", "instrument_type": "Equity",
         "quantity": 1, "symbol": "COST"},
        {"tenant_id": "sara", "account": "Sara", "instrument_type": "Equity",
         "quantity": 2, "symbol": "COST"},
    ]
    sessions = [
        {"tenant_id": "emmory", "account": "Emmory", "status": "Open",
         "options_only": False, "display_leg": 2, "open_date": "2025-12-30",
         "days_held": 264},
        {"tenant_id": "sara", "account": "Sara", "status": "Open",
         "options_only": False, "display_leg": 1, "open_date": "2024-06-17",
         "days_held": 800},
        {"tenant_id": "emmory", "account": "Emmory", "status": "Closed",
         "options_only": False, "display_leg": 1, "open_date": "2020-01-01",
         "days_held": 10},
    ]
    _annotate_open_legs(positions, sessions, "COST", today=date(2026, 9, 21))
    assert positions[0]["open_date"] == "2025-12-30"
    assert positions[0]["leg_num"] == 2
    assert positions[0]["days_held"] == 264
    assert positions[0]["close_date"] == ""
    assert positions[1]["open_date"] == "2024-06-17"
    assert positions[1]["leg_num"] == 1


def test_crypto_qty_is_not_truncated_and_kind_is_crypto():
    assert _format_share_qty(0.00412) == "0.00412"
    assert _format_share_qty(10) == "10"
    assert _format_share_qty(0) == "0"
    positions = [{
        "tenant_id": "coinbase",
        "account": "Coinbase Account",
        "instrument_type": "Equity",
        "quantity": 0.00412,
        "symbol": "BTC",
    }]
    _annotate_open_legs(positions, [], "BTC")
    assert positions[0]["quantity_display"] == "0.00412"
    assert positions[0]["leg_kind"] == "Crypto"


def test_drip_clones_and_dividend_echo_are_collapsed():
    rows = pd.DataFrame([
        {"tenant_id": "e", "trade_date": "2026-08-07", "symbol": "COST",
         "action": "dividend_reinvest", "quantity": 0.0016, "amount": -1.47},
        {"tenant_id": "e", "trade_date": "2026-08-07", "symbol": "COST",
         "action": "dividend", "quantity": 0, "amount": 1.47},
        {"tenant_id": "e", "trade_date": "2026-08-07", "symbol": "COST",
         "action": "other", "quantity": 0, "amount": 1.47},
        {"tenant_id": "e", "trade_date": "2026-08-07", "symbol": "COST",
         "action": "dividend_reinvest", "quantity": 0.0016, "amount": -1.47},
        # A real share lot the same day must stay.
        {"tenant_id": "e", "trade_date": "2026-08-07", "symbol": "COST",
         "action": "equity_buy", "quantity": 1, "amount": -864.61},
        # Zero-qty other with a different amount is not the dividend echo.
        {"tenant_id": "e", "trade_date": "2026-08-07", "symbol": "COST",
         "action": "other", "quantity": 0, "amount": 12.00},
    ])
    out = _dedupe_trade_display_rows(rows)
    actions = list(out["action"])
    assert actions.count("dividend_reinvest") == 1
    assert "dividend" in actions
    assert actions.count("other") == 1
    assert float(out.loc[out["action"] == "other", "amount"].iloc[0]) == 12.00
    assert "equity_buy" in actions


def test_win_loss_uses_summary_unless_leg_filtered():
    # Partial sells of one session must not each count as a trade group.
    equity = pd.DataFrame([
        {"tenant_id": "e", "account": "Emmory", "session_id": 1, "realized_pnl": 400.0},
        {"tenant_id": "e", "account": "Emmory", "session_id": 1, "realized_pnl": -50.0},
        {"tenant_id": "e", "account": "Emmory", "session_id": 2, "realized_pnl": -10.0},
    ])
    options = pd.DataFrame([{"total_pnl": 20.0}, {"total_pnl": -5.0}])
    wins, losses = _wl_from_closed_frames(options, equity)
    assert (wins, losses) == (2, 2)  # two option contracts + one winning session + one losing

    # Unfiltered detail keeps the list's positions_summary counts.
    kept = _resolve_win_loss(
        7, 2, leg_filtered=False, summary_empty=False,
        closed_legs_df=options, closed_equity_df=equity,
    )
    assert kept == (7, 2)
    filtered = _resolve_win_loss(
        7, 2, leg_filtered=True, summary_empty=False,
        closed_legs_df=options, closed_equity_df=equity,
    )
    assert filtered == (2, 2)


def test_snapshot_row_is_not_a_trade():
    assert _resolve_trade_count(0, 0, pd.DataFrame(), pd.DataFrame()) == 0
    assert _resolve_trade_count(0, 3, pd.DataFrame(), pd.DataFrame()) == 3
    equity = pd.DataFrame([
        {"tenant_id": "e", "session_id": 1, "realized_pnl": 1.0},
        {"tenant_id": "e", "session_id": 1, "realized_pnl": 2.0},
    ])
    assert _resolve_trade_count(0, 0, pd.DataFrame(), equity) == 1


def test_strategy_sort_is_full_set_before_page():
    rows = [
        {"account": "A", "strategy": "Long Call", "total_pnl": -100.0, "status": "Closed"},
        {"account": "B", "strategy": "Buy and Hold", "total_pnl": 50.0, "status": "Open"},
        {"account": "C", "strategy": "Long Call", "total_pnl": -81744.51, "status": "Closed"},
    ]
    ordered, key, direction = sort_strategy_detail_rows(rows, "total_return", "asc")
    assert (key, direction) == ("total_return", "asc")
    assert ordered[0]["total_pnl"] == -81744.51
    page = ordered[:1]
    assert page[0]["total_pnl"] == -81744.51
    desc, _, _ = sort_strategy_detail_rows(rows, "nope", "sideways")
    assert desc[0]["total_pnl"] == 50.0


def test_crypto_pair_base():
    assert _crypto_pair_base("BTC-USD") == "BTC"
    assert _crypto_pair_base("btcusd") == "BTC"
    assert _crypto_pair_base("BTC/USDT") == "BTC"
    assert _crypto_pair_base("USDC") == "USDC"
    assert _crypto_pair_base("SEI-USD") == "SEI"
    assert _crypto_pair_base("AAPL") == "AAPL"
    assert _crypto_pair_base("CFLT 02/21/2025 40.00 C") == "CFLT 02/21/2025 40.00 C"


def test_synthetic_chart_does_not_start_yesterday():
    today = date.today()
    chart = _synthetic_cumulative_pnl_for_position(
        {
            "realized_pnl": 0,
            "unrealized_pnl": 211.0,
            "dividend_income": 0,
            "total_return": 211.0,
            "first_trade": str(today),
        },
        [],
        None,
        [],
        pd.DataFrame(),
    )
    assert chart["dates"] == [str(today), str(today)]
    assert str(today - timedelta(days=1)) not in chart["dates"]


def test_unique_strategy_labels_and_breakdown_footer():
    labels = _unique_open_strategy_labels([
        {"status": "Open", "strategy": "Buy and Hold"},
        {"status": "Open", "strategy": "Buy and Hold"},
        {"status": "Closed", "strategy": "Covered Call"},
        {"status": "Open", "strategy": "Crypto"},
    ])
    assert labels == ["Buy and Hold", "Crypto"]
    footer = _breakdown_footer([
        {"realized": 10, "unrealized": 5, "count": 1},
        {"realized": 2, "unrealized": None, "count": 3},
    ])
    assert footer == {"realized": 12.0, "unrealized": 5.0, "count": 4}


def test_open_session_cost_gap_sql_is_gated_on_sells():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    text = (root / "dbt/models/intermediate/int_equity_sessions.sql").read_text()
    assert "coalesce(s.total_sell_qty, 0) > 1e-9" in text


def test_money_sign_sits_before_the_dollar():
    from app.admin import _fmt_money
    assert _fmt_money(-35323) == "-$35,323.00"
    assert _fmt_money(-35323, 0) == "-$35,323"
    assert _fmt_money(12.5) == "$12.50"


def test_human_date_and_zero_win_rate_label():
    from app import app
    assert app.jinja_env.filters["human_date"]("2026-09-21") == "Sep 21, 2026"
    label = app.jinja_env.globals["win_rate_label"]
    assert label(0, 0, 0) == "—"
    assert label(0.5, 1, 1) == "50%"


def test_positions_template_sorts_on_the_server_and_signs_money():
    from app import app
    from app.positions_page import ERROR_DEFAULTS

    ctx = dict(ERROR_DEFAULTS)
    ctx.update({
        "user_accounts": ["Emmory"],
        "view_accounts": ["Emmory"],
        "accounts": ["Emmory"],
        "kpis": {
            "total_return": -35323.2,
            "realized_pnl": -100,
            "unrealized_pnl": 0,
            "dividend_income": 0,
            "win_rate": None,
            "num_winners": 0,
            "num_losers": 0,
            "premium_collected": 0,
            "num_positions": 1,
            "total_trades": 2,
        },
        "rows": [{
            "tenant_id": "t",
            "account": "Emmory",
            "account_display": "Emmory",
            "strategy": "Long Call",
            "status": "Closed",
            "total_pnl": -81744.51,
            "realized_pnl": -81744.51,
            "unrealized_pnl": 0,
            "total_dividend_income": 0,
            "win_rate": None,
            "num_winners": 0,
            "num_losers": 0,
            "num_individual_trades": 2,
            "avg_pnl_per_trade": -100,
            "avg_days_in_trade": 9,
            "total_premium_received": 0,
        }],
        "total_rows": 26,
        "total_pages": 2,
        "page": 1,
        "sort_key": "total_return",
        "sort_dir": "asc",
        "title": "Positions",
    })
    from flask import render_template
    with app.test_request_context("/positions?sort=total_return&dir=asc"):
        html = render_template("positions.html", **ctx)
    assert 'data-server-sort="1"' in html
    assert "sort=total_return" in html
    assert "dir=desc" in html  # header toggles off the active asc sort
    assert "-$81,744.51" in html
    assert "-$35,323" in html
    assert "Positions - HappyTrader" in html
    assert ">—<" in html or ">—</" in html or ">—" in html


def test_position_narrative_spacing_and_open_leg_dates():
    from app import app

    narrative = """
        {% if open_strategy_labels|length == 1 %}
        is <strong>{{ open_strategy_labels[0] }}</strong>.
        {% elif open_strategy_labels|length > 1 %}
        is split across
        {% for name in open_strategy_labels %}<strong>{{ name }}</strong>{% if not loop.last %}, {% endif %}{% endfor %}.
        {% endif %}
        : <strong>{{ kpis.total_trades }}</strong> individual fill{{ 's' if kpis.total_trades != 1 else '' }}
        {%- if kpis.num_winners + kpis.num_losers > 0 %} (closed legs: <strong>{{ kpis.num_winners }}W</strong> / <strong>{{ kpis.num_losers }}L</strong>){% endif -%}.
        from <strong>{{ kpis.first_trade|human_date }}</strong>
    """
    html = app.jinja_env.from_string(narrative).render(
        open_strategy_labels=["Buy and Hold"],
        kpis={
            "total_trades": 25,
            "num_winners": 7,
            "num_losers": 2,
            "first_trade": "2025-07-11",
        },
    )
    assert "Buy and Hold, Buy and Hold" not in html
    assert "fills (closed legs:" in html
    assert "fills(closed" not in html
    assert "Hold ." not in html
    assert "Jul 11, 2025" in html
