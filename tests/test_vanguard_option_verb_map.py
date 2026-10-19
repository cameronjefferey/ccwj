"""Ambiguous Vanguard option Buy/Sell verbs fail closed.

Bare Buy/Sell does not say whether an option opened or closed. In
particular, the first retained Buy can be a buy-to-close for a short
contract opened before the broker history window. Staging must not invent
an opening fill, cost basis, or P&L from that ambiguous row.
"""
from pathlib import Path

_STG = (
    Path(__file__).resolve().parents[1]
    / "dbt" / "models" / "staging" / "stg_history.sql"
)


def remap(rows: list[dict]) -> list[str]:
    """The safe staging decision: retain only the action actually supplied."""
    return [row["action"] for row in rows]


def opened_before_history(actions: list[str]) -> bool:
    opens = sum(
        action in ("option_buy_to_open", "option_sell_to_open")
        for action in actions
    )
    closes = sum(
        action in (
            "option_buy_to_close",
            "option_sell_to_close",
            "option_assigned",
            "option_exercised",
            "option_expired",
        )
        for action in actions
    )
    return opens == 0 and closes > 0


def _row(**overrides) -> dict:
    base = {
        "tenant_id": "t1",
        "user_id": 1,
        "account": "Vanguard Account",
        "instrument_type": "Call",
        "trade_symbol": "SYN260101C00010000",
        "action": "equity_buy",
        "trade_date": "2026-01-02",
        "seq": 1,
    }
    base.update(overrides)
    return base


def test_sql_does_not_promote_ambiguous_vanguard_verbs():
    sql = _STG.read_text()
    assert "vanguard_verb_ranked" not in sql
    assert "vanguard_first_verb" not in sql
    assert "vanguard_option_verbs" not in sql
    assert "from cleaned c" in sql
    # The final table does not publish seed order.
    tail = sql[sql.rfind("select\n    account, user_id"):]
    assert "_row_seq" not in tail


def test_pre_window_buy_is_not_fabricated_as_an_open():
    assert remap([_row()]) == ["equity_buy"]


def test_generic_buy_beside_a_pre_window_close_keeps_unknown_basis():
    actions = remap([
        _row(action="equity_buy", trade_date="2026-01-02", seq=1),
        _row(action="option_buy_to_close", trade_date="2026-01-02", seq=2),
    ])
    assert actions == ["equity_buy", "option_buy_to_close"]
    assert opened_before_history(actions) is True


def test_buy_then_sell_remains_ambiguous():
    rows = [
        _row(action="equity_buy", trade_date="2026-01-02", seq=1),
        _row(action="equity_sell", trade_date="2026-01-15", seq=2),
        _row(action="equity_buy", trade_date="2026-01-20", seq=3),
    ]
    assert remap(rows) == [
        "equity_buy",
        "equity_sell",
        "equity_buy",
    ]


def test_sell_then_buy_remains_ambiguous():
    rows = [
        _row(action="equity_sell", trade_date="2026-01-02", seq=1, instrument_type="Put"),
        _row(action="equity_buy", trade_date="2026-01-20", seq=2, instrument_type="Put"),
        _row(action="equity_sell", trade_date="2026-01-22", seq=3, instrument_type="Put"),
    ]
    assert remap(rows) == [
        "equity_sell",
        "equity_buy",
        "equity_sell",
    ]


def test_same_day_order_does_not_create_direction():
    short_first = [
        _row(action="equity_sell", trade_date="2026-01-02", seq=1),
        _row(action="equity_buy", trade_date="2026-01-02", seq=2),
    ]
    long_first = [
        _row(action="equity_buy", trade_date="2026-01-02", seq=1),
        _row(action="equity_sell", trade_date="2026-01-02", seq=2),
    ]
    assert remap(short_first) == ["equity_sell", "equity_buy"]
    assert remap(long_first) == ["equity_buy", "equity_sell"]


def test_a_dated_fill_does_not_make_an_undated_fill_a_close():
    rows = [
        _row(action="equity_sell", trade_date=None, seq=1),
        _row(action="equity_buy", trade_date="2026-01-02", seq=2),
    ]
    assert remap(rows) == ["equity_sell", "equity_buy"]


def test_vanguard_stock_buy_stays_an_equity_verb():
    assert remap([_row(instrument_type="Equity", trade_symbol="SYN")]) == [
        "equity_buy"
    ]


def test_other_broker_option_buy_stays_an_equity_verb():
    for account in (
        "Schwab Account",
        "Fidelity Account",
        "Fidelity NetBenefits ••••5678",
        "Alpaca Paper Account",
        "Interactive Brokers Account",
        "Robinhood Account",
    ):
        assert remap([_row(account=account)]) == ["equity_buy"]
        assert remap([_row(account=account, action="equity_sell")]) == ["equity_sell"]


def test_fidelity_adapter_does_not_rewrite_option_actions():
    root = Path(__file__).resolve().parents[1]
    fidelity = (
        root / "dbt/models/staging/brokers/stg_broker_fidelity_history.sql"
    ).read_text()
    assert "option_buy_to_open" not in fidelity
    assert "option_sell_to_close" not in fidelity
    assert "vanguard_option_verbs" not in fidelity
    for rel in (
        "dbt/macros/broker_staging_sources.sql",
        "dbt/models/staging/brokers/stg_broker_fidelity_history.sql",
        "dbt/models/staging/brokers/stg_broker_vanguard_history.sql",
        "dbt/models/staging/brokers/stg_broker_alpaca_history.sql",
        "dbt/models/staging/demo/stg_demo_history.sql",
    ):
        assert "_row_seq" in (root / rel).read_text()


def test_explicit_option_actions_keep_buy_cash_out_and_sell_cash_in():
    """Explicit option actions keep their existing cash-sign behavior.

    Buys stay in the cash-out branch (including the option fee net,
    which subtracts the commission from a gross premium). Sells stay cash-in.
    """
    sql = _STG.read_text()
    assert "'equity_buy',\n                'option_buy_to_open',\n                'option_buy_to_close'" in sql
    assert "then -abs(g.amount_raw)" in sql
    assert "'option_sell_to_open', 'option_sell_to_close'" in sql
    assert "then abs(g.quantity) * g.price * 100 - abs(g.fees)" in sql
    assert "else -(abs(g.quantity) * g.price * 100 + abs(g.fees))" in sql
    assert (
        "'equity_sell',\n"
        "                'equity_sell_short',\n"
        "                'option_sell_to_open',\n"
        "                'option_sell_to_close'"
    ) in sql
    assert "then abs(g.amount_raw)" in sql
    assert "vanguard_option_verbs" not in sql


def test_verbs_beside_an_existing_option_open_are_left_alone():
    rows = [
        _row(action="option_sell_to_open", trade_date="2026-01-01", seq=1),
        _row(action="equity_sell", trade_date="2026-01-02", seq=2),
        _row(action="equity_buy", trade_date="2026-01-03", seq=3),
    ]
    assert remap(rows) == [
        "option_sell_to_open",
        "equity_sell",
        "equity_buy",
    ]


def test_an_explicit_open_does_not_promote_a_different_contract():
    rows = [
        _row(action="option_sell_to_open", trade_symbol="SYN260101P00010000"),
        _row(action="equity_buy"),
    ]
    assert remap(rows) == ["option_sell_to_open", "equity_buy"]


def test_blank_symbol_is_not_promoted():
    assert remap([_row(trade_symbol=None)]) == ["equity_buy"]


def test_sell_short_is_not_promoted():
    assert remap([_row(action="equity_sell_short")]) == ["equity_sell_short"]
