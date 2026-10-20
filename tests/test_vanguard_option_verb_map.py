"""Vanguard option verbs: fail closed on opens, close only when sure.

Synthetic rows stand in for warehouse fills. A bare Buy or Sell does not
become an option open. The first retained Buy can be a buy-to-close for
a short opened before the broker history window, and promoting it invents
cost basis. When exactly one explicit open direction exists (Buy to Open
or Sell to Open, not both), only the opposite generic verb becomes the
matching close. Same-side generics and fully ambiguous contracts stay on
the broker verb.
"""
from pathlib import Path

_STG = (
    Path(__file__).resolve().parents[1]
    / "dbt" / "models" / "staging" / "stg_history.sql"
)


def _slug(account: str) -> str:
    return (account or "").strip().split(" ")[0].lower()


def _key(row: dict):
    return (
        row.get("tenant_id"),
        row.get("user_id"),
        row.get("account"),
        row.get("trade_symbol"),
    )


def remap(rows: list[dict]) -> list[str]:
    """Same decision as ``vanguard_option_verbs`` in stg_history."""
    opens = {}
    for row in rows:
        if row["action"] in ("option_buy_to_open", "option_sell_to_open"):
            opens.setdefault(_key(row), set()).add(row["action"])

    out = []
    for row in rows:
        action = row["action"]
        if _eligible(row):
            explicit = opens.get(_key(row), set())
            if explicit == {"option_buy_to_open"} and action == "equity_sell":
                action = "option_sell_to_close"
            elif explicit == {"option_sell_to_open"} and action == "equity_buy":
                action = "option_buy_to_close"
        out.append(action)
    return out


def _eligible(row: dict) -> bool:
    return (
        _slug(row["account"]) == "vanguard"
        and row["instrument_type"] in ("Call", "Put")
        and row.get("trade_symbol")
        and row["action"] in ("equity_buy", "equity_sell")
    )


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


def test_sql_promotes_only_the_opposite_close():
    sql = _STG.read_text()
    assert "vanguard_verb_ranked" not in sql
    assert "vanguard_first_verb" not in sql
    assert "first_action" not in sql
    assert "when 'equity_buy' then 'option_buy_to_open'" not in sql
    assert "when 'equity_sell' then 'option_sell_to_open'" not in sql
    assert "coalesce(g.has_buy_open, false)" in sql
    assert "not coalesce(g.has_sell_open, false)" in sql
    assert "coalesce(g.has_sell_open, false)" in sql
    assert "not coalesce(g.has_buy_open, false)" in sql
    verb_map = sql.split("vanguard_option_verbs as (", 1)[1].split(
        "fee_guard as (", 1
    )[0]
    assert "then 'option_sell_to_close'" in verb_map
    assert "then 'option_buy_to_close'" in verb_map
    assert "option_buy_to_open" not in verb_map
    assert "option_sell_to_open" not in verb_map
    assert "c.* except (_row_seq, action)" in sql
    assert "from vanguard_option_verbs c" in sql
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
    assert remap(rows) == ["equity_buy", "equity_sell", "equity_buy"]


def test_sell_then_buy_remains_ambiguous():
    rows = [
        _row(action="equity_sell", trade_date="2026-01-02", seq=1, instrument_type="Put"),
        _row(action="equity_buy", trade_date="2026-01-20", seq=2, instrument_type="Put"),
        _row(action="equity_sell", trade_date="2026-01-22", seq=3, instrument_type="Put"),
    ]
    assert remap(rows) == ["equity_sell", "equity_buy", "equity_sell"]


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
    which subtracts the commission from a gross premium). Sells stay
    cash-in. The close-only map changes the action label and does not
    touch amount_raw. The gate is the Vanguard account label only.
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
    verb = sql.split("vanguard_option_verbs as (", 1)[1].split("fee_guard as (", 1)[0]
    assert "amount_raw" not in verb
    assert "= 'vanguard'" in verb
    assert "fidelity" not in verb.lower()
    assert "schwab" not in verb.lower()


def test_generic_opposite_verb_beside_explicit_open_closes_contract():
    short_rows = [
        _row(action="option_sell_to_open", trade_date="2026-01-01", seq=1),
        _row(action="equity_sell", trade_date="2026-01-02", seq=2),
        _row(action="equity_buy", trade_date="2026-01-03", seq=3),
    ]
    assert remap(short_rows) == [
        "option_sell_to_open",
        "equity_sell",
        "option_buy_to_close",
    ]
    long_rows = [
        _row(action="option_buy_to_open", trade_date="2026-01-01", seq=1),
        _row(action="equity_buy", trade_date="2026-01-02", seq=2),
        _row(action="equity_sell", trade_date="2026-01-03", seq=3),
    ]
    assert remap(long_rows) == [
        "option_buy_to_open",
        "equity_buy",
        "option_sell_to_close",
    ]


def test_every_opposite_generic_closes_when_one_open_direction():
    rows = [
        _row(action="option_sell_to_open", seq=1),
        _row(action="equity_buy", trade_date="2026-01-10", seq=2),
        _row(action="equity_buy", trade_date="2026-01-20", seq=3),
    ]
    assert remap(rows) == [
        "option_sell_to_open",
        "option_buy_to_close",
        "option_buy_to_close",
    ]


def test_generic_same_side_verb_beside_explicit_open_is_not_double_counted():
    rows = [
        _row(action="option_buy_to_open", trade_date="2026-01-01", seq=1),
        _row(action="equity_buy", trade_date="2026-01-02", seq=2),
    ]
    assert remap(rows) == ["option_buy_to_open", "equity_buy"]


def test_both_open_directions_leave_generics_alone():
    rows = [
        _row(action="option_buy_to_open", trade_date="2026-01-01", seq=1),
        _row(action="option_sell_to_open", trade_date="2026-01-02", seq=2),
        _row(action="equity_buy", trade_date="2026-01-03", seq=3),
        _row(action="equity_sell", trade_date="2026-01-04", seq=4),
    ]
    assert remap(rows) == [
        "option_buy_to_open",
        "option_sell_to_open",
        "equity_buy",
        "equity_sell",
    ]


def test_a_close_is_not_an_open_direction():
    rows = [
        _row(action="option_buy_to_close", trade_date="2026-01-01", seq=1),
        _row(action="equity_sell", trade_date="2026-01-02", seq=2),
    ]
    assert remap(rows) == ["option_buy_to_close", "equity_sell"]
    assert opened_before_history(remap(rows)) is True


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
