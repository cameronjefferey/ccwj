"""Vanguard option Buy/Sell follows whichever verb comes first.

Synthetic rows stand in for warehouse fills. The first equity verb on
a contract (date, then seed sequence) is the open. A later verb in the
other direction is the close. The rule applies only to a Vanguard call
or put that has no option open already.
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
    opens = set()
    for row in rows:
        if row["action"] in ("option_buy_to_open", "option_sell_to_open"):
            opens.add(_key(row))

    first = {}
    for index, row in enumerate(rows):
        if not _eligible(row) or _key(row) in opens:
            continue
        rank = (
            row.get("trade_date") is None,
            row.get("trade_date") or "",
            row.get("seq") is None,
            row.get("seq") if row.get("seq") is not None else 0,
            index,
        )
        previous = first.get(_key(row))
        if previous is None or rank < previous[0]:
            first[_key(row)] = (rank, row["action"])

    out = []
    for row in rows:
        action = row["action"]
        if _eligible(row) and _key(row) not in opens:
            opener = first[_key(row)][1]
            if opener == "equity_buy":
                action = (
                    "option_buy_to_open"
                    if action == "equity_buy"
                    else "option_sell_to_close"
                )
            else:
                action = (
                    "option_sell_to_open"
                    if action == "equity_sell"
                    else "option_buy_to_close"
                )
        out.append(action)
    return out


def _eligible(row: dict) -> bool:
    return (
        _slug(row["account"]) == "vanguard"
        and row["instrument_type"] in ("Call", "Put")
        and row.get("trade_symbol")
        and row["action"] in ("equity_buy", "equity_sell")
    )


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


def test_sql_is_order_aware_and_drops_the_sequence_column():
    sql = _STG.read_text()
    assert "order by c.trade_date nulls last, c._row_seq nulls last" in sql
    assert "f.first_action = 'equity_buy'" in sql
    assert "when 'equity_buy' then 'option_buy_to_open'" in sql
    assert "else 'option_sell_to_close'" in sql
    assert "f.first_action = 'equity_sell'" in sql
    assert "when 'equity_sell' then 'option_sell_to_open'" in sql
    assert "else 'option_buy_to_close'" in sql
    assert "not coalesce(g.has_option_open, false)" in sql
    assert "c.* except (_row_seq, action)" in sql
    assert "from vanguard_option_verbs c" in sql
    # The final table does not publish seed order.
    tail = sql[sql.rfind("select\n    account, user_id"):]
    assert "_row_seq" not in tail


def test_vanguard_buy_on_a_call_is_an_open():
    assert remap([_row()]) == ["option_buy_to_open"]


def test_buy_then_sell_is_a_long_partial_close():
    rows = [
        _row(action="equity_buy", trade_date="2026-01-02", seq=1),
        _row(action="equity_sell", trade_date="2026-01-15", seq=2),
        _row(action="equity_buy", trade_date="2026-01-20", seq=3),
    ]
    assert remap(rows) == [
        "option_buy_to_open",
        "option_sell_to_close",
        "option_buy_to_open",
    ]


def test_sell_then_buy_is_a_short_open():
    rows = [
        _row(action="equity_sell", trade_date="2026-01-02", seq=1, instrument_type="Put"),
        _row(action="equity_buy", trade_date="2026-01-20", seq=2, instrument_type="Put"),
        _row(action="equity_sell", trade_date="2026-01-22", seq=3, instrument_type="Put"),
    ]
    assert remap(rows) == [
        "option_sell_to_open",
        "option_buy_to_close",
        "option_sell_to_open",
    ]


def test_same_day_order_follows_sequence_not_the_verb():
    short_first = [
        _row(action="equity_sell", trade_date="2026-01-02", seq=1),
        _row(action="equity_buy", trade_date="2026-01-02", seq=2),
    ]
    long_first = [
        _row(action="equity_buy", trade_date="2026-01-02", seq=1),
        _row(action="equity_sell", trade_date="2026-01-02", seq=2),
    ]
    assert remap(short_first) == ["option_sell_to_open", "option_buy_to_close"]
    assert remap(long_first) == ["option_buy_to_open", "option_sell_to_close"]


def test_a_dated_fill_beats_an_undated_one():
    rows = [
        _row(action="equity_sell", trade_date=None, seq=1),
        _row(action="equity_buy", trade_date="2026-01-02", seq=2),
    ]
    assert remap(rows) == ["option_sell_to_close", "option_buy_to_open"]


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
        "Empower Account",
        "Empower Retirement ••••4242",
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


def test_remapped_actions_keep_buy_cash_out_and_sell_cash_in():
    """The map changes the action label. It does not touch amount_raw.

    Buys stay in the cash-out branch (including the option fee net,
    which subtracts the commission from a gross premium). Sells stay
    cash-in. The gate is the Vanguard account label only.
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
    verb = sql.split("vanguard_option_verbs as (", 1)[1].split(
        "empower_open_guard as (", 1
    )[0]
    assert "amount_raw" not in verb
    assert "= 'vanguard'" in verb
    assert "empower" not in verb.lower()
    assert "fidelity" not in verb.lower()
    assert "schwab" not in verb.lower()


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


def test_a_different_contract_still_promotes():
    rows = [
        _row(action="option_sell_to_open", trade_symbol="SYN260101P00010000"),
        _row(action="equity_buy"),
    ]
    assert remap(rows) == ["option_sell_to_open", "option_buy_to_open"]


def test_blank_symbol_is_not_promoted():
    assert remap([_row(trade_symbol=None)]) == ["equity_buy"]


def test_sell_short_is_not_promoted():
    assert remap([_row(action="equity_sell_short")]) == ["equity_sell_short"]
