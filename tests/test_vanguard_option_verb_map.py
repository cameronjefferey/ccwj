"""Vanguard option Buy/Sell becomes an open and a close in staging.

Synthetic rows stand in for warehouse fills. A Vanguard call or put
stored as an equity verb is promoted only when that contract has no
option open already. A Sell is the later close, not a second open.
"""
from pathlib import Path

_STG = (
    Path(__file__).resolve().parents[1]
    / "dbt" / "models" / "staging" / "stg_history.sql"
)

# The staging CASE. The test fails if the model drifts from the rows below.
_CASE = """
            when {{ broker_slug_from_account('c.account') }} = 'vanguard'
             and c.instrument_type in ('Call', 'Put')
             and c.trade_symbol is not null
             and c.action in ('equity_buy', 'equity_sell')
             and not coalesce(sib.has_option_open, false)
            then case c.action
                when 'equity_buy' then 'option_buy_to_open'
                when 'equity_sell' then 'option_sell_to_close'
            end
"""


def _slug(account: str) -> str:
    return (account or "").strip().split(" ")[0].lower()


def remap(rows: list[dict]) -> list[str]:
    """Same decision as ``vanguard_option_verbs`` in stg_history."""
    opens = set()
    for row in rows:
        if row["action"] in ("option_buy_to_open", "option_sell_to_open"):
            opens.add(_key(row))
    out = []
    for row in rows:
        action = row["action"]
        if (
            _slug(row["account"]) == "vanguard"
            and row["instrument_type"] in ("Call", "Put")
            and row.get("trade_symbol")
            and action in ("equity_buy", "equity_sell")
            and _key(row) not in opens
        ):
            action = (
                "option_buy_to_open"
                if action == "equity_buy"
                else "option_sell_to_close"
            )
        out.append(action)
    return out


def _key(row: dict):
    return (
        row.get("tenant_id"),
        row.get("user_id"),
        row.get("account"),
        row.get("trade_symbol"),
    )


def _row(**overrides) -> dict:
    base = {
        "tenant_id": "t1",
        "user_id": 1,
        "account": "Vanguard Account",
        "instrument_type": "Call",
        "trade_symbol": "SYN260101C00010000",
        "action": "equity_buy",
    }
    base.update(overrides)
    return base


def test_sql_case_matches_the_synthetic_rule():
    sql = _STG.read_text()
    assert _CASE in sql
    assert "vanguard_option_verbs as (" in sql
    assert "from vanguard_option_verbs c" in sql
    assert "logical_or(\n                action in ('option_buy_to_open', 'option_sell_to_open')" in sql


def test_vanguard_buy_on_a_call_is_an_open():
    assert remap([_row()]) == ["option_buy_to_open"]


def test_vanguard_sell_after_the_buy_is_a_close():
    rows = [
        _row(action="equity_buy"),
        _row(action="equity_sell"),
    ]
    assert remap(rows) == ["option_buy_to_open", "option_sell_to_close"]


def test_vanguard_stock_buy_stays_an_equity_verb():
    assert remap([_row(instrument_type="Equity", trade_symbol="SYN")]) == [
        "equity_buy"
    ]


def test_other_broker_option_buy_stays_an_equity_verb():
    assert remap([_row(account="Schwab Account")]) == ["equity_buy"]


def test_buy_beside_an_existing_option_open_is_left_alone():
    rows = [
        _row(action="option_buy_to_open"),
        _row(action="equity_buy"),
    ]
    assert remap(rows) == ["option_buy_to_open", "equity_buy"]


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
