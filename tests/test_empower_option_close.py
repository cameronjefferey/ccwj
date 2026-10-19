"""Empower option verbs close only when the open is already explicit.

A bare Buy or Sell does not become an option open. The first retained
Buy can be a buy-to-close for a short opened before the broker history
window, and promoting it invents cost basis. When exactly one explicit
open direction exists, only the opposite generic verb becomes the
matching close. This is the same decision as #304's Vanguard rule, and
it applies only to an Empower account label.
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
    """Same decision as ``empower_option_verbs`` in stg_history."""
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
        _slug(row["account"]) == "empower"
        and row["instrument_type"] in ("Call", "Put")
        and row.get("trade_symbol")
        and row["action"] in ("equity_buy", "equity_sell")
    )


def _row(**overrides) -> dict:
    base = {
        "tenant_id": "t1",
        "user_id": 1,
        "account": "Empower Retirement ••••4242",
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
    close = sql.split("empower_option_verbs as (", 1)[1].split("fee_guard as (", 1)[0]
    assert "coalesce(g.has_buy_open, false)" in close
    assert "not coalesce(g.has_sell_open, false)" in close
    assert "coalesce(g.has_sell_open, false)" in close
    assert "not coalesce(g.has_buy_open, false)" in close
    assert "then 'option_sell_to_close'" in close
    assert "then 'option_buy_to_close'" in close
    assert "option_buy_to_open" not in close
    assert "option_sell_to_open" not in close
    assert "first_action" not in close
    assert "amount_raw" not in close
    assert "= 'empower'" in close
    first = sql.split("vanguard_verb_ranked as (", 1)[1].split(
        "empower_open_guard as (", 1
    )[0]
    assert "empower" not in first.lower()


def test_pre_window_buy_is_not_fabricated_as_an_open():
    assert remap([_row()]) == ["equity_buy"]
    assert remap([_row(action="equity_sell")]) == ["equity_sell"]


def test_buy_then_sell_remains_ambiguous():
    rows = [
        _row(action="equity_buy", trade_date="2026-01-02", seq=1),
        _row(action="equity_sell", trade_date="2026-01-15", seq=2),
    ]
    assert remap(rows) == ["equity_buy", "equity_sell"]


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


def test_both_open_directions_leave_generics_alone():
    rows = [
        _row(action="option_buy_to_open", seq=1),
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


def test_hyphenated_label_and_other_brokers_stay_on_the_broker_verb():
    assert remap([_row(account="Empower-Retirement Account")]) == ["equity_buy"]
    for account in ("Vanguard Account", "Schwab Account", "Fidelity Account"):
        assert remap([
            _row(account=account, action="option_buy_to_open"),
            _row(account=account, action="equity_sell", trade_date="2026-01-03", seq=2),
        ]) == ["option_buy_to_open", "equity_sell"]


def test_stock_buy_stays_an_equity_verb():
    assert remap([_row(instrument_type="Equity", trade_symbol="SYN")]) == [
        "equity_buy"
    ]
