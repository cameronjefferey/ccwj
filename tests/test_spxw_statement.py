"""Sara's SPXW statement: as-of cash settlement, net fees, pending ITM.

The broker statement (Oct 2, 2026) is down $821.08. The position page
showed +$2,325.56 because the 10/02-as-of-10/01 closing fills were
dropped and the Oct 1 opens were gross of the $12.22 commission.

Closed P&L, including the Oct 2 20-lot that is already closed and
excluding the still-open 10-lot credit, is -1,796.64. The three spreads
dated before Oct 2 (what a sync that has not yet seen Oct 2 would book)
net to -2,698.88.
"""

from datetime import datetime

import pandas as pd

from app.outcome_units import group_vertical_spreads, leg_outcome
from app.snaptrade_normalize import (
    _net_option_cash,
    _osi_from_broker_text,
    activities_to_history_df,
    orders_to_history_df,
)
from app.upload import HISTORY_SEED_COLUMNS, _dedup_history_rows

TENANT = "snaptrade:sara"
ACCOUNT = "Sara Investment"


def _act(
    date,
    action,
    symbol,
    qty,
    price,
    amount,
    fee=0.0,
    description="",
):
    return {
        "type": action,
        "trade_date": date,
        "symbol": symbol,
        "description": description
        or f"{action} {symbol}",
        "units": qty,
        "price": price,
        "fee": fee,
        "amount": amount,
    }


def _statement_activities():
    """Every SPXW row on the Sara statement, newest first.

    Amounts are the statement Amount (already net of fees). The as-of
    closes have no commission. Symbol is the Schwab symbol cell, which
    is the only place the root appears.
    """
    return [
        _act("2026-10-02", "Buy to Open", "SPXW 10/02/2026 7735.00 C",
             10, 1.79, -1802.22, 12.22),
        _act("2026-10-02", "Sell to Open", "SPXW 10/02/2026 7730.00 C",
             10, 2.79, 2777.78, 12.22),
        _act("2026-10-02", "Sell to Close", "SPXW 10/02/2026 7735.00 C",
             20, 1.824, 3623.56, 24.44),
        _act("2026-10-02", "Buy to Close", "SPXW 10/02/2026 7730.00 C",
             20, 2.824, -5672.44, 24.44),
        _act("2026-10-02", "Buy to Open", "SPXW 10/02/2026 7735.00 C",
             20, 5.35, -10724.44, 24.44),
        _act("2026-10-02", "Sell to Open", "SPXW 10/02/2026 7730.00 C",
             20, 6.85, 13675.56, 24.44),
        _act(
            "10/02/2026 as of 10/01/2026",
            "Buy to Close",
            "SPXW 10/01/2026 7650.00 C",
            10, 16.45, -16450.00, 0,
            "CALL S & P 500 INDEX $7650 EXP 10/01/26 as of 10/01/2026",
        ),
        _act(
            "10/02/2026 as of 10/01/2026",
            "Sell to Close",
            "SPXW 10/01/2026 7655.00 C",
            10, 11.45, 11450.00, 0,
            "CALL S & P 500 INDEX $7655 EXP 10/01/26 as of 10/01/2026",
        ),
        _act("2026-10-01", "Buy to Open", "SPXW 10/01/2026 7655.00 C",
             10, 6.32, -6332.22, 12.22),
        _act("2026-10-01", "Sell to Open", "SPXW 10/01/2026 7650.00 C",
             10, 7.77, 7757.78, 12.22),
        _act("10/01/2026 as of 09/30/2026", "Expired",
             "SPXW 09/30/2026 7730.00 C", -5, 0, 0, 0),
        _act("10/01/2026 as of 09/30/2026", "Expired",
             "SPXW 09/30/2026 7725.00 C", 5, 0, 0, 0),
        _act("2026-09-30", "Buy to Open", "SPXW 09/30/2026 7730.00 C",
             5, 1.97, -991.11, 6.11),
        _act("2026-09-30", "Sell to Open", "SPXW 09/30/2026 7725.00 C",
             5, 2.87, 1428.89, 6.11),
        _act("09/30/2026 as of 09/29/2026", "Expired",
             "SPXW 09/29/2026 7635.00 P", -5, 0, 0, 0),
        _act("09/30/2026 as of 09/29/2026", "Expired",
             "SPXW 09/29/2026 7640.00 P", 5, 0, 0, 0),
        _act("2026-09-29", "Buy to Open", "SPXW 09/29/2026 7635.00 P",
             5, 2.72, -1366.11, 6.11),
        _act("2026-09-29", "Sell to Open", "SPXW 09/29/2026 7640.00 P",
             5, 3.62, 1803.89, 6.11),
    ]


def _history():
    return activities_to_history_df(
        _statement_activities(),
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )


def test_statement_rows_keep_as_of_trade_date_and_net_amounts():
    df = _history()
    assert len(df) == 18
    assert set(df["Symbol"]) == {
        _osi_from_broker_text(s)
        for s in (
            "SPXW 10/02/2026 7735.00 C",
            "SPXW 10/02/2026 7730.00 C",
            "SPXW 10/01/2026 7650.00 C",
            "SPXW 10/01/2026 7655.00 C",
            "SPXW 09/30/2026 7730.00 C",
            "SPXW 09/30/2026 7725.00 C",
            "SPXW 09/29/2026 7635.00 P",
            "SPXW 09/29/2026 7640.00 P",
        )
    }
    closes = df[df["Action"].isin(["Buy to Close", "Sell to Close"])]
    assert set(closes.loc[closes["Price"] == 16.45, "Date"]) == {"10/01/2026"}
    assert set(closes.loc[closes["Price"] == 11.45, "Date"]) == {"10/01/2026"}
    assert float(closes.loc[closes["Price"] == 16.45, "Amount"].iloc[0]) == -16450.0
    assert float(closes.loc[closes["Price"] == 11.45, "Amount"].iloc[0]) == 11450.0
    # Already-net statement amounts are not charged the fee a second time.
    short = df[(df["Action"] == "Sell to Open") & (df["Price"] == 7.77)].iloc[0]
    long = df[(df["Action"] == "Buy to Open") & (df["Price"] == 6.32)].iloc[0]
    assert float(short["Amount"]) == 7757.78
    assert float(long["Amount"]) == -6332.22
    expired = df[df["Action"] == "Expired"]
    assert set(expired["Date"]) == {"09/30/2026", "09/29/2026"}
    assert set(float(a) for a in expired["Amount"]) == {0.0}
    assert round(float(df["Amount"].sum()), 2) == -821.08


def test_closed_total_excludes_the_open_credit():
    df = _history()
    open_lot = df[
        (df["Date"] == "10/02/2026")
        & (df["Quantity"] == 10)
        & (df["Action"].isin(["Buy to Open", "Sell to Open"]))
    ]
    assert round(float(open_lot["Amount"].sum()), 2) == 975.56
    closed = df.drop(index=open_lot.index)
    assert round(float(closed["Amount"].sum()), 2) == -1796.64
    before_oct2 = df[df["Date"].map(_as_date) < _as_date("10/02/2026")]
    # Three spreads (six contracts) before the unsynced Oct 2 session.
    assert round(float(before_oct2["Amount"].sum()), 2) == -2698.88


def test_gross_order_is_netted_once_and_loses_to_the_activity():
    assert _net_option_cash(7770.0, 10, 7.77, 12.22) == 7757.78
    assert _net_option_cash(-6320.0, 10, 6.32, 12.22) == -6332.22
    # Already net: do not subtract the fee again.
    assert _net_option_cash(7757.78, 10, 7.77, 12.22) == 7757.78
    assert _net_option_cash(-6332.22, 10, 6.32, 12.22) == -6332.22

    orders = orders_to_history_df(
        [{
            "action": "SELL_TO_OPEN",
            "option_symbol": {
                "underlying_symbol": "SPXW",
                "expiration_date": "2026-10-01",
                "strike_price": 7650,
                "option_type": "CALL",
            },
            "status": "EXECUTED",
            "time_executed": "2026-10-01T14:30:00Z",
            "filled_quantity": 10,
            "execution_price": 7.77,
            "commission": 12.22,
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    assert float(orders.iloc[0]["Amount"]) == 7757.78

    gross = orders_to_history_df(
        [{
            "action": "SELL_TO_OPEN",
            "option_symbol": {
                "underlying_symbol": "SPXW",
                "expiration_date": "2026-10-01",
                "strike_price": 7650,
                "option_type": "CALL",
            },
            "status": "EXECUTED",
            "time_executed": "2026-10-01T14:30:00Z",
            "filled_quantity": 10,
            "execution_price": 7.77,
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    assert float(gross.iloc[0]["Amount"]) == 7770.0
    activity = activities_to_history_df(
        [_act(
            "2026-10-01",
            "Sell to Open",
            "SPXW 10/01/2026 7650.00 C",
            10, 7.77, 7757.78, 12.22,
            "CALL S & P 500 INDEX $7650 EXP 10/01/26",
        )],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    merged = _dedup_history_rows(
        pd.concat([gross, activity], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(merged) == 1
    assert float(merged.iloc[0]["Amount"]) == 7757.78
    assert "INDEX" in str(merged.iloc[0]["Description"])


def test_cash_settlement_type_uses_the_as_of_date_in_the_description():
    df = activities_to_history_df(
        [{
            "type": "CASH_SETTLEMENT",
            "trade_date": "2026-10-02",
            "description": "Buy to Close SPXW 10/01/2026 7650.00 C as of 10/01/2026",
            "units": 10,
            "price": 16.45,
            "fee": 0,
            "amount": -16450,
            "option_type": "BUY_TO_CLOSE",
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    assert len(df) == 1
    assert df.iloc[0]["Action"] == "Buy to Close"
    assert df.iloc[0]["Date"] == "10/01/2026"
    assert df.iloc[0]["Symbol"] == "SPXW  261001C07650000"
    assert float(df.iloc[0]["Amount"]) == -16450.0


def _as_date(value):
    return datetime.strptime(str(value), "%m/%d/%Y").date()


def _legs_from_history(df):
    """One closed-leg dict per contract, the shape Position Legs groups."""
    rows = []
    for symbol, part in df.groupby("Symbol"):
        amount = round(float(part["Amount"].sum()), 2)
        actions = set(part["Action"])
        if "Buy to Close" in actions or "Sell to Close" in actions:
            close_type = "Closed"
        elif "Expired" in actions:
            close_type = "Expired"
        else:
            close_type = "Closed"
        sold = "Sell to Open" in actions
        text = str(symbol)
        family = "Call Spread" if "C0" in text else "Put Spread"
        opens = part[part["Action"].isin(["Sell to Open", "Buy to Open"])]
        open_amt = round(float(opens["Amount"].sum()), 2) if len(opens) else 0.0
        open_date = str(opens["Date"].iloc[0]) if len(opens) else str(part["Date"].iloc[0])
        close_date = max(part["Date"], key=_as_date)
        qty = float(opens["Quantity"].max()) if len(opens) else 0.0
        rows.append({
            "type": "option",
            "strategy": family,
            "trade_symbol": symbol,
            "direction": "Sold" if sold else "Bought",
            "close_type": close_type,
            "open_date": open_date,
            "close_date": str(close_date),
            "quantity": qty,
            "quantity_display": str(int(qty)) if qty else "",
            "cost": 0,
            "proceeds": open_amt if sold else 0,
            "pnl": amount,
            "premium_received": open_amt if sold and open_amt > 0 else 0,
            "premium_paid": -open_amt if (not sold) and open_amt < 0 else 0,
            "tenant_id": TENANT,
            "account": ACCOUNT,
            "raw_trades": [],
            "is_winner": True if amount > 0 else False if amount < 0 else None,
        })
    return rows


def test_statement_spreads_group_and_the_oct1_credit_is_a_loss():
    df = _history()
    open_lot = df[
        (df["Date"] == "10/02/2026")
        & (df["Quantity"] == 10)
        & (df["Action"].isin(["Buy to Open", "Sell to Open"]))
    ]
    grouped = group_vertical_spreads(_legs_from_history(df.drop(index=open_lot.index)))
    assert len(grouped) == 4
    assert round(sum(row["pnl"] for row in grouped), 2) == -1796.64
    oct1 = next(row for row in grouped if round(row["pnl"], 2) == -3574.44)
    assert oct1["outcome"] == "Closed"
    assert oct1["is_winner"] is False
    assert oct1["strategy"] == "Bear Call Credit Spread"
    assert oct1["net_credit"] == 1425.56
    expired = [row for row in grouped if row["outcome"] == "Expired"]
    assert len(expired) == 2
    assert all(round(row["pnl"], 2) == 437.78 and row["is_winner"] is True for row in expired)
    closed_twenty = next(row for row in grouped if round(row["pnl"], 2) == 902.24)
    assert closed_twenty["is_winner"] is True


def test_pending_itm_short_is_not_an_expiry_win():
    short = {
        "type": "option",
        "strategy": "Call Spread",
        "trade_symbol": "SPXW  261001C07650000",
        "direction": "Sold",
        "close_type": "Settlement pending",
        "open_date": "2026-10-01",
        "close_date": "2026-10-01",
        "quantity": 10,
        "cost": 0,
        "proceeds": 7757.78,
        "pnl": 0,
        "premium_received": 7757.78,
        "tenant_id": TENANT,
        "account": ACCOUNT,
        "raw_trades": [],
    }
    long = {
        **short,
        "trade_symbol": "SPXW  261001C07655000",
        "direction": "Bought",
        "proceeds": 0,
        "cost": 6332.22,
        "premium_received": 0,
        "premium_paid": 6332.22,
    }
    assert leg_outcome(short, "2026-10-01") == "Settlement pending"
    grouped = group_vertical_spreads([short, long])
    assert len(grouped) == 1
    assert grouped[0]["outcome"] == "Settlement pending"
    assert grouped[0]["is_winner"] is None
    assert grouped[0]["close_type"] == "Settlement pending"

    from pathlib import Path

    from app import app

    page = Path("app/templates/position_detail.html").read_text()
    start = page.index('<div class="pd-fold" id="pd-legs">')
    table_at = page.index(
        '<table class="table table-sm table-hover align-middle mb-0 pd-legs">',
        start,
    )
    depth = 0
    i = table_at
    end = None
    while i < len(page):
        next_open = page.find("<table", i)
        next_close = page.find("</table>", i)
        if next_open != -1 and (next_close == -1 or next_open <= next_close):
            depth += 1
            i = next_open + len("<table")
            continue
        depth -= 1
        i = next_close + len("</table>")
        if depth == 0:
            end = i
            break
    source = page[start:end]
    row = grouped[0]
    row["account_display"] = ACCOUNT
    row["quantity_display"] = "10"
    with app.app_context(), app.test_request_context("/"):
        html = app.jinja_env.from_string(source).render(
            current_positions=[],
            trade_outcomes=[row],
            symbol="SPXW",
        )
    assert html.count("Settlement pending") >= 2
    assert "100% kept" not in html
    pnl_cell = html.split("pd-pnl", 1)[1].split("</td>", 1)[0]
    assert "Settlement pending" in pnl_cell
    assert "$0.00" not in pnl_cell
