"""Sara's SPXW statement: as-of cash settlement, net fees, pending ITM.

The broker statement (Oct 2, 2026) is down $821.08. The position page
showed +$2,325.56 because the 10/02-as-of-10/01 closing fills were
dropped and the Oct 1 opens were gross of the $12.22 commission.

The Oct 2 book is two trades, not one 30-lot: the 20-lot closed
intraday for +902.24 and the 10-lot expired worthless for +975.56.
Together with the earlier spreads that is -821.08. Dropping the
10-lot credit leaves -1,796.64. The three spreads dated before Oct 2
net to -2,698.88.
"""

from datetime import datetime

import pandas as pd
import pytest

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


def test_blank_fee_reads_commission_or_a_linked_fee_activity():
    """Oct 1 SPXW opens shipped fee 0. The commission is another field or a FEE row."""
    from app.snaptrade_normalize import activities_to_history_df

    gross = {
        "type": "SELL",
        "option_type": "SELL_TO_OPEN",
        "trade_date": "2026-10-01",
        "description": "CALL S & P 500 INDEX SPXW 10/01/2026 7650.00 C",
        "symbol": "SPXW 10/01/2026 7650.00 C",
        "units": 10,
        "price": 7.77,
        "fee": None,
        "commission": 12.22,
        "amount": 7770.0,
    }
    df = activities_to_history_df(
        [gross], account_name=ACCOUNT, user_id=9, tenant_id=TENANT,
    )
    assert float(df.iloc[0]["Amount"]) == 7757.78
    assert float(df.iloc[0]["fees_and_comm"]) == 12.22

    linked = activities_to_history_df(
        [
            {
                "type": "SELL",
                "option_type": "SELL_TO_OPEN",
                "trade_date": "2026-10-01",
                "description": "CALL S & P 500 INDEX SPXW 10/01/2026 7650.00 C",
                "symbol": "SPXW 10/01/2026 7650.00 C",
                "units": 10,
                "price": 7.77,
                "fee": 0,
                "amount": 7770.0,
                "external_reference_id": "fill-7650",
            },
            {
                "type": "FEE",
                "trade_date": "2026-10-01",
                "description": "Commission",
                "amount": -12.22,
                "fee": 0,
                "external_reference_id": "fill-7650",
            },
        ],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    assert len(linked) == 1
    assert linked.iloc[0]["Action"] == "Sell to Open"
    assert float(linked.iloc[0]["Amount"]) == 7757.78
    assert float(linked.iloc[0]["fees_and_comm"]) == 12.22


def test_order_commission_survives_when_the_activity_fee_is_blank():
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
    activity = activities_to_history_df(
        [{
            "type": "SELL",
            "option_type": "SELL_TO_OPEN",
            "trade_date": "2026-10-01",
            "description": "CALL S & P 500 INDEX SPXW 10/01/2026 7650.00 C",
            "symbol": "SPXW 10/01/2026 7650.00 C",
            "units": 10,
            "price": 7.77,
            "fee": 0,
            "amount": 7770.0,
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    assert float(activity.iloc[0]["Amount"]) == 7770.0
    assert activity.iloc[0]["fees_and_comm"] in ("", 0, 0.0)
    merged = _dedup_history_rows(
        pd.concat([orders, activity], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(merged) == 1
    assert float(merged.iloc[0]["fees_and_comm"]) == 12.22
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


def _fill(action, qty, price, amount, day="2026-10-02"):
    return {
        "trade_date": day,
        "action": action,
        "quantity": qty,
        "price": price,
        "amount": amount,
    }


def _fused_oct2_legs():
    """One warehouse row per contract, the 20-lot and the 10-lot blended."""
    short = {
        "type": "option",
        "strategy": "Call Spread",
        "trade_symbol": "SPXW  261002C07730000",
        "direction": "Sold",
        "close_type": "Closed",
        "open_date": "2026-10-02",
        "close_date": "2026-10-02",
        "quantity": 30,
        "quantity_display": "30",
        "pnl": 13675.56 + 2777.78 - 5672.44,
        "premium_received": 13675.56 + 2777.78,
        "tenant_id": TENANT,
        "account": ACCOUNT,
        "raw_trades": [
            _fill("option_sell_to_open", 20, 6.85, 13675.56),
            _fill("option_buy_to_close", 20, 2.824, -5672.44),
            _fill("option_sell_to_open", 10, 2.79, 2777.78),
        ],
    }
    long = {
        "type": "option",
        "strategy": "Call Spread",
        "trade_symbol": "SPXW  261002C07735000",
        "direction": "Bought",
        "close_type": "Closed",
        "open_date": "2026-10-02",
        "close_date": "2026-10-02",
        "quantity": 30,
        "quantity_display": "30",
        "pnl": -10724.44 - 1802.22 + 3623.56,
        "premium_paid": 10724.44 + 1802.22,
        "tenant_id": TENANT,
        "account": ACCOUNT,
        "raw_trades": [
            _fill("option_buy_to_open", 20, 5.35, -10724.44),
            _fill("option_sell_to_close", 20, 1.824, 3623.56),
            _fill("option_buy_to_open", 10, 1.79, -1802.22),
        ],
    }
    return short, long


def test_oct2_lots_split_into_a_closed_20_and_an_expired_10():
    """20× at $6.85/$5.35 closed; 10× at $2.79/$1.79 expired worthless."""
    short, long = _fused_oct2_legs()
    grouped = group_vertical_spreads([short, long])
    assert len(grouped) == 2
    closed = next(row for row in grouped if row["outcome"] == "Closed")
    expired = next(row for row in grouped if row["outcome"] == "Expired")
    assert closed["quantity_display"] == "20"
    assert expired["quantity_display"] == "10"
    assert round(closed["pnl"], 2) == 902.24
    assert round(expired["pnl"], 2) == 975.56
    assert closed["is_winner"] is True
    assert expired["is_winner"] is True
    assert "30" not in str(closed["quantity_display"])
    assert "7730" in closed["trade_symbol"] and "7735" in closed["trade_symbol"]

    earlier = _legs_from_history(
        _history()[_history()["Date"].map(_as_date) < _as_date("10/02/2026")]
    )
    book = group_vertical_spreads(earlier + [short, long])
    assert len(book) == 5
    assert round(sum(row["pnl"] for row in book), 2) == -821.08
    winners = [row for row in book if row["is_winner"] is True]
    losers = [row for row in book if row["is_winner"] is False]
    assert len(winners) == 4
    assert len(losers) == 1


def test_resync_of_a_posted_settlement_does_not_twin_the_stored_row():
    """The seed keeps the broker's posting date. The warehouse caps it.

    A stored Oct 1 SPXW close is dated 10/02. Rewriting the re-sync to
    10/01 and appending "as of" used to fail dedup and insert a second
    fill. Both rows must collapse to the stored posting date.
    """
    resynced = activities_to_history_df(
        [_act(
            "2026-10-02",
            "Buy to Close",
            "SPXW 10/01/2026 7650.00 C",
            10, 16.45, -16450.0, 0,
            "CALL S & P 500 INDEX $7650 EXP 10/01/26",
        )],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    assert resynced.iloc[0]["Date"] == "10/02/2026"
    assert "as of" not in str(resynced.iloc[0]["Description"]).lower()
    stored = resynced.copy()
    stored.loc[stored.index[0], "Date"] = "10/02/2026"
    stored.loc[stored.index[0], "Description"] = (
        "CALL S & P 500 INDEX $7650 EXP 10/01/26"
    )
    merged = _dedup_history_rows(
        pd.concat([stored, resynced], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(merged) == 1
    assert merged.iloc[0]["Date"] == "10/02/2026"
    assert "as of 10/01" not in str(merged.iloc[0]["Description"]).lower()


def test_blank_order_fee_uses_the_recent_spxw_rate_and_splits_a_parent_fee():
    from app.snaptrade_normalize import ESTIMATED_FEE_MARK, recent_option_fee_rates

    rates = recent_option_fee_rates(_history())
    assert round(rates["SPXW"], 3) == 1.222
    orders = orders_to_history_df(
        [{
            "action": "SELL_TO_OPEN",
            "option_symbol": {
                "underlying_symbol": "SPXW",
                "expiration_date": "2026-10-02",
                "strike_price": 7730,
                "option_type": "CALL",
            },
            "status": "EXECUTED",
            "time_executed": "2026-10-02T15:00:00Z",
            "filled_quantity": 10,
            "execution_price": 2.79,
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
        fee_rates=rates,
    )
    assert float(orders.iloc[0]["fees_and_comm"]) == pytest.approx(12.22)
    assert float(orders.iloc[0]["Amount"]) == pytest.approx(2777.78)
    assert ESTIMATED_FEE_MARK in str(orders.iloc[0]["Description"])

    spread = orders_to_history_df(
        [{
            "brokerage_order_id": "spxw-20",
            "status": "EXECUTED",
            "time_executed": "2026-10-02T14:00:00Z",
            "fee": 48.88,
            "legs": [
                {
                    "leg_id": "short",
                    "action": "SELL_TO_OPEN",
                    "filled_quantity": "20",
                    "execution_price": "6.85",
                    "instrument": {
                        "symbol": "SPXW  261002C07730000",
                        "asset_type": "OPTION",
                    },
                },
                {
                    "leg_id": "long",
                    "action": "BUY_TO_OPEN",
                    "filled_quantity": "20",
                    "execution_price": "5.35",
                    "instrument": {
                        "symbol": "SPXW  261002C07735000",
                        "asset_type": "OPTION",
                    },
                },
            ],
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    by_action = {row["Action"]: row for _, row in spread.iterrows()}
    assert float(by_action["Sell to Open"]["fees_and_comm"]) == pytest.approx(24.44)
    assert float(by_action["Buy to Open"]["fees_and_comm"]) == pytest.approx(24.44)
    # 20 × 6.85 × 100 − 24.44, and the long debit plus the same half.
    assert float(by_action["Sell to Open"]["Amount"]) == pytest.approx(13675.56)
    assert float(by_action["Buy to Open"]["Amount"]) == pytest.approx(-10724.44)

    activity = activities_to_history_df(
        [_act(
            "2026-10-02", "Sell to Open", "SPXW 10/02/2026 7730.00 C",
            10, 2.79, 2777.78, 12.22, "INDEX",
        )],
        account_name=ACCOUNT, user_id=9, tenant_id=TENANT,
    )
    # The estimate mark must not win just because the order text is longer.
    orders.loc[orders.index[0], "Description"] = (
        "SPXW  261002C07730000 est. fee provisional order text that is longer"
    )
    merged = _dedup_history_rows(
        pd.concat([orders, activity], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(merged) == 1
    assert str(merged.iloc[0]["Description"]) == "INDEX"
    assert float(merged.iloc[0]["Amount"]) == pytest.approx(2777.78)


def test_copied_estimate_nets_the_gross_amount_and_the_activity_replaces_it():
    """A stored no-fee order row stays one fill, net of the estimate.

    The 20× 6.85 / 5.35 open and 2.824 / 1.824 close is +902.24 after
    $24.44 per leg. A later activity with that same net and fee must
    not charge the estimate again.
    """
    from app.snaptrade_normalize import recent_option_fee_rates

    rates = recent_option_fee_rates(_history())
    legs = [
        ("SELL_TO_OPEN", "Sell to Open", 7730, "20", "6.85", 13675.56, 24.44),
        ("BUY_TO_OPEN", "Buy to Open", 7735, "20", "5.35", -10724.44, 24.44),
        ("BUY_TO_CLOSE", "Buy to Close", 7730, "20", "2.824", -5672.44, 24.44),
        ("SELL_TO_CLOSE", "Sell to Close", 7735, "20", "1.824", 3623.56, 24.44),
    ]
    estimated = orders_to_history_df(
        [{
            "brokerage_order_id": f"leg-{i}",
            "status": "EXECUTED",
            "time_executed": "2026-10-02T14:00:00Z",
            "action": action,
            "filled_quantity": qty,
            "execution_price": price,
            "option_symbol": {
                "underlying_symbol": "SPXW",
                "expiration_date": "2026-10-02",
                "strike_price": strike,
                "option_type": "CALL",
            },
        } for i, (action, _label, strike, qty, price, _net, _fee) in enumerate(legs)],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
        fee_rates=rates,
    )
    stored = estimated.copy()
    stored["fees_and_comm"] = ""
    stored["Description"] = stored["Description"].map(
        lambda text: str(text).replace(" est. fee", "").replace("est. fee", "").strip()
    )
    for i, row in stored.iterrows():
        gross = round(abs(float(row["Quantity"])) * float(row["Price"]) * 100, 2)
        stored.at[i, "Amount"] = -gross if str(row["Action"]).startswith("Buy") else gross
    with_estimate = _dedup_history_rows(
        pd.concat([stored, estimated], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(with_estimate) == 4
    assert float(with_estimate["Amount"].sum()) == pytest.approx(902.24)
    assert float(with_estimate["fees_and_comm"].astype(float).sum()) == pytest.approx(97.76)

    activities = []
    for _action, label, strike, qty, price, net, fee in legs:
        activities.append(_act(
            "2026-10-02", label, f"SPXW 10/02/2026 {strike:.2f} C",
            float(qty), float(price), net, fee, f"INDEX {strike}",
        ))
    activity_df = activities_to_history_df(
        activities, account_name=ACCOUNT, user_id=9, tenant_id=TENANT,
    )
    replaced = _dedup_history_rows(
        pd.concat([with_estimate, activity_df], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(replaced) == 4
    assert float(replaced["Amount"].sum()) == pytest.approx(902.24)
    assert float(replaced["fees_and_comm"].astype(float).sum()) == pytest.approx(97.76)
    assert not replaced["Description"].astype(str).str.contains("est. fee").any()


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


def test_estimated_itm_spread_is_a_loss_not_a_worthless_win():
    from app.expiry_settlement import ESTIMATE_LABEL

    short = {
        "type": "option",
        "strategy": "Call Spread",
        "trade_symbol": "SPXW  261001C07650000",
        "direction": "Sold",
        "close_type": ESTIMATE_LABEL,
        "open_date": "2026-10-01",
        "close_date": "2026-10-01",
        "quantity": 10,
        "cost": 0,
        "proceeds": 7757.78,
        "pnl": 7757.78 - 50000,
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
        "pnl": -6332.22 + 45000,
        "premium_received": 0,
        "premium_paid": 6332.22,
    }
    assert leg_outcome(short, "2026-10-01") == ESTIMATE_LABEL
    grouped = group_vertical_spreads([short, long])
    assert len(grouped) == 1
    assert grouped[0]["outcome"] == ESTIMATE_LABEL
    assert grouped[0]["is_winner"] is False
    assert round(grouped[0]["pnl"], 2) == -3574.44
    assert grouped[0]["close_type"] == ESTIMATE_LABEL

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
    assert "Settlement pending" not in html
    assert "Settled at expiry (est.)" in html
    assert "100% kept" not in html
    pnl_cell = html.split("pd-pnl", 1)[1].split("</td>", 1)[0]
    assert "-$3,574.44" in pnl_cell
    assert "Settlement pending" not in pnl_cell


def test_settlement_and_expiry_orders_do_not_take_an_estimated_fee():
    """Oct 1 7650/7655C cash settlement stays 16.45 / 11.45 with no commission.

    A same-day 0DTE close still estimates. A $50 fee on a settlement row
    must not become the SPXW rate.
    """
    from app.snaptrade_normalize import ESTIMATED_FEE_MARK, recent_option_fee_rates

    rates = recent_option_fee_rates(_history())
    assert round(rates["SPXW"], 3) == 1.222

    poison = _history().iloc[0:1].copy()
    poison.loc[poison.index[0], "Date"] = "10/02/2026"
    poison.loc[poison.index[0], "Action"] = "Buy to Close"
    poison.loc[poison.index[0], "Symbol"] = "SPXW  261001C07650000"
    poison.loc[poison.index[0], "Description"] = "Cash settlement as of 10/01/2026 est. fee"
    poison.loc[poison.index[0], "Quantity"] = 1
    poison.loc[poison.index[0], "Price"] = 16.45
    poison.loc[poison.index[0], "fees_and_comm"] = 50
    poison.loc[poison.index[0], "Amount"] = -1695
    assert round(recent_option_fee_rates(
        pd.concat([_history(), poison], ignore_index=True)
    )["SPXW"], 3) == 1.222

    posted_after = orders_to_history_df(
        [
            {
                "action": "BUY_TO_CLOSE",
                "description": "Buy to Close",
                "option_symbol": {
                    "underlying_symbol": "SPXW",
                    "expiration_date": "2026-10-01",
                    "strike_price": 7650,
                    "option_type": "CALL",
                },
                "status": "EXECUTED",
                "time_executed": "2026-10-02T14:00:00Z",
                "filled_quantity": 10,
                "execution_price": 16.45,
            },
            {
                "action": "SELL_TO_CLOSE",
                "option_symbol": {
                    "underlying_symbol": "SPXW",
                    "expiration_date": "2026-10-01",
                    "strike_price": 7655,
                    "option_type": "CALL",
                },
                "status": "EXECUTED",
                "time_executed": "2026-10-02T14:00:00Z",
                "filled_quantity": 10,
                "execution_price": 11.45,
            },
        ],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
        fee_rates=rates,
    )
    by_price = {float(row["Price"]): row for _, row in posted_after.iterrows()}
    assert float(by_price[16.45]["Amount"]) == -16450.0
    assert float(by_price[11.45]["Amount"]) == 11450.0
    assert by_price[16.45]["fees_and_comm"] in ("", 0, 0.0)
    assert by_price[11.45]["fees_and_comm"] in ("", 0, 0.0)
    assert not posted_after["Description"].astype(str).str.contains(ESTIMATED_FEE_MARK).any()

    # Same calendar day, but the text is a cash settlement.
    same_day = orders_to_history_df(
        [{
            "action": "BUY_TO_CLOSE",
            "description": "Cash settlement as of 10/01/2026",
            "option_symbol": {
                "underlying_symbol": "SPXW",
                "expiration_date": "2026-10-01",
                "strike_price": 7650,
                "option_type": "CALL",
            },
            "status": "EXECUTED",
            "time_executed": "2026-10-01T20:00:00Z",
            "filled_quantity": 10,
            "execution_price": 16.45,
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
        fee_rates=rates,
    )
    assert float(same_day.iloc[0]["Amount"]) == -16450.0
    assert same_day.iloc[0]["fees_and_comm"] in ("", 0, 0.0)

    # The Oct 2 20-lot close expires that day, so the estimate still applies.
    intraday = orders_to_history_df(
        [{
            "action": "BUY_TO_CLOSE",
            "option_symbol": {
                "underlying_symbol": "SPXW",
                "expiration_date": "2026-10-02",
                "strike_price": 7730,
                "option_type": "CALL",
            },
            "status": "EXECUTED",
            "time_executed": "2026-10-02T18:00:00Z",
            "filled_quantity": 20,
            "execution_price": 2.824,
        }],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
        fee_rates=rates,
    )
    assert float(intraday.iloc[0]["fees_and_comm"]) == pytest.approx(24.44)
    assert float(intraday.iloc[0]["Amount"]) == pytest.approx(-5672.44)
    assert ESTIMATED_FEE_MARK in str(intraday.iloc[0]["Description"])


def test_statement_zero_fee_wins_over_a_copied_settlement_estimate():
    """The next sync drops the $12.22 that was copied onto each settlement.

    10 × 16.45 and 10 × 11.45 with those fees is −$3,598.88. The statement
    cash is −$3,574.44 with the Oct 1 opens.
    """
    bad = pd.DataFrame([
        {
            "Account": ACCOUNT, "user_id": 9, "tenant_id": TENANT,
            "Date": "10/01/2026", "Action": "Buy to Close",
            "Symbol": "SPXW  261001C07650000",
            "Description": (
                "CALL S & P 500 INDEX $7650 EXP 10/01/26 as of 10/01/2026 est. fee"
            ),
            "Quantity": 10, "Price": 16.45, "fees_and_comm": 12.22,
            "Amount": -16462.22,
        },
        {
            "Account": ACCOUNT, "user_id": 9, "tenant_id": TENANT,
            "Date": "10/02/2026", "Action": "Sell to Close",
            "Symbol": "SPXW  261001C07655000",
            "Description": "SPXW  261001C07655000 est. fee",
            "Quantity": 10, "Price": 11.45, "fees_and_comm": 12.22,
            "Amount": 11437.78,
        },
    ], columns=HISTORY_SEED_COLUMNS)
    clean = activities_to_history_df(
        [
            _act(
                "10/02/2026 as of 10/01/2026",
                "Buy to Close",
                "SPXW 10/01/2026 7650.00 C",
                10, 16.45, -16450.0, 0,
                "CALL S & P 500 INDEX $7650 EXP 10/01/26 as of 10/01/2026",
            ),
            _act(
                "2026-10-02",
                "Sell to Close",
                "SPXW 10/01/2026 7655.00 C",
                10, 11.45, 11450.0, 0,
                "CALL S & P 500 INDEX $7655 EXP 10/01/26",
            ),
        ],
        account_name=ACCOUNT,
        user_id=9,
        tenant_id=TENANT,
    )
    merged = _dedup_history_rows(
        pd.concat([bad, clean], ignore_index=True),
        HISTORY_SEED_COLUMNS,
    )
    assert len(merged) == 2
    by_price = {float(row["Price"]): row for _, row in merged.iterrows()}
    assert float(by_price[16.45]["Amount"]) == -16450.0
    assert float(by_price[11.45]["Amount"]) == 11450.0
    assert by_price[16.45]["fees_and_comm"] in ("", 0, 0.0)
    assert by_price[11.45]["fees_and_comm"] in ("", 0, 0.0)
    assert not merged["Description"].astype(str).str.contains("est. fee").any()

    opens = _history()
    opens = opens[
        (opens["Date"] == "10/01/2026")
        & (opens["Action"].isin(["Buy to Open", "Sell to Open"]))
    ]
    spread = pd.concat([opens, merged], ignore_index=True)
    assert round(float(spread["Amount"].sum()), 2) == -3574.44
    poisoned = -3574.44 - 12.22 - 12.22
    assert round(poisoned, 2) == -3598.88
