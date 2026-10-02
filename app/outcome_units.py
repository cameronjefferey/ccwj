"""Win/loss and collected-premium rules shared by Positions and Position Detail.

A same-day spread is one outcome. $0 is neither a win nor a loss.
Collected on a spread is the net credit at open (money received), never
the gross premium on the short leg alone.
"""

from __future__ import annotations

SPREAD_STRATEGIES = frozenset({
    "Call Spread",
    "Put Spread",
    "Iron Condor",
    "Diagonal Call Spread",
    "Diagonal Put Spread",
    "Straddle",
    "Strangle",
})

OPEN_OPTION_ACTIONS = frozenset({
    "option_sell_to_open",
    "option_buy_to_open",
})

CLOSE_OPTION_ACTIONS = frozenset({
    "option_buy_to_close",
    "option_sell_to_close",
    "option_assigned",
    "option_exercised",
    "option_expired",
})


def net_collected(received, paid, strategy) -> float:
    """Premium received. Spreads report the net credit, floored at zero."""
    try:
        rec = float(received or 0)
    except (TypeError, ValueError):
        rec = 0.0
    if strategy not in SPREAD_STRATEGIES:
        return rec
    try:
        paid_v = abs(float(paid or 0))
    except (TypeError, ValueError):
        paid_v = 0.0
    return max(rec - paid_v, 0.0)


def apply_spread_collected(df):
    """Rewrite total_premium_received on spread rows to the net credit."""
    if df is None or getattr(df, "empty", True):
        return df
    if "strategy" not in df.columns or "total_premium_received" not in df.columns:
        return df
    if "total_premium_paid" not in df.columns:
        return df
    import pandas as pd

    out = df.copy()
    mask = out["strategy"].isin(SPREAD_STRATEGIES)
    if not bool(mask.any()):
        return out
    rec = pd.to_numeric(out.loc[mask, "total_premium_received"], errors="coerce").fillna(0)
    paid = pd.to_numeric(out.loc[mask, "total_premium_paid"], errors="coerce").fillna(0).abs()
    out.loc[mask, "total_premium_received"] = (rec - paid).clip(lower=0)
    return out


def _unit_key(row) -> tuple:
    strategy = row.get("strategy")
    if strategy in SPREAD_STRATEGIES:
        return (
            "spread",
            str(row.get("tenant_id") or ""),
            str(row.get("account") or ""),
            str(row.get("user_id") or ""),
            str(row.get("symbol") or ""),
            str(strategy or ""),
            str(row.get("open_date") or "")[:10],
            str(row.get("option_expiry") or "")[:10],
        )
    return ("leg", id(row))


def outcome_win_loss(rows) -> tuple[int, int]:
    """(winners, losers) after collapsing a same-day spread to one outcome.

    A unit counts only when every leg is Closed. Rounded $0 is neither.
    """
    buckets: dict[tuple, dict] = {}
    order = []
    for row in rows or []:
        key = _unit_key(row)
        if key not in buckets:
            buckets[key] = {"closed": True, "pnl": 0.0}
            order.append(key)
        bucket = buckets[key]
        status = str(row.get("status") or "")
        if status != "Closed":
            bucket["closed"] = False
        try:
            bucket["pnl"] += float(row.get("total_pnl") or 0)
        except (TypeError, ValueError):
            pass
    winners = losers = 0
    for key in order:
        bucket = buckets[key]
        if not bucket["closed"]:
            continue
        pnl = round(bucket["pnl"], 2)
        if pnl > 0:
            winners += 1
        elif pnl < 0:
            losers += 1
    return winners, losers


def closing_without_an_open(trades) -> list[str]:
    """Option symbols whose history has a close and no opening fill."""
    opened = set()
    closed = set()
    for trade in trades or []:
        action = str(trade.get("action") or "")
        symbol = str(trade.get("trade_symbol") or "").strip()
        if not symbol:
            continue
        if action in OPEN_OPTION_ACTIONS:
            opened.add(symbol)
        elif action in CLOSE_OPTION_ACTIONS:
            closed.add(symbol)
    return sorted(closed - opened)


def adjusted_contract_strike(raw_strike, factor_at_open, factor_at_close) -> float:
    """Post-split strike. Factors are cumulative_split_factor on each date."""
    scale = float(factor_at_open) / float(factor_at_close)
    if scale == 0:
        return float(raw_strike)
    return float(raw_strike) / scale


def adjusted_contract_qty(raw_qty, factor_at_open, factor_at_close) -> float:
    scale = float(factor_at_open) / float(factor_at_close)
    return float(raw_qty) * scale
