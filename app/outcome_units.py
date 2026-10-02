"""Win/loss and collected-premium rules shared by Positions and Position Detail.

A same-day spread is one outcome. $0 is neither a win nor a loss.
Collected on a spread is the net credit at open (money received), never
the gross premium on the short leg alone.
"""

from __future__ import annotations

from app.option_formatting import parse_occ

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


# A vertical is one structure. Strategies still rolls these up as
# "Call Spread" / "Put Spread" (a book can mix credits and debits).
# The position page names the structure from the two legs.
VERTICAL_FAMILIES = frozenset({"Call Spread", "Put Spread"})

_EXPIRED_CLOSE_TYPES = frozenset({
    "expired",
    "expiredotm",
    "expired otm",
})


def vertical_display_name(cp, short_strike, long_strike, net_credit) -> str:
    """Bear call credit, bull put credit, and the two debit siblings.

    ``net_credit`` is premium collected on the short minus the long's
    protection. Positive is a credit. The family on Strategies stays
    Call Spread or Put Spread.
    """
    try:
        credit = float(net_credit) >= -0.005
    except (TypeError, ValueError):
        credit = True
    try:
        short_k = float(short_strike)
        long_k = float(long_strike)
    except (TypeError, ValueError):
        return "Call Spread" if cp == "C" else "Put Spread"
    if abs(short_k - long_k) < 1e-6:
        return "Call Spread" if cp == "C" else "Put Spread"
    if cp == "C":
        bearish = short_k < long_k
        if bearish and credit:
            return "Bear Call Credit Spread"
        if (not bearish) and not credit:
            return "Bull Call Debit Spread"
        return "Bear Call Credit Spread" if bearish else "Bull Call Debit Spread"
    bullish = short_k > long_k
    if bullish and credit:
        return "Bull Put Credit Spread"
    if (not bullish) and not credit:
        return "Bear Put Debit Spread"
    return "Bull Put Credit Spread" if bullish else "Bear Put Debit Spread"


def _expiry_iso(parsed) -> str:
    yy = int(parsed["yy"])
    year = 2000 + yy if yy < 80 else 1900 + yy
    return f"{year:04d}-{int(parsed['mm']):02d}-{int(parsed['dd']):02d}"


def _strike_text(strike) -> str:
    if abs(strike - round(strike)) < 1e-6:
        return f"${int(round(strike))}"
    return f"${strike:.2f}"


def _width_label(width) -> str:
    if abs(width - round(width)) < 1e-6:
        return f"${int(round(width))} wide"
    return f"${width:.2f} wide"


def _abs_num(value) -> float:
    try:
        return abs(float(value or 0))
    except (TypeError, ValueError):
        return 0.0


def _premium_collected(row) -> float:
    """Money collected to open the short. Close proceeds are not premium."""
    received = _abs_num(row.get("premium_received"))
    if received >= 0.01:
        return received
    if str(row.get("direction") or "") == "Sold":
        return _abs_num(row.get("proceeds"))
    return received


def _protection_paid(row) -> float:
    """What the long cost. That is protection, not premium."""
    paid = _abs_num(row.get("premium_paid"))
    if paid >= 0.01:
        return paid
    if str(row.get("direction") or "") == "Bought":
        return _abs_num(row.get("cost"))
    return paid


def leg_outcome(row, expiry) -> str:
    """Expired, Closed, or Assigned.

    A same-day expiry with no buy-to-close (short) and no sell-to-close
    (long) is Expired even when the broker close type says Closed.
    A buy-to-close on expiry day stays Closed. Assignment wins.
    """
    close_type = str(row.get("close_type") or "").strip()
    folded = close_type.lower()
    direction = str(row.get("direction") or "")
    if close_type == "Exercised" and direction == "Sold":
        return "Assigned"
    if close_type in ("Assigned", "Exercised"):
        return close_type
    if folded in _EXPIRED_CLOSE_TYPES:
        return "Expired"
    close = str(row.get("close_date") or "")[:10]
    expiry_s = str(expiry or "")[:10]
    cost = _abs_num(row.get("cost"))
    proceeds = _abs_num(row.get("proceeds"))
    if expiry_s and close == expiry_s:
        if direction == "Sold" and cost < 0.01:
            return "Expired"
        if direction == "Bought" and proceeds < 0.01:
            return "Expired"
    if close_type in ("Closed", "Sold", ""):
        return "Closed"
    return close_type


def _vertical_meta(row):
    if str(row.get("type") or "") not in ("", "option"):
        if row.get("type") and row.get("type") != "option":
            return None
    strategy = str(row.get("strategy") or "")
    if strategy not in VERTICAL_FAMILIES:
        return None
    parsed = parse_occ(row.get("trade_symbol"))
    if not parsed:
        return None
    direction = str(row.get("direction") or "")
    if direction == "Sold":
        side = "short"
    elif direction == "Bought":
        side = "long"
    else:
        return None
    # The contract symbol is the expiry. A missing warehouse date often
    # arrives as the string "nan", which would split the two legs.
    expiry = _expiry_iso(parsed)
    return {
        "key": (
            str(row.get("tenant_id") or ""),
            str(row.get("account") or ""),
            str(row.get("open_date") or "")[:10],
            expiry,
            parsed["cp"],
            strategy,
        ),
        "expiry": expiry,
        "cp": parsed["cp"],
        "strike": float(parsed["strike"]),
        "side": side,
        "root": parsed["root"],
        "mm": int(parsed["mm"]),
        "dd": int(parsed["dd"]),
        "yy": parsed["yy"],
    }


def _pair_nearest(shorts, longs, metas):
    unused = set(longs)
    pairs = []
    for short_i in shorts:
        if not unused:
            break
        short_k = metas[short_i]["strike"]
        match = min(unused, key=lambda j: (abs(metas[j]["strike"] - short_k), j))
        unused.remove(match)
        pairs.append((short_i, match))
    return pairs


def _spread_label(short_meta, long_meta) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month = months[int(short_meta["mm"]) - 1]
    date_part = f"{month} {int(short_meta['dd'])} '{short_meta['yy']}"
    return (
        f"{short_meta['root']} {date_part} "
        f"{_strike_text(short_meta['strike'])} / "
        f"{_strike_text(long_meta['strike'])}{short_meta['cp']}"
    )


def _spread_outcome(short_outcome, long_outcome) -> str:
    if short_outcome == "Expired" and long_outcome == "Expired":
        return "Expired"
    if "Assigned" in (short_outcome, long_outcome):
        return "Assigned"
    if "Exercised" in (short_outcome, long_outcome):
        return "Exercised"
    return "Closed"


def _make_vertical(short, long, short_meta, long_meta):
    collected = round(_premium_collected(short), 2)
    protection = round(_protection_paid(long), 2)
    net = round(collected - protection, 2)
    try:
        pnl = round(float(short.get("pnl") or 0) + float(long.get("pnl") or 0), 2)
    except (TypeError, ValueError):
        pnl = 0.0
    width = abs(float(short_meta["strike"]) - float(long_meta["strike"]))
    short_outcome = leg_outcome(short, short_meta["expiry"])
    long_outcome = leg_outcome(long, long_meta["expiry"])
    outcome = _spread_outcome(short_outcome, long_outcome)
    name = vertical_display_name(
        short_meta["cp"], short_meta["strike"], long_meta["strike"], net,
    )
    if net >= 0:
        basis = collected
        direction = "Sold"
    else:
        basis = protection
        direction = "Bought"
    return_pct = round(pnl / basis * 100, 1) if basis >= 0.01 else None
    child_short = dict(short)
    child_long = dict(long)
    child_short["outcome"] = short_outcome
    child_long["outcome"] = long_outcome
    child_short["role"] = "short"
    child_long["role"] = "long"
    child_short["role_label"] = "Premium collected"
    child_long["role_label"] = "Protection"
    child_short["role_amount"] = collected
    child_long["role_amount"] = protection
    raw = list(short.get("raw_trades") or []) + list(long.get("raw_trades") or [])
    leg_num = short.get("leg_num")
    if leg_num is None:
        leg_num = long.get("leg_num")
    days = short.get("days_held")
    if days is None:
        days = long.get("days_held")
    return {
        "vertical": True,
        "legs": [child_short, child_long],
        "strategy": name,
        "strategy_family": short.get("strategy") or long.get("strategy") or "",
        "trade_symbol": _spread_label(short_meta, long_meta),
        "quantity": short.get("quantity") if short.get("quantity") is not None else long.get("quantity"),
        "quantity_display": short.get("quantity_display") or long.get("quantity_display"),
        "width": width,
        "width_label": _width_label(width),
        "net_credit": net,
        "collected": collected,
        "protection": protection,
        "pnl": pnl,
        "is_winner": True if pnl > 0 else False if pnl < 0 else None,
        "outcome": outcome,
        "close_type": outcome if outcome != "Closed" else "Closed",
        "open_date": short.get("open_date") or long.get("open_date") or "",
        "close_date": short.get("close_date") or long.get("close_date") or "",
        "days_held": days,
        "leg_num": leg_num,
        "account": short.get("account") or long.get("account"),
        "account_display": short.get("account_display") or long.get("account_display"),
        "tenant_id": short.get("tenant_id") or long.get("tenant_id"),
        "type": "option",
        "direction": direction,
        "cost": 0.0,
        "proceeds": 0.0,
        "return_pct": return_pct,
        "raw_trades": raw,
        "fill_count": len(raw),
    }


def group_vertical_spreads(rows):
    """One row per vertical. Other legs pass through in the same order.

    A vertical is the same account, open date, and expiry, one short and
    one long of a Call Spread or Put Spread. Several verticals on the
    same day pair by nearest strike. The long is not its own win/loss row.
    """
    indexed = list(rows or [])
    metas = [_vertical_meta(row) for row in indexed]
    groups: dict[tuple, list[int]] = {}
    for i, meta in enumerate(metas):
        if meta:
            groups.setdefault(meta["key"], []).append(i)
    used = set()
    out = []
    for i, row in enumerate(indexed):
        if i in used:
            continue
        meta = metas[i]
        if not meta:
            out.append(row)
            continue
        members = [j for j in groups[meta["key"]] if j not in used]
        shorts = [j for j in members if metas[j]["side"] == "short"]
        longs = [j for j in members if metas[j]["side"] == "long"]
        if not shorts or not longs:
            out.append(row)
            used.add(i)
            continue
        pairs = _pair_nearest(shorts, longs, metas)
        for short_i, long_i in sorted(pairs, key=lambda pair: pair[0]):
            used.add(short_i)
            used.add(long_i)
            out.append(_make_vertical(
                indexed[short_i], indexed[long_i], metas[short_i], metas[long_i],
            ))
        for j in members:
            if j not in used:
                used.add(j)
                out.append(indexed[j])
    return out


def legs_activity_summary(outcomes, fill_count) -> dict:
    """Fills, contracts, and spreads so the narrative can explain a gap.

    Eight fills across six contracts means some contracts were partial
    fills. The win/loss count stays on the spread, not the long leg.
    """
    contracts = 0
    spreads = 0
    for outcome in outcomes or []:
        if outcome.get("vertical"):
            spreads += 1
            contracts += len(outcome.get("legs") or [])
        elif str(outcome.get("type") or "") == "option":
            contracts += 1
    try:
        fills = int(fill_count or 0)
    except (TypeError, ValueError):
        fills = 0
    return {
        "fills": fills,
        "contracts": contracts,
        "spreads": spreads,
        "partial": bool(contracts and fills > contracts),
    }


def annotate_strategy_structures(strategy_rows, outcomes):
    """Name a strategy row when every vertical in that family agrees.

    Mixed credits and debits stay Call Spread / Put Spread, which is
    the Strategies card.
    """
    names: dict[str, set] = {}
    for outcome in outcomes or []:
        if not outcome.get("vertical"):
            continue
        family = str(outcome.get("strategy_family") or "")
        structure = str(outcome.get("strategy") or "")
        if family and structure:
            names.setdefault(family, set()).add(structure)
    for row in strategy_rows or []:
        found = names.get(str(row.get("strategy") or ""))
        if found and len(found) == 1:
            structure = next(iter(found))
            if structure != row.get("strategy"):
                row["structure_name"] = structure
    return strategy_rows


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
