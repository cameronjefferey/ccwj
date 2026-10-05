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
            str(row.get("lot_id") or ""),
        )
    return ("leg", id(row))


def decided_win_loss(status, total_pnl) -> tuple[int, int]:
    """(winner, loser) for one contract.

    Open and settlement-pending are not results. A closed round that
    lands on $0 is neither — the same rule as Positions. Booking a
    missing ITM settlement as a loss (or as the opening credit) is the
    bug this guards.
    """
    if str(status or "") != "Closed":
        return (0, 0)
    try:
        pnl = round(float(total_pnl or 0), 2)
    except (TypeError, ValueError):
        return (0, 0)
    if pnl > 0:
        return (1, 0)
    if pnl < 0:
        return (0, 1)
    return (0, 0)


def annotate_contract_outcomes(df):
    """Win/loss and realized/unrealized for a per-contract frame.

    Drops rows still waiting on settlement so a $0 pending contract
    does not dilute expectancy or count as a decided trade. Open marks
    stay in unrealized and out of the win rate.
    """
    if df is None or getattr(df, "empty", True):
        return df
    import pandas as pd

    out = df.copy()
    if "status" in out.columns:
        pending = out["status"].astype(str) == "Settlement pending"
        out = out.loc[~pending].copy()
    if out.empty:
        return out
    pnl = pd.to_numeric(out.get("total_pnl"), errors="coerce").fillna(0.0)
    status = out["status"] if "status" in out.columns else pd.Series("", index=out.index)
    winners = []
    losers = []
    for st, amount in zip(status.tolist(), pnl.tolist()):
        w, l = decided_win_loss(st, amount)
        winners.append(w)
        losers.append(l)
    out["num_winners"] = winners
    out["num_losers"] = losers
    closed = status.astype(str) == "Closed"
    opened = status.astype(str) == "Open"
    out["realized_pnl"] = pnl.where(closed, 0.0)
    out["unrealized_pnl"] = pnl.where(opened, 0.0)
    out["total_pnl"] = pnl
    return out


def closed_outcome_units(rows) -> list[dict]:
    """One closed result per unit.

    A same-day spread (same strategy, account, symbol, open date, and
    expiry) is one unit. The unit is dropped when any leg is still open.
    Rounded $0 stays in the list so callers can see it is neither a win
    nor a loss. ``pnl`` is the sum of ``total_pnl`` (already net of fees).
    """
    buckets: dict[tuple, dict] = {}
    order = []
    for row in rows or []:
        key = _unit_key(row)
        if key not in buckets:
            symbol = row.get("symbol") or row.get("underlying_symbol") or ""
            buckets[key] = {
                "closed": True,
                "pnl": 0.0,
                "symbol": str(symbol or ""),
                "strategy": str(row.get("strategy") or ""),
                "tenant_id": str(row.get("tenant_id") or ""),
            }
            order.append(key)
        bucket = buckets[key]
        if str(row.get("status") or "") != "Closed":
            bucket["closed"] = False
        try:
            bucket["pnl"] += float(row.get("total_pnl") or 0)
        except (TypeError, ValueError):
            pass
    units = []
    for key in order:
        bucket = buckets[key]
        if not bucket["closed"]:
            continue
        units.append({
            "symbol": bucket["symbol"],
            "strategy": bucket["strategy"],
            "tenant_id": bucket["tenant_id"],
            "pnl": round(bucket["pnl"], 2),
        })
    return units


def outcome_win_loss(rows) -> tuple[int, int]:
    """(winners, losers) after collapsing a same-day spread to one outcome.

    A unit counts only when every leg is Closed. Rounded $0 is neither.
    """
    winners = losers = 0
    for unit in closed_outcome_units(rows):
        if unit["pnl"] > 0:
            winners += 1
        elif unit["pnl"] < 0:
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
    """Expired, Closed, Assigned, or the expiry estimate.

    A same-day expiry with no buy-to-close (short) and no sell-to-close
    (long) is Expired even when the broker close type says Closed.
    A buy-to-close on expiry day stays Closed. Assignment wins.
    ``Settled at expiry (est.)`` wins over that zero-cash expiry
    heuristic: an ITM short has no closing fill and cost ≈ $0, which
    would otherwise read as a worthless expiry.
    """
    from app.expiry_settlement import ESTIMATE_LABEL, PENDING_LABEL

    close_type = str(row.get("close_type") or "").strip()
    folded = close_type.lower()
    direction = str(row.get("direction") or "")
    if close_type == PENDING_LABEL:
        return PENDING_LABEL
    if close_type == ESTIMATE_LABEL:
        return ESTIMATE_LABEL
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


# Weekly index series share a book with the parent root. A same-day
# SPXW spread must not pair with an ASTS or BE leg just because the
# contract counts match. XSP stays its own root.
_VERTICAL_ROOT = {
    "SPX": "SPXW",
    "SPXW": "SPXW",
    "NDX": "NDXP",
    "NDXP": "NDXP",
    "RUT": "RUTW",
    "RUTW": "RUTW",
}


def _vertical_root(root) -> str:
    text = str(root or "").strip().upper()
    return _VERTICAL_ROOT.get(text, text)


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
            _vertical_root(parsed["root"]),
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


def _pair_nearest(shorts, longs, metas, rows):
    """Nearest strike, and the same contract count when several lots share it.

    Two 7730/7735 trades (20 closed, 10 left to expire) are four legs at
    two strikes. Pairing by strike alone can match the 20-lot short with
    the 10-lot long. Quantity agrees first, then strike, then order.
    """
    unused = set(longs)
    pairs = []
    for short_i in shorts:
        if not unused:
            break
        short_k = metas[short_i]["strike"]
        short_q = _abs_num((rows[short_i] or {}).get("quantity"))

        def _key(j, _short_k=short_k, _short_q=short_q):
            qty_gap = 0 if abs(_abs_num((rows[j] or {}).get("quantity")) - _short_q) < 1e-6 else 1
            return (qty_gap, abs(metas[j]["strike"] - _short_k), j)

        match = min(unused, key=_key)
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
    from app.expiry_settlement import ESTIMATE_LABEL, PENDING_LABEL

    if PENDING_LABEL in (short_outcome, long_outcome):
        return PENDING_LABEL
    if ESTIMATE_LABEL in (short_outcome, long_outcome):
        return ESTIMATE_LABEL
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
    is_winner = True if pnl > 0 else False if pnl < 0 else None
    if outcome == "Settlement pending":
        # An unpriced index spread is not a win, even if a child row
        # still carries the opening credit.
        is_winner = None
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
        "is_winner": is_winner,
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
        "lot_id": short.get("lot_id") or long.get("lot_id") or "",
    }


_OPEN_SHORT = frozenset({"option_sell_to_open", "sell to open"})
_OPEN_LONG = frozenset({"option_buy_to_open", "buy to open"})
_CLOSE_SHORT = frozenset({"option_buy_to_close", "buy to close"})
_CLOSE_LONG = frozenset({"option_sell_to_close", "sell to close"})
_EXPIRE_ACTIONS = frozenset({
    "option_expired", "expired", "option_assigned", "assigned",
    "option_exercised", "exercised", "exchange or exercise",
})


def _fill_text(trade, *names) -> str:
    for name in names:
        if isinstance(trade, dict) and trade.get(name) not in (None, ""):
            return str(trade.get(name))
    return ""


def _fill_action(trade) -> str:
    return _fill_text(trade, "action", "Action").strip().lower()


def _fill_qty(trade) -> float:
    for name in ("quantity", "Quantity"):
        if isinstance(trade, dict) and trade.get(name) not in (None, ""):
            return _abs_num(trade.get(name))
    return 0.0


def _fill_amount(trade) -> float:
    for name in ("amount", "Amount"):
        if isinstance(trade, dict) and trade.get(name) not in (None, ""):
            try:
                return float(trade.get(name) or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _fill_price(trade) -> float:
    for name in ("price", "Price"):
        if isinstance(trade, dict) and trade.get(name) not in (None, ""):
            return _abs_num(trade.get(name))
    qty = _fill_qty(trade)
    if qty >= 1e-9:
        return round(abs(_fill_amount(trade)) / qty / 100.0, 4)
    return 0.0


def _fill_date(trade) -> str:
    raw = _fill_text(trade, "trade_date", "Date", "date")
    return raw[:10]


def _qty_display(qty) -> str:
    if abs(qty - round(qty)) < 1e-6:
        return str(int(round(qty)))
    return f"{qty:.4f}".rstrip("0").rstrip(".")


def _outcome_expiry(row) -> str:
    parsed = parse_occ(row.get("trade_symbol"))
    if parsed:
        return _expiry_iso(parsed)
    return str(row.get("option_expiry") or row.get("close_date") or "")[:10]


def split_option_lots(outcome):
    """One display row per open lot when a contract was traded twice.

    The warehouse keeps one row per contract, so a 20-lot that was closed
    and a 10-lot that expired at the same strikes become ``30×``. Same-price
    partial fills stay one lot (the Oct 1 SPXW short filled 4,000 + 3,770
    and is still one 10-lot). A later open at a different price, or a
    close that does not cover the whole open, is its own row.
    """
    if not isinstance(outcome, dict) or outcome.get("vertical"):
        return [outcome]
    if str(outcome.get("type") or "") not in ("", "option"):
        return [outcome]
    fills = [t for t in (outcome.get("raw_trades") or []) if isinstance(t, dict)]
    if not fills:
        return [outcome]
    direction = str(outcome.get("direction") or "")
    if direction == "Sold":
        open_actions, close_actions = _OPEN_SHORT, _CLOSE_SHORT
    elif direction == "Bought":
        open_actions, close_actions = _OPEN_LONG, _CLOSE_LONG
    else:
        return [outcome]

    class _Lot:
        def __init__(self, price, index):
            self.price = price
            self.index = index
            self.qty = 0.0
            self.remaining = 0.0
            self.open_cash = 0.0
            self.opens = []
            self.closes = []
            self.open_date = ""

    lots = []
    by_price = {}
    closes = []
    for trade in fills:
        action = _fill_action(trade)
        if action in open_actions:
            price = round(_fill_price(trade), 4)
            lot = by_price.get(price)
            if lot is None:
                lot = _Lot(price, len(lots))
                by_price[price] = lot
                lots.append(lot)
            qty = _fill_qty(trade)
            lot.qty += qty
            lot.remaining += qty
            lot.open_cash += _fill_amount(trade)
            lot.opens.append(trade)
            opened = _fill_date(trade)
            if opened and (not lot.open_date or opened < lot.open_date):
                lot.open_date = opened
        elif action in close_actions or action in _EXPIRE_ACTIONS:
            closes.append(trade)
    if len(lots) <= 1 and not closes:
        return [outcome]
    open_qty = sum(lot.qty for lot in lots)
    try:
        outcome_qty = abs(float(outcome.get("quantity") or 0))
    except (TypeError, ValueError):
        outcome_qty = 0.0
    # Placeholder fills (qty 1, price 1) must not explode a real 10-lot.
    if outcome_qty >= 1 and abs(open_qty - outcome_qty) > max(0.05, 0.02 * outcome_qty):
        return [outcome]
    if len(lots) <= 1:
        covered = sum(_fill_qty(t) for t in closes if _fill_action(t) in close_actions or _fill_action(t) in _EXPIRE_ACTIONS)
        if open_qty <= 1e-9 or covered + 1e-6 >= open_qty:
            return [outcome]

    for trade in closes:
        action = _fill_action(trade)
        qty = _fill_qty(trade)
        amount = _fill_amount(trade)
        if qty <= 1e-9:
            continue
        exact = [lot for lot in lots if abs(lot.remaining - qty) < 1e-6]
        if exact:
            target = min(exact, key=lambda lot: (lot.open_date or "9999", lot.index))
            target.closes.append(trade)
            target.remaining = 0.0
            continue
        left_qty = qty
        left_amt = amount
        for lot in lots:
            if left_qty <= 1e-9:
                break
            if lot.remaining <= 1e-9:
                continue
            take = min(lot.remaining, left_qty)
            frac = take / left_qty if left_qty else 0.0
            piece = dict(trade)
            piece["quantity"] = take
            piece["amount"] = left_amt * frac
            lot.closes.append(piece)
            lot.remaining -= take
            left_amt -= piece["amount"]
            left_qty -= take

    expiry = _outcome_expiry(outcome)
    parent_close = str(outcome.get("close_date") or "")[:10]
    slices = []
    for lot in lots:
        closed_qty = lot.qty - max(lot.remaining, 0.0)
        close_cash = sum(_fill_amount(t) for t in lot.closes)
        if closed_qty > 1e-6:
            frac = closed_qty / lot.qty if lot.qty else 0.0
            open_part = lot.open_cash * frac
            close_type = "Closed"
            if lot.closes and all(_fill_action(t) in _EXPIRE_ACTIONS for t in lot.closes):
                close_type = "Expired"
            close_dates = [_fill_date(t) for t in lot.closes]
            close_dates = [d for d in close_dates if len(d) == 10]
            slices.append(_lot_slice(
                outcome, lot,
                qty=closed_qty,
                open_cash=open_part,
                pnl=open_part + close_cash,
                close_type=close_type,
                close_date=max(close_dates) if close_dates else parent_close,
                raw=list(lot.opens) + list(lot.closes),
                suffix="closed",
            ))
        if lot.remaining > 1e-6:
            frac = lot.remaining / lot.qty if lot.qty else 0.0
            open_part = lot.open_cash * frac
            expired = bool(expiry) and (
                (parent_close and expiry <= parent_close)
                or (lot.open_date and expiry <= lot.open_date)
            )
            if not expired and str(outcome.get("close_type") or "") in ("Expired", "ExpiredOTM"):
                expired = True
            slices.append(_lot_slice(
                outcome, lot,
                qty=lot.remaining,
                open_cash=open_part,
                pnl=open_part,
                close_type="Expired" if expired else str(outcome.get("close_type") or "Open"),
                close_date=expiry if expired else "",
                raw=list(lot.opens),
                suffix="open",
            ))
    if len(slices) <= 1:
        return [outcome]
    return slices


def _lot_slice(parent, lot, *, qty, open_cash, pnl, close_type, close_date, raw, suffix):
    row = dict(parent)
    pnl_r = round(float(pnl), 2)
    direction = str(parent.get("direction") or "")
    row["quantity"] = qty
    row["quantity_display"] = _qty_display(qty)
    row["pnl"] = pnl_r
    row["is_winner"] = True if pnl_r > 0 else False if pnl_r < 0 else None
    row["close_type"] = close_type
    row["close_date"] = close_date or ""
    if lot.open_date:
        row["open_date"] = lot.open_date
    row["raw_trades"] = raw
    row["lot_id"] = f"{lot.price:.4f}:{suffix}"
    basis = abs(open_cash)
    if direction == "Sold":
        row["premium_received"] = round(basis, 2)
        row["premium_paid"] = 0.0
        row["proceeds"] = round(basis, 2)
        row["cost"] = 0.0
    else:
        row["premium_paid"] = round(basis, 2)
        row["premium_received"] = 0.0
        row["cost"] = round(basis, 2)
        row["proceeds"] = 0.0
    # A buy-to-close on expiry day is Closed, not a worthless expiry.
    # leg_outcome reads cost (short) / proceeds (long) for that.
    close_cash = 0.0
    for trade in raw or []:
        action = _fill_action(trade)
        if direction == "Sold" and action in _CLOSE_SHORT:
            close_cash += abs(_fill_amount(trade))
        elif direction == "Bought" and action in _CLOSE_LONG:
            close_cash += abs(_fill_amount(trade))
    if close_cash >= 0.01:
        if direction == "Sold":
            row["cost"] = round(close_cash, 2)
            row["cost_to_close"] = row["cost"]
        else:
            row["proceeds"] = round(close_cash, 2)
            row["proceeds_from_close"] = row["proceeds"]
    row["return_pct"] = round(pnl_r / basis * 100, 1) if basis >= 0.01 else None
    return row


def group_vertical_spreads(rows):
    """One row per vertical. Other legs pass through in the same order.

    A vertical is the same account, root, open date, and expiry, one short
    and one long of a Call Spread or Put Spread. A contract that was opened
    in two lots (different price, or a close between them) is two
    verticals, each with its own status and win. Several verticals on
    the same day pair by contract count, then nearest strike. The long
    is not its own win/loss row.
    """
    indexed = []
    for row in rows or []:
        indexed.extend(split_option_lots(row))
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
        pairs = _pair_nearest(shorts, longs, metas, indexed)
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


def _fill_rows(trades):
    """Normalize a DataFrame or a list of fill dicts."""
    if trades is None:
        return []
    if hasattr(trades, "to_dict") and hasattr(trades, "empty"):
        if trades.empty:
            return []
        return trades.to_dict("records")
    return list(trades)


def _contract_cluster(row) -> tuple:
    source = row
    if row.get("vertical") and row.get("legs"):
        source = row["legs"][0]
    parsed = parse_occ(source.get("trade_symbol"))
    if not parsed:
        return (
            str(row.get("tenant_id") or source.get("tenant_id") or ""),
            str(row.get("account") or source.get("account") or ""),
            "",
            str(row.get("open_date") or source.get("open_date") or "")[:10],
            "",
        )
    return (
        str(row.get("tenant_id") or source.get("tenant_id") or ""),
        str(row.get("account") or source.get("account") or ""),
        _vertical_root(parsed["root"]),
        str(source.get("open_date") or row.get("open_date") or "")[:10],
        _expiry_iso(parsed),
    )


def _contract_cp(row) -> str:
    source = row["legs"][0] if row.get("vertical") and row.get("legs") else row
    parsed = parse_occ(source.get("trade_symbol"))
    return parsed["cp"] if parsed else ""


def _pair_structures(calls, puts):
    """Pair a call structure with a put structure of the same size.

    Two condors opened the same day stay two results when the contract
    counts differ. Leftovers stay their own results.
    """
    unused = set(range(len(puts)))
    pairs = []
    for call in calls:
        if not unused:
            break
        try:
            call_qty = abs(float(call.get("quantity") or 0))
        except (TypeError, ValueError):
            call_qty = 0.0

        def _key(index, _qty=call_qty):
            try:
                put_qty = abs(float(puts[index].get("quantity") or 0))
            except (TypeError, ValueError):
                put_qty = 0.0
            gap = 0 if abs(put_qty - _qty) < 1e-6 else 1
            return (gap, index)

        match = min(unused, key=_key)
        unused.remove(match)
        pairs.append((call, puts[match]))
    leftover = [puts[index] for index in sorted(unused)]
    if len(calls) > len(pairs):
        leftover.extend(calls[len(pairs):])
    return pairs, leftover


def _combine_structures(left, right) -> dict:
    try:
        pnl = round(float(left.get("pnl") or 0) + float(right.get("pnl") or 0), 2)
    except (TypeError, ValueError):
        pnl = 0.0
    try:
        qty = abs(float(left.get("quantity") or right.get("quantity") or 0))
    except (TypeError, ValueError):
        qty = 0.0
    return {"pnl": pnl, "quantity": qty, "vertical": True, "legs": []}


def option_record_from_fills(trades, tenant_ids=None) -> dict:
    """Closed option results counted from raw fills.

    A vertical is one result. A same-day call structure plus put
    structure (iron condor or strangle) is one result. Two lots at the
    same strikes stay two results. A contract that is still open, only
    partly closed, or closed with no opening fill is left out. If any
    leg of a same-day structure is still open, the whole structure is
    left out. Rounded $0 is neither a win nor a loss. Fill ``amount``
    is already net of broker fees.

    ``tenant_ids`` is an allow-list. Pass the real-book ids to keep a
    paper account out of the record.
    """
    allowed = None if tenant_ids is None else {str(tid) for tid in tenant_ids}
    grouped: dict[tuple, list] = {}
    for trade in _fill_rows(trades):
        if not isinstance(trade, dict):
            continue
        action = str(trade.get("action") or "")
        if action not in OPEN_OPTION_ACTIONS and action not in CLOSE_OPTION_ACTIONS:
            continue
        symbol = str(trade.get("trade_symbol") or "").strip()
        if not symbol or parse_occ(symbol) is None:
            continue
        tenant = str(trade.get("tenant_id") or "")
        if allowed is not None and tenant not in allowed:
            continue
        account = str(trade.get("account") or "")
        grouped.setdefault((tenant, account, symbol), []).append(trade)

    built = []
    tainted = set()
    for (tenant, account, symbol), fills in grouped.items():
        opened_qty = 0.0
        closed_qty = 0.0
        sto_qty = 0.0
        bto_qty = 0.0
        pnl = 0.0
        open_date = ""
        saw_open = False
        saw_close = False
        for trade in fills:
            action = str(trade.get("action") or "")
            qty = _fill_qty(trade)
            pnl += _fill_amount(trade)
            if action in OPEN_OPTION_ACTIONS:
                saw_open = True
                opened_qty += qty
                if action == "option_sell_to_open":
                    sto_qty += qty
                else:
                    bto_qty += qty
                opened = _fill_date(trade)
                if opened and (not open_date or opened < open_date):
                    open_date = opened
            elif action in CLOSE_OPTION_ACTIONS:
                saw_close = True
                closed_qty += qty
        parsed = parse_occ(symbol)
        cluster = (
            tenant,
            account,
            _vertical_root(parsed["root"]),
            open_date,
            _expiry_iso(parsed),
        )
        if saw_close and not saw_open:
            continue
        if not saw_open:
            continue
        if abs(opened_qty - closed_qty) > 1e-6:
            tainted.add(cluster)
            continue
        direction = "Sold" if sto_qty >= bto_qty and sto_qty > 0 else "Bought"
        built.append({
            "type": "option",
            "strategy": "Call Spread" if parsed["cp"] == "C" else "Put Spread",
            "direction": direction,
            "trade_symbol": symbol,
            "tenant_id": tenant,
            "account": account,
            "open_date": open_date,
            "quantity": max(sto_qty, bto_qty),
            "pnl": round(pnl, 2),
            "status": "Closed",
            "close_type": "Closed",
            "raw_trades": fills,
        })

    units = []
    by_cluster: dict[tuple, list] = {}
    for row in group_vertical_spreads(built):
        cluster = _contract_cluster(row)
        if cluster in tainted:
            continue
        if str(row.get("close_type") or "") == "Open":
            continue
        by_cluster.setdefault(cluster, []).append(row)

    for cluster, members in by_cluster.items():
        if cluster in tainted:
            continue
        calls = [row for row in members if _contract_cp(row) == "C"]
        puts = [row for row in members if _contract_cp(row) == "P"]
        if calls and puts:
            pairs, leftover = _pair_structures(calls, puts)
            for left, right in pairs:
                units.append(_combine_structures(left, right))
            units.extend(leftover)
        else:
            units.extend(members)

    winners = losers = zeros = 0
    for unit in units:
        try:
            pnl = round(float(unit.get("pnl") or 0), 2)
        except (TypeError, ValueError):
            pnl = 0.0
        if pnl > 0:
            winners += 1
        elif pnl < 0:
            losers += 1
        else:
            zeros += 1
    return {
        "winners": winners,
        "losers": losers,
        "zeros": zeros,
        "decided": winners + losers,
    }
