"""Weekly digest trades, in the same groups the app already shows.

Net return is the realized result of trades that closed in the ISO week.
A vertical spread is one trade: the same lot split and same short/long
pairing as the movers card and the position page (one root, so SPX and
SPXW share a book; each order is its own lot). The dollar is the net
fill cash — premiums in and out, minus broker fees, plus a non-zero
expiry credit. Dividends are a separate line and are not added in.
That sum is the same number as Overview's "Trades this week" realized
total (each closed leg's ``total_pnl``) once the two legs of a spread
are added together instead of ranked on their own.

Paper / Alpaca Paper accounts are left out of a mixed book, the same
way Overview drops them. A paper-only book stays paper.
"""

from __future__ import annotations

from datetime import date, timedelta

from app.paper_accounts import drop_paper_from_mixed_book, is_paper_row


def signed_money(val) -> str:
    """``+$975.56`` / ``-$3,574.44``. Zero is ``$0.00``."""
    try:
        n = round(float(val or 0), 2)
    except (TypeError, ValueError):
        n = 0.0
    if n > 0:
        return f"+${n:,.2f}"
    if n < 0:
        return f"-${abs(n):,.2f}"
    return "$0.00"


def scope_digest_tenants(tenant_ids, broker_rows):
    """Tenant ids for one digest, with paper removed from a mixed book.

    ``broker_rows`` are this user's ``broker_tenants`` rows (account
    name, broker label, nickname). Paper is matched on those labels,
    not on the warehouse broker slug — Alpaca Paper and live Alpaca
    share the slug ``alpaca``.
    """
    rows_by_id = {}
    for row in broker_rows or []:
        tid = str((row or {}).get("tenant_id") or "").strip()
        if tid:
            rows_by_id[tid] = row
    return drop_paper_from_mixed_book(list(tenant_ids or []), rows_by_id)


def _as_date(value):
    if value is None:
        return None
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    text = value.isoformat()[:10] if hasattr(value, "isoformat") else str(value).strip()[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _records(fills):
    if fills is None:
        return []
    if hasattr(fills, "to_dict"):
        if getattr(fills, "empty", False):
            return []
        return fills.to_dict(orient="records")
    return list(fills)


def grouped_option_trades(fills, week_start, week_end):
    """Closed option trades in ``[week_start, week_end]``, one row per spread lot.

    ``fills`` is every fill of those contracts, including an open from
    before the week. Same-day 0DTE lots split the way the movers card
    does: the Oct 2 SPXW 7730/7735 book is a 20-lot that closed and a
    10-lot that expired, not one 30-lot and not two naked legs.
    """
    from app.option_formatting import parse_occ
    from app.outcome_units import group_vertical_spreads
    from app.snaptrade_normalize import _net_option_cash
    from app.weekly_review import (
        _LOT_CLOSE_ACTIONS,
        _LOT_DONE,
        _LOT_OPEN_ACTIONS,
        _LOT_SKIP_ACTIONS,
        _dedupe_lot_fills,
        _fill_action_text,
        _fill_expiry_iso,
        _fill_field,
        _iso_day,
        option_contract_detail,
    )

    start = _as_date(week_start)
    end = _as_date(week_end)
    if start is None or end is None:
        return []
    records = _dedupe_lot_fills(_records(fills))
    groups = {}
    for row in records:
        action = _fill_action_text(row)
        if action in _LOT_SKIP_ACTIONS:
            try:
                settle_amt = float(_fill_field(row, "amount", "Amount", default=0) or 0)
            except (TypeError, ValueError):
                continue
            # A zero expiry line is a placeholder. The credit is the
            # opening fill. A non-zero expiry credit is real cash.
            if abs(settle_amt) < 0.01:
                continue
        elif action not in (_LOT_OPEN_ACTIONS | _LOT_CLOSE_ACTIONS):
            continue
        parsed = parse_occ(_fill_field(row, "trade_symbol", "Symbol", default=""))
        if not parsed:
            continue
        try:
            qty = abs(float(_fill_field(row, "quantity", "Quantity", default=0) or 0))
            price = abs(float(_fill_field(row, "price", "Price", default=0) or 0))
            amount = float(_fill_field(row, "amount", "Amount", default=0) or 0)
            fee = abs(float(_fill_field(row, "fees", "fees_and_comm", "fee", default=0) or 0))
        except (TypeError, ValueError):
            continue
        net = _net_option_cash(amount, qty, price, fee)
        tenant = str(_fill_field(row, "tenant_id", default="") or "")
        account = str(_fill_field(row, "account", "Account", default="") or "")
        trade_symbol = str(_fill_field(row, "trade_symbol", "Symbol", default="") or "")
        iso = _iso_day(_fill_field(row, "trade_date", "Date", "date")) or ""
        groups.setdefault((tenant, account, trade_symbol), []).append({
            "action": action,
            "quantity": qty,
            "price": price,
            "amount": net,
            "trade_date": iso,
            "trade_symbol": trade_symbol,
            "tenant_id": tenant,
            "account": account,
            "_parsed": parsed,
        })

    outcomes = []
    for (tenant, account, trade_symbol), raw in groups.items():
        parsed = raw[0]["_parsed"]
        opens = [t for t in raw if t["action"] in _LOT_OPEN_ACTIONS]
        if not opens:
            continue
        open_dates = [t["trade_date"] for t in opens if t["trade_date"]]
        open_date = min(open_dates) if open_dates else ""
        sto = sum(t["quantity"] for t in opens if "sell" in t["action"])
        bto = sum(t["quantity"] for t in opens if "buy" in t["action"])
        direction = "Sold" if sto >= bto else "Bought"
        cp = parsed.get("cp") or ""
        expiry = f"20{parsed['yy']}-{int(parsed['mm']):02d}-{int(parsed['dd']):02d}"
        closes = [t for t in raw if t["action"] in _LOT_CLOSE_ACTIONS]
        close_dates = [t["trade_date"] for t in closes if t["trade_date"]]
        if closes:
            close_type = "Closed"
            close_date = max(close_dates) if close_dates else expiry
        else:
            close_type = "Expired"
            close_date = expiry
        close_d = _as_date(close_date)
        if close_d is None or not (start <= close_d <= end):
            continue
        # A buy-to-close on the expiry date is a close, not a worthless
        # expiry. The outcome check reads this cash; leaving it at zero
        # labeled the Oct 1 7650/7655 spread "expired".
        close_cash = round(sum(abs(t["amount"]) for t in closes), 2)
        outcomes.append({
            "type": "option",
            "strategy": "Call Spread" if cp == "C" else "Put Spread",
            "trade_symbol": trade_symbol,
            "direction": direction,
            "close_type": close_type,
            "open_date": open_date,
            "close_date": close_date,
            "quantity": sum(t["quantity"] for t in opens),
            "pnl": round(sum(t["amount"] for t in raw), 2),
            "tenant_id": tenant,
            "account": account,
            "option_expiry": expiry,
            "cost": close_cash if direction == "Sold" else 0.0,
            "proceeds": close_cash if direction == "Bought" else 0.0,
            "raw_trades": [
                {k: v for k, v in t.items() if k != "_parsed"} for t in raw
            ],
        })
    if not outcomes:
        return []

    trades = []
    for row in group_vertical_spreads(outcomes):
        status = str(row.get("outcome") or row.get("close_type") or "")
        if status not in _LOT_DONE:
            continue
        close_d = _as_date(row.get("close_date"))
        if close_d is None or not (start <= close_d <= end):
            continue
        try:
            pnl = round(float(row.get("pnl") or 0), 2)
        except (TypeError, ValueError):
            continue
        legs = row.get("legs") or [row]
        detail_rows = []
        symbol = ""
        for leg in legs:
            parsed = parse_occ(leg.get("trade_symbol"))
            if not parsed:
                continue
            symbol = symbol or str(parsed["root"]).upper()
            detail_rows.append({
                "symbol": parsed["root"],
                "tenant_id": leg.get("tenant_id") or row.get("tenant_id"),
                "option_type": "C" if parsed.get("cp") == "C" else "P",
                "option_strike": parsed.get("strike"),
                "option_expiry": (
                    f"20{parsed['yy']}-{int(parsed['mm']):02d}-{int(parsed['dd']):02d}"
                ),
                "quantity": leg.get("quantity"),
                "direction": leg.get("direction"),
                "trade_symbol": leg.get("trade_symbol"),
            })
        if not symbol or not detail_rows:
            continue
        detail = option_contract_detail(detail_rows)
        if not detail:
            continue
        expired = status == "Expired"
        base = f"{symbol} {detail}"
        if expired:
            base += " (expired)"
        trades.append({
            "symbol": symbol,
            "detail": detail,
            "label": f"{base} {signed_money(pnl)}",
            "pnl": pnl,
            "expired": expired,
            "kind": "option",
            "close_date": close_d.isoformat(),
            "tenant_id": str(row.get("tenant_id") or ""),
        })
    trades.sort(key=lambda t: (t["close_date"], t["label"]))
    return trades


def equity_closed_trades(rows):
    """One trade per equity session that closed in the week.

    These are already one position, not option legs. The dollar is the
    session's realized P&L from strategy classification — the same
    figure Overview adds into "Trades this week".
    """
    out = []
    for row in _records(rows):
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        try:
            pnl = round(float(row.get("total_pnl") or row.get("pnl") or 0), 2)
        except (TypeError, ValueError):
            continue
        out.append({
            "symbol": symbol,
            "detail": symbol,
            "label": f"{symbol} {signed_money(pnl)}",
            "pnl": pnl,
            "expired": False,
            "kind": "equity",
            "close_date": str(row.get("close_date") or "")[:10],
            "tenant_id": str(row.get("tenant_id") or ""),
        })
    return out


def summarize_closed_trades(trades, *, dividends=0.0, week_start=None):
    """Headline numbers for the weekly email.

    ``net_return`` / ``total_return`` is the sum of grouped closed-trade
    realized P&L only. Dividends are returned on their own and are not
    inside that sum. Wins and losses count a spread once. A scratch
    (exactly $0) is a closed trade and is neither a win nor a loss.
    Best and worst are the grouped trades, labeled the way the app
    labels a mover (``SPXW 10× 7730/7735C spread +$975.56``).
    """
    rows = list(trades or [])
    net = round(sum(float(t["pnl"]) for t in rows), 2)
    winners = sum(1 for t in rows if float(t["pnl"]) > 0)
    losers = sum(1 for t in rows if float(t["pnl"]) < 0)
    try:
        div = round(float(dividends or 0), 2)
    except (TypeError, ValueError):
        div = 0.0

    best = max(rows, key=lambda t: float(t["pnl"])) if rows else None
    worst = min(rows, key=lambda t: float(t["pnl"])) if rows else None
    if best is not None and worst is not None and best is worst:
        if float(best["pnl"]) >= 0:
            worst = None
        else:
            best = None
    elif (
        best is not None and worst is not None
        and abs(float(best["pnl"]) - float(worst["pnl"])) < 0.005
        and len(rows) > 1
    ):
        # Every trade tied. Still name one best; don't repeat it as worst.
        worst = None

    start = _as_date(week_start)
    week_label = f"week of {start:%b %d}" if start else "this past week"
    return {
        "week_start": start.isoformat() if start else "",
        "week_label": week_label,
        # Realized closed trades only. Dividends stay in ``dividends``.
        "total_return": net,
        "total_pnl": net,
        "net_return": net,
        "dividends": div,
        "trades_closed": len(rows),
        "num_winners": winners,
        "num_losers": losers,
        "best_symbol": (best or {}).get("symbol"),
        "best_pnl": None if best is None else float(best["pnl"]),
        "best_label": None if best is None else best["label"],
        "worst_symbol": (worst or {}).get("symbol"),
        "worst_pnl": None if worst is None else float(worst["pnl"]),
        "worst_label": None if worst is None else worst["label"],
        "trades": rows,
    }


def week_end(week_start):
    start = _as_date(week_start)
    if start is None:
        return None
    return start + timedelta(days=6)


def is_paper_tenant(row) -> bool:
    return is_paper_row(row)
