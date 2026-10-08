"""Replay the demo seed the way the account Cumulative P&L chart does.

The Accounts chart (``_build_account_chart_from_daily_pnl``) walks
average-cost equity and adds option P&L as realize-on-close: a seed
contract that was never snapshotted contributes $0 until it closes,
then the full signed cash on the close date. Open seed contracts add
the current-file unrealized only on the last chart session.

``tests/fixtures/demo_mirror_chart_residual.json`` freezes

    live chart total − this seed's equity walk − this seed's options

from the public demo chart *before* the summer income spreads were
added. Share lots are unchanged, so the equity walk cancels and

    residual + replayed equity + replayed options

is the combined mirror + seed series. New option credits move only
the option term.
"""

from __future__ import annotations

import csv
from pathlib import Path

OPTION_ORDERS = {
    "sell to open",
    "buy to open",
    "sell to close",
    "buy to close",
}
FLATTEN = {"expired", "assigned", "exchange or exercise"}


def num(raw: str) -> float:
    text = (raw or "").strip().replace(",", "").replace("$", "")
    if not text:
        return 0.0
    return float(text)


def signed_amount(row: dict) -> float:
    """Match ``amount_signed`` in ``stg_history`` for seed rows."""
    action = row["Action"].strip().lower()
    amount = num(row["Amount"])
    qty = num(row["Quantity"])
    price = num(row["Price"])
    fees = num(row["fees_and_comm"])
    if (
        action in OPTION_ORDERS
        and abs(fees) > 0.005
        and qty
        and price
        and abs(abs(amount) - abs(qty) * price * 100) <= 0.05
    ):
        gross = abs(qty) * price * 100
        if action in ("sell to open", "sell to close"):
            return gross - abs(fees)
        return -(gross + abs(fees))
    if action in ("buy", "buy to open", "buy to close"):
        return -abs(amount)
    if action in ("sell", "sell to open", "sell to close"):
        return abs(amount)
    return amount


def _pos_delta(action: str, qty: float) -> float | None:
    if action == "sell to open":
        return -qty
    if action == "buy to open":
        return qty
    if action == "sell to close":
        return -qty
    if action == "buy to close":
        return qty
    if action in FLATTEN:
        return None
    return 0.0


def option_cash_by_close_date(rows: list[dict]) -> dict[str, float]:
    """Full signed cash of each contract, booked on the date it goes flat."""
    contracts: dict[str, dict] = {}
    for row in rows:
        symbol = (row["Symbol"] or "").strip()
        action = row["Action"].strip().lower()
        if " " not in symbol:
            continue
        if action not in OPTION_ORDERS and action not in FLATTEN:
            continue
        slot = contracts.setdefault(
            symbol, {"pos": 0.0, "cash": 0.0, "close": None, "opened": False}
        )
        slot["cash"] += signed_amount(row)
        if action in ("sell to open", "buy to open"):
            slot["opened"] = True
        delta = _pos_delta(action, num(row["Quantity"]))
        if delta is None:
            slot["pos"] = 0.0
            slot["close"] = row["Date"]
        else:
            slot["pos"] += delta
            if slot["opened"] and abs(slot["pos"]) < 1e-9:
                slot["close"] = row["Date"]
    by_day: dict[str, float] = {}
    for slot in contracts.values():
        if slot["close"]:
            by_day[slot["close"]] = by_day.get(slot["close"], 0.0) + slot["cash"]
    return by_day


def open_option_mtm(current_rows: list[dict]) -> float:
    """Snapshot unrealized on option rows. Equity rows are not included."""
    total = 0.0
    for row in current_rows:
        if " " not in (row.get("Symbol") or ""):
            continue
        total += num(row.get("gain_or_loss_dollat") or "")
    return total


def equity_pnl_on(rows: list[dict], closes: dict[str, dict[str, float]], day: str) -> float:
    """Average-cost equity marked at the last stored close on or before ``day``."""
    shares: dict[str, float] = {}
    cost: dict[str, float] = {}
    realized = 0.0
    for row in rows:
        if row["Date"] > day:
            continue
        action = row["Action"].strip()
        symbol = (row["Symbol"] or "").strip()
        if action not in ("Buy", "Sell") or not symbol or " " in symbol:
            continue
        qty = num(row["Quantity"])
        amount = abs(num(row["Amount"]))
        held = shares.get(symbol, 0.0)
        basis = cost.get(symbol, 0.0)
        if action == "Sell" and qty > 0 and held > 0:
            sold = min(qty, held)
            avg = basis / held
            realized += amount * (sold / qty) - avg * sold
            basis = max(0.0, basis - avg * sold)
            held = max(0.0, held - sold)
            shares[symbol] = held
            cost[symbol] = basis
        elif action == "Buy" and qty > 0:
            shares[symbol] = held + qty
            cost[symbol] = basis + amount
    total = realized
    for symbol, qty in shares.items():
        if qty <= 0:
            continue
        series = closes.get(symbol) or {}
        prior = [d for d in series if d <= day]
        if not prior:
            continue
        px = series[max(prior)]
        total += qty * px - cost.get(symbol, 0.0)
    return total


def option_pnl_on(
    by_close: dict[str, float],
    day: str,
    *,
    open_mtm: float,
    open_mtm_date: str,
) -> float:
    total = 0.0
    for close_day, cash in by_close.items():
        if close_day <= day:
            total += cash
    if day == open_mtm_date:
        total += open_mtm
    return total


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def replay_totals(
    history: list[dict],
    dates: list[str],
    closes: dict[str, dict[str, float]],
    *,
    open_mtm: float,
    open_mtm_date: str,
) -> list[float]:
    """Seed-only equity + options on each chart date."""
    by_close = option_cash_by_close_date(history)
    out = []
    for day in dates:
        out.append(
            equity_pnl_on(history, closes, day)
            + option_pnl_on(
                by_close, day, open_mtm=open_mtm, open_mtm_date=open_mtm_date
            )
        )
    return out
