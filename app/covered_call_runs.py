"""Covered-call and wheel runs for the position page.

A run is one share lot plus the calls written against it. It starts when
shares are bought (or a short put is assigned into shares) and ends when
those shares are sold or called away. If the shares are still held, the
run stays open through today.

Each short call written while the shares were held is one row inside
an outcome group (expired, closed, assigned, rolled, or still open).
The row's net is the option result with broker fees left out — a roll
or a buy-to-close is a net, not the opening credit. Groups sum those
nets and sort largest first. The run's calls net is the sum of every
group. The share result is separate. The whole-run number is the calls
net plus the share result.

Puts are included only when they are assigned and the shares actually
show up — that is the wheel entry, not every put on the symbol. Naked
calls written while flat stay off the run. Two accounts never share a
run; the grain is the tenant.
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from app.position_story import parse_occ

__all__ = ["build_covered_call_runs"]

# Below this, a leftover float quantity is flat. Tighter than a real
# fractional fill (broker minimum is about 0.0001) and looser than the
# 1e-17 dust a round trip can leave behind.
_FLAT = 1e-4
_MULT = 100

_MONTHS = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)

_OUTCOME_LABEL = {
    "expired": "Expired",
    "closed": "Closed",
    "assigned": "Assigned",
    "rolled": "Rolled",
    "open": "Open",
}


def _count_label(n):
    return "1 call" if n == 1 else f"{n} calls"


def _outcome_groups(rows):
    """Bundle call rows that share an outcome.

    Rows arrive in date order. Each group's calls stay in that order.
    Groups themselves sort by net, largest first, so the outcome that
    made the most money is the first row. Equal nets break ties by the
    outcome label.
    """
    buckets = []
    index = {}
    for row in rows:
        key = row["outcome"]
        if key not in index:
            index[key] = len(buckets)
            buckets.append({
                "outcome": key,
                "outcome_label": row["outcome_label"],
                "calls": [],
                "net": 0.0,
            })
        group = buckets[index[key]]
        group["calls"].append(row)
        group["net"] = round(group["net"] + row["premium"], 2)
    for group in buckets:
        group["count"] = len(group["calls"])
        group["count_label"] = _count_label(group["count"])
    buckets.sort(key=lambda g: (-g["net"], g["outcome_label"]))
    return buckets


def build_covered_call_runs(
    trades_df,
    current_df=None,
    splits_df=None,
    opening_df=None,
    as_of=None,
    label_map=None,
):
    """Group share lots and the calls written against them.

    ``trades_df`` is the position page's fill frame (already limited to
    one symbol and to the viewer's tenants). ``opening_df`` supplies
    inferred pre-history shares from ``int_opening_balances``; its
    today-unit quantity is converted back to opening-date units before
    the split-aware state machine consumes it. ``current_df`` supplies
    the live share price for a run that is still open. ``splits_df`` is
    the public split calendar so a later sell in post-split units still
    closes the same lot. ``as_of`` decides whether an untouched contract
    has expired. ``label_map`` is tenant_id → display name.

    Returns a list of plain dicts, oldest run first. Empty when this
    symbol has no covered-call or wheel cycle.
    """
    fills = _normalize_fills(trades_df)
    splits = _normalize_splits(splits_df)
    fills.extend(_normalize_openings(opening_df, splits))
    fills.sort(key=lambda f: (f["date"], f["trade_symbol"], f["kind"]))
    if not fills:
        return []
    if as_of is None:
        as_of = date.today()
    elif isinstance(as_of, datetime):
        as_of = as_of.date()

    marks = _equity_marks(current_df)
    labels = label_map or {}

    by_tenant = {}
    for fill in fills:
        by_tenant.setdefault(fill["tenant_key"], []).append(fill)

    runs = []
    for tenant_key, tenant_fills in by_tenant.items():
        tenant_id = tenant_fills[0]["tenant_id"]
        account = tenant_fills[0]["account"]
        label = ""
        if tenant_id and tenant_id in labels:
            label = str(labels[tenant_id] or "")
        if not label:
            label = account
        if marks is None:
            mark, broker_qty = None, None
        else:
            info = marks.get(tenant_key)
            if info is None:
                mark, broker_qty = None, 0.0
            else:
                mark, broker_qty = info["price"], info["qty"]
        runs.extend(
            _build_tenant(
                tenant_fills, splits, mark, as_of, label, tenant_id,
                broker_qty=broker_qty,
            )
        )

    runs.sort(key=lambda r: (r["start"] or date.min, r.get("tenant_id") or ""))
    many = len(runs) > 1
    tenants = {r.get("tenant_id") or r.get("account_label") for r in runs}
    show_account = len(tenants) > 1
    for i, run in enumerate(runs, start=1):
        kind = "Wheel" if run["kind"] == "wheel" else "Covered call"
        run["label"] = f"Run {i} · {kind}" if many else kind
        run["show_account"] = show_account
        run["start"] = _iso(run["start"])
        run["end"] = _iso(run["end"])
    return runs


# ── Fill normalization ───────────────────────────────────────────────────


def _cell(row, name):
    if isinstance(row, dict):
        value = row.get(name)
    else:
        try:
            value = row[name]
        except Exception:
            return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("nan", "none", "nat"):
        return ""
    return text


def _num(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return number


def _as_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        parsed = pd.to_datetime(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    try:
        return parsed.date()
    except (AttributeError, ValueError):
        return None


def _tenant_key(tenant_id, account):
    if tenant_id:
        return tenant_id
    return "account:" + account


def _option_cash(fill):
    """Premium in dollars, fees left out.

    The option price times contracts times 100 is the exchange premium.
    Fill ``amount`` is the cash the account actually saw, which already
    has the broker fee netted in, so it is only a fallback — and when we
    use it we add the fee back.
    """
    price = _num(fill.get("price"))
    qty = abs(_num(fill.get("qty")) or 0.0)
    if price is not None and qty > 0 and abs(price) > 0:
        return round(abs(price) * qty * _MULT, 2)
    amount = abs(_num(fill.get("amount")) or 0.0)
    fees = abs(_num(fill.get("fees")) or 0.0)
    if amount > 0:
        return round(amount + fees, 2)
    return 0.0


def _share_price(fill):
    price = _num(fill.get("price"))
    if price is not None and abs(price) > 0:
        return abs(price)
    qty = abs(_num(fill.get("qty")) or 0.0)
    amount = abs(_num(fill.get("amount")) or 0.0)
    if qty > 0 and amount > 0:
        return amount / qty
    return 0.0


def _classify(action, instrument):
    if action in ("equity_buy", "buy", "dividend_reinvest"):
        return "buy"
    if action in ("equity_sell", "sell"):
        return "sell"
    if action == "option_sell_to_open":
        return "sto"
    if action == "option_buy_to_open":
        return "bto"
    if action in ("option_buy_to_close", "option_sell_to_close"):
        return "btc"
    if action in ("option_assigned", "option_exercised"):
        return "assign"
    if action == "option_expired":
        return "expire"
    if instrument in ("call", "put"):
        return None
    return None


def _normalize_fills(trades_df):
    if trades_df is None:
        return []
    try:
        empty = trades_df.empty
    except AttributeError:
        empty = False
    if empty:
        return []
    rows = trades_df.to_dict(orient="records")
    out = []
    for row in rows:
        action = _text(_cell(row, "action")).lower()
        instrument = _text(_cell(row, "instrument_type")).lower()
        kind = _classify(action, instrument)
        if kind is None:
            continue
        when = _as_date(_cell(row, "trade_date"))
        if when is None:
            continue
        qty = abs(_num(_cell(row, "quantity")) or 0.0)
        if qty <= 0 and kind not in ("expire",):
            continue
        occ = parse_occ(_cell(row, "trade_symbol"))
        option_type = None
        strike = None
        expiry = None
        if occ:
            option_type = occ["option_type"]
            strike = occ["strike"]
            expiry = occ["expiry"]
        elif instrument in ("call", "put"):
            option_type = instrument
        if kind in ("sto", "btc", "assign", "expire") and option_type not in ("call", "put"):
            continue
        tenant_id = _text(_cell(row, "tenant_id"))
        account = _text(_cell(row, "account"))
        out.append({
            "date": when,
            "kind": kind,
            "action": action,
            "qty": qty,
            "price": _num(_cell(row, "price")),
            "amount": _num(_cell(row, "amount")),
            "fees": _num(_cell(row, "fees")),
            "trade_symbol": _text(_cell(row, "trade_symbol")),
            "option_type": option_type,
            "strike": strike,
            "expiry": expiry,
            "tenant_id": tenant_id,
            "account": account,
            "tenant_key": _tenant_key(tenant_id, account),
        })
    out.sort(key=lambda f: (f["date"], f["trade_symbol"], f["kind"]))
    return out


def _normalize_splits(splits_df):
    if splits_df is None:
        return []
    try:
        empty = splits_df.empty
    except AttributeError:
        empty = False
    if empty:
        return []
    out = []
    for row in splits_df.to_dict(orient="records"):
        when = _as_date(_cell(row, "split_date"))
        ratio = _num(_cell(row, "split_ratio"))
        if when and ratio and ratio > 0 and abs(ratio - 1.0) > 1e-9:
            out.append((when, ratio))
    out.sort()
    return out


def _normalize_openings(opening_df, splits):
    """Synthetic buys for positions whose acquisition predates history.

    ``opening_qty`` is already in today's units. The run simulator applies
    split events chronologically, so convert it back to opening-date units
    first. Cash is split-invariant and comes from the warehouse estimate.
    Unpriced openings stay omitted rather than inventing a zero-cost lot.
    """
    if opening_df is None:
        return []
    try:
        empty = opening_df.empty
    except AttributeError:
        empty = False
    if empty:
        return []

    out = []
    for row in opening_df.to_dict(orient="records"):
        when = _as_date(_cell(row, "opening_date"))
        qty_today = _num(_cell(row, "opening_qty"))
        amount = _num(_cell(row, "est_amount"))
        if when is None or qty_today is None or qty_today <= _FLAT:
            continue
        if amount is None or abs(amount) <= _FLAT:
            continue
        factor = 1.0
        for split_date, ratio in splits:
            if split_date > when:
                factor *= ratio
        if factor <= 0:
            continue
        qty = qty_today / factor
        if qty <= _FLAT:
            continue
        tenant_id = _text(_cell(row, "tenant_id"))
        account = _text(_cell(row, "account"))
        out.append({
            "date": when,
            "kind": "buy",
            "action": "synthetic_opening_balance",
            "qty": qty,
            "price": abs(amount) / qty,
            "amount": amount,
            "fees": 0.0,
            "trade_symbol": _text(_cell(row, "symbol")),
            "option_type": None,
            "strike": None,
            "expiry": None,
            "tenant_id": tenant_id,
            "account": account,
            "tenant_key": _tenant_key(tenant_id, account),
        })
    return out


def _equity_marks(current_df):
    """tenant_key → {qty, price}, or None when no snapshot was passed.

    Qty 0 is meaningful: the broker does not hold the shares. A missing
    key in a provided frame means the same thing.
    """
    if current_df is None:
        return None
    try:
        empty = current_df.empty
    except AttributeError:
        empty = False
    if empty:
        return {}
    best = {}
    for row in current_df.to_dict(orient="records"):
        instrument = _text(_cell(row, "instrument_type")).lower()
        if instrument not in ("equity", "stock", ""):
            continue
        if instrument == "" and _text(_cell(row, "option_type")):
            continue
        qty = _num(_cell(row, "quantity")) or 0.0
        price = _num(_cell(row, "current_price"))
        tenant_id = _text(_cell(row, "tenant_id"))
        account = _text(_cell(row, "account"))
        key = _tenant_key(tenant_id, account)
        held = abs(qty)
        prev = best.get(key)
        if prev is None or held > prev["qty"]:
            best[key] = {
                "qty": held,
                "price": price if price and price > 0 else None,
            }
    return best


# ── Per-tenant simulation ────────────────────────────────────────────────


class _Contract:
    def __init__(self, trade_symbol, option_type, strike, expiry):
        self.trade_symbol = trade_symbol
        self.option_type = option_type
        self.strike = strike
        self.expiry = expiry
        self.sto_qty = 0.0
        self.bto_qty = 0.0
        self.btc_qty = 0.0
        self.assigned_qty = 0.0
        self.expired_qty = 0.0
        self.credit = 0.0
        self.debit = 0.0
        self.open_date = None
        self.close_date = None
        self.shares_at_open = None
        self.partial = False
        self.rolled = False
        self.attached = False

    @property
    def short(self):
        return self.sto_qty > self.bto_qty + _FLAT or (
            self.sto_qty <= _FLAT and self.assigned_qty > _FLAT and self.bto_qty <= _FLAT
        )


class _Run:
    def __init__(self, start, tenant_id, account_label):
        self.start = start
        self.end = None
        self.status = "open"
        self.kind = "covered_call"
        self.tenant_id = tenant_id
        self.account_label = account_label
        self.buys = []
        self.sells = []
        self.splits = []
        self.shares = 0.0
        self.cost = 0.0
        self.realized = 0.0
        self.fees = 0.0
        self.contracts = []
        self.entry_put = False
        self.exit_reason = None
        self.missing_close = False


def _contract_for(book, fill):
    key = fill["trade_symbol"] or f"{fill['option_type']}:{fill['strike']}:{fill['expiry']}"
    found = book.get(key)
    if found is None:
        found = _Contract(key, fill["option_type"], fill["strike"], fill["expiry"])
        book[key] = found
    elif fill["expiry"] and not found.expiry:
        found.expiry = fill["expiry"]
        found.strike = fill["strike"]
        found.option_type = fill["option_type"]
    return found


def _attach(run, contract, shares):
    if contract.option_type == "call" and not contract.short and contract.assigned_qty <= _FLAT:
        return
    if not contract.attached:
        contract.attached = True
        run.contracts.append(contract)
    if contract.shares_at_open is None:
        contract.shares_at_open = shares
    else:
        contract.shares_at_open = min(contract.shares_at_open, shares)
    covered = contract.sto_qty * _MULT
    if covered > _FLAT and shares + 0.01 < covered:
        contract.partial = True


def _put_takes_delivery(contract, fill, buys):
    """A short put assignment adds shares. Exercising a long put does not."""
    if contract.bto_qty > contract.sto_qty + _FLAT:
        return False
    if contract.sto_qty > _FLAT or fill["action"] == "option_assigned":
        return True
    return _matched_share_fill(buys, contract.strike, fill["qty"] * _MULT)


def _call_calls_shares_away(contract, fill, sells):
    """A short call assignment sells the shares. Exercising a long call does not."""
    if contract.bto_qty > contract.sto_qty + _FLAT:
        return False
    if contract.sto_qty > _FLAT or fill["action"] == "option_assigned":
        return True
    return _matched_share_fill(sells, contract.strike, fill["qty"] * _MULT)


def _near(a, b, tol=0.02):
    if a is None or b is None:
        return False
    return abs(a - b) <= tol


def _matched_share_fill(fills, strike, share_qty):
    if strike is None:
        return False
    for fill in fills:
        if _near(abs(fill["qty"]), share_qty, tol=0.5) and _near(_share_price(fill), strike):
            return True
    return False


def _note_fee(run, fill):
    if run is None:
        return
    run.fees += abs(_num(fill.get("fees")) or 0.0)


def _build_tenant(fills, splits, mark, as_of, account_label, tenant_id,
                  broker_qty=None):
    fills_by_day = {}
    for fill in fills:
        fills_by_day.setdefault(fill["date"], []).append(fill)
    splits_by_day = {}
    for when, ratio in splits:
        splits_by_day.setdefault(when, []).append(ratio)

    runs = []
    active = None
    book = {}

    def add_shares(day, qty, price):
        nonlocal active
        qty = abs(qty)
        if qty <= _FLAT:
            return
        if active is None:
            active = _Run(day, tenant_id, account_label)
            runs.append(active)
        active.buys.append((qty, abs(price), day))
        active.shares += qty
        active.cost += qty * abs(price)

    def remove_shares(day, qty, price, reason):
        nonlocal active
        if active is None or active.shares <= _FLAT:
            return
        qty = min(abs(qty), active.shares)
        if qty <= _FLAT:
            return
        price = abs(price)
        avg = active.cost / active.shares if active.shares > _FLAT else 0.0
        active.realized += (price - avg) * qty
        active.cost -= avg * qty
        active.shares -= qty
        active.sells.append((qty, price, reason, day))
        if active.shares <= _FLAT:
            active.shares = 0.0
            active.cost = 0.0
            active.end = day
            active.status = "closed"
            active.exit_reason = reason
            active = None

    for day in sorted(set(fills_by_day) | set(splits_by_day)):
        shares_before = 0.0 if active is None else active.shares
        if active is not None and active.shares > _FLAT:
            for ratio in splits_by_day.get(day, []):
                active.shares *= ratio
                active.splits.append((day, ratio))

        day_fills = fills_by_day.get(day, [])
        buys = [f for f in day_fills if f["kind"] == "buy"]
        sells = [f for f in day_fills if f["kind"] == "sell"]
        btos = [f for f in day_fills if f["kind"] == "bto"]
        stos = [f for f in day_fills if f["kind"] == "sto"]
        btcs = [f for f in day_fills if f["kind"] == "btc"]
        assigns = [f for f in day_fills if f["kind"] == "assign"]
        expires = [f for f in day_fills if f["kind"] == "expire"]

        for fill in buys:
            add_shares(day, fill["qty"], _share_price(fill))
            _note_fee(active, fill)

        for fill in btos:
            contract = _contract_for(book, fill)
            contract.bto_qty += fill["qty"]
            if contract.open_date is None:
                contract.open_date = day

        # Puts first so a same-day assignment is already a known short,
        # and the shares exist before any call written that day.
        for fill in stos:
            if fill["option_type"] != "put":
                continue
            contract = _contract_for(book, fill)
            contract.open_date = day if contract.open_date is None else min(contract.open_date, day)
            contract.sto_qty += fill["qty"]
            contract.credit += _option_cash(fill)
            _note_fee(active, fill)

        for fill in assigns:
            if fill["option_type"] != "put":
                continue
            contract = _contract_for(book, fill)
            if not _put_takes_delivery(contract, fill, buys):
                continue
            contract.assigned_qty += fill["qty"]
            contract.close_date = day
            if contract.open_date is None:
                contract.open_date = day
            share_qty = fill["qty"] * _MULT
            if not _matched_share_fill(buys, contract.strike, share_qty):
                add_shares(day, share_qty, contract.strike or _share_price(fill))
            if active is not None:
                _attach(active, contract, active.shares)
                if shares_before <= _FLAT:
                    active.kind = "wheel"
                    active.entry_put = True

        sto_calls = []
        for fill in stos:
            if fill["option_type"] != "call":
                continue
            contract = _contract_for(book, fill)
            contract.open_date = day if contract.open_date is None else min(contract.open_date, day)
            contract.sto_qty += fill["qty"]
            contract.credit += _option_cash(fill)
            _note_fee(active, fill)
            if active is not None and active.shares > _FLAT and contract.short:
                _attach(active, contract, active.shares)
            sto_calls.append(contract)

        btc_today = []
        for fill in btcs:
            contract = _contract_for(book, fill)
            contract.btc_qty += fill["qty"]
            contract.debit += _option_cash(fill)
            contract.close_date = day
            _note_fee(active, fill)
            btc_today.append(contract)

        for fill in expires:
            contract = _contract_for(book, fill)
            contract.expired_qty += fill["qty"] or contract.sto_qty
            contract.close_date = day

        if sto_calls:
            for contract in btc_today:
                if (
                    contract.option_type == "call"
                    and contract not in sto_calls
                    and contract.assigned_qty <= _FLAT
                    and contract.btc_qty + _FLAT >= contract.sto_qty
                    and contract.sto_qty > _FLAT
                ):
                    contract.rolled = True

        call_assigns = []
        for fill in assigns:
            if fill["option_type"] != "call":
                continue
            contract = _contract_for(book, fill)
            if not _call_calls_shares_away(contract, fill, sells):
                continue
            contract.assigned_qty += fill["qty"]
            contract.close_date = day
            if contract.open_date is None:
                contract.open_date = day
            if active is not None:
                _attach(active, contract, active.shares)
            call_assigns.append((contract, fill["qty"] * _MULT))

        for fill in sells:
            _note_fee(active, fill)
            reason = "sold"
            for contract, share_qty in call_assigns:
                if _near(fill["qty"], share_qty, tol=0.5) and _near(
                    _share_price(fill), contract.strike
                ):
                    reason = "assigned"
                    break
            remove_shares(day, fill["qty"], _share_price(fill), reason)

        for contract, share_qty in call_assigns:
            if active is None:
                continue
            if _matched_share_fill(sells, contract.strike, share_qty):
                continue
            remove_shares(day, share_qty, contract.strike or 0.0, "assigned")

    if broker_qty is not None and broker_qty <= _FLAT:
        for run in runs:
            if run.status == "open" and run.shares > _FLAT:
                run.missing_close = True
                run.status = "closed"
                run.exit_reason = "missing"

    out = []
    for run in runs:
        viewed = _present(run, mark, as_of)
        if viewed is not None:
            out.append(viewed)
    return out


def _outcome(contract, as_of):
    opened = contract.sto_qty if contract.sto_qty > _FLAT else contract.assigned_qty
    remaining = opened - contract.btc_qty - contract.assigned_qty
    if contract.assigned_qty > _FLAT and remaining <= _FLAT:
        return "assigned"
    if (
        contract.btc_qty > _FLAT
        and remaining <= _FLAT
        and contract.assigned_qty <= _FLAT
    ):
        return "rolled" if contract.rolled else "closed"
    expired = contract.expired_qty > _FLAT or (
        contract.expiry is not None and as_of is not None and contract.expiry < as_of
    )
    if expired and contract.assigned_qty <= _FLAT:
        return "expired"
    return "open"


def _present(run, mark, as_of):
    contracts = []
    for contract in run.contracts:
        if contract.option_type == "put":
            if contract.assigned_qty <= _FLAT:
                continue
        elif not contract.short:
            continue
        contracts.append(contract)
    if not contracts:
        return None

    contracts.sort(key=lambda c: (
        c.open_date or c.expiry or date.min,
        0 if c.option_type == "put" else 1,
        c.expiry or date.min,
        c.strike or 0.0,
    ))

    rows = []
    running = 0.0
    has_open_call = False
    for contract in contracts:
        outcome = _outcome(contract, as_of)
        if outcome == "open":
            has_open_call = True
        premium = round(contract.credit - contract.debit, 2)
        running = round(running + premium, 2)
        qty = contract.sto_qty if contract.sto_qty > _FLAT else contract.assigned_qty
        rows.append({
            "side": contract.option_type,
            "strike": contract.strike,
            "expiry": _iso(contract.expiry),
            "expiry_label": _fmt_date(contract.expiry),
            "title": _contract_title(contract, qty),
            "premium": premium,
            "running_premium": running,
            "outcome": outcome,
            "outcome_label": _OUTCOME_LABEL[outcome],
            "contracts": qty,
            "partial": bool(contract.partial) and contract.option_type == "call",
            "partial_note": _partial_note(contract) if contract.partial else "",
        })

    share_pnl, share_note = _share_pnl(run, mark)
    _buys, _sells, prices_adjusted = _display_share_lots(run)
    premium_total = rows[-1]["running_premium"] if rows else 0.0
    groups = _outcome_groups(rows)
    status = run.status
    return {
        "tenant_id": run.tenant_id,
        "account_label": run.account_label,
        "kind": run.kind,
        "status": status,
        "status_label": "Open" if status == "open" else "Closed",
        "start": run.start,
        "end": run.end,
        "when": _when(run),
        "share_sentence": _share_sentence(run, buys=_buys, sells=_sells),
        "share_note": share_note,
        "price_note": "Prices are adjusted for the stock split." if prices_adjusted else "",
        "share_pnl": share_pnl,
        "premium_total": premium_total,
        "premium_label": "Calls net",
        "call_count": len(rows),
        "call_count_label": _count_label(len(rows)),
        "outcome_groups": groups,
        "fees": round(run.fees, 2),
        "missing_close": bool(run.missing_close),
        "net": round(premium_total + share_pnl - run.fees, 2),
        "net_label": (
            ("Whole run so far, after fees" if status == "open" else "Whole run, after fees")
            if abs(run.fees) >= 0.005
            else ("Whole run so far" if status == "open" else "Whole run")
        ),
        "has_open_call": has_open_call,
        "calls": rows,
    }


def _share_pnl(run, mark):
    pnl = run.realized
    note = ""
    if run.status == "open" and run.shares > _FLAT:
        if mark is not None and run.shares > _FLAT:
            avg = run.cost / run.shares
            pnl += (mark - avg) * run.shares
            note = "Includes the live price on shares still held."
        else:
            note = "Live share price isn't in yet, so this is only shares already sold."
    return round(pnl, 2), note


def _factor_after(splits, fill_date):
    """Product of splits that happen after this fill.

    A split is applied to the running share count before that day's fills,
    so a fill on the split date is already in post-split units.
    """
    factor = 1.0
    for split_date, ratio in splits or []:
        if fill_date is not None and split_date > fill_date and ratio:
            factor *= float(ratio)
    return factor or 1.0


def _display_share_lots(run):
    """Share counts and prices in the same split-adjusted units.

    P&L stays on the running lot (cost is split-invariant). The sentence
    was mixing a pre-split buy price with a post-split sale price.
    """
    splits = getattr(run, "splits", None) or []
    buys = []
    sells = []
    adjusted = False
    for qty, price, day in run.buys:
        factor = _factor_after(splits, day)
        if abs(factor - 1.0) > 1e-9:
            adjusted = True
        buys.append((qty * factor, price / factor))
    for qty, price, reason, day in run.sells:
        factor = _factor_after(splits, day)
        if abs(factor - 1.0) > 1e-9:
            adjusted = True
        sells.append((qty * factor, price / factor, reason))
    return buys, sells, adjusted


def _share_sentence(run, buys=None, sells=None):
    if buys is None or sells is None:
        buys, sells, _adjusted = _display_share_lots(run)
    buy_qty = sum(qty for qty, _price in buys)
    buy_cost = sum(qty * price for qty, price in buys)
    prices = {round(price, 2) for _qty, price in buys}
    bits = []
    if run.entry_put:
        puts = [c for c in run.contracts if c.option_type == "put" and c.assigned_qty > _FLAT]
        strike = puts[0].strike if puts and puts[0].strike is not None else (
            buy_cost / buy_qty if buy_qty else 0.0
        )
        from_put = buy_qty > _FLAT and all(_near(price, strike) for _qty, price in buys)
        if from_put:
            bits.append(f"{_shares(buy_qty)} from the {_px(strike)} put")
        elif buy_qty > _FLAT:
            entry = buy_cost / buy_qty
            if len(prices) > 1:
                bits.append(f"{_shares(buy_qty)} bought at an average of {_px(entry)}")
            else:
                bits.append(f"{_shares(buy_qty)} bought at {_px(entry)}")
    elif buy_qty > _FLAT:
        entry = buy_cost / buy_qty
        if len(prices) > 1:
            bits.append(f"{_shares(buy_qty)} bought at an average of {_px(entry)}")
        else:
            bits.append(f"{_shares(buy_qty)} bought at {_px(entry)}")

    # A flat broker snapshot closes the run. The missing-sale note is only
    # for a lot that is still open; a closed position does not need it.
    if run.missing_close and run.status != "closed":
        bits.append("sale or transfer missing from broker history")
    elif run.status == "open":
        if sells:
            sold_qty, avg_exit = _exit_avg(sells)
            bits.append(f"sold {_shares(sold_qty)} at {_px(avg_exit)}")
            bits.append(f"{_shares(run.shares)} still held")
        elif buy_qty > _FLAT and abs(run.shares - buy_qty) < 0.01:
            bits.append("still holding them")
        elif run.shares > _FLAT:
            bits.append(f"{_shares(run.shares)} still held")
    elif sells and all(reason == "assigned" for _q, _p, reason in sells):
        _sold, avg_exit = _exit_avg(sells)
        bits.append(f"called away at {_px(avg_exit)}")
    elif sells:
        _sold, avg_exit = _exit_avg(sells)
        bits.append(f"sold at {_px(avg_exit)}")

    if not bits:
        return ""
    return ". ".join(_cap(bit) for bit in bits) + "."


def _exit_avg(sells):
    qty = sum(q for q, _p, _r in sells)
    proceeds = sum(q * p for q, p, _r in sells)
    if qty <= _FLAT:
        return 0.0, 0.0
    return qty, proceeds / qty


def _when(run):
    start = _fmt_date(run.start)
    if run.status == "closed" and run.end:
        return f"{start} to {_fmt_date(run.end)}"
    if start:
        return f"Since {start}"
    return ""


def _contract_title(contract, qty):
    side = "put" if contract.option_type == "put" else "call"
    if contract.strike is None:
        title = side.capitalize()
    else:
        title = f"{_px(contract.strike)} {side}"
    shown = qty if qty > _FLAT else 0.0
    if shown > 1.001:
        n = int(round(shown)) if abs(shown - round(shown)) < 0.001 else shown
        word = "contract" if n == 1 else "contracts"
        title = f"{title} · {n:g} {word}" if isinstance(n, int) else f"{title} · {n} {word}"
    return title


def _partial_note(contract):
    held = contract.shares_at_open or 0.0
    contracts = contract.sto_qty if contract.sto_qty > _FLAT else contract.assigned_qty
    return (
        f"Partly covered — {_shares(held)} held for "
        f"{contracts:g} {'contract' if abs(contracts - 1) < 0.001 else 'contracts'}."
    )


def _shares(qty):
    if abs(qty - round(qty)) < 0.001:
        n = int(round(qty))
        return "1 share" if n == 1 else f"{n:,} shares"
    return f"{qty:,.2f} shares"


def _px(price):
    if price is None:
        return "$0"
    if abs(price - round(price)) < 0.001:
        return f"${int(round(price)):,}"
    return f"${price:,.2f}"


def _fmt_date(value):
    if not isinstance(value, date):
        return ""
    return f"{_MONTHS[value.month - 1]} {value.day}, {value.year}"


def _iso(value):
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    return None


def _cap(text):
    if not text:
        return text
    return text[:1].upper() + text[1:]
