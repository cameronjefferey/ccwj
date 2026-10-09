"""All-in cost per share.

Equity cost per share is the broker share cost basis divided by the shares
still held. All-in cost per share subtracts net option premium on that
same underlying during the current holding cycle:

    all-in = (equity cost basis − net option premium) / shares held

Net premium is credits received minus debits paid, after fees. Dividends
are ignored. The cycle starts at the first put sold that leads into the
current shares (including rolls of that put), or at the share purchase
when the shares were not assigned. It resets when the share count goes
to zero.

A still-open option counts at the premium collected so far. That amount
is not final: the figure uses fill cash, not a live mark. Closed options
from an earlier cycle stay out. An open contract that is only on the
broker snapshot (its sale predates the fill tape) counts at the
snapshot premium.

With no shares and an open cash-secured put (not the short leg of an
open spread), the figure is an if-assigned price: strike minus net
premium on that put's roll chain, per share the assignment would
deliver. That is not a cost of shares held.

Spreads and rolls are one grouped trade. Each fill's cash is counted once.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import date, datetime

OPEN_NOTE = (
    "Open options count at the premium collected so far. "
    "That amount is not final."
)
IF_ASSIGNED_NOTE = (
    "If assigned is not a cost of shares you hold. "
    "It is the strike minus net premium on this put, per share you would receive."
)
ESTIMATED_BASIS_NOTE = (
    "Share cost is estimated because the broker did not report a cost basis."
)

_CREDIT = {"option_sell_to_open", "option_sell_to_close"}
_DEBIT = {"option_buy_to_open", "option_buy_to_close"}
_CASH_ACTIONS = _CREDIT | _DEBIT
_CLOSE_QTY = _DEBIT | {
    "option_sell_to_close",
    "option_assigned",
    "option_exercised",
    "option_expired",
}
_EQUITY_ADD = {"equity_buy", "dividend_reinvest"}
_EQUITY_SUB = {"equity_sell", "equity_sell_short"}
_MULTIPLIER = 100.0
_EPS = 1e-6
_SHARE_TOL = 1.0
_PRICE_TOL = 0.05
_GROSS_TOL = 0.05

_BUCKET_LABELS = {
    "puts_before_assignment": "Puts before assignment",
    "calls": "Calls",
    "put_spreads": "Put spreads",
    "other": "Other options",
}
_BUCKET_ORDER = ("puts_before_assignment", "calls", "put_spreads", "other")

# Holdings and fills are tenant-scoped. Both project tenant_id so the
# DataFrame filter does not fail closed. Splits are public market data
# (stg_split_events has no tenant column) and must not go through that filter.
ALL_IN_HOLDINGS_QUERY = """
    SELECT
        tenant_id,
        account,
        underlying_symbol AS symbol,
        instrument_type,
        quantity,
        cost_basis,
        option_type,
        option_strike,
        option_expiry,
        trade_symbol
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE UPPER(TRIM(COALESCE(instrument_type, ''))) IN ('EQUITY', 'CALL', 'PUT')
      AND ABS(COALESCE(quantity, 0)) > 1e-6
      {tenant_filter}
"""

# Pre-window share lots. Used only when the broker cost basis is missing.
# est_amount is a signed cash flow (negative = cash out).
ALL_IN_OPENINGS_QUERY = """
    SELECT
        tenant_id,
        account,
        symbol,
        opening_qty,
        est_amount,
        price_source
    FROM `ccwj-dbt.analytics.int_opening_balances`
    WHERE COALESCE(opening_qty, 0) > 0.01
      {tenant_filter}
"""

ALL_IN_FILLS_QUERY = """
    SELECT
        tenant_id,
        account,
        underlying_symbol AS symbol,
        trade_date,
        action,
        trade_symbol,
        instrument_type,
        quantity,
        price,
        fees,
        amount,
        option_type,
        option_strike,
        option_expiry
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE action IN (
        'equity_buy', 'equity_sell', 'equity_sell_short',
        'option_sell_to_open', 'option_buy_to_open',
        'option_buy_to_close', 'option_sell_to_close',
        'option_assigned', 'option_exercised', 'option_expired'
    )
    {tenant_filter}
"""

ALL_IN_SPLITS_QUERY = """
    SELECT symbol, split_date, split_ratio
    FROM `ccwj-dbt.analytics.stg_split_events`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) IN ({symbols})
"""


def all_in_query_sql(tenant_filter):
    """Holdings and fills SQL, same strings the page and the cache warmer use."""
    clause = tenant_filter or ""
    return {
        "holdings": ALL_IN_HOLDINGS_QUERY.format(tenant_filter=clause),
        "fills": ALL_IN_FILLS_QUERY.format(tenant_filter=clause),
        "openings": ALL_IN_OPENINGS_QUERY.format(tenant_filter=clause),
    }


class _Fill:
    __slots__ = (
        "id", "date", "action", "qty", "raw_qty", "price", "fees", "amount",
        "cash", "option_type", "strike", "expiry", "trade_symbol", "contract",
        "symbol", "tenant_id", "synthetic", "from_snapshot",
    )

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))


class _ShareEvent:
    __slots__ = ("date", "delta", "origin", "contract", "fill_id", "cash_basis", "seq")

    def __init__(self, date, delta, origin, contract, fill_id, cash_basis, seq):
        self.date = date
        self.delta = delta
        self.origin = origin
        self.contract = contract
        self.fill_id = fill_id
        self.cash_basis = cash_basis
        self.seq = seq


def all_in_cost(
    fills,
    *,
    shares,
    cost_basis=None,
    opening_shares=0.0,
    opening_basis=0.0,
    splits=None,
    snapshot_options=None,
    tenant_id=None,
    symbol=None,
):
    """Cost per share for one currently held stock lot.

    ``shares`` is the snapshot quantity the number is divided by. ``cost_basis``
    is the broker equity cost (dollars). When it is missing, the basis is the
    cash paid for the shares added in this cycle, plus ``opening_basis`` when
    the broker never reported a cost (an estimate). With no shares and an
    open cash-secured put, returns an if-assigned price instead of None.
    """
    held = _num(shares)
    parsed = _parse_fills(fills, splits=splits, tenant_id=tenant_id, symbol=symbol)
    _append_snapshot_options(parsed, snapshot_options, tenant_id=tenant_id, symbol=symbol)
    if held <= _EPS:
        return _if_assigned_result(parsed, tenant_id=tenant_id, symbol=symbol)

    events, replay = _replay_shares(parsed, _num(opening_shares))
    episode = replay["episode"]
    broker = _num(cost_basis) if cost_basis is not None and _num(cost_basis) > _EPS else 0.0
    inferred = _infer_basis(events, episode) if episode is not None else 0.0
    extra = _num(opening_basis) if _num(opening_basis) > _EPS else 0.0
    if broker > _EPS:
        basis = broker
        basis_estimated = False
    else:
        basis = inferred + extra
        basis_estimated = extra > _EPS
    if basis <= _EPS:
        return None

    chain_ids = set()
    if episode and episode["origin"] == "put_assignment" and episode["contract"]:
        chain_ids = _assignment_chain(parsed, episode["contract"], episode["start"])

    window = [
        fill for fill in parsed
        if _in_premium_window(fill, episode, chain_ids)
    ]
    # Snapshot-only opens are not in the fill tape, so the episode date
    # cannot see them. They are open now, so they count. Tape fills from
    # a previous cycle stay out — a real flat still resets the cycle.
    seen = {fill.id for fill in window}
    for fill in parsed:
        if fill.from_snapshot and fill.id not in seen:
            window.append(fill)
            seen.add(fill.id)
    groups = _group_fills(window, chain_ids)
    net_premium = round(sum(group["cash"] for group in groups), 2)
    remaining = _contract_remaining(parsed)
    open_contracts = set()
    window_ids = {fill.id for fill in window}
    for fill in window:
        if remaining.get(fill.contract, 0.0) > _EPS:
            open_contracts.add(fill.contract)
    for group in groups:
        group["open"] = any(
            fill_id in window_ids and _contract_of(parsed, fill_id) in open_contracts
            for fill_id in group["fill_ids"]
        )

    equity_per_share = round(basis / held, 2)
    all_in_per_share = round((basis - net_premium) / held, 2)
    open_options = bool(open_contracts)
    show_all_in = open_options or abs(equity_per_share - all_in_per_share) >= 0.005
    buckets = {name: 0.0 for name in _BUCKET_ORDER}
    for group in groups:
        buckets[group["bucket"]] = round(buckets[group["bucket"]] + group["cash"], 2)
    lines = _allocate_lines(equity_per_share - all_in_per_share, buckets)
    cycle_start = _cycle_start(parsed, episode, chain_ids)
    note = OPEN_NOTE if open_options else ""
    if basis_estimated:
        note = f"{ESTIMATED_BASIS_NOTE} {note}".strip()
    hover = _hover(equity_per_share, all_in_per_share, lines, OPEN_NOTE if open_options else "")
    if basis_estimated:
        hover = f"{ESTIMATED_BASIS_NOTE} {hover}"

    result = {
        "shares": held,
        "if_assigned": False,
        "basis_estimated": basis_estimated,
        "equity_basis": round(basis, 2),
        "equity_per_share": equity_per_share,
        "net_premium": net_premium,
        "net_premium_per_share": round(equity_per_share - all_in_per_share, 2),
        "all_in_per_share": all_in_per_share,
        "has_option_activity": bool(window),
        "show_all_in": show_all_in,
        "open_options": open_options,
        "note": note,
        "lines": lines,
        "groups": groups,
        "buckets": buckets,
        "cycle_start": cycle_start.isoformat() if cycle_start else None,
        "replay_shares": round(replay["qty"], 4),
        "tenant_id": tenant_id,
        "symbol": symbol,
        "hover": hover,
    }
    return result


def position_all_in_costs(trades, current, openings, splits, label_for, symbol):
    """One result per tenant that currently holds ``symbol``.

    Uses the pre-leg fill tape and the pre-leg equity snapshot so a leg
    click does not drop the put chain or the share basis. Opening share
    counts come from the caller; they are not inferred on top of that.
    """
    equity = [
        row for row in _records(current)
        if str(row.get("instrument_type") or "").strip().lower() == "equity"
        and _num(row.get("quantity")) > _EPS
    ]
    if symbol:
        equity = [
            row for row in equity
            if str(row.get("symbol") or "").strip().upper() == str(symbol).strip().upper()
        ]
    by_tenant = {}
    for row in equity:
        tenant = str(row.get("tenant_id") or "").strip()
        slot = by_tenant.setdefault(tenant, {
            "shares": 0.0,
            "basis": 0.0,
            "account": row.get("account"),
            "tenant_id": tenant or None,
        })
        slot["shares"] += _num(row.get("quantity"))
        slot["basis"] += _num(row.get("cost_basis"))

    opening_by_tenant = defaultdict(float)
    opening_basis_by_tenant = defaultdict(float)
    for row in openings or []:
        if not isinstance(row, dict):
            continue
        tenant = str(row.get("tenant_id") or "").strip()
        opening_by_tenant[tenant] += _num(row.get("qty") or row.get("opening_qty"))
        source = str(row.get("price_source") or "").strip()
        if source == "broker_cost_basis":
            continue
        amount = row.get("est_cost")
        if amount is None:
            amount = row.get("est_amount")
        opening_basis_by_tenant[tenant] += abs(_num(amount))

    current_rows = _records(current)
    option_rows = [row for row in current_rows if not _is_equity_holding(row)]
    split_rows = _records(splits)
    trade_rows = _records(trades)
    tenants = set(by_tenant)
    for row in trade_rows:
        tenants.add(str(row.get("tenant_id") or "").strip())
    for row in option_rows:
        tenants.add(str(row.get("tenant_id") or "").strip())

    results = []
    for tenant in tenants:
        slot = by_tenant.get(tenant) or {
            "shares": 0.0,
            "basis": 0.0,
            "account": None,
            "tenant_id": tenant or None,
        }
        if slot["shares"] <= _EPS and not any(
            _row_matches(row, tenant, symbol) for row in trade_rows
        ) and not any(_row_matches(row, tenant, symbol) for row in option_rows):
            continue
        tenant_fills = [
            row for row in trade_rows
            if _row_matches(row, tenant, symbol)
        ]
        tenant_options = [
            row for row in option_rows
            if _row_matches(row, tenant, symbol)
        ]
        basis = slot["basis"] if slot["basis"] > _EPS else None
        result = all_in_cost(
            tenant_fills,
            shares=slot["shares"],
            cost_basis=basis,
            opening_shares=opening_by_tenant.get(tenant, 0.0),
            opening_basis=opening_basis_by_tenant.get(tenant, 0.0),
            splits=_splits_for(symbol, split_rows),
            snapshot_options=tenant_options,
            tenant_id=slot["tenant_id"],
            symbol=symbol,
        )
        if not result:
            continue
        account = slot.get("account")
        if account is None and tenant_fills:
            account = tenant_fills[0].get("account")
        if label_for is not None:
            try:
                result["account_label"] = label_for(slot["tenant_id"], account)
            except Exception:
                result["account_label"] = account
        else:
            result["account_label"] = account
        results.append(result)
    results.sort(key=lambda row: str(row.get("account_label") or ""))
    return results


def attach_positions_all_in(symbol_rows, client, tenant_ids, tenant_filter):
    """Stamp cost/share fields onto positions symbol rows.

    Failures propagate to the caller. The positions page catches them so a
    warehouse miss leaves the table up without these two columns filled.
    Returns the open-option note when any row counted an open contract,
    else "".
    """
    from app.query_cache import cached_query_df
    from app.tenant_scope import filter_df_by_tenant_ids

    for row in symbol_rows or []:
        _blank_all_in_fields(row)

    if not symbol_rows or client is None:
        return ""

    sql = all_in_query_sql(tenant_filter)
    holdings = filter_df_by_tenant_ids(
        cached_query_df(client, sql["holdings"], label="all_in_holdings"),
        tenant_ids,
    )
    fills = filter_df_by_tenant_ids(
        cached_query_df(client, sql["fills"], label="all_in_fills"),
        tenant_ids,
    )
    openings = filter_df_by_tenant_ids(
        cached_query_df(client, sql["openings"], label="all_in_openings"),
        tenant_ids,
    )
    # Public split calendar. No tenant column — do not filter it.
    splits = _query_splits(client, cached_query_df, symbol_rows)
    _stamp_rows(
        symbol_rows,
        _records(holdings),
        _records(fills),
        splits,
        openings=_records(openings),
    )
    if any(row.get("open_options") for row in symbol_rows):
        return OPEN_NOTE
    return ""


def _stamp_rows(symbol_rows, holdings, fills, splits, openings=None):
    grouped = {}
    options_by_key = defaultdict(list)
    for row in holdings:
        tenant = str(row.get("tenant_id") or "").strip()
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        if not _is_equity_holding(row):
            options_by_key[(tenant, sym)].append(row)
            continue
        slot = grouped.setdefault((tenant, sym), {"shares": 0.0, "basis": 0.0})
        slot["shares"] += _num(row.get("quantity"))
        slot["basis"] += _num(row.get("cost_basis"))

    fills_by_key = defaultdict(list)
    for row in fills:
        tenant = str(row.get("tenant_id") or "").strip()
        sym = str(row.get("symbol") or "").strip().upper()
        fills_by_key[(tenant, sym)].append(row)

    opening_basis_by_key = defaultdict(float)
    for row in openings or []:
        if not isinstance(row, dict):
            continue
        source = str(row.get("price_source") or "").strip()
        if source == "broker_cost_basis":
            continue
        tenant = str(row.get("tenant_id") or "").strip()
        sym = str(row.get("symbol") or "").strip().upper()
        amount = row.get("est_cost")
        if amount is None:
            amount = row.get("est_amount")
        opening_basis_by_key[(tenant, sym)] += abs(_num(amount))

    for row in symbol_rows:
        tenant = str(row.get("tenant_id") or "").strip()
        sym = str(row.get("symbol") or "").strip().upper()
        _blank_all_in_fields(row)
        slot = grouped.get((tenant, sym))
        key_fills = fills_by_key.get((tenant, sym), [])
        key_options = options_by_key.get((tenant, sym), [])
        shares = slot["shares"] if slot else 0.0
        if shares <= _EPS and not key_fills and not key_options:
            continue
        opening = _inferred_opening(shares, key_fills, _splits_for(sym, splits)) if shares > _EPS else 0.0
        basis = slot["basis"] if slot and slot["basis"] > _EPS else None
        result = all_in_cost(
            key_fills,
            shares=shares,
            cost_basis=basis,
            opening_shares=opening,
            opening_basis=opening_basis_by_key.get((tenant, sym), 0.0),
            splits=_splits_for(sym, splits),
            snapshot_options=key_options,
            tenant_id=tenant or None,
            symbol=sym,
        )
        if not result:
            continue
        row["equity_per_share"] = result["equity_per_share"]
        row["all_in_per_share"] = result["all_in_per_share"]
        row["show_all_in"] = result["show_all_in"]
        row["open_options"] = result["open_options"]
        row["if_assigned"] = result.get("if_assigned", False)
        row["basis_estimated"] = result.get("basis_estimated", False)
        row["all_in_lines"] = result.get("lines") or []
        row["all_in_hover"] = result.get("hover") or ""
        row["all_in_note"] = result.get("note") or ""


def _blank_all_in_fields(row):
    row["equity_per_share"] = None
    row["all_in_per_share"] = None
    row["show_all_in"] = False
    row["open_options"] = False
    row["if_assigned"] = False
    row["basis_estimated"] = False
    row["all_in_lines"] = []
    row["all_in_hover"] = ""
    row["all_in_note"] = ""


def _is_equity_holding(row):
    kind = str(row.get("instrument_type") or "Equity").strip().lower()
    return kind in {"", "equity"}


def _append_snapshot_options(parsed, snapshot_options, tenant_id=None, symbol=None):
    """Open option rows whose fills are not in the tape.

    A covered call sold before the broker's history window still sits on
    the snapshot. Its cost basis is the premium. Matching strike and
    expiry to a tape fill avoids counting that premium twice.
    """
    next_id = 20_000 + len(parsed)
    for row in _records(snapshot_options):
        if not isinstance(row, dict) or not _row_matches(row, tenant_id, symbol):
            continue
        if _is_equity_holding(row):
            continue
        side = _snapshot_side(row)
        if side is None:
            continue
        qty = _num(row.get("quantity"))
        contracts = abs(qty)
        if contracts <= _EPS:
            continue
        short = qty < 0
        if _tape_has_contract(parsed, side, row):
            continue
        strike = _optional_num(
            row.get("option_strike") if "option_strike" in row else row.get("strike")
        )
        expiry = _as_date(row.get("option_expiry") or row.get("expiry"))
        trade_symbol = str(row.get("trade_symbol") or "").strip()
        cash = abs(_num(row.get("cost_basis")))
        if not short:
            cash = -cash
        parsed.append(_Fill(
            id=next_id,
            date=date(1970, 1, 1),
            action="option_sell_to_open" if short else "option_buy_to_open",
            qty=contracts,
            raw_qty=contracts,
            price=0.0,
            fees=0.0,
            amount=cash,
            cash=round(cash, 2),
            option_type=side,
            strike=strike,
            expiry=expiry,
            trade_symbol=trade_symbol,
            contract=_contract_key(side, strike, expiry, trade_symbol, next_id),
            symbol=str(row.get("symbol") or symbol or "").strip().upper(),
            tenant_id=str(row.get("tenant_id") or tenant_id or "").strip(),
            synthetic=False,
            from_snapshot=True,
        ))
        next_id += 1


def _snapshot_side(row):
    raw = str(row.get("option_type") or row.get("instrument_type") or "").strip().lower()
    if raw in {"call", "c"}:
        return "call"
    if raw in {"put", "p"}:
        return "put"
    return None


def _tape_has_contract(parsed, side, row):
    strike = _optional_num(
        row.get("option_strike") if "option_strike" in row else row.get("strike")
    )
    expiry = _as_date(row.get("option_expiry") or row.get("expiry"))
    trade_symbol = str(row.get("trade_symbol") or "").strip().upper()
    if strike is None and not expiry and not trade_symbol:
        return False
    for fill in parsed:
        if fill.option_type != side:
            continue
        if trade_symbol and str(fill.trade_symbol or "").strip().upper() == trade_symbol:
            return True
        if strike is None or fill.strike is None or abs(fill.strike - strike) > _PRICE_TOL:
            continue
        if expiry is not None and fill.expiry is not None and fill.expiry != expiry:
            continue
        if expiry is None or fill.expiry is None:
            continue
        return True
    return False


def _if_assigned_result(parsed, tenant_id=None, symbol=None):
    """Effective share price if the open cash-secured put is assigned."""
    remaining = _contract_remaining(parsed)
    opened = _primary_open_csp(parsed, remaining)
    if opened is None or opened.strike is None:
        return None
    chain_ids = _assignment_chain(parsed, opened.contract, None)
    chain_fills = [
        fill for fill in parsed
        if fill.id in chain_ids and fill.action in _CASH_ACTIONS and not fill.synthetic
    ]
    if not chain_fills:
        return None
    groups = _group_fills(chain_fills, chain_ids)
    for group in groups:
        group["open"] = True
    net = round(sum(group["cash"] for group in groups), 2)
    contracts = remaining.get(opened.contract, 0.0)
    assigned_shares = contracts * _MULTIPLIER
    if assigned_shares <= _EPS:
        return None
    premium_ps = round(net / assigned_shares, 2)
    price = round(opened.strike - premium_ps, 2)
    buckets = {name: 0.0 for name in _BUCKET_ORDER}
    for group in groups:
        buckets[group["bucket"]] = round(buckets[group["bucket"]] + group["cash"], 2)
    lines = [{
        "bucket": "strike",
        "label": "Strike",
        "per_share": round(opened.strike, 2),
        "credit": None,
    }]
    lines.extend(_allocate_lines(premium_ps, buckets))
    note = f"{IF_ASSIGNED_NOTE} {OPEN_NOTE}"
    hover_bits = [
        f"If assigned ${price:,.2f}",
        "Not a cost of shares held",
        f"Strike ${opened.strike:,.2f}",
    ]
    for line in lines:
        if line["bucket"] == "strike":
            continue
        sign = "−" if line["credit"] else "+"
        hover_bits.append(f"{line['label']} {sign}${line['per_share']:,.2f}")
    return {
        "shares": 0.0,
        "assigned_shares": assigned_shares,
        "if_assigned": True,
        "basis_estimated": False,
        "equity_basis": None,
        "equity_per_share": None,
        "net_premium": net,
        "net_premium_per_share": premium_ps,
        "all_in_per_share": price,
        "has_option_activity": True,
        "show_all_in": True,
        "open_options": True,
        "note": note,
        "lines": lines,
        "groups": groups,
        "buckets": buckets,
        "cycle_start": None,
        "replay_shares": 0.0,
        "tenant_id": tenant_id,
        "symbol": symbol,
        "hover": ". ".join(hover_bits) + f". {OPEN_NOTE}",
    }


def _primary_open_csp(parsed, remaining):
    puts = [fill for fill in parsed if fill.option_type == "put" and not fill.synthetic]
    by_contract = {}
    for fill in puts:
        if fill.action != "option_sell_to_open":
            continue
        if remaining.get(fill.contract, 0.0) <= _EPS:
            continue
        if _open_long_put_sibling(puts, fill, remaining):
            continue
        prev = by_contract.get(fill.contract)
        if prev is None or (fill.date, fill.id) >= (prev.date, prev.id):
            by_contract[fill.contract] = fill
    if not by_contract:
        return None
    return sorted(
        by_contract.values(),
        key=lambda fill: (fill.expiry or date.max, fill.strike or 0, fill.id),
    )[0]


def _open_long_put_sibling(puts, short, remaining):
    for fill in puts:
        if fill.action != "option_buy_to_open":
            continue
        if fill.contract == short.contract:
            continue
        if fill.expiry != short.expiry:
            continue
        if remaining.get(fill.contract, 0.0) > _EPS:
            return True
    return False


def _inferred_opening(shares, fills, splits):
    """Shares already held when the fill tape starts.

    Used on the positions list, which does not load ``int_opening_balances``.
    The position page passes that mart's quantity instead and must not also
    infer — the two would stack.
    """
    parsed = _parse_fills(fills, splits=splits)
    _events, replay = _replay_shares(parsed, 0.0)
    opening = shares - replay["qty"]
    if opening <= 0.01:
        return 0.0
    return opening


def _query_splits(client, cached_query_df, symbol_rows):
    symbols = []
    for row in symbol_rows:
        sym = str(row.get("symbol") or "").strip().upper()
        if sym and re.fullmatch(r"[A-Z0-9.\-]+", sym):
            symbols.append(sym)
    symbols = sorted(set(symbols))
    if not symbols:
        return []
    quoted = ", ".join(f"'{sym}'" for sym in symbols)
    frame = cached_query_df(
        client,
        ALL_IN_SPLITS_QUERY.format(symbols=quoted),
        label="all_in_splits",
    )
    return _records(frame)


def _parse_fills(fills, splits=None, tenant_id=None, symbol=None):
    split_pairs = _normalize_splits(splits)
    parsed = []
    for index, row in enumerate(_records(fills)):
        if not isinstance(row, dict):
            continue
        if not _row_matches(row, tenant_id, symbol):
            continue
        action = str(row.get("action") or "").strip()
        if not action:
            continue
        trade_date = _as_date(row.get("trade_date") or row.get("date"))
        if trade_date is None:
            continue
        qty = abs(_num(row.get("quantity") or row.get("qty")))
        factor = _split_factor(trade_date, split_pairs)
        option_type = _option_type(row)
        strike = _optional_num(row.get("option_strike") if "option_strike" in row else row.get("strike"))
        expiry = _as_date(row.get("option_expiry") or row.get("expiry"))
        trade_symbol = str(row.get("trade_symbol") or "").strip()
        price = abs(_num(row.get("price")))
        fees = abs(_num(row.get("fees")))
        # A missing amount stays None so the price × multiplier path runs.
        # An explicit 0 is a real zero-cash fill.
        amount = _optional_num(row["amount"]) if "amount" in row else None
        cash = _option_cash(action, qty, price, fees, amount)
        adjusted_qty = qty * factor
        fill = _Fill(
            id=index,
            date=trade_date,
            action=action,
            qty=adjusted_qty,
            raw_qty=qty,
            price=price,
            fees=fees,
            amount=amount,
            cash=cash,
            option_type=option_type,
            strike=strike,
            expiry=expiry,
            trade_symbol=trade_symbol,
            contract=_contract_key(option_type, strike, expiry, trade_symbol, index),
            symbol=str(row.get("symbol") or "").strip().upper(),
            tenant_id=str(row.get("tenant_id") or "").strip(),
            synthetic=False,
        )
        parsed.append(fill)
    parsed.sort(key=lambda fill: (fill.date, fill.id))
    return parsed


def _replay_shares(fills, opening_shares):
    """Share events with assignment and the matching equity fill counted once.

    Reductions sort ahead of additions on the same day, so a flat close and
    a same-day reopen start a new cycle.
    """
    by_day = defaultdict(list)
    for fill in fills:
        by_day[fill.date].append(fill)

    short_puts = defaultdict(float)
    events = []
    seq = 0
    synthetic_id = 10_000
    for day in sorted(by_day):
        day_fills = by_day[day]
        assignments = []
        equities = []
        for fill in day_fills:
            delta = _option_share_delta(fill)
            if abs(delta) > _EPS and fill.action in {"option_assigned", "option_exercised"}:
                assignments.append((fill, delta))
            elif fill.action in _EQUITY_ADD or fill.action in _EQUITY_SUB:
                equities.append(fill)
            _apply_option_qty(short_puts, fill)

        used = set()
        for fill, delta in assignments:
            match = _match_equity(equities, used, delta, fill.strike)
            if match is not None:
                used.add(match.id)
            origin = _assignment_origin(fill)
            seq += 1
            events.append(_ShareEvent(
                day, delta, origin, fill.contract, fill.id,
                _assignment_cash_basis(fill), seq,
            ))

        for fill in equities:
            if fill.id in used:
                continue
            delta = _equity_delta(fill)
            origin = "share_purchase" if delta > 0 else "share_sale"
            contract = None
            cash_basis = abs(fill.raw_qty) * abs(fill.price) if delta > 0 else 0.0
            if delta > 0:
                put = _matching_short_put(short_puts, fills, fill)
                if put is not None:
                    origin = "put_assignment"
                    contract = put
                    contracts = abs(delta) / _MULTIPLIER
                    short_puts[put] = max(0.0, short_puts[put] - contracts)
                    cash_basis = abs(delta) * abs(fill.price)
                    synthetic_id += 1
                    fills.append(_Fill(
                        id=synthetic_id,
                        date=day,
                        action="option_assigned",
                        qty=contracts,
                        raw_qty=contracts,
                        price=fill.price,
                        fees=0.0,
                        amount=0.0,
                        cash=0.0,
                        option_type="put",
                        strike=fill.price,
                        expiry=None,
                        trade_symbol="",
                        contract=put,
                        symbol=fill.symbol,
                        tenant_id=fill.tenant_id,
                        synthetic=True,
                    ))
            seq += 1
            events.append(_ShareEvent(
                day, delta, origin, contract, fill.id, cash_basis, seq,
            ))

    events.sort(key=lambda event: (event.date, 0 if event.delta < 0 else 1, event.seq))
    qty = _num(opening_shares)
    episode = None
    if qty > _EPS:
        episode = {"start": None, "origin": "opening_balance", "contract": None}
    for event in events:
        prev = qty
        qty += event.delta
        if prev <= _EPS < qty:
            episode = {
                "start": event.date,
                "origin": event.origin,
                "contract": event.contract,
            }
        if qty <= _EPS:
            episode = None
            qty = 0.0
    return events, {"qty": qty, "episode": episode}


def _in_premium_window(fill, episode, chain_ids):
    if fill.synthetic or fill.action not in _CASH_ACTIONS:
        return False
    if fill.id in chain_ids:
        return True
    if episode is None:
        return False
    if episode["origin"] == "opening_balance" and episode["start"] is None:
        return True
    return episode["start"] is not None and fill.date >= episode["start"]


def _assignment_chain(fills, contract, as_of):
    """Puts that were rolled into the assigned put, plus same-day long puts.

    Walks backward from the assigned contract: a sell-to-open that shares
    a day with a buy-to-close of another put is a roll. The long leg of a
    put spread opened that same day is included so the credit is the net.
    """
    puts = [fill for fill in fills if fill.option_type == "put" and not fill.synthetic]
    included = set()
    cursor = contract
    seen = set()
    while cursor and cursor not in seen:
        seen.add(cursor)
        opens = [
            fill for fill in puts
            if fill.contract == cursor
            and fill.action == "option_sell_to_open"
            and (as_of is None or fill.date <= as_of)
        ]
        if not opens:
            break
        opened = max(opens, key=lambda fill: (fill.date, fill.id))
        included.add(opened.id)
        closes = [
            fill for fill in puts
            if fill.action == "option_buy_to_close"
            and fill.date == opened.date
            and fill.contract != cursor
            and fill.contract not in seen
        ]
        if not closes:
            break
        closes.sort(key=lambda fill: (abs((fill.strike or 0) - (opened.strike or 0)), fill.id))
        included.add(closes[0].id)
        cursor = closes[0].contract

    sto_fills = [fill for fill in puts if fill.id in included and fill.action == "option_sell_to_open"]
    for opened in sto_fills:
        for fill in puts:
            if fill.action != "option_buy_to_open" or fill.date != opened.date:
                continue
            if fill.expiry != opened.expiry or fill.contract == opened.contract:
                continue
            included.add(fill.id)
    return included


def _group_fills(fills, chain_ids):
    used = set()
    groups = []
    closes = [fill for fill in fills if fill.action in {"option_buy_to_close", "option_sell_to_close"}]
    opens = [fill for fill in fills if fill.action in {"option_sell_to_open", "option_buy_to_open"}]
    pair_with = {
        "option_buy_to_close": "option_sell_to_open",
        "option_sell_to_close": "option_buy_to_open",
    }
    for close in closes:
        if close.id in used:
            continue
        want = pair_with[close.action]
        candidates = [
            opened for opened in opens
            if opened.id not in used
            and opened.action == want
            and opened.date == close.date
            and opened.option_type == close.option_type
            and opened.contract != close.contract
        ]
        if not candidates:
            continue
        candidates.sort(key=lambda opened: abs((opened.strike or 0) - (close.strike or 0)))
        opened = candidates[0]
        used.add(close.id)
        used.add(opened.id)
        groups.append(_make_group("roll", [close, opened], chain_ids))

    shorts = [
        fill for fill in fills
        if fill.id not in used and fill.action == "option_sell_to_open"
    ]
    longs = [
        fill for fill in fills
        if fill.id not in used and fill.action == "option_buy_to_open"
    ]
    for short in shorts:
        if short.id in used:
            continue
        candidates = [
            long for long in longs
            if long.id not in used
            and long.date == short.date
            and long.option_type == short.option_type
            and long.expiry == short.expiry
            and long.contract != short.contract
        ]
        if not candidates:
            continue
        candidates.sort(key=lambda long: abs((long.strike or 0) - (short.strike or 0)))
        long = candidates[0]
        used.add(short.id)
        used.add(long.id)
        members = [short, long]
        # A later close, expiry, or assignment of either leg is the same
        # spread. Leaving those fills as their own groups named the open
        # debit "Put spreads" and the close loss "Other options".
        contracts = {short.contract, long.contract}
        for fill in fills:
            if fill.id in used or fill.contract not in contracts:
                continue
            if fill.action not in _CLOSE_QTY:
                continue
            used.add(fill.id)
            members.append(fill)
        groups.append(_make_group("spread", members, chain_ids))

    for fill in fills:
        if fill.id in used:
            continue
        used.add(fill.id)
        groups.append(_make_group("option", [fill], chain_ids))
    return groups


def _make_group(kind, fills, chain_ids):
    types = {fill.option_type for fill in fills}
    if types == {"call"}:
        bucket = "calls"
    elif any(fill.id in chain_ids for fill in fills):
        bucket = "puts_before_assignment"
    elif kind == "spread" and types == {"put"}:
        bucket = "put_spreads"
    else:
        bucket = "other"
    return {
        "kind": kind,
        "bucket": bucket,
        "cash": round(sum(fill.cash for fill in fills), 2),
        "open": False,
        "fill_ids": [fill.id for fill in fills],
        "option_type": next(iter(types)) if len(types) == 1 else None,
    }


def _allocate_lines(target, buckets):
    """Per-share bucket lines that add back to equity − all-in."""
    cents_target = int(round(target * 100))
    if cents_target == 0:
        return []
    weights = [(name, buckets.get(name, 0.0)) for name in _BUCKET_ORDER if abs(buckets.get(name, 0.0)) >= 0.005]
    total = sum(cash for _, cash in weights)
    if abs(total) < _EPS:
        return []
    raw = []
    for name, cash in weights:
        raw.append((name, int(round(cents_target * (cash / total)))))
    drift = cents_target - sum(cents for _, cents in raw)
    if raw and drift:
        index = max(range(len(raw)), key=lambda item: abs(raw[item][1]))
        name, cents = raw[index]
        raw[index] = (name, cents + drift)
    lines = []
    for name, cents in raw:
        if cents == 0:
            continue
        lines.append({
            "bucket": name,
            "label": _BUCKET_LABELS[name],
            "per_share": abs(cents) / 100.0,
            "credit": cents > 0,
        })
    return lines


def _cycle_start(fills, episode, chain_ids):
    if episode is None:
        return None
    if episode["origin"] == "put_assignment" and chain_ids:
        dated = [fill.date for fill in fills if fill.id in chain_ids]
        if dated:
            return min(dated)
    return episode["start"]


def _infer_basis(events, episode):
    total = 0.0
    start = episode.get("start") if episode else None
    origin = episode.get("origin") if episode else None
    for event in events:
        if event.delta <= _EPS or event.cash_basis <= 0:
            continue
        if origin != "opening_balance" and start is not None and event.date < start:
            continue
        if origin != "opening_balance" and start is None:
            continue
        total += event.cash_basis
    return round(total, 2)


def _hover(equity, all_in, lines, note):
    parts = [f"Equity basis ${equity:,.2f}"]
    for line in lines:
        sign = "−" if line["credit"] else "+"
        parts.append(f"{line['label']} {sign}${line['per_share']:,.2f}")
    parts.append(f"All-in ${all_in:,.2f}")
    text = ". ".join(parts)
    if note:
        text = f"{text}. {note}"
    return text


def _contract_remaining(fills):
    qty = defaultdict(float)
    ordered = sorted(fills, key=lambda fill: (fill.date, fill.id))
    for fill in ordered:
        if not fill.contract or fill.option_type is None:
            continue
        if fill.action in {"option_sell_to_open", "option_buy_to_open"}:
            qty[fill.contract] += abs(fill.raw_qty)
        elif fill.action in _CLOSE_QTY:
            qty[fill.contract] -= abs(fill.raw_qty)
    return qty


def _contract_of(fills, fill_id):
    for fill in fills:
        if fill.id == fill_id:
            return fill.contract
    return None


def _apply_option_qty(short_puts, fill):
    if fill.option_type != "put" or not fill.contract:
        return
    qty = abs(fill.raw_qty)
    if fill.action == "option_sell_to_open":
        short_puts[fill.contract] += qty
    elif fill.action in {"option_buy_to_close", "option_assigned", "option_exercised", "option_expired"}:
        short_puts[fill.contract] = max(0.0, short_puts[fill.contract] - qty)


def _matching_short_put(short_puts, fills, equity):
    """Open short put whose strike matches a same-day share buy, when the broker omitted the assignment row."""
    contracts = abs(equity.raw_qty) / _MULTIPLIER
    best = None
    best_gap = None
    for contract, qty in short_puts.items():
        if qty <= _EPS:
            continue
        strike = _strike_of(fills, contract)
        if strike is None or abs(strike - equity.price) > _PRICE_TOL:
            continue
        if abs(qty * _MULTIPLIER - abs(equity.raw_qty)) > _SHARE_TOL and abs(qty - contracts) > 0.02:
            continue
        gap = abs(strike - equity.price)
        if best is None or gap < best_gap:
            best = contract
            best_gap = gap
    return best


def _strike_of(fills, contract):
    for fill in fills:
        if fill.contract == contract and fill.strike is not None:
            return fill.strike
    return None


def _match_equity(equities, used, delta, strike):
    if strike is None:
        return None
    for fill in equities:
        if fill.id in used:
            continue
        equity_delta = _equity_delta(fill)
        if equity_delta == 0 or (equity_delta > 0) != (delta > 0):
            continue
        if abs(abs(equity_delta) - abs(delta)) > _SHARE_TOL:
            continue
        if abs(fill.price - strike) > _PRICE_TOL:
            continue
        return fill
    return None


def _assignment_origin(fill):
    if fill.action == "option_assigned" and fill.option_type == "put":
        return "put_assignment"
    if fill.action == "option_assigned" and fill.option_type == "call":
        return "call_assignment"
    return "exercise"


def _assignment_cash_basis(fill):
    if fill.strike is None:
        return 0.0
    return abs(fill.raw_qty) * _MULTIPLIER * abs(fill.strike)


def _option_share_delta(fill):
    contracts = abs(fill.qty)
    shares = contracts * _MULTIPLIER
    if fill.action == "option_assigned" and fill.option_type == "put":
        return shares
    if fill.action == "option_assigned" and fill.option_type == "call":
        return -shares
    if fill.action == "option_exercised" and fill.option_type == "call":
        return shares
    if fill.action == "option_exercised" and fill.option_type == "put":
        return -shares
    return 0.0


def _equity_delta(fill):
    if fill.action in _EQUITY_ADD:
        return abs(fill.qty)
    if fill.action in _EQUITY_SUB:
        return -abs(fill.qty)
    return 0.0


def _option_cash(action, qty, price, fees, amount):
    if action not in _CASH_ACTIONS:
        return 0.0
    gross = abs(qty) * abs(price) * _MULTIPLIER
    fee = abs(fees or 0.0)
    sign = 1.0 if action in _CREDIT else -1.0
    if amount is None:
        return round(sign * gross - fee, 2)
    # Warehouse amount is already net of fees unless it still matches the
    # gross premium. Subtract the fee only in that gross case.
    if fee > 0.005 and abs(abs(amount) - gross) <= _GROSS_TOL:
        return round(sign * abs(amount) - fee, 2)
    if amount == 0:
        return 0.0
    if (amount > 0) == (sign > 0):
        return round(amount, 2)
    return round(sign * abs(amount), 2)


def _contract_key(option_type, strike, expiry, trade_symbol, index):
    if option_type is None and strike is None and not trade_symbol:
        return None
    if trade_symbol:
        return ("occ", trade_symbol.upper())
    expiry_key = expiry.isoformat() if isinstance(expiry, date) else ""
    strike_key = round(strike, 4) if strike is not None else None
    return ("opt", option_type or "", strike_key, expiry_key, index if strike is None else 0)


def _option_type(row):
    raw = str(row.get("option_type") or "").strip().lower()
    if raw in {"call", "c"}:
        return "call"
    if raw in {"put", "p"}:
        return "put"
    instrument = str(row.get("instrument_type") or "").strip().lower()
    if instrument == "call":
        return "call"
    if instrument == "put":
        return "put"
    return None


def _split_factor(trade_date, splits):
    factor = 1.0
    for split_date, ratio in splits:
        if split_date > trade_date and ratio:
            factor *= ratio
    return factor


def _normalize_splits(splits):
    out = []
    for item in splits or []:
        if isinstance(item, dict):
            split_date = _as_date(item.get("split_date") or item.get("date"))
            ratio = _num(item.get("split_ratio") or item.get("ratio"), 0.0)
        else:
            split_date = _as_date(item[0])
            ratio = _num(item[1], 0.0)
        if split_date and ratio:
            out.append((split_date, ratio))
    return out


def _splits_for(symbol, rows):
    if not symbol:
        return _normalize_splits(rows)
    wanted = str(symbol).strip().upper()
    matched = []
    for row in rows or []:
        if isinstance(row, dict):
            if str(row.get("symbol") or "").strip().upper() != wanted:
                continue
        matched.append(row)
    return matched if matched else []


def _row_matches(row, tenant_id, symbol):
    if symbol:
        row_symbol = str(row.get("symbol") or "").strip().upper()
        if row_symbol and row_symbol != str(symbol).strip().upper():
            return False
    if tenant_id:
        row_tenant = str(row.get("tenant_id") or "").strip()
        if row_tenant and row_tenant != str(tenant_id).strip():
            return False
    return True


def _records(frame):
    if frame is None:
        return []
    if isinstance(frame, list):
        return frame
    if isinstance(frame, tuple):
        return list(frame)
    to_dict = getattr(frame, "to_dict", None)
    empty = getattr(frame, "empty", None)
    if callable(to_dict) and empty is not None:
        if empty:
            return []
        return to_dict(orient="records")
    return list(frame)


def _as_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nat", "none", "nan", "null"}:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _num(value, default=0.0):
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _optional_num(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number
