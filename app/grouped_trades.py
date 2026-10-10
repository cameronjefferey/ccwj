"""One trade per spread and per lot for Strategies, Sectors, and Fit.

The warehouse stores one row per option contract. A vertical, iron condor,
straddle, or strangle is several of those rows and several fills. These
pages were labeling that fill count "trades" and, on the monthly chart,
scoring each leg as its own win or loss.

Dollars stay on the existing marts (they already sum every leg, and they
include dividends). This module only rebuilds the counts: how many
trades, how many wins and losses, the average, and which result was
best or worst. A missing classification read returns None so the page
can keep the mart numbers and call them fills instead of trades.

Same-contract lots split only when the fill history is attached. Two
different strikes are two trades even without fills.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from app.option_formatting import parse_occ
from app.outcome_units import (
    decided_win_loss,
    group_vertical_spreads,
    split_option_lots,
)

CLASSIFICATION_GROUPS_QUERY = """
SELECT
  tenant_id,
  account,
  symbol,
  strategy,
  status,
  trade_group_type,
  trade_symbol,
  direction,
  open_date,
  close_date,
  option_expiry,
  option_type,
  total_pnl,
  realized_pnl,
  unrealized_pnl,
  days_in_trade
FROM `ccwj-dbt.analytics.int_strategy_classification`
WHERE strategy IS NOT NULL AND TRIM(strategy) != ''
  {tenant_filter}
"""

# Fills let a contract that was opened twice become two lots. The read is
# best-effort: a failure leaves contract-level grouping in place.
OPTION_FILLS_QUERY = """
SELECT
  tenant_id,
  account,
  trade_symbol,
  action,
  quantity,
  price,
  amount,
  trade_date
FROM `ccwj-dbt.analytics.stg_history`
WHERE instrument_type IN ('Call', 'Put')
  AND action IN (
    'option_sell_to_open', 'option_buy_to_open',
    'option_buy_to_close', 'option_sell_to_close',
    'option_expired', 'option_assigned', 'option_exercised'
  )
  {tenant_filter}
"""

_MONTHS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]
_PAIR_DAYS = 3
_DIAGONAL_FAMILIES = frozenset({"Diagonal Call Spread", "Diagonal Put Spread"})
_STRADDLE_FAMILIES = frozenset({"Straddle", "Strangle"})


def _num(value) -> float:
    try:
        if value is None or value == "":
            return 0.0
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number:  # NaN
        return 0.0
    return number


def _iso(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text.lower() in {"", "nat", "nan", "none", "null"}:
        return ""
    return text[:10]


def _day_gap(left, right):
    a, b = _iso(left), _iso(right)
    if len(a) != 10 or len(b) != 10:
        return None
    try:
        return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)
    except ValueError:
        return None


def _parsed(row) -> dict | None:
    source = row
    legs = row.get("legs") or []
    if legs:
        source = legs[0]
    return parse_occ(source.get("trade_symbol"))


def _cp(row) -> str:
    parsed = _parsed(row)
    if parsed:
        return parsed["cp"]
    kind = str(row.get("option_type") or "").strip().upper()
    if kind.startswith("C"):
        return "C"
    if kind.startswith("P"):
        return "P"
    return ""


def _expiry(row) -> str:
    parsed = _parsed(row)
    if parsed:
        return f"20{parsed['yy']}-{int(parsed['mm']):02d}-{int(parsed['dd']):02d}"
    return _iso(row.get("option_expiry") or row.get("close_date"))


def _family(row) -> str:
    for leg in row.get("legs") or []:
        name = str(leg.get("_book_family") or "").strip()
        if name:
            return name
    name = str(row.get("_book_family") or "").strip()
    if name:
        return name
    return str(row.get("strategy_family") or row.get("strategy") or "").strip()


def _symbol(row) -> str:
    for source in (row, *(row.get("legs") or [])):
        text = str(source.get("symbol") or source.get("underlying_symbol") or "").strip()
        if text and text.lower() != "nan":
            return text
    parsed = _parsed(row)
    return parsed["root"] if parsed else ""


def _is_option(row) -> bool:
    if str(row.get("trade_group_type") or "") == "equity_session":
        return False
    if str(row.get("trade_group_type") or "") == "option_contract":
        return True
    return parse_occ(row.get("trade_symbol")) is not None


def _as_records(rows) -> list[dict]:
    if rows is None:
        return []
    if hasattr(rows, "to_dict") and hasattr(rows, "empty"):
        if rows.empty:
            return []
        return rows.to_dict("records")
    return [dict(row) for row in rows if isinstance(row, dict)]


def _attach_fills(rows, fills) -> list[dict]:
    if not fills:
        return rows
    buckets: dict[tuple, list] = {}
    for fill in _as_records(fills):
        key = (
            str(fill.get("tenant_id") or ""),
            str(fill.get("trade_symbol") or "").strip(),
        )
        if key[1]:
            buckets.setdefault(key, []).append(fill)
    attached = []
    for row in rows:
        item = dict(row)
        if not item.get("raw_trades"):
            key = (
                str(item.get("tenant_id") or ""),
                str(item.get("trade_symbol") or "").strip(),
            )
            if key in buckets:
                item["raw_trades"] = list(buckets[key])
        attached.append(item)
    return attached


def _prepare_option(row) -> dict:
    pnl = _num(row.get("total_pnl") if row.get("total_pnl") is not None else row.get("pnl"))
    family = str(row.get("strategy") or "").strip()
    item = dict(row)
    item.update({
        "type": "option",
        "strategy": family,
        "_book_family": family,
        "pnl": pnl,
        "total_pnl": pnl,
        "_source_pnl": pnl,
        "_source_status": str(row.get("status") or ""),
        "symbol": _symbol(row),
        "open_date": _iso(row.get("open_date")),
        "close_date": _iso(row.get("close_date")),
        "option_expiry": _iso(row.get("option_expiry")),
        "days_in_trade": row.get("days_in_trade") if row.get("days_in_trade") is not None else row.get("days_held"),
        "raw_trades": list(row.get("raw_trades") or []),
    })
    if family == "Iron Condor":
        cp = _cp(item)
        if cp == "C":
            item["strategy"] = "Call Spread"
        elif cp == "P":
            item["strategy"] = "Put Spread"
    return item


def _apply_slice_status(row) -> dict:
    close = str(row.get("close_type") or "")
    source = str(row.get("_source_status") or row.get("status") or "")
    if source == "Settlement pending":
        row["status"] = "Settlement pending"
    elif close == "Open":
        row["status"] = "Open"
    elif close:
        row["status"] = "Closed"
    return row


def _with_residual(slices, parent_pnl) -> list[dict]:
    if len(slices) <= 1:
        return slices
    total = round(sum(_num(row.get("pnl")) for row in slices), 2)
    gap = round(parent_pnl - total, 2)
    if abs(gap) >= 0.01:
        slices[-1]["pnl"] = round(_num(slices[-1].get("pnl")) + gap, 2)
    for row in slices:
        _apply_slice_status(row)
    return slices


def _expand_lots(row) -> list[dict]:
    prepared = _prepare_option(row)
    if prepared["_source_status"] == "Settlement pending":
        return [prepared]
    parent_pnl = prepared["pnl"]
    slices = split_option_lots(prepared)
    if len(slices) <= 1:
        return [prepared]
    return _with_residual(slices, parent_pnl)


def _anchor_value(row, name):
    for source in (row, *(row.get("legs") or [])):
        value = source.get(name)
        if value not in (None, ""):
            text = str(value).strip()
            if text.lower() not in {"nan", "none", "nat"}:
                return value
    return None


def _structure_pnl(row) -> float:
    if row.get("vertical"):
        return round(_num(row.get("pnl")), 2)
    return round(_num(row.get("pnl") if row.get("pnl") is not None else row.get("total_pnl")), 2)


def _statuses(row) -> list[str]:
    sources = list(row.get("legs") or []) or [row]
    found = []
    for source in sources:
        status = str(source.get("status") or source.get("_source_status") or "")
        if not status and row.get("vertical"):
            close = str(row.get("close_type") or row.get("outcome") or "")
            if close == "Settlement pending":
                status = "Settlement pending"
            elif close == "Open":
                status = "Open"
            elif close:
                status = "Closed"
        found.append(status or str(row.get("status") or ""))
    return found


def _finish(rows, strategy) -> dict:
    """One counted trade from one or more already-paired structures."""
    structures = list(rows)
    first = structures[0]
    pnl = round(sum(_structure_pnl(row) for row in structures), 2)
    statuses = []
    for row in structures:
        statuses.extend(_statuses(row))
    symbol = _symbol(first)
    tenant = str(_anchor_value(first, "tenant_id") or "")
    account = str(_anchor_value(first, "account") or "")
    opens = [_iso(_anchor_value(row, "open_date")) for row in structures]
    closes = [_iso(_anchor_value(row, "close_date")) for row in structures]
    opens = [item for item in opens if item]
    closes = [item for item in closes if item]
    days_values = []
    for row in structures:
        days = _anchor_value(row, "days_in_trade")
        if days is None:
            days = _anchor_value(row, "days_held")
        if days is not None and str(days).strip().lower() not in {"", "nan", "none"}:
            days_values.append(_num(days))
    group_type = "option_contract"
    for row in structures:
        kind = str(_anchor_value(row, "trade_group_type") or "")
        if kind:
            group_type = kind
            break
    if any(status == "Settlement pending" for status in statuses):
        state = "Settlement pending"
        counts = False
        winners = losers = 0
        realized = unrealized = 0.0
    elif any(status == "Open" for status in statuses):
        state = "Open"
        counts = True
        winners = losers = 0
        realized = 0.0
        unrealized = pnl
    else:
        state = "Closed"
        counts = True
        winners, losers = decided_win_loss("Closed", pnl)
        realized = pnl
        unrealized = 0.0
    return {
        "strategy": strategy,
        "symbol": symbol,
        "tenant_id": tenant,
        "account": account,
        "pnl": pnl,
        "realized": round(realized, 2),
        "unrealized": round(unrealized, 2),
        "status": state,
        "open_date": min(opens) if opens else "",
        "close_date": max(closes) if closes else "",
        "num_winners": winners,
        "num_losers": losers,
        "counts_as_trade": counts,
        "days_in_trade": (sum(days_values) / len(days_values)) if days_values else None,
        "dte_bucket": str(_anchor_value(first, "dte_bucket") or ""),
        "moneyness_at_open": str(_anchor_value(first, "moneyness_at_open") or ""),
        "trade_group_type": group_type,
        "label": f"{symbol} · {strategy}" if symbol else strategy,
        "sector": str(_anchor_value(first, "sector") or ""),
        "subsector": str(_anchor_value(first, "subsector") or ""),
    }


def _single_unit(row) -> dict:
    if _is_option(row) or row.get("vertical") or row.get("legs"):
        return _finish([row], _family(row) or str(row.get("strategy") or ""))
    pnl = round(_num(row.get("total_pnl") if row.get("total_pnl") is not None else row.get("pnl")), 2)
    status = str(row.get("status") or "")
    if not status:
        status = "Closed" if _iso(row.get("close_date")) else "Open"
    wrapped = dict(row)
    wrapped["pnl"] = pnl
    wrapped["status"] = status
    wrapped["trade_group_type"] = row.get("trade_group_type") or "equity_session"
    return _finish([wrapped], str(row.get("strategy") or ""))


def _cluster_key(row) -> tuple:
    return (
        str(_anchor_value(row, "tenant_id") or ""),
        str(_anchor_value(row, "account") or ""),
        _symbol(row),
        _expiry(row),
        _family(row),
    )


def _pair_call_put(rows, *, same_direction: bool) -> list:
    """Pair a call structure with a put structure. Leftovers stay separate."""
    used_puts = set()
    pairs = []
    calls = [row for row in rows if _cp(row) == "C"]
    puts = [row for row in rows if _cp(row) == "P"]
    for call in calls:
        match = None
        for index, put in enumerate(puts):
            if index in used_puts:
                continue
            gap = _day_gap(
                _anchor_value(call, "open_date"),
                _anchor_value(put, "open_date"),
            )
            if gap is None or gap > _PAIR_DAYS:
                continue
            if same_direction:
                if str(_anchor_value(call, "direction") or "") != str(_anchor_value(put, "direction") or ""):
                    continue
            if match is None:
                match = index
                continue
            # Prefer the same contract count, then the nearer open.
            def _rank(idx, _call=call):
                try:
                    qty = abs(float(_anchor_value(puts[idx], "quantity") or 0))
                    want = abs(float(_anchor_value(_call, "quantity") or 0))
                except (TypeError, ValueError):
                    qty = want = 0.0
                gap_now = _day_gap(
                    _anchor_value(_call, "open_date"),
                    _anchor_value(puts[idx], "open_date"),
                )
                return (0 if abs(qty - want) < 1e-6 else 1, gap_now if gap_now is not None else 99, idx)

            if _rank(index) < _rank(match):
                match = index
        if match is None:
            pairs.append([call])
        else:
            used_puts.add(match)
            pairs.append([call, puts[match]])
    for index, put in enumerate(puts):
        if index not in used_puts:
            pairs.append([put])
    other = [row for row in rows if _cp(row) not in {"C", "P"}]
    pairs.extend([[row] for row in other])
    return pairs


def _pair_diagonals(rows) -> list:
    """A sold leg with the longer-dated long of the same type is one trade."""
    used_longs = set()
    pairs = []
    shorts = [row for row in rows if str(_anchor_value(row, "direction") or "") == "Sold"]
    longs = [row for row in rows if str(_anchor_value(row, "direction") or "") == "Bought"]
    for short in shorts:
        match = None
        for index, long in enumerate(longs):
            if index in used_longs or _cp(short) != _cp(long):
                continue
            if _expiry(long) <= _expiry(short):
                continue
            long_open = _iso(_anchor_value(long, "open_date"))
            short_open = _iso(_anchor_value(short, "open_date"))
            if long_open and short_open and long_open > short_open:
                continue
            gap = _day_gap(long_open, short_open)
            if gap is not None and gap > 7:
                continue
            match = index
            break
        if match is None:
            pairs.append([short])
        else:
            used_longs.add(match)
            pairs.append([short, longs[match]])
    for index, long in enumerate(longs):
        if index not in used_longs:
            pairs.append([long])
    other = [
        row for row in rows
        if str(_anchor_value(row, "direction") or "") not in {"Sold", "Bought"}
    ]
    pairs.extend([[row] for row in other])
    return pairs


def _collapse_families(structures) -> list[dict]:
    by_family: dict[str, list] = {}
    order = []
    for row in structures:
        family = _family(row) or str(row.get("strategy") or "")
        if family not in by_family:
            order.append(family)
            by_family[family] = []
        by_family[family].append(row)
    units = []
    for family in order:
        rows = by_family[family]
        if family == "Iron Condor":
            clusters: dict[tuple, list] = {}
            for row in rows:
                clusters.setdefault(_cluster_key(row), []).append(row)
            for members in clusters.values():
                for group in _pair_call_put(members, same_direction=False):
                    units.append(_finish(group, family))
        elif family in _STRADDLE_FAMILIES:
            clusters = {}
            for row in rows:
                clusters.setdefault(_cluster_key(row), []).append(row)
            for members in clusters.values():
                for group in _pair_call_put(members, same_direction=True):
                    units.append(_finish(group, family))
        elif family in _DIAGONAL_FAMILIES:
            clusters = {}
            for row in rows:
                key = (
                    str(_anchor_value(row, "tenant_id") or ""),
                    str(_anchor_value(row, "account") or ""),
                    _symbol(row),
                    family,
                )
                clusters.setdefault(key, []).append(row)
            for members in clusters.values():
                for group in _pair_diagonals(members):
                    units.append(_finish(group, family))
        else:
            for row in rows:
                units.append(_single_unit(row))
    return units


def group_book_trades(rows, fills=None) -> list[dict]:
    """Group classification or trade-kind rows into one result each.

    Equity sessions and single contracts stay one trade each (a fill
    count on the row is ignored). Call and put spreads pair by contract
    count, then nearest strike. An iron condor is those two spreads
    together. A straddle or strangle is the call plus the put. A diagonal
    pairs only with another row that already carries the diagonal label.
    """
    records = _attach_fills(_as_records(rows), fills)
    options = []
    units = []
    for row in records:
        if _is_option(row):
            options.extend(_expand_lots(row))
        else:
            units.append(_single_unit(row))
    if options:
        paired = group_vertical_spreads(options)
        units.extend(_collapse_families(paired))
    return units


def _load_group_frames(client, tenant_filter, tenant_ids):
    """Classification frame (None when that read fails) and option fills.

    A failed fill read leaves fills empty. Lot splits then stay at one
    row per contract, which is still a trade — just not two lots.
    """
    from app.query_cache import cached_query_df
    from app.tenant_scope import filter_df_by_tenant_ids

    try:
        frame = cached_query_df(
            client,
            CLASSIFICATION_GROUPS_QUERY.format(tenant_filter=tenant_filter),
            label="strategy_groups",
        )
        frame = filter_df_by_tenant_ids(frame, tenant_ids)
    except Exception:
        from app import app
        app.logger.exception("grouped trade classification read failed")
        return None, []
    fills = []
    try:
        fill_frame = cached_query_df(
            client,
            OPTION_FILLS_QUERY.format(tenant_filter=tenant_filter),
            label="strategy_group_fills",
        )
        fill_frame = filter_df_by_tenant_ids(fill_frame, tenant_ids)
        if fill_frame is not None and not getattr(fill_frame, "empty", True):
            fills = fill_frame.to_dict("records")
    except Exception:
        fills = []
    return frame, fills


def fetch_grouped_book(client, tenant_filter, tenant_ids):
    """(grouped trades or None, option fills).

    None means the classification read failed and callers should keep
    the mart's fill counts. An empty list means there is nothing classified.
    """
    frame, fills = _load_group_frames(client, tenant_filter, tenant_ids)
    if frame is None:
        return None, []
    if getattr(frame, "empty", True):
        return [], fills
    return group_book_trades(frame.to_dict("records"), fills), fills


def fetch_book_units(client, tenant_filter, tenant_ids):
    """Classification rows grouped into trades, or None when the read fails."""
    units, _fills = fetch_grouped_book(client, tenant_filter, tenant_ids)
    return units


def _key_part(row, name):
    value = row[name] if name in getattr(row, "index", ()) else row.get(name)
    text = "" if value is None else str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    return text


def counts_by(units, keys) -> dict:
    """Summed trade counts for rows that count. Pending rows are left out."""
    tallies: dict[tuple, dict] = {}
    for unit in units or []:
        if not unit.get("counts_as_trade"):
            continue
        key = tuple(str(unit.get(name) or "") for name in keys)
        slot = tallies.setdefault(key, {"num_trades": 0, "num_winners": 0, "num_losers": 0})
        slot["num_trades"] += 1
        slot["num_winners"] += int(unit.get("num_winners") or 0)
        slot["num_losers"] += int(unit.get("num_losers") or 0)
    return tallies


def stamp_grouped_counts(df, units, keys, trade_col="num_individual_trades"):
    """Replace fill counts with grouped counts. Dollars on ``df`` stay put.

    The first row of each key receives the count so a later sum is the
    grouped total. Rows with no grouped key keep the number they had.
    """
    if df is None or getattr(df, "empty", True) or not units:
        return df
    if any(name not in df.columns for name in keys):
        return df
    tallies = counts_by(units, keys)
    if not tallies:
        return df
    out = df.copy()
    for col in (trade_col, "num_winners", "num_losers"):
        if col not in out.columns:
            out[col] = 0
    seen = set()
    for idx in list(out.index):
        key = tuple(_key_part(out.loc[idx], name) for name in keys)
        if key not in tallies or key in seen:
            if key in seen and key in tallies:
                out.at[idx, trade_col] = 0
                out.at[idx, "num_winners"] = 0
                out.at[idx, "num_losers"] = 0
                if "win_rate" in out.columns:
                    out.at[idx, "win_rate"] = pd.NA
            continue
        seen.add(key)
        tally = tallies[key]
        out.at[idx, trade_col] = tally["num_trades"]
        out.at[idx, "num_winners"] = tally["num_winners"]
        out.at[idx, "num_losers"] = tally["num_losers"]
        if "win_rate" in out.columns:
            decided = tally["num_winners"] + tally["num_losers"]
            out.at[idx, "win_rate"] = (tally["num_winners"] / decided) if decided else pd.NA
    return out


def months_from_units(units) -> pd.DataFrame:
    """Closed trades by strategy and close month, pooled across accounts."""
    columns = [
        "strategy", "month_start", "trades_closed", "num_winners",
        "num_losers", "total_pnl", "trend_signal",
    ]
    buckets: dict[tuple, dict] = {}
    for unit in units or []:
        if not unit.get("counts_as_trade") or unit.get("status") != "Closed":
            continue
        closed = _iso(unit.get("close_date"))
        if len(closed) < 7:
            continue
        key = (str(unit.get("strategy") or ""), closed[:7] + "-01")
        slot = buckets.setdefault(key, {"trades": 0, "winners": 0, "losers": 0, "pnl": 0.0})
        slot["trades"] += 1
        slot["winners"] += int(unit.get("num_winners") or 0)
        slot["losers"] += int(unit.get("num_losers") or 0)
        slot["pnl"] += _num(unit.get("pnl"))
    if not buckets:
        return pd.DataFrame(columns=columns)
    rows = []
    by_strategy: dict[str, list] = {}
    for (strategy, month), slot in buckets.items():
        item = {
            "strategy": strategy,
            "month_start": month,
            "trades_closed": slot["trades"],
            "num_winners": slot["winners"],
            "num_losers": slot["losers"],
            "total_pnl": round(slot["pnl"], 2),
            "trend_signal": "stable",
        }
        rows.append(item)
        by_strategy.setdefault(strategy, []).append(item)
    for strategy, months in by_strategy.items():
        months.sort(key=lambda item: item["month_start"])
        signal = _trend_signal(months)
        months[-1]["trend_signal"] = signal
    frame = pd.DataFrame(rows, columns=columns)
    frame["month_start"] = pd.to_datetime(frame["month_start"])
    return frame.sort_values(["strategy", "month_start"]).reset_index(drop=True)


def overlay_month_counts(mart_months, grouped_months) -> pd.DataFrame:
    """Use grouped trade counts without replacing the mart's monthly dollars.

    A structure can close its legs in different months. Its grouped outcome
    belongs to the final close month, while each leg's realized P&L belongs to
    the month that leg actually closed. Replacing the mart frame with grouped
    units moved all dollars to the final month; this overlays only the count
    fields that grouping owns.
    """
    if mart_months is None or getattr(mart_months, "empty", True):
        return mart_months
    if grouped_months is None or getattr(grouped_months, "empty", True):
        return mart_months

    out = mart_months.copy()
    grouped = grouped_months.copy()
    out["month_start"] = pd.to_datetime(out["month_start"], errors="coerce")
    grouped["month_start"] = pd.to_datetime(
        grouped["month_start"], errors="coerce"
    )
    grouped = grouped.dropna(subset=["month_start"])
    grouped_strategies = set(grouped["strategy"].astype(str))
    tallies = {
        (str(row["strategy"]), row["month_start"]): {
            "trades_closed": int(_num(row.get("trades_closed"))),
            "num_winners": int(_num(row.get("num_winners"))),
            "num_losers": int(_num(row.get("num_losers"))),
        }
        for _, row in grouped.iterrows()
    }

    for idx, row in out.iterrows():
        strategy = str(row.get("strategy") or "")
        if strategy not in grouped_strategies:
            continue
        key = (strategy, row["month_start"])
        tally = tallies.get(key, {
            "trades_closed": 0, "num_winners": 0, "num_losers": 0,
        })
        for name, value in tally.items():
            out.at[idx, name] = value

    decided = out["num_winners"] + out["num_losers"]
    out["win_rate_pct"] = (
        out["num_winners"] / decided.replace(0, pd.NA) * 100
    )
    trades = out["trades_closed"].replace(0, pd.NA)
    out["avg_pnl"] = out["total_pnl"] / trades
    return out.sort_values(["strategy", "month_start"]).reset_index(drop=True)


def _trend_signal(months) -> str:
    """Same rule as mart_strategy_trend: two prior months, then ±10%."""
    if len(months) < 3:
        return "new"
    prior = months[-4:-1] if len(months) >= 4 else months[:-1]
    prior = prior[-3:]
    rates = []
    for month in prior:
        decided = month["num_winners"] + month["num_losers"]
        if decided:
            rates.append(month["num_winners"] / decided)
    if len(rates) < 2:
        return "new"
    latest = months[-1]
    decided = latest["num_winners"] + latest["num_losers"]
    if not decided:
        return "stable"
    latest_rate = latest["num_winners"] / decided
    baseline = sum(rates) / len(rates)
    if baseline <= 0:
        return "improving" if latest_rate > 0 else "stable"
    if latest_rate > baseline * 1.10:
        return "improving"
    if latest_rate < baseline * 0.90:
        return "declining"
    return "stable"


def avg_days_by_strategy(units) -> dict:
    buckets: dict[str, list] = {}
    for unit in units or []:
        if not unit.get("counts_as_trade") or unit.get("days_in_trade") is None:
            continue
        buckets.setdefault(str(unit.get("strategy") or ""), []).append(_num(unit.get("days_in_trade")))
    return {name: (sum(days) / len(days)) for name, days in buckets.items() if days}


def symbol_trade_counts(units, strategy) -> dict:
    counts: dict[str, int] = {}
    for unit in units or []:
        if not unit.get("counts_as_trade") or unit.get("strategy") != strategy:
            continue
        symbol = str(unit.get("symbol") or "")
        if symbol:
            counts[symbol] = counts.get(symbol, 0) + 1
    return counts


def best_and_worst(units, strategy=None, sector=None):
    """The closed result with the highest and lowest net, or (None, None)."""
    closed = []
    for unit in units or []:
        if not unit.get("counts_as_trade") or unit.get("status") != "Closed":
            continue
        if strategy is not None and unit.get("strategy") != strategy:
            continue
        if sector is not None and unit.get("sector") != sector:
            continue
        closed.append(unit)
    if not closed:
        return None, None
    best = max(closed, key=lambda unit: _num(unit.get("pnl")))
    worst = min(closed, key=lambda unit: _num(unit.get("pnl")))
    return best, worst


def overlay_breakdown_counts(breakdown_df, units):
    """Swap leg counts for grouped trades. Dollar columns stay as they are."""
    if breakdown_df is None or getattr(breakdown_df, "empty", True) or not units:
        return breakdown_df
    if "trade_group_type" not in breakdown_df.columns:
        return breakdown_df
    counts: dict[str, dict] = {}
    for unit in units:
        if unit.get("status") == "Settlement pending":
            continue
        kind = str(unit.get("trade_group_type") or "option_contract")
        slot = counts.setdefault(kind, {"num_groups": 0, "num_open_groups": 0})
        if unit.get("counts_as_trade"):
            slot["num_groups"] += 1
        if unit.get("status") == "Open":
            slot["num_open_groups"] += 1
    out = breakdown_df.copy()
    seen = set()
    for idx in list(out.index):
        kind = str(out.at[idx, "trade_group_type"] or "")
        if kind not in counts:
            continue
        if kind in seen:
            out.at[idx, "num_groups"] = 0
            out.at[idx, "num_open_groups"] = 0
            continue
        seen.add(kind)
        out.at[idx, "num_groups"] = counts[kind]["num_groups"]
        out.at[idx, "num_open_groups"] = counts[kind]["num_open_groups"]
    return out


def units_as_matrix_rows(units) -> pd.DataFrame:
    """One matrix row per counted trade. Pending structures are omitted."""
    rows = []
    for unit in units or []:
        if not unit.get("counts_as_trade"):
            continue
        rows.append({
            "account": unit.get("account") or "",
            "tenant_id": unit.get("tenant_id") or "",
            "symbol": unit.get("symbol") or "",
            "strategy": unit.get("strategy") or "",
            "status": unit.get("status") or "",
            "dte_bucket": unit.get("dte_bucket") or "Unknown",
            "moneyness_at_open": unit.get("moneyness_at_open") or "Unknown",
            "total_pnl": unit.get("pnl") or 0.0,
            "realized_pnl": unit.get("realized") or 0.0,
            "unrealized_pnl": unit.get("unrealized") or 0.0,
            "num_individual_trades": 1,
            "num_winners": unit.get("num_winners") or 0,
            "num_losers": unit.get("num_losers") or 0,
            "sector": unit.get("sector") or "",
            "subsector": unit.get("subsector") or "",
        })
    return pd.DataFrame(rows)


def fit_options_frame(df, fills=None) -> pd.DataFrame:
    """Group an option-contract frame before the fit matrix sums it."""
    if df is None or getattr(df, "empty", True):
        return df
    if "trade_symbol" not in df.columns or "strategy" not in df.columns:
        return df
    units = group_book_trades(df.to_dict("records"), fills)
    framed = units_as_matrix_rows(units)
    if framed.empty:
        return framed
    return framed


def attach_sectors(units, labeled_df) -> list[dict]:
    """Copy a symbol's display sector onto each grouped trade."""
    from app.sector_labels import classify_symbol

    lookup = {}
    if labeled_df is not None and not getattr(labeled_df, "empty", True) and "symbol" in labeled_df.columns:
        for _, row in labeled_df.iterrows():
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            lookup[symbol] = (
                str(row.get("sector") or ""),
                str(row.get("subsector") or ""),
            )
    for unit in units or []:
        symbol = str(unit.get("symbol") or "").strip().upper()
        if symbol in lookup:
            unit["sector"], unit["subsector"] = lookup[symbol]
        else:
            unit["sector"], unit["subsector"] = classify_symbol(symbol, None, None)
    return units


def format_record_span(first, last) -> str:
    """``Jan 2, 2024 – Oct 8, 2026``. Blank when neither date parses."""
    def _fmt(value):
        text = _iso(value)
        if len(text) != 10:
            return ""
        try:
            parsed = date.fromisoformat(text)
        except ValueError:
            return ""
        return f"{_MONTHS[parsed.month - 1]} {parsed.day}, {parsed.year}"

    start, end = _fmt(first), _fmt(last)
    if start and end:
        if start == end:
            return start
        return f"{start} – {end}"
    return start or end


def grouped_strategies(units) -> set:
    return {str(unit.get("strategy") or "") for unit in units or [] if unit.get("strategy")}


# Warehouse bucket codes, shown in words on these pages.
_PLAIN_LABELS = {
    "0-7 DTE": "0–7 days",
    "8-30 DTE": "8–30 days",
    "31-60 DTE": "31–60 days",
    "61-90 DTE": "61–90 days",
    "91+ DTE": "91 or more days",
    "ITM": "In the money",
    "ATM": "At the money",
    "OTM": "Out of the money",
}


def plain_label(value) -> str:
    """The words a trader should see for a warehouse bucket code."""
    text = str(value or "").strip()
    return _PLAIN_LABELS.get(text, text)
