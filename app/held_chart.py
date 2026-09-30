"""Price path for a closed option: what you traded, then if you'd held.

The expiry counterfactual already lives on ``int_option_exit_quality``:

    early_close_vs_expiry_delta = closing cash − settlement cash
    pnl if held                  = realized P&L − that delta

Settlement cash is intrinsic value at expiration (``max(0, S−K)`` for a
call, ``max(0, K−S)`` for a put) times 100 times contracts, signed long
positive / short negative. This module does not re-derive that dollar.
It only turns the same row into a per-share price series:

  * open and close are the fill prices (cash / (contracts × 100))
  * days in between use a recorded option mark when one exists
  * otherwise, and for every day after the close, the point is intrinsic
    value from the underlying's daily close — an estimate, not the
    option's market price

Open contracts are absent from the exit-quality grain. Rows that are
not a gradeable early close (held to expiration, assigned, exercised,
or missing an expiry price) produce no chart.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime

import pandas as pd

from app.money import fmt_money

_log = logging.getLogger(__name__)

# Public market data. stg_daily_prices is stamped per account, so a raw
# join fans out; ANY_VALUE collapses to one close per symbol-date.
# No tenant column — do NOT run the frame through the tenant filter.
UNDERLYING_CLOSES_QUERY = """
    SELECT
        date,
        ANY_VALUE(close_price) AS close_price
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE UPPER(TRIM(symbol)) = UPPER(TRIM('{symbol}'))
      AND date IS NOT NULL
      AND close_price IS NOT NULL
    GROUP BY date
    ORDER BY date
"""

# Tenant-scoped option marks (snapshot history since 2026-08-04).
# Projects tenant_id so the fail-closed DataFrame filter keeps the rows.
OPTION_MARKS_QUERY = """
    SELECT
        tenant_id,
        trade_symbol,
        date,
        current_price,
        market_value,
        quantity
    FROM `ccwj-dbt.analytics.int_option_marks_daily`
    WHERE UPPER(TRIM(underlying_symbol)) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

def fetch_held_series(client, safe_symbol, tenant_filter):
    """Underlying closes and option marks for the if-held chart.

    These two reads sit outside the shared position-detail batch on
    purpose. ``_bq_parallel`` already turns one failed query into an
    empty frame, but a failure here must not be able to take the
    position page or the peek drawer down with it — a missing marks
    table, a renamed column, or a price-query error returns empty
    frames and the chart falls back to fill prices plus intrinsic
    value (or disappears). Never raises.
    """
    from app.query_cache import cached_query_df

    closes = pd.DataFrame()
    marks = pd.DataFrame()
    try:
        frame = cached_query_df(
            client,
            UNDERLYING_CLOSES_QUERY.format(symbol=safe_symbol),
            label="underlying_closes",
        )
        if frame is not None:
            closes = frame
    except Exception as exc:
        _log.warning(
            "if-held underlying closes failed for %s: %s", safe_symbol, exc
        )
    try:
        frame = cached_query_df(
            client,
            OPTION_MARKS_QUERY.format(
                symbol=safe_symbol, tenant_filter=tenant_filter or ""
            ),
            label="option_marks",
        )
        if frame is not None:
            marks = frame
    except Exception as exc:
        _log.warning(
            "if-held option marks failed for %s: %s", safe_symbol, exc
        )
    return closes, marks


CONTRACT_MULTIPLIER = 100
# How many charts sit open before the rest collapse. Every closed
# early option still renders; the rest open on demand so a wheel
# symbol doesn't push the page down by a hundred canvases.
HELD_CHARTS_EXPANDED = 3

FEES_NOTE = "Fees aren't included."
SCOPE_NOTE = (
    "This is the option only. It doesn't include shares from assignment."
)
ESTIMATE_TAIL = (
    "After the close, the dashed line is intrinsic value from the stock's "
    "daily close through expiration — an estimate, not the option's market price. "
    + FEES_NOTE
    + " "
    + SCOPE_NOTE
)


def pnl_if_held(realized_pnl, early_close_vs_expiry_delta):
    """P&L if the contract had been held to expiration.

    ``realized − delta`` is the identity in ``int_option_exit_quality``:
    delta is closing cash minus settlement cash, and realized is opening
    cash plus closing cash, so the difference drops the close and puts
    the expiry settlement in its place. Returns None when either input
    is missing.
    """
    realized = _num(realized_pnl)
    delta = _num(early_close_vs_expiry_delta)
    if realized is None or delta is None:
        return None
    return round(realized - delta, 2)


def intrinsic_per_share(option_type, strike, underlying_close):
    """Per-share intrinsic value. None when the inputs aren't usable.

    OTM and exactly-at-the-money are 0. Calls use ``max(0, S−K)``,
    puts ``max(0, K−S)``. ``option_type`` may be ``C``/``P`` or
    ``Call``/``Put``.
    """
    strike_n = _num(strike)
    spot = _num(underlying_close)
    if strike_n is None or spot is None:
        return None
    kind = str(option_type or "").strip().upper()
    if not kind:
        return None
    if kind[0] == "C":
        return max(spot - strike_n, 0.0)
    if kind[0] == "P":
        return max(strike_n - spot, 0.0)
    return None


def option_mark_per_share(current_price, market_value, quantity):
    """Per-share option premium from a snapshot row.

    Brokers disagree on whether ``current_price`` is the premium or the
    premium × 100. ``market_value / (quantity × 100)`` is the per-share
    premium either way; when the two disagree by about 100×, the market
    value wins. A recorded 0 (worthless) stays 0.
    """
    px = _num(current_price)
    mv = _num(market_value)
    qty = _num(quantity)
    per_share = None
    if qty is not None and abs(qty) > 1e-9 and mv is not None:
        per_share = abs(mv) / (abs(qty) * CONTRACT_MULTIPLIER)
    if px is None and per_share is None:
        return None
    if px is None:
        return per_share
    if px < 0:
        return per_share
    if per_share is None or per_share == 0:
        return px
    if px > 0 and 50 <= (px / per_share) <= 150:
        return per_share
    return px


def build_held_charts(execution_df, prices_df, marks_df=None, *,
                      keep=None, label_for=None):
    """One chart payload per gradeable early close, largest |difference| first.

    ``keep(open_date)`` drops contracts outside a leg filter. ``label_for``
    is ``(tenant_id, account) -> display name``. Open and ungraded rows
    are omitted — there is no dashed "if held" path until the contract
    is closed and the expiry outcome is known.
    """
    if execution_df is None or getattr(execution_df, "empty", True):
        return []
    prices = _price_map(prices_df)
    marks = _marks_index(marks_df)
    charts = []
    seen = set()
    for _, row in execution_df.iterrows():
        chart = _chart_for_row(row, prices, marks, keep=keep, label_for=label_for)
        if chart is None:
            continue
        key = (chart["tenant_id"], chart["trade_symbol"], chart["open_date"])
        if key in seen:
            continue
        seen.add(key)
        charts.append(chart)
    charts.sort(key=lambda c: abs(c["difference"]), reverse=True)
    accounts = {c["account"] for c in charts if c.get("account")}
    multi = len(accounts) > 1
    for i, chart in enumerate(charts):
        chart["dom_id"] = f"held-{i}"
        chart["collapsed"] = i >= HELD_CHARTS_EXPANDED
        chart["show_account"] = multi
    return charts


def held_page_summary(charts):
    """Headline above the trades table, or None when nothing closed early.

    The warehouse ``difference`` is realized minus the expiry outcome, so
    a negative sum means holding finished higher: closing early cost that
    amount. A positive sum means the early exits came out ahead.
    """
    if not charts:
        return None
    net_cost = -sum(float(c.get("difference") or 0) for c in charts)
    count = len(charts)
    trade_word = "trade" if count == 1 else "trades"
    across = f" across {count} {trade_word}"
    if abs(net_cost) < 0.5:
        headline = f"Closing early matched holding to expiration{across}"
    elif net_cost > 0:
        headline = (
            f"Closing early cost you {_abs_money(net_cost)}{across}"
            " vs holding to expiration"
        )
    else:
        headline = (
            f"Closing early saved you {_abs_money(net_cost)}{across}"
            " vs holding"
        )
    return {
        "headline": headline,
        "count": count,
        "open_id": charts[0].get("dom_id") or "held-0",
        "net_cost": round(net_cost, 2),
        "rows": [_held_table_row(c) for c in charts],
    }


def _held_table_row(chart):
    """One expanded-table line. Contract reads like ``$41 call Sep 12``."""
    expired = ""
    if chart.get("expiry_date"):
        exp = _as_date(chart.get("expiry_date"))
        expired = _fmt_day(exp) if exp else ""
    label = chart.get("label") or "Option"
    contract = f"{label} {expired}".strip()
    delta = chart.get("difference")
    try:
        better = float(delta) < 0
    except (TypeError, ValueError):
        better = False
    return {
        "contract": contract,
        "contracts": chart.get("contracts"),
        "closed": chart.get("close_date_label") or "",
        "realized_display": chart.get("realized_display") or "",
        "if_held_display": chart.get("if_held_display") or "",
        "realized_pnl": chart.get("realized_pnl"),
        "pnl_if_held": chart.get("pnl_if_held"),
        "pill": outcome_pill(delta),
        "held_better": better,
        "dom_id": chart.get("dom_id") or "",
    }


def outcome_pill(difference):
    """If-held result in the trader's words, on the table and the panel.

    The warehouse delta is realized minus the expiry outcome. Negative
    means holding finished higher, so the label is "$X more if held".
    Positive means the early close came out ahead: "$X less if held".
    """
    if difference is None:
        return None
    try:
        delta = float(difference)
    except (TypeError, ValueError):
        return None
    if abs(delta) < 0.5:
        return "Same"
    amount = _abs_money(delta)
    if delta < 0:
        return f"{amount} more if held"
    return f"{amount} less if held"


def stamp_held_column(charts, outcomes):
    """Mark early-closed option rows that have a chart. Equity rows stay blank.

    Match ``(tenant_id, trade_symbol, open_date)`` first. A symbol with
    exactly one chart still matches when the outcome date is missing.
    """
    if not outcomes:
        return outcomes
    by_exact = {}
    by_symbol = {}
    for chart in charts or []:
        tenant = str(chart.get("tenant_id") or "")
        symbol = str(chart.get("trade_symbol") or "")
        opened = str(chart.get("open_date") or "")[:10]
        by_exact[(tenant, symbol, opened)] = chart
        by_symbol.setdefault((tenant, symbol), []).append(chart)
    for outcome in outcomes:
        if str(outcome.get("type") or "") != "option":
            continue
        tenant = str(outcome.get("tenant_id") or "")
        symbol = str(outcome.get("trade_symbol") or "")
        opened = str(outcome.get("open_date") or "")[:10]
        chart = by_exact.get((tenant, symbol, opened))
        if chart is None:
            matches = by_symbol.get((tenant, symbol), [])
            if len(matches) == 1:
                chart = matches[0]
        if chart is None:
            continue
        delta = chart.get("difference")
        outcome["held_id"] = chart.get("dom_id")
        outcome["held_pill"] = outcome_pill(delta)
        outcome["held_better"] = delta is not None and float(delta) < 0
    return outcomes


def peek_held_summary(charts, limit=48):
    """Compact drawer payload for the largest early-close chart, or None."""
    if not charts:
        return None
    chart = charts[0]
    points = _downsample(chart["points"], limit)
    return {
        "label": chart["label"],
        "expiry_label": chart["expiry_label"],
        "direction_label": chart["direction_label"],
        "account": chart.get("account") or "",
        "show_account": bool(chart.get("show_account")),
        "realized_pnl": chart["realized_pnl"],
        "pnl_if_held": chart["pnl_if_held"],
        "difference": chart["difference"],
        "realized_display": chart["realized_display"],
        "if_held_display": chart["if_held_display"],
        "difference_display": chart["difference_display"],
        "path_note": chart["path_note"],
        "fees_note": chart["fees_note"],
        "points": [
            {"price": p["price"], "kind": p["kind"]}
            for p in points
        ],
    }


def _chart_for_row(row, prices, marks, *, keep, label_for):
    if not _flag(row.get("gradeable_early_close")):
        return None
    realized = _num(row.get("realized_pnl"))
    delta = _num(row.get("early_close_vs_expiry_delta"))
    if_held = pnl_if_held(realized, delta)
    if if_held is None:
        return None
    contracts = _num(row.get("contracts"))
    if contracts is None or contracts <= 0:
        return None
    open_d = _as_date(row.get("open_date"))
    close_d = _as_date(row.get("close_date"))
    exp_d = _as_date(row.get("option_expiry"))
    if open_d is None or close_d is None or exp_d is None:
        return None
    if not (open_d <= close_d < exp_d):
        return None
    if keep is not None and not keep(open_d):
        return None

    strike = _num(row.get("option_strike"))
    option_type = row.get("option_type")
    expiry_intrinsic = _num(row.get("intrinsic_at_expiry"))
    if expiry_intrinsic is None:
        return None
    open_px, close_px = _fill_prices(realized, delta, row, contracts)
    # The close marker is the fill. Fall back to intrinsic only when the
    # cash columns aren't on the row — the chart still has to end the
    # solid segment somewhere.
    if close_px is None:
        close_px = intrinsic_per_share(
            option_type, strike, prices.get(close_d)
        )
    if open_px is None:
        open_px = intrinsic_per_share(
            option_type, strike, prices.get(open_d)
        )
    if open_px is None or close_px is None:
        return None

    tenant = _text(row.get("tenant_id"))
    trade_symbol = _text(row.get("trade_symbol"))
    mark_days = marks.get((tenant, trade_symbol), {})
    points, held_from_marks, held_estimated = _build_points(
        open_d=open_d,
        close_d=close_d,
        exp_d=exp_d,
        open_px=open_px,
        close_px=close_px,
        expiry_intrinsic=expiry_intrinsic,
        option_type=option_type,
        strike=strike,
        contracts=contracts,
        prices=prices,
        mark_days=mark_days,
    )
    if len(points) < 2:
        return None

    label, expiry_label = _contract_label(option_type, strike, exp_d)
    account_raw = _text(row.get("account"))
    account = account_raw
    if label_for is not None:
        try:
            account = _text(label_for(tenant, account_raw)) or account_raw
        except Exception:
            account = account_raw
    direction = _text(row.get("direction"))
    if direction == "Bought":
        direction_label = "Bought to open"
    elif direction == "Sold":
        direction_label = "Sold to open"
    else:
        direction_label = "Opened"
    realized_r = round(realized, 2)
    delta_r = round(delta, 2)
    return {
        "tenant_id": tenant,
        "trade_symbol": trade_symbol,
        "label": label,
        "expiry_label": expiry_label,
        "account": account,
        "direction": direction,
        "direction_label": direction_label,
        "option_type": "call" if str(option_type or "").upper().startswith("C")
        else "put" if str(option_type or "").upper().startswith("P")
        else "option",
        "contracts": int(round(contracts)),
        "strike": None if strike is None else round(strike, 4),
        "open_date": open_d.isoformat(),
        "close_date": close_d.isoformat(),
        "expiry_date": exp_d.isoformat(),
        "open_date_label": _fmt_day(open_d),
        "close_date_label": _fmt_day(close_d),
        "open_price": round(open_px, 4),
        "close_price": round(close_px, 4),
        "expiry_price": round(expiry_intrinsic, 4),
        "open_price_display": _price_display(open_px),
        "close_price_display": _price_display(close_px),
        "expiry_price_display": _price_display(expiry_intrinsic),
        "realized_pnl": realized_r,
        "pnl_if_held": if_held,
        "difference": delta_r,
        "realized_display": _signed_money(realized_r),
        "if_held_display": _signed_money(if_held),
        # Same words as the table pill. A negative warehouse delta means
        # holding finished higher: "$X more if held", not a minus sign.
        "difference_display": outcome_pill(delta_r),
        "held_from_marks": held_from_marks,
        "held_estimated": held_estimated,
        "if_held_estimated": True,
        "held_legend": (
            "While held (estimated)" if held_estimated and not held_from_marks
            else "While held"
        ),
        "if_held_legend": "If held (estimated)",
        "fees_note": FEES_NOTE,
        "path_note": _path_note(held_from_marks, held_estimated),
        "points": points,
    }


def _fill_prices(realized, delta, row, contracts):
    """Per-share open and close premiums from the contract's cash.

    Opening cash is realized minus closing cash. Closing cash is
    ``cost_to_close + proceeds_from_close`` (buy-to-close is negative,
    sell-to-close is positive). The price is the absolute premium.
    """
    cost = _num(row.get("cost_to_close"))
    proceeds = _num(row.get("proceeds_from_close"))
    if cost is None and proceeds is None:
        return None, None
    closing = (cost or 0.0) + (proceeds or 0.0)
    opening = realized - closing
    scale = contracts * CONTRACT_MULTIPLIER
    if scale <= 0:
        return None, None
    return abs(opening) / scale, abs(closing) / scale


def _build_points(*, open_d, close_d, exp_d, open_px, close_px,
                  expiry_intrinsic, option_type, strike, contracts,
                  prices, mark_days):
    spine = [d for d in prices if open_d <= d <= exp_d]
    for anchor in (open_d, close_d, exp_d):
        if anchor not in spine:
            spine.append(anchor)
    spine = sorted(set(spine))

    points = []
    held_from_marks = False
    held_estimated = False
    last_mark = None
    same_day = open_d == close_d
    crosses_year = open_d.year != exp_d.year

    for d in spine:
        spot = prices.get(d)
        intr = intrinsic_per_share(option_type, strike, spot)
        if d == exp_d:
            intr = expiry_intrinsic

        if same_day and d == open_d:
            price, kind, source = close_px, "open_close", "fill"
        elif d == open_d:
            price, kind, source = open_px, "open", "fill"
            # A mark on the open day doesn't replace the fill, but it
            # seeds the carry so the next session isn't a gap.
            fresh = mark_days.get(d)
            if fresh is not None:
                last_mark = fresh
                held_from_marks = True
        elif d == close_d:
            price, kind, source = close_px, "close", "fill"
        elif d < close_d:
            fresh = mark_days.get(d)
            if fresh is not None:
                last_mark = fresh
                held_from_marks = True
                price, source = fresh, "mark"
            elif last_mark is not None:
                price, source = last_mark, "mark"
            elif intr is not None:
                price, source = intr, "intrinsic"
                held_estimated = True
            else:
                continue
            kind = "held"
        elif d == exp_d:
            if intr is None:
                continue
            price, kind, source = intr, "expiry", "intrinsic"
        else:
            if intr is None:
                continue
            price, kind, source = intr, "if_held", "intrinsic"

        price = max(float(price), 0.0)
        if crosses_year:
            tick = f"{d.strftime('%b')} {d.day} '{d.strftime('%y')}"
        else:
            tick = _fmt_day(d)
        points.append({
            "date": d.isoformat(),
            "tick": tick,
            "price": round(price, 4),
            "total": round(price * CONTRACT_MULTIPLIER * contracts, 2),
            "kind": kind,
            "source": source,
        })
    return points, held_from_marks, held_estimated


def _path_note(held_from_marks, held_estimated):
    if held_from_marks and held_estimated:
        lead = (
            "While you held it, recorded option prices are shown where they "
            "exist, and intrinsic value from the stock's close fills the gaps "
            "— an estimate. Open and close are your fill prices. "
        )
    elif held_from_marks:
        lead = (
            "While you held it, the line is the option's recorded price, "
            "with your fill prices at the open and the close. "
        )
    elif held_estimated:
        lead = (
            "Daily option prices weren't recorded, so between the open and "
            "the close the line is intrinsic value from the stock's close "
            "— an estimate — pinned to your fill prices. "
        )
    else:
        lead = (
            "Open and close are your fill prices. Daily option prices "
            "weren't recorded in between. "
        )
    return lead + ESTIMATE_TAIL


def _price_map(df):
    out = {}
    if df is None or getattr(df, "empty", True) or "date" not in getattr(df, "columns", []):
        return out
    price_col = "close_price" if "close_price" in df.columns else None
    if price_col is None:
        return out
    for _, row in df.iterrows():
        d = _as_date(row.get("date"))
        px = _num(row.get(price_col))
        if d is None or px is None:
            continue
        out[d] = float(px)
    return out


def _marks_index(df):
    """{(tenant_id, trade_symbol): {date: per-share premium}}."""
    out = {}
    if df is None or getattr(df, "empty", True):
        return out
    cols = set(getattr(df, "columns", []))
    if "date" not in cols or "trade_symbol" not in cols:
        return out
    for _, row in df.iterrows():
        d = _as_date(row.get("date"))
        symbol = _text(row.get("trade_symbol"))
        if d is None or not symbol:
            continue
        px = option_mark_per_share(
            row.get("current_price") if "current_price" in cols else None,
            row.get("market_value") if "market_value" in cols else None,
            row.get("quantity") if "quantity" in cols else None,
        )
        if px is None:
            continue
        tenant = _text(row.get("tenant_id")) if "tenant_id" in cols else ""
        out.setdefault((tenant, symbol), {})[d] = float(px)
    return out


def _downsample(points, limit):
    if len(points) <= limit:
        return list(points)
    anchors = {
        i for i, p in enumerate(points)
        if p.get("kind") in ("open", "close", "expiry", "open_close")
    }
    step = max(1, int(math.ceil((len(points) - 1) / max(limit - len(anchors), 1))))
    keep = set(anchors)
    keep.update(range(0, len(points), step))
    keep.add(len(points) - 1)
    return [points[i] for i in sorted(keep)]


def _contract_label(option_type, strike, expiry):
    kind = str(option_type or "").strip().upper()
    word = "call" if kind.startswith("C") else "put" if kind.startswith("P") else "option"
    if strike is None:
        strike_txt = ""
    elif float(strike) == int(float(strike)):
        strike_txt = f"${int(float(strike))}"
    else:
        strike_txt = f"${float(strike):g}"
    label = f"{strike_txt} {word}".strip()
    expiry_label = _fmt_long(expiry) if expiry else ""
    return label, expiry_label


def _fmt_day(d):
    return f"{d.strftime('%b')} {d.day}"


def _fmt_long(d):
    return f"{d.strftime('%b')} {d.day}, {d.year}"


def _signed_money(v):
    if v is None:
        return "—"
    dec = 0 if abs(v) >= 100 else 2
    return fmt_money(v, decimals=dec, signed=True)


def _abs_money(v):
    dec = 0 if abs(float(v)) >= 100 else 2
    return fmt_money(abs(float(v)), decimals=dec, signed=False)


def _price_display(v):
    return f"${float(v):,.2f}"


def _num(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(n) or math.isinf(n):
        return None
    return n


def _flag(v):
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "t", "yes")
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    return bool(v)


def _text(v):
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(v).strip()
    if text.lower() == "nan":
        return ""
    return text


def _as_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    ts = pd.to_datetime(v, errors="coerce")
    try:
        if pd.isna(ts):
            return None
    except (TypeError, ValueError):
        return None
    return ts.date()
