"""Practice ticket: one paper call or put after Options 101.

The learner picks the trade in lesson words. HappyTrader sends it as one
Alpaca Paper order through SnapTrade, then the existing sync reads it
back. This module does not keep a paper ledger and does not talk to
Alpaca directly.

Learn (a separate branch) can link to ``paper_practice`` later. Selling
a covered call is a later ticket, only once the paper account holds at
least 100 shares.
"""
from __future__ import annotations

import logging
import re
import secrets
import threading
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from flask import flash, jsonify, redirect, render_template, request, session, url_for
from flask_login import current_user, login_required

from app import app
from app.bigquery_client import get_bigquery_client
from app.plan import plan_block_writes
from app.query_cache import cached_query_df
from app.paper_accounts import position_link_symbol
from app.snaptrade import (
    _portal_custom_redirect,
    account_buying_power,
    alpaca_paper_trade_account,
    cancel_account_order,
    list_account_recent_orders,
    open_connection_portal,
    place_single_leg_option_order,
    practice_portal_login_kwargs,
    queue_account_read_sync,
    quote_option_premium,
    remember_portal_session,
    snaptrade_enabled,
    _get_snaptrade_client,
)
from app.utils import demo_block_writes

_log = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")
TICKET_KEY = "paper_practice_ticket"
SENT_KEY = "paper_practice_sent"
ORDERS_KEY = "paper_practice_orders"
LOOK_KEY = "paper_practice_look"
VOICE_KEY = "paper_practice_voice"
VOICES = ("beginner", "intermediate", "brokerage")

SYMBOLS = ("SPY", "QQQ", "SPX")
STRIKE_STEP = {"SPY": 1, "QQQ": 1, "SPX": 5}
# Daily and weekly SPX options are the PM-settled SPXW series.
_OPTION_ROOT = {"SPX": "SPXW"}
# The warehouse has no SPX close. SPY is sized at one tenth of the index.
_SPX_PER_SPY = 10
SIDES = ("call", "put")
DISTANCES = ("below", "at", "above")
TENORS = ("soon", "weekly", "multi")
# All three expire every weekday. SPX dailies are listed; AAPL is not.
_EVERY_WEEKDAY = (0, 1, 2, 3, 4)
_LISTED_WEEKDAYS = {symbol: _EVERY_WEEKDAY for symbol in SYMBOLS}
CONTRACT_SHARES = 100

# Latest close for the three practice underlyings. Public market data
# (stg_daily_prices is stamped per account, so the row number collapses
# it). No tenant column — do not run this frame through the tenant filter.
_SPOTS_SQL = """
SELECT symbol, close_price, price_date
FROM (
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        close_price,
        date AS price_date,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(symbol))
            ORDER BY date DESC
        ) AS rn
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE UPPER(TRIM(symbol)) IN ('SPY', 'QQQ')
      AND close_price IS NOT NULL
      AND close_price > 0
)
WHERE rn = 1
"""

# Penny-pilot names quote in $0.01. Other equity options use the
# $0.05 / $0.10 schedule. SPX / SPXW use that same schedule.
_PENNY_PILOT = {"SPY", "QQQ"}
_OPEN_ORDER_STATUSES = {
    "PENDING", "OPEN", "ACCEPTED", "QUEUED", "SUBMITTED", "NEW",
    "PARTIAL", "PARTIALLY_FILLED",
}

_order_lock = threading.Lock()
_last_order_at: dict[str, float] = {}


def market_today(now=None) -> date:
    moment = now or datetime.now(_NY)
    if isinstance(moment, datetime) and moment.tzinfo is None:
        moment = moment.replace(tzinfo=_NY)
    if isinstance(moment, datetime):
        return moment.astimezone(_NY).date()
    return moment


def _next_weekday(day: date) -> date:
    nxt = day + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def _next_listed(day: date, weekdays) -> date:
    cursor = day
    while cursor.weekday() not in weekdays:
        cursor += timedelta(days=1)
    return cursor


def _soon_label(symbol: str, expiry: date) -> str:
    """Daily when the symbol expires every weekday. Otherwise the weekday."""
    listed = _LISTED_WEEKDAYS[symbol]
    if len(listed) == 5:
        return "Daily"
    return expiry.strftime("%A")


def option_root(symbol: str) -> str:
    return _OPTION_ROOT.get(symbol, symbol)


def contract_blurb(symbol: str) -> str:
    if symbol == "SPX":
        return "SPX pays cash. One contract is $100 per point, not 100 shares."
    return "One contract is 100 shares."


def spx_level_from_spy(spy_close: float) -> float:
    return float(spy_close) * _SPX_PER_SPY


def _pill_text(choice) -> str:
    return f"{choice['label']} · {choice['date'].strftime('%b %-d')}"


def _ny_now(now=None) -> datetime:
    moment = now or datetime.now(_NY)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_NY)
    return moment.astimezone(_NY)


def _session_open(now=None) -> bool:
    """A same-day option is still the daily trade until the 4:00pm ET close."""
    local = _ny_now(now)
    if local.weekday() >= 5:
        return False
    close = local.replace(hour=16, minute=0, second=0, microsecond=0)
    return local < close


def regular_session_open(now=None) -> bool:
    """Regular US cash session, 9:30–16:00 ET. Premarket and after hours are closed."""
    local = _ny_now(now)
    if local.weekday() >= 5:
        return False
    opened = local.replace(hour=9, minute=30, second=0, microsecond=0)
    closed = local.replace(hour=16, minute=0, second=0, microsecond=0)
    return opened <= local < closed


def next_session_date(now=None) -> date:
    """The session an order placed now would wait for, or today while the session is open."""
    local = _ny_now(now)
    day = local.date()
    if local.weekday() < 5 and local < local.replace(hour=16, minute=0, second=0, microsecond=0):
        return day
    return _next_weekday(day)


def session_wait_note(expiry=None, now=None) -> str | None:
    """Closed-market copy. None while the regular session is open."""
    if regular_session_open(now):
        return None
    text = "The market is closed. This order waits for the next session."
    if isinstance(expiry, date) and expiry == next_session_date(now):
        text += " This option expires next session."
    return text


def expiry_choices(day: date, symbol: str, *, session_open: bool = True) -> dict:
    """Nearest listed expiration, a later Friday, and a Friday two weeks after.

    The three dates are never the same day. The nearest label is Daily
    only when that symbol expires every weekday.
    """
    start = day if (day.weekday() < 5 and session_open) else _next_weekday(day)
    soon = _next_listed(start, _LISTED_WEEKDAYS[symbol])
    weekly = soon + timedelta(days=(4 - soon.weekday()) % 7)
    if weekly <= soon:
        weekly = soon + timedelta(days=7)
    multi = weekly + timedelta(days=14)
    choices = {
        "soon": {"date": soon, "label": _soon_label(symbol, soon)},
        "weekly": {"date": weekly, "label": "Weekly"},
        "multi": {"date": multi, "label": "Multi-week"},
    }
    for choice in choices.values():
        choice["pill"] = _pill_text(choice)
    return choices


def nearest_strike(spot: float, step: int) -> int:
    strike = int(Decimal(str(spot)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    # Round to the nearest listed increment, then keep it positive.
    snapped = int(round(strike / step) * step)
    return snapped if snapped > 0 else step


def chain_strikes(spot: float, step: int, width: int = 4) -> list[int]:
    """A window of listed strikes around the price. Width is strikes each side."""
    at = nearest_strike(spot, step)
    return [at + step * offset for offset in range(-width, width + 1)]


def strike_choices(spot: float, step: int) -> dict[str, int]:
    at = nearest_strike(spot, step)
    below = at - step
    if below <= 0:
        below = at
        at = at + step
    return {"below": below, "at": at, "above": at + step}


def occ_symbol(root: str, expiry: date, side: str, strike: int) -> str:
    """21-character OCC symbol. Root is space-padded to 6."""
    padded = root.strip().upper().ljust(6)
    if len(padded) != 6:
        raise ValueError(f"option root must fit in 6 characters: {root}")
    cp = "C" if side == "call" else "P"
    millis = int(strike) * 1000
    if millis < 0 or millis > 99_999_999:
        raise ValueError(f"strike does not fit OCC: {strike}")
    return f"{padded}{expiry.strftime('%y%m%d')}{cp}{millis:08d}"


def money(amount) -> str:
    return f"${Decimal(str(amount)):,.2f}"


def money_strike(amount: int) -> str:
    return f"${int(amount):,}"


def practice_receipt(ticket, tenant_id=None) -> dict:
    """What the learner can look at while the mirror catches up to the order."""
    expiry = date.fromisoformat(ticket["expiry"])
    views = _views_as_placed(ticket.get("views") or {}, ticket)
    symbol = ticket["symbol"]
    return {
        "sentence": ticket["sentence"],
        "symbol": symbol,
        "link_symbol": position_link_symbol(symbol),
        "side": ticket["side"],
        "strike_label": money_strike(ticket["strike"]),
        "expiry_label": f"{ticket['expiry_label']} · {expiry.strftime('%b %-d')}",
        "limit_label": ticket["limit_label"],
        "cost_label": ticket["cost_label"],
        "cash_settled": bool(ticket.get("cash_settled")),
        "voice": ticket.get("voice") or "beginner",
        "views": views,
        "chain": ticket.get("chain") or [],
        "strike": ticket.get("strike"),
        "status": "open",
        "status_label": "Open",
        "brokerage_order_id": ticket.get("brokerage_order_id") or "",
        "occ": ticket.get("occ") or "",
        "tenant_id": tenant_id or ticket.get("tenant_id") or "",
    }


PAPER_READOUT_SQL = """
WITH prices AS (
    SELECT symbol, date, MIN(close_price) AS close_price
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    GROUP BY symbol, date
)
SELECT
    c.tenant_id,
    c.underlying_symbol,
    c.option_type,
    c.direction,
    c.option_strike,
    c.option_expiry,
    c.status,
    c.premium_paid,
    c.contracts_bought_to_open,
    c.net_cash_flow,
    c.open_date,
    p.close_price AS finish_price
FROM `ccwj-dbt.analytics.int_option_contracts` c
LEFT JOIN prices p
    ON p.symbol = c.underlying_symbol
   AND p.date = c.option_expiry
WHERE c.contracts_bought_to_open > 0
  {tenant_filter}
  {symbol_filter}
ORDER BY c.open_date, c.underlying_symbol
LIMIT 2
"""


def money_spot(amount) -> str:
    n = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if n == n.to_integral_value():
        return f"${int(n):,}"
    return f"${n:,.2f}"


def _when_label(expiry: date) -> str:
    return expiry.strftime("%a %b %-d").replace("  ", " ")


def _paper_tenant_ids(user_id) -> list[str]:
    """Alpaca Paper tenants this user owns. The demo mirror is not one of them."""
    from app.models import get_broker_tenants_for_user
    from app.paper_accounts import is_paper_row

    out = []
    for row in get_broker_tenants_for_user(user_id) or []:
        tid = str(row.get("tenant_id") or "")
        if tid and is_paper_row(row):
            out.append(tid)
    return out


def _readout_symbol(symbol) -> str:
    """Ticker characters only. Digits and a class dot stay (BRK.B)."""
    return re.sub(r"[^A-Z0-9.]", "", str(symbol or "").upper())[:12]


def beginner_trade_sentence(row) -> str | None:
    """One paper buy, in the shape: made money if X, the stock went to Y, therefore."""
    direction = str(row.get("direction") or "")
    if direction.casefold() != "bought":
        return None
    try:
        contracts = Decimal(str(row.get("contracts_bought_to_open")))
        paid = abs(Decimal(str(row.get("premium_paid"))))
        strike = Decimal(str(row.get("option_strike")))
    except Exception:
        return None
    if contracts <= 0 or paid <= 0 or strike <= 0:
        return None
    per = paid / (contracts * CONTRACT_SHARES)
    kind = str(row.get("option_type") or "")
    side = "put" if kind[:1].upper() == "P" else "call"
    symbol = str(row.get("underlying_symbol") or "").upper()
    if not symbol:
        return None
    line = strike - per if side == "put" else strike + per
    where = "below" if side == "put" else "above"
    n = int(contracts) if contracts == contracts.to_integral_value() else contracts
    expiry = row.get("option_expiry")
    when = ""
    if hasattr(expiry, "strftime"):
        when = f", expiring {expiry.strftime('%b %-d')}"
    bought = (
        f"You bought {n} {symbol} {side} at {money_spot(strike)}{when}. "
        f"You made money if {symbol} finished {where} {money_spot(line)}."
    )
    finish = row.get("finish_price")
    closed = str(row.get("status") or "").casefold() == "closed"
    try:
        finish_n = Decimal(str(finish)) if finish is not None and str(finish) not in ("", "None", "nan") else None
    except Exception:
        finish_n = None
    if finish_n is None or finish_n <= 0:
        if closed:
            return bought + f" We don't have where {symbol} finished."
        return (
            f"You bought {n} {symbol} {side} at {money_spot(strike)}{when}. "
            f"You make money if {symbol} finishes {where} {money_spot(line)}. "
            "Therefore this trade is still open."
        )
    went = f"{symbol} finished at {money_spot(finish_n)}."
    made_it = finish_n < line if side == "put" else finish_n > line
    try:
        net = Decimal(str(row.get("net_cash_flow")))
    except Exception:
        net = Decimal(0)
    if made_it and net > 0:
        therefore = f"Therefore this trade made {money_spot(net)}."
    elif made_it:
        therefore = f"Therefore {symbol} finished {where} {money_spot(line)}."
    else:
        therefore = f"Therefore the {side} expired, and the {money_spot(paid)} is gone."
    return f"{bought} {went} {therefore}"


def beginner_readouts(tenant_ids, symbol=None) -> list[dict]:
    """First two bought contracts, and only when this scope is entirely paper.

    A Postgres or warehouse miss returns no readout. It must not 500 the page.
    """
    try:
        return _beginner_readouts(tenant_ids, symbol)
    except Exception as exc:
        _log.warning("beginner readout failed: %s", exc)
        return []


def _beginner_readouts(tenant_ids, symbol=None) -> list[dict]:
    if _remember_voice() != "beginner":
        return []
    if not tenant_ids or not getattr(current_user, "is_authenticated", False):
        return []
    paper = set(_paper_tenant_ids(current_user.id))
    scope = set(tenant_ids)
    if not scope or not scope <= paper:
        return []
    from app.bigquery_client import get_bigquery_client
    from app.query_cache import cached_query_df
    from app.routes import _filter_df_by_tenant_ids, _tenant_sql_and

    safe_symbol = _readout_symbol(symbol)
    symbol_filter = f"AND c.underlying_symbol = '{safe_symbol}'" if safe_symbol else ""
    sql = PAPER_READOUT_SQL.format(
        tenant_filter=_tenant_sql_and(list(scope), col="c.tenant_id"),
        symbol_filter=symbol_filter,
    )
    try:
        frame = cached_query_df(get_bigquery_client(), sql, label="paper_beginner_readout")
    except Exception as exc:
        _log.warning("beginner readout failed: %s", exc)
        return []
    frame = _filter_df_by_tenant_ids(frame, list(scope))
    if frame is None or getattr(frame, "empty", True):
        return []
    out = []
    for rec in frame.to_dict(orient="records"):
        text = beginner_trade_sentence(rec)
        if text:
            out.append({"text": text, "symbol": rec.get("underlying_symbol")})
    return out


def _remember_voice() -> str:
    """Beginner, intermediate, or brokerage. The order underneath does not change."""
    raw = (
        request.values.get("voice")
        or request.cookies.get("pp_voice")
        or session.get(VOICE_KEY)
        or "beginner"
    )
    voice = str(raw).strip().lower()
    if voice not in VOICES:
        voice = "beginner"
    session[VOICE_KEY] = voice
    return voice


def _share_math(limit_label, cost_label, cash_settled) -> str:
    unit = "point" if cash_settled else "share"
    return (
        f"The limit is the price per {unit}. "
        f"One contract is 100 {unit}s, so {limit_label} x 100 = {cost_label}."
    )


def _order_notes(limit_label, cost_label, cash_settled, placed: bool) -> dict:
    math = _share_math(limit_label, cost_label, cash_settled)
    cash_note = " SPX is cash-settled." if cash_settled else ""
    unit = "point" if cash_settled else "share"
    brokerage = (
        "Paper. Bid is the highest price a buyer is paying "
        "(what you would get selling now). Ask is the lowest price a seller "
        "will take (what you would pay buying now). Mid is halfway between them. "
        f"The limit is per {unit}, rounded to the option's tick. "
    )
    if placed:
        brokerage += f"Status: sent. Total cost {cost_label}.{cash_note}"
        return {
            "beginner": (
                f"This order was sent to the paper account. You pay about {cost_label}. {math}"
            ),
            "intermediate": (
                f"{math} This order was sent on the paper account.{cash_note}"
            ),
            "brokerage": brokerage,
        }
    brokerage += f"Total cost {cost_label}.{cash_note}"
    return {
        "beginner": (
            f"This reviews the trade on the paper account. You would pay about {cost_label}. {math}"
        ),
        "intermediate": (
            f"{math} Placing it sends this order on the paper account.{cash_note}"
        ),
        "brokerage": brokerage,
    }


def _views_as_placed(views, ticket) -> dict:
    """Receipt copy is past tense. The review ticket stays in the present."""
    if not views:
        return {}
    notes = _order_notes(
        ticket.get("limit_label") or "",
        ticket.get("cost_label") or "",
        bool(ticket.get("cash_settled")),
        True,
    )
    out = {}
    for name, view in views.items():
        view = dict(view or {})
        if name in notes:
            view["note"] = notes[name]
        rows = []
        for row in view.get("rows") or []:
            row = dict(row)
            if row.get("label") in {"Premium per share", "Premium per point"}:
                row["label"] = (
                    "Price per point" if ticket.get("cash_settled") else "Price per share"
                )
            rows.append(row)
        view["rows"] = rows
        out[name] = view
    return out


def trade_views(
    symbol, side, strike, distance, expiry: date, limit_label, cost_label, occ, cash_settled,
    bid_label=None, mid_label=None, ask_label=None, as_of_label=None, placed=False,
) -> dict:
    """Three readings of one paper order. Beginner is the lesson. Brokerage is the ticket."""
    side_name = "Call" if side == "call" else "Put"
    when = expiry.strftime("%b %-d, %Y")
    price_label = "Price per point" if cash_settled else "Price per share"
    notes = _order_notes(limit_label, cost_label, cash_settled, placed)
    beginner_rows = [
        {"label": price_label, "value": limit_label},
        {"label": "Cost for 1 contract", "value": cost_label},
        {
            "label": "Dollars per point" if cash_settled else "Shares this contract covers",
            "value": "$100" if cash_settled else "100",
        },
    ]
    return {
        "beginner": {
            "headline": practice_sentence(
                symbol, side, strike, distance, expiry, as_of_label=as_of_label,
            ),
            "rows": beginner_rows,
            "note": notes["beginner"],
        },
        "intermediate": {
            "headline": (
                f"Buy to open 1 {symbol} {side_name.lower()}, strike {money_strike(strike)}, "
                f"expiring {when}. Limit {limit_label}."
            ),
            "rows": [
                {"label": "Contracts", "value": "1"},
                {"label": price_label, "value": limit_label},
                {"label": "Total cost", "value": cost_label},
                {"label": "Expiration", "value": when},
            ],
            "note": notes["intermediate"],
        },
        "brokerage": {
            "headline": f"{symbol}  {expiry.strftime('%-d %b %y').upper()}",
            "rows": [
                {"label": "Order", "value": f"Buy to open 1 {side_name}"},
                {"label": "Strike", "value": money_strike(strike)},
                {"label": "Bid", "value": bid_label or "—"},
                {"label": "Mid", "value": mid_label or "—"},
                {"label": "Ask", "value": ask_label or "—"},
                {"label": "Limit", "value": limit_label},
                {"label": "Total cost", "value": cost_label},
                {"label": "Quantity", "value": "1"},
                {"label": "Time in force", "value": "Day"},
            ],
            "note": notes["brokerage"],
        },
    }


def practice_sentence(symbol, side, strike, distance, expiry: date, as_of_label=None) -> str:
    side_word = "call" if side == "call" else "put"
    quoted = as_of_label or "the latest close"
    if distance == "at":
        where = f"about {quoted}"
    elif distance == "below":
        where = f"a bit below {quoted}"
    else:
        where = f"a bit above {quoted}"
    if symbol == "SPX":
        right = "the cash value above the strike" if side == "call" else "the cash value below the strike"
        cover = "This one pays cash. One contract is $100 per point, not 100 shares."
    else:
        right = "buy the shares" if side == "call" else "sell the shares"
        cover = f"One contract is {CONTRACT_SHARES} shares."
    return (
        f"Buy 1 {symbol} {side_word} at {money_strike(strike)}, {where}. "
        f"It expires {_when_label(expiry)}. {cover} "
        f"You are paying for the right to {right}, and you can let it expire."
    )


def option_tick(symbol: str, price: Decimal) -> Decimal:
    """Minimum price increment for one option limit."""
    root = str(symbol or "").strip().upper()
    listed = option_root(root)
    if root in {"SPX", "SPXW"} or listed in {"SPX", "SPXW"}:
        return Decimal("0.05") if price < 3 else Decimal("0.10")
    if root in _PENNY_PILOT:
        return Decimal("0.01")
    return Decimal("0.05") if price < 3 else Decimal("0.10")


def round_to_tick(price: Decimal, symbol: str) -> Decimal:
    tick = option_tick(symbol, price)
    units = (price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    snapped = units * tick
    return snapped.quantize(tick)


def limit_from_quote(raw_premium, symbol: str = "SPY") -> str | None:
    """Limit string the confirm step and the order both use, on a valid tick."""
    if raw_premium is None:
        return None
    try:
        price = Decimal(str(raw_premium))
    except Exception:
        return None
    if price <= 0:
        return None
    snapped = round_to_tick(price, symbol)
    if snapped <= 0:
        return None
    return f"{snapped:.2f}"


def contract_cost(limit_price: str) -> Decimal:
    return (Decimal(limit_price) * CONTRACT_SHARES).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def _claim_order_slot(account_id: str) -> bool:
    """SnapTrade asks for about one trade request per second per account."""
    now = time.monotonic()
    with _order_lock:
        prev = _last_order_at.get(account_id, 0.0)
        if now - prev < 1.0:
            return False
        _last_order_at[account_id] = now
        return True


def latest_spots() -> dict[str, float]:
    try:
        client = get_bigquery_client()
    except Exception as exc:
        _log.warning("practice spots: no BigQuery client: %s", exc)
        return {}
    if client is None:
        return {}
    try:
        frame = cached_query_df(client, _SPOTS_SQL, label="paper_practice_spots")
    except Exception as exc:
        _log.warning("practice spots query failed: %s", exc)
        return {}
    spots = {}
    if frame is None or getattr(frame, "empty", True):
        return spots
    for _, row in frame.iterrows():
        symbol = str(row.get("symbol") or "").upper()
        if symbol not in SYMBOLS:
            continue
        try:
            price = float(row.get("close_price"))
        except (TypeError, ValueError):
            continue
        if price > 0:
            spots[symbol] = price
            label = _close_label(row.get("price_date"))
            if label:
                _remember_spot_label(symbol, label)
    spy = spots.get("SPY")
    if spy:
        spots["SPX"] = spx_level_from_spy(spy)
        spy_label = spot_as_of_label("SPY")
        if spy_label != "the latest close":
            _remember_spot_label("SPX", spy_label)
    return spots


def _close_label(raw) -> str | None:
    """Name the close we actually used. A date with no clock stays a close."""
    day = None
    if hasattr(raw, "date") and not isinstance(raw, date):
        try:
            day = raw.date()
        except Exception:
            day = None
    elif isinstance(raw, date):
        day = raw
    else:
        text = str(raw or "")[:10]
        try:
            day = date.fromisoformat(text)
        except ValueError:
            day = None
    if day is None:
        return None
    return f"the {day.strftime('%b')} {day.day} close"


def _remember_spot_label(symbol: str, label: str) -> None:
    try:
        from flask import g
        labels = dict(getattr(g, "paper_spot_labels", {}) or {})
        labels[symbol] = label
        g.paper_spot_labels = labels
    except Exception:
        return


def spot_as_of_label(symbol: str | None = None) -> str:
    try:
        from flask import g
        labels = getattr(g, "paper_spot_labels", {}) or {}
    except Exception:
        labels = {}
    if symbol and labels.get(symbol):
        return labels[symbol]
    if labels.get("SPY"):
        return labels["SPY"]
    return "the latest close"


_CHAIN_CACHE: dict = {}
_CHAIN_LOCK = threading.Lock()
_CHAIN_TTL = 60.0


def _px(price) -> str | None:
    """A chain price. Keeps a half-cent so the mid can sit between bid and ask."""
    if price is None:
        return None
    try:
        amount = Decimal(str(price))
    except Exception:
        return None
    if amount <= 0:
        return None
    shown = amount.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    text = f"{shown:.3f}"
    if text.endswith("0"):
        text = f"{shown:.2f}"
    return text


def quote_triplet(bid, ask) -> dict:
    """Bid, mid, and ask for one contract. Mid is halfway between the two."""
    bid_txt = _px(bid)
    ask_txt = _px(ask)
    mid_txt = None
    if bid_txt and ask_txt:
        mid_txt = _px((Decimal(bid_txt) + Decimal(ask_txt)) / 2)
    return {"bid": bid_txt, "mid": mid_txt, "ask": ask_txt}


def _chain_cache_key(symbol, expiry: date):
    return (symbol, expiry.isoformat())


def _chain_from_cache(symbol, expiry: date):
    key = _chain_cache_key(symbol, expiry)
    now = time.monotonic()
    with _CHAIN_LOCK:
        hit = _CHAIN_CACHE.get(key)
    if not hit or now - hit[0] > _CHAIN_TTL:
        return None
    return hit[1]


def load_option_chain(symbol, expiry: date, strikes: list[int]) -> list[dict]:
    """Bid, mid, and ask for a strike window. Public option quotes, not a tenant read."""
    ticker = "^SPX" if symbol == "SPX" else symbol
    try:
        import yfinance as yf
        chain = yf.Ticker(ticker).option_chain(expiry.isoformat())
    except Exception as exc:
        _log.warning("practice chain failed for %s %s: %s", symbol, expiry, exc)
        raise
    wanted = set(strikes)
    sided = {strike: {"call": None, "put": None} for strike in strikes}
    for side, frame in (("call", chain.calls), ("put", chain.puts)):
        if frame is None or getattr(frame, "empty", True):
            continue
        for rec in frame.itertuples(index=False):
            try:
                strike = int(round(float(rec.strike)))
            except (TypeError, ValueError):
                continue
            if strike not in wanted:
                continue
            sided[strike][side] = quote_triplet(rec.bid, rec.ask)
    rows = [
        {"strike": strike, "call": sided[strike]["call"], "put": sided[strike]["put"]}
        for strike in strikes
    ]
    with _CHAIN_LOCK:
        _CHAIN_CACHE[_chain_cache_key(symbol, expiry)] = (time.monotonic(), rows)
    return rows


def _side_quote(chain, strike, side) -> dict:
    for row in chain or []:
        if int(row.get("strike") or 0) == int(strike):
            quote = row.get(side) or {}
            return quote if isinstance(quote, dict) else {}
    return {}


def _selection_from_form():
    symbol = (request.form.get("symbol") or "").strip().upper()
    side = (request.form.get("side") or "").strip().lower()
    tenor = (request.form.get("tenor") or "").strip().lower()
    distance = (request.form.get("distance") or "").strip().lower()
    strike_raw = (request.form.get("strike") or "").strip()
    strike = None
    if strike_raw:
        try:
            strike = int(strike_raw)
        except ValueError:
            return None
        if strike <= 0:
            return None
    if symbol not in SYMBOLS or side not in SIDES or tenor not in TENORS:
        return None
    if strike is None and distance not in DISTANCES:
        return None
    picked = {"symbol": symbol, "side": side, "tenor": tenor, "distance": distance}
    if strike is not None:
        picked["strike"] = strike
    return picked


def _build_ticket(selection, spots, account, today=None):
    spot = spots.get(selection["symbol"])
    if not spot:
        return None, "We don't have today's price for those shares yet."
    expiries = expiry_choices(
        today or market_today(), selection["symbol"], session_open=_session_open(),
    )
    expiry = expiries[selection["tenor"]]["date"]
    step = STRIKE_STEP[selection["symbol"]]
    choices = strike_choices(spot, step)
    if selection.get("strike"):
        strike = int(selection["strike"])
        if strike == choices["at"]:
            distance = "at"
        elif strike < choices["at"]:
            distance = "below"
        else:
            distance = "above"
    else:
        distance = selection["distance"]
        strike = choices[distance]
    occ = occ_symbol(option_root(selection["symbol"]), expiry, selection["side"], strike)
    symbol = selection["symbol"]
    try:
        chain = _chain_from_cache(symbol, expiry) or load_option_chain(
            symbol, expiry, chain_strikes(spot, step),
        )
    except Exception as exc:
        _log.warning("practice chain failed: %s", exc)
        chain = []
    quote = _side_quote(chain, strike, selection["side"])
    shown = quote.get("mid") or quote.get("ask") or quote.get("bid")
    try:
        raw = quote_option_premium(
            current_user.id, account["snaptrade_account_id"], occ
        )
    except Exception as exc:
        _log.warning("practice option quote failed: %s", exc)
        raw = None
    if not limit_from_quote(raw, symbol):
        return None, (
            "We couldn't get a price for that contract. Pick it again in a moment."
        )
    limit_price = limit_from_quote(shown, symbol)
    if not limit_price:
        return None, "No bid and ask for that contract. Pick another strike."
    as_of = spot_as_of_label(symbol)
    sentence = practice_sentence(
        selection["symbol"], selection["side"], strike, distance, expiry,
        as_of_label=as_of,
    )
    return {
        "nonce": secrets.token_urlsafe(16),
        "symbol": selection["symbol"],
        "side": selection["side"],
        "tenor": selection["tenor"],
        "distance": distance,
        "strike": strike,
        "chain": chain,
        "expiry": expiry.isoformat(),
        "expiry_label": expiries[selection["tenor"]]["label"],
        "occ": occ,
        "limit_price": limit_price,
        "cost": str(contract_cost(limit_price)),
        "cost_label": money(contract_cost(limit_price)),
        "limit_label": money(limit_price),
        "sentence": sentence,
        "account_id": account["snaptrade_account_id"],
        "spot": spot,
        "cash_settled": selection["symbol"] == "SPX",
        "look": False,
        "voice": _remember_voice(),
        "views": trade_views(
            selection["symbol"],
            selection["side"],
            strike,
            distance,
            expiry,
            money(limit_price),
            money(contract_cost(limit_price)),
            occ,
            selection["symbol"] == "SPX",
            bid_label=f"${quote['bid']}" if quote.get("bid") else None,
            mid_label=f"${quote['mid']}" if quote.get("mid") else None,
            ask_label=f"${quote['ask']}" if quote.get("ask") else None,
            as_of_label=as_of,
        ),
        "session_note": session_wait_note(expiry),
    }, None


def _build_look_ticket(selection, spots, today=None):
    """Same ticket words, with no quote and no order.

    Used when the learner wants to click the flow and the paper account
    is not connected. Nothing is sent to SnapTrade.
    """
    spot = spots.get(selection["symbol"])
    if not spot:
        return None, "We don't have today's price for those shares yet."
    expiries = expiry_choices(
        today or market_today(), selection["symbol"], session_open=_session_open(),
    )
    expiry = expiries[selection["tenor"]]["date"]
    strike = strike_choices(spot, STRIKE_STEP[selection["symbol"]])[selection["distance"]]
    sentence = practice_sentence(
        selection["symbol"], selection["side"], strike, selection["distance"], expiry,
        as_of_label=spot_as_of_label(selection["symbol"]),
    )
    return {
        "nonce": secrets.token_urlsafe(16),
        "symbol": selection["symbol"],
        "side": selection["side"],
        "tenor": selection["tenor"],
        "distance": selection["distance"],
        "strike": strike,
        "expiry": expiry.isoformat(),
        "expiry_label": expiries[selection["tenor"]]["label"],
        "occ": occ_symbol(option_root(selection["symbol"]), expiry, selection["side"], strike),
        "cash_settled": selection["symbol"] == "SPX",
        "limit_price": None,
        "cost": None,
        "cost_label": None,
        "limit_label": None,
        "sentence": sentence,
        "account_id": None,
        "spot": spot,
        "look": True,
    }, None


def _order_accepted(body) -> bool:
    if not isinstance(body, dict):
        return False
    status = str(body.get("status") or "").upper()
    if status in {"FAILED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED"}:
        return False
    if body.get("brokerage_order_id"):
        return True
    orders = body.get("orders") or []
    if isinstance(orders, list):
        for order in orders:
            if isinstance(order, dict) and (
                order.get("brokerage_order_id") or order.get("status")
            ):
                order_status = str(order.get("status") or "").upper()
                if order_status in {"FAILED", "REJECTED"}:
                    return False
                return True
    return False


def order_status_bucket(raw) -> str:
    status = str(raw or "").strip().upper().replace(" ", "_").replace("-", "_")
    if status in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED"}:
        return "filled"
    if status in {"CANCELED", "CANCELLED"}:
        return "cancelled"
    if status in {"REJECTED", "FAILED"}:
        return "rejected"
    if status == "EXPIRED":
        return "expired"
    if status in _OPEN_ORDER_STATUSES:
        return "open"
    return "unknown"


def _status_label(bucket: str, raw: str) -> str:
    labels = {
        "open": "Open",
        "filled": "Filled",
        "cancelled": "Cancelled",
        "cancel_requested": "Cancel requested",
        "rejected": "Rejected",
        "expired": "Expired",
    }
    return labels.get(bucket) or (str(raw or "Unknown").replace("_", " ").title() or "Unknown")


def _brokerage_order_id(body) -> str:
    if not isinstance(body, dict):
        return ""
    if body.get("brokerage_order_id"):
        return str(body["brokerage_order_id"])
    for order in body.get("orders") or []:
        if isinstance(order, dict) and order.get("brokerage_order_id"):
            return str(order["brokerage_order_id"])
    return ""


def _underlying_from_order(order) -> str:
    opt = order.get("option_symbol") if isinstance(order, dict) else None
    text = ""
    if isinstance(opt, dict):
        underlying = opt.get("underlying_symbol")
        if isinstance(underlying, dict):
            text = str(
                underlying.get("symbol")
                or underlying.get("raw_symbol")
                or underlying.get("description")
                or ""
            )
        elif underlying:
            text = str(underlying)
        if not text:
            text = str(opt.get("ticker") or opt.get("id") or "")
    elif isinstance(opt, str):
        text = opt
    if not text and isinstance(order, dict):
        universal = order.get("universal_symbol") or {}
        if isinstance(universal, dict):
            text = str(universal.get("symbol") or universal.get("raw_symbol") or "")
        elif universal:
            text = str(universal)
    token = re.sub(r"[^A-Z0-9.]", "", text.upper())
    if token.startswith("SPXW") or token == "SPX":
        return "SPXW"
    match = re.match(r"[A-Z0-9.]{1,12}", token)
    return match.group(0) if match else ""


def normalize_broker_order(order) -> dict | None:
    if not isinstance(order, dict):
        return None
    raw = str(order.get("status") or "")
    bucket = order_status_bucket(raw)
    symbol = _underlying_from_order(order)
    return {
        "brokerage_order_id": str(order.get("brokerage_order_id") or ""),
        "status": bucket,
        "status_label": _status_label(bucket, raw),
        "raw_status": raw,
        "symbol": symbol,
        "link_symbol": position_link_symbol(symbol),
        "cancelable": bucket == "open" and bool(order.get("brokerage_order_id")),
        "sentence": "",
        "source": "broker",
    }


def _session_orders() -> list:
    raw = session.get(ORDERS_KEY) or []
    return [item for item in raw if isinstance(item, dict)]


def _remember_order(receipt) -> None:
    """Every placed ticket stays visible. A repeated brokerage id replaces itself."""
    kept = []
    new_id = receipt.get("brokerage_order_id") or ""
    for item in _session_orders():
        if new_id and item.get("brokerage_order_id") == new_id:
            continue
        kept.append(item)
    kept.insert(0, receipt)
    session[ORDERS_KEY] = kept[:20]


def merged_paper_orders(user_id) -> list[dict]:
    """Broker orders plus anything just placed that SnapTrade has not listed yet."""
    live = []
    account = None
    try:
        account = alpaca_paper_trade_account(user_id)
    except Exception as exc:
        _log.warning("paper order account lookup failed: %s", exc)
        account = None
    if account:
        try:
            raw_orders = list_account_recent_orders(
                user_id, account["snaptrade_account_id"],
            )
        except Exception as exc:
            _log.warning("paper order list failed: %s", exc)
            raw_orders = []
        for raw in raw_orders or []:
            item = normalize_broker_order(raw)
            if item:
                live.append(item)
    by_id = {item["brokerage_order_id"]: item for item in live if item.get("brokerage_order_id")}
    merged = list(live)
    for local in _session_orders():
        local_id = str(local.get("brokerage_order_id") or "")
        if local_id and local_id in by_id:
            broker = by_id[local_id]
            if not broker.get("sentence"):
                broker["sentence"] = local.get("sentence") or ""
            if not broker.get("symbol"):
                broker["symbol"] = local.get("symbol") or ""
                broker["link_symbol"] = position_link_symbol(broker["symbol"])
            # A still-open broker row must not hide a cancel we already sent.
            if (
                str(local.get("status") or "") == "cancel_requested"
                and broker.get("status") == "open"
            ):
                broker["status"] = "cancel_requested"
                broker["status_label"] = local.get("status_label") or "Cancel requested"
                broker["cancelable"] = False
            continue
        row = dict(local)
        row["status"] = row.get("status") or "open"
        row["status_label"] = row.get("status_label") or "Open"
        row["cancelable"] = bool(row.get("brokerage_order_id")) and row["status"] == "open"
        row["link_symbol"] = position_link_symbol(row.get("link_symbol") or row.get("symbol"))
        row["source"] = "session"
        merged.insert(0, row)
    return merged


def paper_orders_for_page(user_id, symbol=None) -> list[dict]:
    """Orders for one position page. SPX and SPXW are the same contract root."""
    wanted = position_link_symbol(symbol) if symbol else ""
    out = []
    for order in merged_paper_orders(user_id):
        link = position_link_symbol(order.get("link_symbol") or order.get("symbol"))
        if wanted and link != wanted:
            continue
        out.append(order)
    return out


def _write_blocked(action: str):
    blocked = demo_block_writes(action)
    if blocked:
        return blocked
    blocked = plan_block_writes(action)
    if blocked:
        return blocked
    from app.auth import email_block_writes
    return email_block_writes(action)


def _choice_view(spots, today=None, *, session_open: bool = True):
    day = today or market_today()
    view = []
    default = None
    for symbol in SYMBOLS:
        spot = spots.get(symbol)
        strikes = strike_choices(spot, STRIKE_STEP[symbol]) if spot else None
        expiries = expiry_choices(day, symbol, session_open=session_open)
        if spot and default is None:
            default = expiries
        view.append({
            "symbol": symbol,
            "spot": spot,
            "spot_label": money_spot(spot) if spot else None,
            "strikes": strikes,
            "expiries": expiries,
            "contract_blurb": contract_blurb(symbol),
        })
    return view, default or view[0]["expiries"]


@app.route("/practice", methods=["GET"])
@login_required
def paper_practice():
    """The ticket Learn can link to. Prices are today's closes, not a sketch."""
    session.pop(LOOK_KEY, None)
    look = False
    placed = (request.args.get("placed") or "") == "1"
    confirming = (request.args.get("confirm") or "") == "1"
    sent = session.get(SENT_KEY) if placed else None
    ticket = session.get(TICKET_KEY) if confirming and not placed else None
    if ticket and ticket.get("look"):
        session.pop(TICKET_KEY, None)
        ticket = None
    if not confirming:
        session.pop(TICKET_KEY, None)
        ticket = None
    if not placed:
        session.pop(SENT_KEY, None)
        sent = None
    voice = _remember_voice()
    if ticket:
        ticket["voice"] = voice
        session[TICKET_KEY] = ticket
    if sent:
        sent["voice"] = voice
        session[SENT_KEY] = sent
    spots = {} if sent else latest_spots()
    symbols, expiries = _choice_view(spots, session_open=_session_open())
    account = None
    buying_power_label = None
    if snaptrade_enabled():
        try:
            account = alpaca_paper_trade_account(current_user.id)
        except Exception as exc:
            _log.warning("practice account lookup failed: %s", exc)
            account = None
        if account:
            try:
                power = account_buying_power(
                    current_user.id, account["snaptrade_account_id"],
                )
            except Exception as exc:
                _log.warning("practice buying power failed: %s", exc)
                power = None
            if power is not None:
                buying_power_label = money(power)
    orders = []
    try:
        orders = merged_paper_orders(current_user.id)
    except Exception as exc:
        _log.warning("practice orders failed: %s", exc)
        orders = _session_orders()
    session_note = None
    if ticket and ticket.get("session_note"):
        session_note = ticket["session_note"]
    elif not regular_session_open():
        session_note = session_wait_note()
    return render_template(
        "paper_practice.html",
        title="Practice a trade",
        symbols=symbols,
        expiries=expiries,
        account=account,
        ticket=ticket,
        sent=sent,
        orders=orders,
        voice=voice,
        snaptrade_ready=snaptrade_enabled(),
        look=look and not account,
        buying_power_label=buying_power_label,
        session_note=session_note,
    )


@app.route("/practice/connect", methods=["POST"])
@login_required
def paper_practice_connect():
    """Open Alpaca Paper with trading access. Does not change read-only connects."""
    blocked = _write_blocked("opening a paper account")
    if blocked:
        return blocked
    client = _get_snaptrade_client()
    if not client:
        flash("Paper practice is not configured yet. Contact the administrator.", "danger")
        return redirect(url_for("paper_practice"))
    user_id = current_user.id
    try:
        # open_connection_portal replaces user id and secret after register.
        login_kwargs = practice_portal_login_kwargs(
            {"snaptrade_user_id": "pending", "snaptrade_secret": "pending"},
            _portal_custom_redirect(),
        )
        portal_url = open_connection_portal(client, user_id, login_kwargs)
    except Exception as exc:
        _log.exception("Practice portal failed for user_id=%s: %s", user_id, exc)
        flash(
            "Couldn't open the paper-account sign-in. Try again, or "
            "contact support if it keeps happening.",
            "danger",
        )
        return redirect(url_for("paper_practice"))
    remember_portal_session(user_id, practice=True)
    return redirect(portal_url)


@app.route("/practice/chain", methods=["GET"])
@login_required
def paper_practice_chain():
    """Bid, mid, and ask for the strike window."""
    symbol = (request.args.get("symbol") or "").strip().upper()
    tenor = (request.args.get("tenor") or "").strip().lower()
    if symbol not in SYMBOLS or tenor not in TENORS:
        return jsonify(error="Pick the shares and an expiration."), 400
    spots = latest_spots()
    spot = spots.get(symbol)
    if not spot:
        return jsonify(error="No price for those shares yet."), 400
    try:
        account = alpaca_paper_trade_account(current_user.id)
    except Exception as exc:
        _log.warning("practice chain account lookup failed: %s", exc)
        account = None
    if not account:
        return jsonify(error="Sign in to the paper account first."), 400
    expiry = expiry_choices(
        market_today(), symbol, session_open=_session_open(),
    )[tenor]["date"]
    strikes = chain_strikes(spot, STRIKE_STEP[symbol])
    cached = _chain_from_cache(symbol, expiry)
    try:
        rows = cached or load_option_chain(symbol, expiry, strikes)
    except Exception:
        return jsonify(error="Couldn't load bid and ask. Try again in a moment."), 400
    return jsonify(
        symbol=symbol,
        expiry=expiry.isoformat(),
        expiry_label=expiry.strftime("%-d %b %y").upper(),
        strikes=rows,
    )


@app.route("/practice/review", methods=["POST"])
@login_required
def paper_practice_review():
    blocked = _write_blocked("reviewing a paper trade")
    if blocked:
        return blocked
    selection = _selection_from_form()
    if not selection:
        flash("Pick a call or a put, the shares, an expiration, and a strike.", "warning")
        return redirect(url_for("paper_practice"))
    if _remember_voice() == "brokerage" and not selection.get("strike"):
        flash("Pick a call or a put on the chain.", "warning")
        return redirect(url_for("paper_practice"))
    account = None
    try:
        account = alpaca_paper_trade_account(current_user.id)
    except Exception as exc:
        _log.warning("practice account lookup failed: %s", exc)
        account = None
    if not account:
        flash("Sign in to the paper account before placing a trade.", "warning")
        return redirect(url_for("paper_practice"))
    ticket, err = _build_ticket(selection, latest_spots(), account)
    if err or not ticket:
        flash(err or "Couldn't review that trade.", "warning")
        return redirect(url_for("paper_practice"))
    session[TICKET_KEY] = ticket
    session.pop(SENT_KEY, None)
    return redirect(url_for("paper_practice", confirm=1))


@app.route("/practice/place", methods=["POST"])
@login_required
def paper_practice_place():
    """Send the confirmed ticket. A missing confirm never places an order."""
    blocked = _write_blocked("placing a paper trade")
    if blocked:
        return blocked
    ticket = session.get(TICKET_KEY)
    nonce = (request.form.get("nonce") or "").strip()
    if not ticket or not nonce or nonce != ticket.get("nonce"):
        flash("Review the trade again before placing it.", "warning")
        return redirect(url_for("paper_practice"))
    account = alpaca_paper_trade_account(current_user.id)
    if not account or account["snaptrade_account_id"] != ticket.get("account_id"):
        session.pop(TICKET_KEY, None)
        flash("The paper account changed. Review the trade again.", "warning")
        return redirect(url_for("paper_practice"))
    if not _claim_order_slot(account["snaptrade_account_id"]):
        flash("Wait a second, then place this paper trade again.", "warning")
        return redirect(url_for("paper_practice", confirm=1))
    limit_price = ticket["limit_price"]
    occ = ticket["occ"]
    try:
        body = place_single_leg_option_order(
            current_user.id,
            account["snaptrade_account_id"],
            occ,
            limit_price,
        )
    except Exception as exc:
        _log.exception("Practice option order failed for user_id=%s: %s", current_user.id, exc)
        flash(
            "The paper account did not take that order. Nothing was filled here. "
            "Review it and try again.",
            "danger",
        )
        return redirect(url_for("paper_practice", confirm=1))
    if not _order_accepted(body):
        _log.warning(
            "Practice option order refused for user_id=%s status=%s",
            current_user.id, (body or {}).get("status") if isinstance(body, dict) else None,
        )
        flash(
            "The paper account refused that option order. Nothing was filled here.",
            "danger",
        )
        return redirect(url_for("paper_practice", confirm=1))
    session.pop(TICKET_KEY, None)
    ticket = dict(ticket)
    ticket["brokerage_order_id"] = _brokerage_order_id(body if isinstance(body, dict) else {})
    receipt = practice_receipt(ticket, tenant_id=account.get("tenant_id"))
    session[SENT_KEY] = receipt
    _remember_order(receipt)
    queue_account_read_sync(current_user.id, account["row"])
    return redirect(url_for("paper_practice", placed=1))


@app.route("/practice/orders/cancel", methods=["POST"])
@login_required
def paper_practice_cancel():
    """Cancel one open order. The account id comes from the server, not the form."""
    blocked = _write_blocked("cancelling a paper order")
    if blocked:
        return blocked
    order_id = (request.form.get("brokerage_order_id") or "").strip()
    if not order_id or len(order_id) > 128:
        flash("That order can't be cancelled.", "warning")
        return redirect(url_for("paper_practice"))
    try:
        account = alpaca_paper_trade_account(current_user.id)
    except Exception as exc:
        _log.warning("paper cancel account lookup failed: %s", exc)
        account = None
    if not account:
        flash("Only an open order on your paper account can be cancelled.", "warning")
        return redirect(url_for("paper_practice"))
    known_open = set()
    try:
        live = list_account_recent_orders(
            current_user.id, account["snaptrade_account_id"],
        )
    except Exception as exc:
        _log.warning("paper cancel list failed: %s", exc)
        live = []
    live_bucket = None
    for raw in live or []:
        item = normalize_broker_order(raw)
        if not item or item.get("brokerage_order_id") != order_id:
            continue
        live_bucket = item["status"]
        if item["cancelable"]:
            known_open.add(order_id)
    session_open_ids = {
        str(item.get("brokerage_order_id") or "")
        for item in _session_orders()
        if item.get("status", "open") == "open"
    }
    if live_bucket and live_bucket != "open":
        flash(f"That order is already {live_bucket}.", "warning")
        return redirect(url_for("paper_practice"))
    if order_id not in known_open and order_id not in session_open_ids:
        flash("That order is not an open order on your paper account.", "warning")
        return redirect(url_for("paper_practice"))
    try:
        cancel_account_order(
            current_user.id,
            account["snaptrade_account_id"],
            order_id,
        )
    except Exception as exc:
        _log.exception("Paper cancel failed for user_id=%s: %s", current_user.id, exc)
        flash("The paper account did not cancel that order.", "danger")
        return redirect(url_for("paper_practice"))
    reported = _refetch_order_status(
        current_user.id, account["snaptrade_account_id"], order_id,
    )
    if reported and reported.get("status") != "open":
        bucket = reported["status"]
        label = reported.get("status_label") or _status_label(bucket, "")
    else:
        bucket = "cancel_requested"
        label = "Cancel requested"
    updated = []
    found = False
    for item in _session_orders():
        if str(item.get("brokerage_order_id") or "") == order_id:
            item = dict(item)
            item["status"] = bucket
            item["status_label"] = label
            item["cancelable"] = False
            found = True
        updated.append(item)
    if not found:
        updated.insert(0, {
            "brokerage_order_id": order_id,
            "status": bucket,
            "status_label": label,
            "cancelable": False,
            "symbol": (reported or {}).get("symbol") or "",
            "link_symbol": (reported or {}).get("link_symbol") or "",
        })
    session[ORDERS_KEY] = updated[:20]
    if bucket == "cancel_requested":
        flash(
            "Cancel requested. Refresh to see when the paper account reports it.",
            "success",
        )
    else:
        flash("Cancel sent to the paper account.", "success")
    return redirect(url_for("paper_practice"))


def _refetch_order_status(user_id, account_id, order_id) -> dict | None:
    """One read after cancel. None when the order is missing or the read failed."""
    try:
        live = list_account_recent_orders(user_id, account_id) or []
    except Exception as exc:
        _log.warning("paper cancel refetch failed: %s", exc)
        return None
    for raw in live:
        item = normalize_broker_order(raw)
        if item and item.get("brokerage_order_id") == order_id:
            return item
    return None
