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
from app.snaptrade import (
    _portal_custom_redirect,
    alpaca_paper_trade_account,
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
SELECT symbol, close_price
FROM (
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        close_price,
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


def _session_open(now=None) -> bool:
    """A same-day option is still the daily trade until the 4:00pm ET close."""
    moment = now or datetime.now(_NY)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_NY)
    local = moment.astimezone(_NY)
    if local.weekday() >= 5:
        return False
    close = local.replace(hour=16, minute=0, second=0, microsecond=0)
    return local < close


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


def practice_receipt(ticket) -> dict:
    """What the learner can look at while the mirror catches up to the order."""
    expiry = date.fromisoformat(ticket["expiry"])
    return {
        "sentence": ticket["sentence"],
        "symbol": ticket["symbol"],
        "side": ticket["side"],
        "strike_label": money_strike(ticket["strike"]),
        "expiry_label": f"{ticket['expiry_label']} · {expiry.strftime('%b %-d')}",
        "limit_label": ticket["limit_label"],
        "cost_label": ticket["cost_label"],
        "cash_settled": bool(ticket.get("cash_settled")),
        "voice": ticket.get("voice") or "beginner",
        "views": ticket.get("views") or {},
        "chain": ticket.get("chain") or [],
        "strike": ticket.get("strike"),
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

    out = []
    for row in get_broker_tenants_for_user(user_id) or []:
        tid = str(row.get("tenant_id") or "")
        name = (row.get("account_name") or "").casefold()
        if not tid or tid.startswith("demo:"):
            continue
        if "alpaca paper" in name:
            out.append(tid)
    return out


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
    """First two bought contracts, and only when this scope is entirely paper."""
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

    safe_symbol = re.sub(r"[^A-Z]", "", str(symbol or "").upper())
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


def trade_views(
    symbol, side, strike, distance, expiry: date, limit_label, cost_label, occ, cash_settled,
    bid_label=None, mid_label=None, ask_label=None,
) -> dict:
    """Three readings of one paper order. Beginner is the lesson. Brokerage is the ticket."""
    side_name = "Call" if side == "call" else "Put"
    when = expiry.strftime("%b %-d, %Y")
    beginner_rows = [
        {
            "label": "Premium per point" if cash_settled else "Premium per share",
            "value": limit_label,
        },
        {"label": "Paper cost for 1 contract", "value": cost_label},
        {
            "label": "Dollars per point" if cash_settled else "Shares this contract covers",
            "value": "$100" if cash_settled else "100",
        },
    ]
    return {
        "beginner": {
            "headline": practice_sentence(symbol, side, strike, distance, expiry),
            "rows": beginner_rows,
            "note": (
                f"This places the trade on the paper account. You pay about {cost_label}. "
                "The premium above is the limit on the order."
            ),
        },
        "intermediate": {
            "headline": (
                f"Buy to open 1 {symbol} {side_name.lower()}, strike {money_strike(strike)}, "
                f"expiring {when}. Limit {limit_label}."
            ),
            "rows": [
                {"label": "Contracts", "value": "1"},
                {"label": "Limit", "value": limit_label},
                {"label": "Estimated debit", "value": cost_label},
                {"label": "Expiration", "value": when},
            ],
            "note": (
                "The limit is the premium for one contract. "
                "Placing it sends this order on the paper account."
            ),
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
                {"label": "Quantity", "value": "1"},
                {"label": "Time in force", "value": "Day"},
            ],
            "note": (
                "Paper. Bid is what a seller will take. Ask is what you pay to buy it. "
                "Mid is halfway between them. The limit is the mid, rounded to a cent."
            ),
        },
    }


def practice_sentence(symbol, side, strike, distance, expiry: date) -> str:
    side_word = "call" if side == "call" else "put"
    if distance == "at":
        where = "about today's price"
    elif distance == "below":
        where = "a bit below today's price"
    else:
        where = "a bit above today's price"
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


def limit_from_quote(raw_premium) -> str | None:
    """Cents string the confirm step and the order both use."""
    if raw_premium is None:
        return None
    try:
        price = Decimal(str(raw_premium))
    except Exception:
        return None
    if price <= 0:
        return None
    cents = price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if cents <= 0:
        return None
    return f"{cents:.2f}"


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
    spy = spots.get("SPY")
    if spy:
        spots["SPX"] = spx_level_from_spy(spy)
    return spots


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
    if not limit_from_quote(raw):
        return None, (
            "We couldn't get a premium for that contract. Pick it again in a moment."
        )
    limit_price = limit_from_quote(shown)
    if not limit_price:
        return None, "No bid and ask for that contract. Pick another strike."
    sentence = practice_sentence(
        selection["symbol"], selection["side"], strike, distance, expiry
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
        ),
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
        selection["symbol"], selection["side"], strike, selection["distance"], expiry
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
    if snaptrade_enabled() and not sent:
        try:
            account = alpaca_paper_trade_account(current_user.id)
        except Exception as exc:
            _log.warning("practice account lookup failed: %s", exc)
            account = None
    return render_template(
        "paper_practice.html",
        title="Practice a trade",
        symbols=symbols,
        expiries=expiries,
        account=account,
        ticket=ticket,
        sent=sent,
        voice=voice,
        snaptrade_ready=snaptrade_enabled(),
        look=look and not account,
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
    session[SENT_KEY] = practice_receipt(ticket)
    queue_account_read_sync(current_user.id, account["row"])
    return redirect(url_for("paper_practice", placed=1))
