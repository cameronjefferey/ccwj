"""Settle an expired option from the official close, before the broker line.

Lockstep with ``dbt/models/intermediate/int_option_contracts.sql``. The
warehouse is what the pages read. This function is the same rule so tests
can pin the dollars without a BigQuery build.

After the expiry session (4:00 PM ET, 4:15 PM ET for cash-settled index
roots) an option with no closing activity realizes against the underlying's
official close on the expiry date:

- OTM or ATM: settlement cash is $0. A short keeps the premium; a long
  loses the debit.
- ITM cash-settled index (SPX, SPXW, XSP, NDX, RUT, and the weekly aliases
  plus the other cash roots listed below): settlement cash is intrinsic
  (strike vs close × 100 × contracts). A short pays it; a long receives it.
- ITM equity: option P&L stays the fill cash only. Assignment of the shares
  is the equity line, matching an ``option_assigned`` row. This does not
  invent a share fill.

The row is labeled ``Settled at expiry (est.)`` until a broker close
(expired, as-of, assignment, exercise) arrives. That close already carries
the cash, so the estimate adds nothing on top of it.

A missing official close stays open through the expiry session and does
not book the opening credit. Equity with no close by the next trading
day (Friday → Monday) falls back to the old calendar close: expired at
$0 value, still an estimate, dated on the expiry. Cash-settled index
options never take that $0 fallback. With no official print they stay
``Settlement pending`` and realized P&L stays $0 — an ITM index spread
must not be booked as a worthless win just because ^GSPC was missing.
SPXW prices live under SPX or SPXW (the loader fetches ^GSPC for both);
the caller passes that close (exact symbol, then SPXW→SPX, NDXP→NDX,
RUTW→RUT).

Standard SPX, NDX, RUT, and VIX expiries are AM-settled against a special
opening quotation (SET/VRO), not that day's closing index value. The current
market-data source does not carry those settlement prints, so those roots stay
pending until the broker line arrives. Their PM-settled aliases remain eligible
for the close-based estimate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

ESTIMATE_LABEL = "Settled at expiry (est.)"
PENDING_LABEL = "Settlement pending"

# Cash-settled underlyings. The parenthetical in the product note is
# SPX/SPXW/XSP/NDX/RUT; the weekly aliases and the other cash roots stay
# on this path so they are not treated as a share assignment.
CASH_INDEX_ROOTS = frozenset({
    "SPX", "SPXW", "XSP", "NDX", "NDXP", "RUT", "RUTW",
    "VIX", "DJX", "OEX", "XEO", "RVX",
})

# These roots settle from a special opening quotation that is not the daily
# close in stg_daily_prices. Never substitute the close for SET/VRO.
AM_SETTLED_INDEX_ROOTS = frozenset({"SPX", "NDX", "RUT", "VIX"})

# Used only when the exact symbol has no close. Never joined in a way
# that can also match an SPXW price row.
PARENT_UNDERLYING = {
    "SPXW": "SPX",
    "NDXP": "NDX",
    "RUTW": "RUT",
}

EQUITY_CUTOFF = time(16, 0)
INDEX_CUTOFF = time(16, 15)

_NY = ZoneInfo("America/New_York")


def _root(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _as_et(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now
    return now.astimezone(_NY)


def _as_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def next_trading_day(expiry: date) -> date:
    """The next weekday after ``expiry``. Friday lands on Monday."""
    weekday = expiry.weekday()  # Monday=0 … Sunday=6
    if weekday == 4:
        return expiry + timedelta(days=3)
    if weekday == 5:
        return expiry + timedelta(days=2)
    return expiry + timedelta(days=1)


def calendar_fallback_due(expiry, now_et: datetime) -> bool:
    """True once the next trading day after expiry has started in New York.

    That is the deadline for an official close. Past it, an equity with
    no price row uses the $0 calendar close. A cash index does not.
    """
    expiry_d = _as_date(expiry)
    if expiry_d is None or now_et is None:
        return False
    return _as_et(now_et).date() >= next_trading_day(expiry_d)


def session_is_over(expiry, now_et: datetime, root: str) -> bool:
    """True once the expiry session has finished in New York."""
    expiry_d = _as_date(expiry)
    if expiry_d is None or now_et is None:
        return False
    now = _as_et(now_et)
    if expiry_d < now.date():
        return True
    if expiry_d > now.date():
        return False
    cutoff = INDEX_CUTOFF if _root(root) in CASH_INDEX_ROOTS else EQUITY_CUTOFF
    return now.time() >= cutoff


def intrinsic_per_share(option_type: str, strike, close) -> float:
    if strike is None or close is None:
        return 0.0
    kind = str(option_type or "").strip().upper()
    try:
        strike_f = float(strike)
        close_f = float(close)
    except (TypeError, ValueError):
        return 0.0
    if kind in ("C", "CALL"):
        return max(close_f - strike_f, 0.0)
    if kind in ("P", "PUT"):
        return max(strike_f - close_f, 0.0)
    return 0.0


def settlement_cash(
    *,
    root: str,
    option_type: str,
    direction: str,
    strike,
    close,
    quantity: float,
) -> float:
    """Signed cash to add on top of the opening fills. $0 for equity."""
    if _root(root) not in CASH_INDEX_ROOTS:
        return 0.0
    per = intrinsic_per_share(option_type, strike, close)
    try:
        qty = float(quantity or 0)
    except (TypeError, ValueError):
        qty = 0.0
    cash = per * 100.0 * qty
    if str(direction or "") == "Sold":
        return -cash
    return cash


@dataclass(frozen=True)
class ExpirySettlement:
    settled: bool
    close_type: str | None
    close_date: date | None
    settlement_cash: float
    realized_pnl: float


def settle_expired_option(
    *,
    root: str,
    option_type: str,
    direction: str,
    strike,
    close,
    quantity: float,
    net_cash_flow: float,
    expiry,
    now_et: datetime,
    broker_closed: bool = False,
    opened_before_history: bool = False,
) -> ExpirySettlement:
    """Estimate, or the broker close with no second cash add.

    ``broker_closed`` is a buy-to-close, sell-to-close, expired, assigned,
    or exercised line. ``net_cash_flow`` already includes that cash.
    ``opened_before_history`` has no known cost, so realized stays $0.
    """
    try:
        flows = float(net_cash_flow or 0)
    except (TypeError, ValueError):
        flows = 0.0
    if broker_closed:
        realized = 0.0 if opened_before_history else flows
        return ExpirySettlement(False, None, None, 0.0, realized)
    expiry_d = _as_date(expiry)
    if expiry_d is None or not session_is_over(expiry_d, now_et, root):
        return ExpirySettlement(False, None, None, 0.0, 0.0)
    if _root(root) in AM_SETTLED_INDEX_ROOTS:
        return ExpirySettlement(False, PENDING_LABEL, None, 0.0, 0.0)
    # No price by the next trading day. Equity expires at $0, still an
    # estimate, close dated on the expiry. A cash index stays pending:
    # booking the opening credit is the worthless-ITM win.
    if close is None and calendar_fallback_due(expiry_d, now_et):
        if _root(root) in CASH_INDEX_ROOTS:
            return ExpirySettlement(False, PENDING_LABEL, None, 0.0, 0.0)
        realized = 0.0 if opened_before_history else flows
        return ExpirySettlement(True, ESTIMATE_LABEL, expiry_d, 0.0, realized)
    if close is None or strike is None:
        return ExpirySettlement(False, None, None, 0.0, 0.0)
    cash = settlement_cash(
        root=root,
        option_type=option_type,
        direction=direction,
        strike=strike,
        close=close,
        quantity=quantity,
    )
    realized = 0.0 if opened_before_history else flows + cash
    return ExpirySettlement(True, ESTIMATE_LABEL, expiry_d, cash, realized)
