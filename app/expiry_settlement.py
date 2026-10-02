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

A missing official close does not settle and does not book the opening
credit. SPXW prices often live under SPX; the caller passes that close
(the model prefers the exact symbol, then SPXW→SPX, NDXP→NDX, RUTW→RUT).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

ESTIMATE_LABEL = "Settled at expiry (est.)"

# Cash-settled underlyings. The parenthetical in the product note is
# SPX/SPXW/XSP/NDX/RUT; the weekly aliases and the other cash roots stay
# on this path so they are not treated as a share assignment.
CASH_INDEX_ROOTS = frozenset({
    "SPX", "SPXW", "XSP", "NDX", "NDXP", "RUT", "RUTW",
    "VIX", "DJX", "OEX", "XEO", "RVX",
})

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
    if (
        close is None
        or strike is None
        or expiry_d is None
        or not session_is_over(expiry_d, now_et, root)
    ):
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
