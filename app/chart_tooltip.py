"""Compact labels for the cumulative P&L chart tooltip.

The position review keeps its full sentences. The chart dot uses a short
card: date, action, the cash that changed hands, and the running total.

Labels name the fill. They do not call a net result "premium", and they
do not say "collecting" — a credit is a positive amount, a debit is a
negative amount.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)

# How many action lines the card shows before "+N more".
TIP_VISIBLE_MAX = 3

_MINUS = "\u2212"


def whole_dollars(value):
    """Round half away from zero to a whole dollar. None if value isn't a number."""
    if value is None:
        return None
    try:
        num = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not num.is_finite():
        return None
    sign = -1 if num < 0 else 1
    whole = abs(num).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return sign * int(whole)


def format_tip_date(d, today=None):
    """'May 12' in the current year, otherwise 'May 12, 2025'."""
    if not isinstance(d, date):
        return ""
    today = today or date.today()
    label = f"{_MONTHS[d.month - 1]} {d.day}"
    if d.year != today.year:
        return f"{label}, {d.year}"
    return label


def format_tip_amount(value):
    """Signed whole dollars for one fill. '+$3,909' / '−$730'.

    None when the cash rounds to $0 — an expiry or assignment did not
    move cash that day, and the card omits the amount line.
    """
    n = whole_dollars(value)
    if n is None or n == 0:
        return None
    body = f"${abs(n):,}"
    if n > 0:
        return f"+{body}"
    return f"{_MINUS}{body}"


def format_running_total(value):
    """'Running total $19,370' / 'Running total −$1,957'."""
    n = whole_dollars(value)
    if n is None:
        return None
    body = f"${abs(n):,}"
    if n < 0:
        body = f"{_MINUS}{body}"
    return f"Running total {body}"


def format_running_number(value):
    """The number half of the running-total line. '$19,370' / '−$1,957'."""
    phrase = format_running_total(value)
    if not phrase:
        return None
    return phrase[len("Running total "):]


def clip_tips(tips, limit=TIP_VISIBLE_MAX):
    """(shown, hidden_count). Hidden count is what '+2 more' reports."""
    tips = [t for t in (tips or []) if t]
    if limit < 1:
        limit = 1
    if len(tips) <= limit:
        return tips, 0
    return tips[:limit], len(tips) - limit


def present_tip(tip):
    """JSON-ready tip for the chart. Amounts are already display strings."""
    if not tip or not (tip.get("label") or "").strip():
        return None
    out = {
        "label": str(tip["label"]).strip(),
        "kind": tip.get("kind") or "buy",
    }
    shown = format_tip_amount(tip.get("amount"))
    if shown:
        out["amt"] = shown
        try:
            out["sign"] = -1 if float(tip.get("amount")) < 0 else 1
        except (TypeError, ValueError):
            out["sign"] = 1 if shown.startswith("+") else -1
    acct = (tip.get("acct") or "").strip()
    if acct:
        out["acct"] = acct
    return out
