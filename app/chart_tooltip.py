"""Compact labels for the cumulative P&L chart tooltip.

The position review keeps its full sentences. The chart dot uses a short
card: date, one line per fill (verb, quantity, signed cash), and the
running total.

Labels name the fill. They do not call a net result "premium", and they
do not say "collecting" — a credit is a positive amount, a debit is a
negative amount. A repeated account name is left off the line. A day
whose lines come from different accounts keeps a short nickname on the
lines that need one.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)

# How many action lines the card shows before "+N more".
TIP_VISIBLE_MAX = 3

_MINUS = "\u2212"

# 'Account 1' and 'Account 2 · Paper' are the privacy mask. They are
# already the label the page is allowed to show, so they are not shortened.
_ACCOUNT_MASK = re.compile(r"^Account(?: \d+)?(?: · Paper)?$")

# A nickname the trader already set, when it fits on one tooltip line.
# Longer broker labels ('Cameron Investment') shorten to the first word
# when that word is unique among the lines on the card.
_SHORT_NICKNAME = 16


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


def _second_token(words):
    """A short second word so two 'Cameron …' lines stay distinct."""
    word = words[1]
    if len(word) <= 4:
        return word
    return word[:3].rstrip(".") + "."


def _short_account(name, peers):
    """Short tag for one line when the card mixes accounts.

    A nickname of 16 characters or fewer stays — that is the name the
    account uses. A longer label keeps its first word when no other line
    starts with that word. The privacy mask and any Paper label stay
    whole so the screen mask and the paper marker are not rewritten.
    """
    name = (name or "").strip()
    if not name:
        return ""
    if _ACCOUNT_MASK.match(name) or "paper" in name.casefold():
        return name
    if len(name) <= _SHORT_NICKNAME:
        return name
    words = name.split()
    if len(words) <= 1:
        return name
    first = words[0]
    for peer in peers:
        peer = (peer or "").strip()
        if not peer or peer == name:
            continue
        if peer.split()[:1] == [first]:
            if len(words) >= 2:
                return f"{first} {_second_token(words)}"
            return name
    return first


def tighten_tip_accounts(tips):
    """Drop a repeated account. Shorten a nickname only on mixed lines.

    One account on the page, or a day whose lines all share one account,
    leaves every line unlabeled. When two nicknames appear, each line
    that has one gets a short tag. A line with no account (a dividend
    on a mixed day) stays unlabeled.
    """
    tips = list(tips or [])
    names = []
    for tip in tips:
        if not tip:
            continue
        acct = (tip.get("acct") or "").strip()
        if acct and acct not in names:
            names.append(acct)
    if len(names) <= 1:
        for tip in tips:
            if tip:
                tip.pop("acct", None)
        return tips
    shorts = {name: _short_account(name, names) for name in names}
    by_short = {}
    for name, short in shorts.items():
        by_short.setdefault(short, []).append(name)
    for group in by_short.values():
        if len(group) > 1:
            for name in group:
                shorts[name] = name
    for tip in tips:
        if not tip:
            continue
        acct = (tip.get("acct") or "").strip()
        if not acct:
            tip.pop("acct", None)
            continue
        tip["acct"] = shorts.get(acct, acct)
    return tips
