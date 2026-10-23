"""Display helpers for Overview, Today, and the day review.

Whole-dollar figures use the same half-even rounding as ``"{:,.0f}"``.
A multi-account total is the sum of those already-rounded cells, so the
header matches the rows. A value that rounds to zero prints as ``$0``
with no sign. Dates keep the year when they are not in the anchor year.
"""

from datetime import date, datetime

from app.money import MINUS


def _as_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        if value != value:
            return None
    except Exception:
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def whole_dollars(value):
    """Nearest dollar, half-even, matching ``"{:,.0f}"``. None stays None."""
    number = _as_float(value)
    if number is None:
        return None
    return int(f"{number:.0f}")


def signed_whole(value):
    """``+$1,234``, ``−$1,318``, or ``$0``. None is an em dash."""
    n = whole_dollars(value)
    if n is None:
        return "—"
    if n == 0:
        return "$0"
    mag = f"{abs(n):,}"
    if n < 0:
        return f"{MINUS}${mag}"
    return f"+${mag}"


def plain_whole(value):
    """Unsigned ``$1,234``. A negative still uses the typographic minus."""
    n = whole_dollars(value)
    if n is None:
        return "—"
    mag = f"{abs(n):,}"
    if n < 0:
        return f"{MINUS}${mag}"
    return f"${mag}"


def signed_pct(value, places=2):
    """``+1.25%`` / ``−0.40%`` / ``0.00%``. A rounded zero has no sign."""
    number = _as_float(value)
    if number is None:
        return "—"
    try:
        places = int(places)
    except (TypeError, ValueError):
        places = 2
    if places < 0:
        places = 0
    rounded = float(f"{number:.{places}f}")
    mag = f"{abs(rounded):.{places}f}"
    if rounded == 0:
        return f"{mag}%"
    if rounded < 0:
        return f"{MINUS}{mag}%"
    return f"+{mag}%"


def _coerce_day(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


_DATE_STYLES = {
    "short": "%b %-d",
    "dow": "%a %b %-d",
    "long": "%A, %b %-d",
}


def session_date(value, style="short", anchor=None):
    """``Oct 22`` in the anchor year, ``Dec 29, 2025`` otherwise.

    ``style`` is ``short`` (``Oct 22``), ``dow`` (``Wed Oct 22``), or
    ``long`` (``Wednesday, Oct 22``). Unparseable text passes through.
    """
    day = _coerce_day(value)
    if day is None:
        if value is None or value == "":
            return ""
        return str(value)
    label = day.strftime(_DATE_STYLES.get(style, _DATE_STYLES["short"]))
    anchor_day = _coerce_day(anchor) if anchor is not None else date.today()
    year = anchor_day.year if anchor_day is not None else date.today().year
    if day.year != year:
        return f"{label}, {day.year}"
    return label


def mover_parts(movers):
    """Whole-dollar stocks, options, and dividends, plus their sum.

    ``combined_impact`` stays the exact sum in Python. The labeled parts
    on the page are whole dollars, so the impact figure is the sum of
    those whole dollars. Returns None when no part is present.
    """
    getter = getattr(movers, "get", None)
    if getter is None:
        return None

    def part(key):
        raw = getter(key)
        if raw is None:
            return None
        return whole_dollars(raw)

    stocks = part("total_impact")
    options = part("options_impact")
    dividends = part("dividends_impact")
    if stocks is None and options is None and dividends is None:
        return None
    stocks = 0 if stocks is None else stocks
    options = 0 if options is None else options
    dividends = 0 if dividends is None else dividends
    return {
        "stocks": stocks,
        "options": options,
        "dividends": dividends,
        "impact": stocks + options + dividends,
    }


def display_parts(equity, option, dividend):
    """Whole dollars for stock, options, and dividends, and their sum."""
    eq = whole_dollars(equity)
    opt = whole_dollars(option)
    div = whole_dollars(dividend)
    eq = 0 if eq is None else eq
    opt = 0 if opt is None else opt
    div = 0 if div is None else div
    return eq, opt, div, eq + opt + div


def stamp_comparison(comp):
    """Set ``display_delta`` on one day/week/month comparison."""
    if not comp:
        return
    if comp.get("has_data") and comp.get("delta") is not None:
        comp["display_delta"] = whole_dollars(comp.get("delta"))
    else:
        comp["display_delta"] = None


def stamp_snapshot(snap):
    """Whole-dollar value and deltas for one account snapshot row."""
    if not isinstance(snap, dict):
        return
    tv = snap.get("today_value")
    snap["display_value"] = whole_dollars(tv) if tv is not None else None
    comps = snap.get("comparisons") or {}
    for key in ("day", "week", "month"):
        stamp_comparison(comps.get(key))
    stamp_comparison(snap.get("vs_week_start"))


def sum_ints(values):
    """Sum of ints, ignoring None. None when every value is missing."""
    total = 0
    seen = False
    for value in values:
        if value is None:
            continue
        total += int(value)
        seen = True
    return total if seen else None
