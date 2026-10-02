"""Human-friendly formatting for option contract symbols.

Schwab / OCC delivers option symbols in a packed format that reads like
random characters to a trader. Example:

    PLTR 260424C00141000

Decoded, that's the PLTR April 24, 2026 $141 Call. Showing the raw OCC
string in the UI forces the trader to mentally parse "26 04 24" as a
date and divide the strike by 1000. We surface the decoded form
instead, falling back to the raw string when the input doesn't match
the OCC layout (e.g. plain equity tickers).

The parser tolerates:
- Optional space between the underlying root and the YYMMDD block
  (Schwab includes the space; some sources don't).
- Roots up to 6 chars, with `.` and `-` (e.g. "BRK.B", "BF-B").
- Strikes from $1 to $99999 (8-digit OCC strike field, divisor 1000).
"""
from __future__ import annotations

import re

# OCC option symbol layout: ROOT [space] YYMMDD C|P 8-digit-strike
# `\.\-` lets uncommon roots like BRK.B or BF-B parse cleanly.
_OCC_RE = re.compile(
    r"""^
    (?P<root>[A-Z][A-Z0-9\.\-]{0,5})    # underlying ticker (1-6 chars)
    \s*                                  # Schwab puts a space here; others don't
    (?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})  # YYMMDD expiry
    (?P<cp>[CP])                         # Call or Put
    (?P<strike>\d{8})                    # strike * 1000
    $""",
    re.VERBOSE,
)

# Schwab export "long form": e.g. "PLTR 12/19/2025 95.00 C" or "AAPL 04/30/2026 230 P".
# The web export ships option rows in this human-readable shape, so the parser
# must recognize it directly — otherwise the position detail and weekly review
# render raw strings the trader can't quickly read.
_LONG_RE = re.compile(
    r"""^
    (?P<root>[A-Z][A-Z0-9\.\-]{0,5})       # underlying ticker
    \s+
    (?P<mm>\d{1,2})/(?P<dd>\d{1,2})/(?P<yyyy>\d{4})   # MM/DD/YYYY expiry
    \s+
    (?P<strike>\d+(?:\.\d+)?)              # strike with optional decimals
    \s+
    (?P<cp>[CP])                           # Call or Put
    $""",
    re.VERBOSE,
)

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def parse_occ(symbol):
    """Return a dict of decoded OCC fields, or None when `symbol` isn't
    a recognizable OCC option string. Pure parser — no formatting."""
    if not symbol:
        return None
    s = str(symbol).strip().upper()

    # Schwab "long form" first (the export the demo + most users see).
    long_m = _LONG_RE.match(s)
    if long_m:
        try:
            mm = int(long_m["mm"])
            dd = int(long_m["dd"])
            yyyy = int(long_m["yyyy"])
            strike = float(long_m["strike"])
            if not (1 <= mm <= 12 and 1 <= dd <= 31):
                return None
        except (ValueError, KeyError):
            return None
        return {
            "root": long_m["root"],
            "yy": f"{yyyy % 100:02d}",
            "mm": mm,
            "dd": dd,
            "cp": long_m["cp"],
            "strike": strike,
        }

    # Fall back to the OCC packed form.
    m = _OCC_RE.match(s) or _OCC_RE.match(s.replace(" ", ""))
    if not m:
        return None
    try:
        mm = int(m["mm"])
        dd = int(m["dd"])
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            return None
        strike = int(m["strike"]) / 1000.0
    except (ValueError, KeyError):
        return None
    return {
        "root": m["root"],
        "yy": m["yy"],
        "mm": mm,
        "dd": dd,
        "cp": m["cp"],
        "strike": strike,
    }


_SPREAD_STRIKES = re.compile(
    r"\$(\d+(?:\.\d+)?)\s*/\s*\$(\d+(?:\.\d+)?)([CP])\s*$"
)


def _strike_num(strike) -> str:
    if abs(float(strike) - round(float(strike))) < 1e-6:
        return str(int(round(float(strike))))
    return f"{float(strike):.2f}"


def _qty_prefix(quantity) -> str:
    # Jinja passes Undefined when a row has no quantity key. That is not
    # None, and float() raises UndefinedError rather than TypeError.
    if quantity is None or quantity == "":
        return ""
    try:
        qty = abs(float(quantity))
    except Exception:
        return ""
    if qty < 1e-9:
        return ""
    if abs(qty - round(qty)) < 1e-6:
        return f"{int(round(qty))}× "
    return f"{qty:.4f}".rstrip("0").rstrip(".") + "× "


def compact_contract_label(symbol, quantity=None):
    """Short contract label for the legs table: ``2× $297.50C``.

    A vertical's display symbol (``SPXW Oct 1 '26 $7650 / $7655C``)
    collapses to ``10× 7650/7655C``. The full name stays on the title.
    """
    text = str(symbol or "").strip()
    if not text:
        return ""
    prefix = _qty_prefix(quantity)
    spread = _SPREAD_STRIKES.search(text)
    if spread:
        return (
            f"{prefix}{_strike_num(spread.group(1))}/"
            f"{_strike_num(spread.group(2))}{spread.group(3)}"
        )
    parsed = parse_occ(text)
    if parsed is None:
        return f"{prefix}{text}" if prefix else text
    return f"{prefix}${_strike_num(parsed['strike'])}{parsed['cp']}"


def format_option_symbol(symbol, *, with_ticker=True):
    """Render an OCC symbol as e.g. "PLTR Apr 24 '26 $141C".

    Falls back to returning the input unchanged when it isn't a
    recognizable OCC string (e.g. plain equity tickers, blank cells).
    `with_ticker=False` drops the underlying root so it can be paired
    with a ticker rendered separately (the Position Detail row already
    has a Symbol column, for example).
    """
    if not symbol:
        return ""
    parsed = parse_occ(symbol)
    if parsed is None:
        return str(symbol)

    month = _MONTHS[parsed["mm"] - 1]
    strike = parsed["strike"]
    # Whole-dollar strikes drop the trailing ".00" for readability;
    # fractional strikes (e.g. SPX $4252.50) keep two decimals.
    strike_str = f"${strike:.0f}" if strike == int(strike) else f"${strike:.2f}"
    cp = parsed["cp"]

    # Date format: "Apr 24 '26" — short month, no leading zeros, two-digit year.
    date_part = f"{month} {parsed['dd']} '{parsed['yy']}"
    contract = f"{date_part} {strike_str}{cp}"
    if with_ticker:
        return f"{parsed['root']} {contract}"
    return contract
