"""Shared currency formatting.

The sign sits in front of the dollar sign. ``"${:,.2f}".format`` on a
negative prints ``$-1,234.56``. Callers (Jinja ``money()`` and the
``htMoney`` JS helper) should both use this shape: ``-$1,318``.
"""


# Typographic minus (U+2212). Execution review and the trader profile
# already use this; Insights points go through ``signed_points`` so a
# loss is not an ASCII hyphen.
MINUS = "\u2212"


def signed_points(value, decimals=1, suffix=""):
    """``+10.3`` or ``−10.3``. Zero has no sign. Non-numeric is an em dash."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if number != number:
        return "—"
    places = int(decimals) if decimals is not None else 1
    if places < 0:
        places = 0
    magnitude = f"{abs(number):.{places}f}"
    if number < 0:
        return MINUS + magnitude + suffix
    if number > 0:
        return "+" + magnitude + suffix
    return magnitude + suffix


def fmt_money(v, decimals=2, signed=False):
    """Format ``v`` as ``$1,234.56`` or ``-$1,234.56``.

    ``signed=True`` prefixes a gain with ``+`` (``+$1,234``). Zero stays
    ``$0``. ``None`` and non-numeric values render as an em dash.
    """
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:  # NaN
        return "—"
    try:
        places = int(decimals)
    except (TypeError, ValueError):
        places = 2
    if places < 0:
        places = 0
    spec = "{:,.%df}" % places
    if f < 0:
        return "-$" + spec.format(-f)
    if signed and f > 0:
        return "+$" + spec.format(f)
    return "$" + spec.format(f)
