"""Shared currency formatting.

The sign sits in front of the dollar sign. ``"${:,.2f}".format`` on a
negative prints ``$-1,234.56``. Callers (Jinja ``money()`` and the
``htMoney`` JS helper) should both use this shape: ``-$1,318``.
"""


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
