"""Negative money is '-$123', never '$-123'."""

import re
from pathlib import Path

import pytest

from app.money import fmt_money


def test_fmt_money_sign_sits_in_front_of_the_dollar():
    assert fmt_money(-1318) == "-$1,318.00"
    assert fmt_money(-1318, 0) == "-$1,318"
    assert fmt_money(-35323, 0) == "-$35,323"
    assert fmt_money(-30042.5) == "-$30,042.50"
    assert fmt_money(12.5) == "$12.50"
    assert fmt_money(0, 0) == "$0"
    assert fmt_money(668, 0, signed=True) == "+$668"
    assert fmt_money(-668, 0, signed=True) == "-$668"
    assert fmt_money(None) == "—"
    assert fmt_money(float("nan")) == "—"
    assert "$-" not in fmt_money(-1)
    assert "$-" not in fmt_money(-1234.5, 2, signed=True)


@pytest.mark.parametrize("value", [-0.4, -0.5, -1, -1234.56, 0, 10])
def test_fmt_money_never_prefixes_dollar_before_a_minus(value):
    rendered = fmt_money(value, 0)
    assert "$-" not in rendered
    assert rendered.startswith("-") or rendered.startswith("$") or rendered == "—"


def test_templates_do_not_prefix_dollar_onto_a_signed_format():
    """`${{ '{:,.0f}'.format(n) }}` prints `$-123` when n is negative.

    A leading `$` is only safe when the formatted value is already an
    absolute magnitude (`|abs` or `.abs`). Signed amounts go through
    ``money()`` / ``htMoney``.
    """
    root = Path(__file__).resolve().parents[1] / "app" / "templates"
    rx = re.compile(
        r"\$\{\{(?P<body>[^}]*?)(?:\.format|\|format)\((?P<arg>[^)]*)\)"
    )
    bad = []
    for path in sorted(root.rglob("*.html")):
        text = path.read_text()
        for match in rx.finditer(text):
            arg = match.group("arg")
            if "|abs" in arg or ".abs" in arg or "abs(" in arg:
                continue
            line = text[: match.start()].count("\n") + 1
            bad.append(f"{path.relative_to(root.parent)}:{line}: {match.group(0)[:80]}")
    assert bad == [], "dollar prefix on a signed format:\n" + "\n".join(bad)


def test_ht_money_matches_python_sign_order():
    script = (Path(__file__).resolve().parents[1] / "app/templates/base.html").read_text()
    assert "var sign = v < 0 ? '-' : (signed && v > 0 ? '+' : '');" in script
    assert "return sign + '$' + abs;" in script
