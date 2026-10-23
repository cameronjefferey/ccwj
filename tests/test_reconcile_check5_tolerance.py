"""CHECK 5 must use the audit's one-cent tolerance, not a raw 0.01 compare.

Run 39743232647 failed Warehouse reconcile audit on a single
positions_summary row (QTUM, Buy and Hold). The printed parts already
differ by one cent, which CHECK 1 accepts via EPS. FLOAT64 makes that
cent look larger than 0.01, so a literal `> 0.01` fails the job.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RECONCILE = ROOT / "scripts/audit/reconcile.py"

# Figures printed by CHECK 5 on run 39743232647.
QTUM_TOTAL = 32516.53
QTUM_REALIZED = 0.0
QTUM_UNREALIZED = 31294.88
QTUM_DIVIDENDS = 1221.66


def _source():
    return RECONCILE.read_text()


def _eps():
    match = re.search(r"^EPS = ([0-9.]+)", _source(), re.M)
    assert match, "reconcile.py must define EPS"
    return float(match.group(1))


def _check5_sql():
    src = _source()
    return src.split("sql5 = ", 1)[1].split("d5 = ", 1)[0]


def test_check5_predicate_uses_shared_eps():
    sql = _check5_sql()
    assert "> {EPS}" in sql
    assert "> 0.01" not in sql


def test_qtum_one_cent_remainder_is_inside_eps_and_outside_raw_0_01():
    """The row that failed run 39743232647."""
    eps = _eps()
    # More than a float ulp, less than two cents. 0.011 is the bound
    # diff() already uses; a looser EPS would hide a real break.
    assert 0.01 < eps < 0.02
    delta = QTUM_TOTAL - (QTUM_REALIZED + QTUM_UNREALIZED + QTUM_DIVIDENDS)
    assert round(delta, 2) == -0.01
    # The predicate that failed the job.
    assert abs(delta) > 0.01
    # The tolerance the rest of the audit already applies.
    assert abs(delta) <= eps


def test_two_cent_break_still_fails_eps():
    eps = _eps()
    assert abs(100.02 - (50.00 + 50.00)) > eps
    assert abs(-0.02) > eps
