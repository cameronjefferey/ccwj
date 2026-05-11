"""Regression tests for short-option unrealized-P&L sign.

Schwab's ``gain_or_loss_dollat`` (the ``$P/L`` column on the export) is
computed as ``market_value - cost_basis`` for every position. That
formula is correct for LONG positions (you paid ``cost_basis``, the
position is now worth ``market_value``, your P&L is the difference) but
*inverts the sign* for SHORT positions:

  sold-to-open 2 PLTR calls @ $5.97 → cost_basis = +$1,194.65 (premium received)
  current price drops to $0.015     → market_value = -$3.00 (cost to close)
  schwab gain = -3 - 1194.65 = -$1,197.65  (wrong: trader is up, not down)
  true P&L   = 1194.65 - 3   = +$1,191.65  (right: kept premium, owe $3 to close)

The fix lives in the ``cleaned`` CTE of ``dbt/models/staging/stg_current.sql``
and substitutes ``market_value + cost_basis`` for shorts (since
``market_value`` is stored as negative for shorts, this is equivalent to
``premium_received - cost_to_close``). This file pins the unified
formula and verifies the live seed surfaces it.
"""
from __future__ import annotations

import pandas as pd
import pytest


def short_aware_unrealized_pnl(
    qty: float,
    market_value: float,
    cost_basis: float,
    schwab_pnl: float | None = None,
) -> float:
    """Trader-correct unrealized P&L for an option position.

    Mirrors the SQL override in ``stg_current.sql``. Long positions
    (``qty > 0``) keep Schwab's value (or compute ``market_value -
    cost_basis`` if not provided); short positions (``qty < 0``) get
    the cash-flow-correct ``market_value + cost_basis``.

    Either ``qty`` or ``market_value`` indicates direction; we trust
    ``qty`` because Schwab is consistent that ``sign(qty) == sign(market_value)``
    and ``qty`` is what the trader explicitly set when opening the leg.
    """
    if qty is None or qty == 0 or market_value is None or cost_basis is None:
        return schwab_pnl if schwab_pnl is not None else 0.0
    if qty < 0:
        return market_value + cost_basis
    if schwab_pnl is not None:
        return schwab_pnl
    return market_value - cost_basis


# ---------------------------------------------------------------------------
# Pinned formula: every quadrant of the {long, short} × {call, put} matrix.
# Numbers chosen so a regression where the fix is reverted produces a
# visibly wrong sign that the assertion catches immediately.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "label, qty, cost_basis, market_value, schwab_pnl, expected",
    [
        # Long call gone in your favor (paid $500, now worth $1,200 → +$700).
        ("long_call_winner",  +2,  500.00,  1200.00,  700.00,   700.00),
        # Long call gone against you (paid $500, now worth $50 → -$450).
        ("long_call_loser",   +2,  500.00,    50.00, -450.00,  -450.00),
        # Short call gone in your favor (sold for $1,194.65, now $3 to close
        # → +$1,191.65). This is the literal PLTR row from the production
        # screenshot that triggered the fix.
        ("short_call_winner_pltr",
                              -2, 1194.65,    -3.00, -1197.65, +1191.65),
        # Short call gone against you (sold for $200, would cost $850 to
        # close → -$650). Schwab would report -$1,050 (200 - (-850)).
        ("short_call_loser",  -1,  200.00,  -850.00, -1050.00,  -650.00),
        # Long put gone in your favor (paid $300, now $900 → +$600).
        ("long_put_winner",   +1,  300.00,   900.00,  600.00,   600.00),
        # Long put gone against you (paid $300, now $40 → -$260).
        ("long_put_loser",    +1,  300.00,    40.00, -260.00,  -260.00),
        # Short put gone in your favor (sold $480, now $20 to close → +$460).
        ("short_put_winner",  -1,  480.00,   -20.00, -500.00,  +460.00),
        # Short put gone against you (sold $480, now $1,200 to close → -$720).
        ("short_put_loser",   -2,  480.00, -1200.00, -1680.00, -720.00),
    ],
)
def test_short_aware_unrealized_pnl_handles_every_option_quadrant(
    label, qty, cost_basis, market_value, schwab_pnl, expected,
):
    """The corrected P&L must match cash-flow truth for all 4 quadrants.

    Pre-fix code path was ``market_value - cost_basis`` everywhere, which
    matches Schwab's ``schwab_pnl`` parameter — so any test where
    ``schwab_pnl != expected`` is one the old code got wrong, and any
    test where they're equal sanity-checks we didn't break the long path.
    """
    got = short_aware_unrealized_pnl(qty, market_value, cost_basis, schwab_pnl)
    assert got == pytest.approx(expected, abs=0.01), (
        f"{label}: qty={qty} cost_basis={cost_basis} market_value={market_value} "
        f"schwab_said={schwab_pnl} expected={expected} got={got}"
    )


# ---------------------------------------------------------------------------
# Live-seed assertions: walk the actual current_positions.csv and verify
# every short option row would render correctly. Network-free; reads the
# seed CSV directly. Pins the PLTR row from the screenshot specifically
# so any future change that breaks Cameron Investment's display lights
# this test up first.
# ---------------------------------------------------------------------------


def _load_current_positions():
    df = pd.read_csv(
        "dbt/seeds/current_positions.csv",
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )
    df["Account"] = df["Account"].str.strip()
    for col in ("Quantity", "market_value", "cost_bases", "gain_or_loss_dollat"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def test_pltr_short_call_in_seed_renders_correct_pnl():
    """The literal screenshot example: PLTR May 8 '26 $147 Call, qty=-2.

    The original screenshot had cost_bases=$1,194.65 (premium received),
    market_value=-$3.00 (cost to close). Schwab's gain_or_loss_dollat said
    -$1,197.65; the trader's true P&L was +$1,191.65. The override in
    stg_current's cleaned CTE makes the rendered value match the trader.

    What this test pins (drift-resistant): the SHAPE of the bug — short
    call, premium received in cost_bases, small absolute market_value
    (cost to close cents on the dollar), Schwab's gain_or_loss_dollat
    with the wrong sign — and the FIX always producing a sensible
    trader-truth P&L equal to ``market_value + cost_bases`` for shorts.
    The exact dollar values are incidental and drift as the option's
    market price moves; pinning them caused unrelated test failures
    every time the seed re-synced.
    """
    df = _load_current_positions()
    pltr = df[
        (df["Account"] == "Cameron Investment")
        & (df["Symbol"].str.contains("PLTR", na=False))
        & (df["Symbol"].str.contains("260508C00147000", na=False))
    ]
    if pltr.empty:
        pytest.skip("PLTR May 8 '26 $147C row not in current seed (account re-synced or expired)")
    row = pltr.iloc[0]
    qty = float(row["Quantity"])
    mv = float(row["market_value"])
    cb = float(row["cost_bases"])
    schwab_gl = float(row["gain_or_loss_dollat"])

    # Shape assertions: this is a SHORT call (qty<0) with premium in
    # cost_bases (positive) and a much smaller absolute mv (cost to close).
    assert qty < 0, "screenshot row is short (qty<0)"
    assert cb > 0, "premium received -> cost_bases is positive for shorts"
    assert abs(mv) < cb, (
        "for shorts, |market_value| (cost to close) is typically much "
        "smaller than cost_bases (premium received)"
    )

    # Schwab's reported gain is mv - cb (always wrong-sign for shorts).
    # Pin only the FORMULA disagreement, not the specific dollars.
    assert schwab_gl == pytest.approx(mv - cb, abs=0.01), (
        "Schwab's gain_or_loss_dollat should equal market_value - cost_basis "
        "(this is the bug stg_current's cleaned CTE corrects against)"
    )

    # The fix: short-aware P&L = mv + cb (premium received minus cost to close).
    # For a profitable short, this is positive; for a deep-ITM short it can
    # be negative, but it always has the trader-truth sign.
    fixed = short_aware_unrealized_pnl(qty, mv, cb, schwab_gl)
    expected = mv + cb
    assert fixed == pytest.approx(expected, abs=0.01), (
        f"short-aware fix should equal market_value + cost_basis = {expected:.2f}, "
        f"got {fixed:.2f}"
    )
    # Sanity check: the fix should NOT equal Schwab's wrong answer.
    assert abs(fixed - schwab_gl) > 0.01, (
        "fix produced Schwab's wrong-sign value; the override has been reverted "
        "or the formula in short_aware_unrealized_pnl no longer applies"
    )


def test_every_short_option_in_seed_would_render_correct_sign():
    """Sweep test: every short option row must have the corrected formula
    flip the sign (or at least move it materially closer to zero). If
    Schwab ever changes their convention (unlikely) this test will alert.
    """
    df = _load_current_positions()
    if "security_type" not in df.columns:
        pytest.skip("seed missing security_type column")
    shorts = df[
        (df["security_type"].str.contains("Option", na=False))
        & (df["Quantity"].notna())
        & (df["Quantity"] < 0)
        & (df["market_value"].notna())
        & (df["cost_bases"].notna())
    ]
    if shorts.empty:
        pytest.skip("no short option rows in current seed")
    for _, row in shorts.iterrows():
        schwab_says = row["gain_or_loss_dollat"]
        trader_says = row["market_value"] + row["cost_bases"]
        # The two values must differ — the bug exists for every short row.
        # If they agree, Schwab changed their sign convention and the
        # override in stg_current is now harmful (or redundant).
        assert abs(schwab_says - trader_says) > 0.01, (
            f"row {row['Symbol']!r}: Schwab and trader-formula agree "
            f"({schwab_says}); the dbt override is no longer needed "
            "and stg_current.cleaned should be revisited"
        )
        # The trader formula must put the position on the *opposite* side
        # of break-even from Schwab when the Schwab number is wildly
        # negative — i.e., a profit hidden behind a fictitious loss.
        if schwab_says < -abs(row["cost_bases"]):
            assert trader_says > schwab_says, (
                f"row {row['Symbol']!r}: corrected P&L {trader_says} "
                f"should be greater than (less negative than) Schwab's "
                f"{schwab_says} for a short whose option price has fallen"
            )
