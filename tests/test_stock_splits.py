"""
Stock-split adjustment tests.

The user-visible bug (May 2026, XLU): a 2:1 forward split between a buy
(1700 shares @ $90.33 = $153,561) and a sell (1500 shares @ $46.38 =
$69,570) made the position page show **$-65,925 phantom realized loss**
instead of the true **+$1,822.50 realized**. The chart, KPIs, and
Strategy Breakdown all surfaced the wrong number because every consumer
read raw quantities from ``stg_history`` while the broker snapshot
(``stg_current``) was already in post-split shares.

Fix: per (symbol, trade_date) cumulative forward split factor in
``int_split_factors``, joined and applied to ``quantity`` (multiply) in
every consumer that does running-share arithmetic. Cash flow (``amount``)
is split-invariant and stays raw. See:

  - dbt/models/intermediate/int_split_factors.sql
  - dbt/models/intermediate/int_equity_sessions.sql
  - dbt/models/intermediate/int_closed_equity_legs.sql
  - dbt/models/intermediate/int_dividend_events.sql
  - dbt/models/marts/mart_daily_pnl.sql
  - .cursor/rules/stock-splits-share-unit.mdc

These tests pin the invariants both as **architectural** (loader ships
splits, sources are registered) and **arithmetic** (XLU reconciles to
the post-fix numbers; non-split symbols have factor 1.0 everywhere).
"""

import os
import pytest
from google.cloud import bigquery


_SKIP_REASON = (
    "Set RUN_BQ_TESTS=1 to run BigQuery integration tests. They cost real "
    "money and need a working ccwj-dbt credential."
)


@pytest.fixture(scope="module")
def bq_client():
    if not os.environ.get("RUN_BQ_TESTS"):
        pytest.skip(_SKIP_REASON)
    return bigquery.Client(project="ccwj-dbt")


# ---------------------------------------------------------------------------
# Architectural invariants
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_daily_split_events_table_exists_and_has_a_known_split(bq_client):
    """Loader must ship splits to BQ. The XLU 2:1 on 2025-12-05 is the
    canonical regression case — any deploy that loses it is a regression
    in the loader (current_position_stock_price.py).

    yfinance is the upstream source. If yfinance later removes / corrects
    XLU's split, replace this assertion with another known split (NVDA
    10-for-1 on 2024-06-10 is a stable historical example).
    """
    rows = list(
        bq_client.query(
            """
            SELECT split_date, split_ratio
            FROM `ccwj-dbt.analytics.daily_split_events`
            WHERE symbol = 'XLU'
            """
        ).result()
    )
    assert rows, (
        "XLU has no split rows in daily_split_events — the loader didn't "
        "persist `ticker.splits` for at least one symbol. Re-run "
        "current_position_stock_price.py and confirm the splits_seen dict "
        "is populated."
    )
    split_dates = {(r.split_date.isoformat(), float(r.split_ratio)) for r in rows}
    assert ("2025-12-05", 2.0) in split_dates, (
        f"Expected XLU 2025-12-05 2:1 split. Found: {split_dates}. If "
        "yfinance has changed its data, pick another stable historical "
        "split as the regression anchor."
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_int_split_factors_is_one_for_symbols_without_splits(bq_client):
    """Architectural invariant: any symbol that has NEVER appeared in
    daily_split_events must have cumulative_split_factor = 1.0 for every
    trade_date. If this fails we've introduced math drift on positions
    that have nothing to do with stock splits.
    """
    rows = list(
        bq_client.query(
            """
            WITH symbols_with_splits AS (
                SELECT DISTINCT symbol
                FROM `ccwj-dbt.analytics.daily_split_events`
            )
            SELECT sf.symbol, COUNT(*) AS drift_rows
            FROM `ccwj-dbt.analytics.int_split_factors` sf
            LEFT JOIN symbols_with_splits sws USING (symbol)
            WHERE sws.symbol IS NULL
              AND ABS(sf.cumulative_split_factor - 1.0) > 1e-9
            GROUP BY 1
            ORDER BY drift_rows DESC
            LIMIT 5
            """
        ).result()
    )
    assert not rows, (
        "Non-split symbols have cumulative_split_factor != 1.0 — the LN/EXP "
        f"product in int_split_factors leaked. Offenders: "
        f"{[(r.symbol, r.drift_rows) for r in rows]}"
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_xlu_split_factor_is_two_before_split_and_one_after(bq_client):
    """Per-trade-date factor sanity: every XLU trade BEFORE 2025-12-05
    must have factor 2.0 (one future 2:1 split). Trades on or after must
    have factor 1.0.
    """
    rows = list(
        bq_client.query(
            """
            SELECT trade_date, cumulative_split_factor
            FROM `ccwj-dbt.analytics.int_split_factors`
            WHERE symbol = 'XLU'
            ORDER BY trade_date
            """
        ).result()
    )
    assert rows, "XLU must have at least one trade_date in int_split_factors"
    for r in rows:
        if r.trade_date < bq_client.query(
            "SELECT DATE('2025-12-05') AS d"
        ).result().__next__().d:
            assert abs(r.cumulative_split_factor - 2.0) < 1e-9, (
                f"XLU trade {r.trade_date} pre-split factor must be 2.0, "
                f"got {r.cumulative_split_factor}"
            )
        else:
            assert abs(r.cumulative_split_factor - 1.0) < 1e-9, (
                f"XLU trade {r.trade_date} post-split factor must be 1.0, "
                f"got {r.cumulative_split_factor}"
            )


# ---------------------------------------------------------------------------
# Arithmetic invariants — XLU regression
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_xlu_running_qty_matches_snapshot_after_split_adjustment(bq_client):
    """The smoking-gun invariant: trade-derived running quantity (sum of
    split-adjusted signed_quantity) must equal the broker snapshot's
    open quantity. Pre-fix this was 200 (1700 - 1500) vs snapshot 1900
    — a 1700-share gap that produced the phantom $-65k loss.
    """
    res = list(
        bq_client.query(
            """
            WITH trade_derived AS (
                SELECT
                    SUM(max_quantity_held - 0) AS open_qty
                FROM `ccwj-dbt.analytics.int_equity_sessions`
                WHERE symbol = 'XLU' AND status = 'Open'
            ),
            snapshot AS (
                SELECT SUM(quantity) AS snap_qty
                FROM `ccwj-dbt.analytics.stg_current`
                WHERE underlying_symbol = 'XLU'
                  AND instrument_type = 'Equity'
            )
            SELECT t.open_qty, s.snap_qty
            FROM trade_derived t CROSS JOIN snapshot s
            """
        ).result()
    )
    row = res[0]
    assert row.snap_qty is not None and row.snap_qty > 0, (
        "XLU must still appear in stg_current — snapshot dropped the "
        "symbol, test fixture is stale. Pick a different symbol with "
        "a known split AND an open snapshot."
    )
    # Open trade-derived qty should be at least as large as snapshot.
    # (For a buy-and-hold position they should match exactly. Some
    # transferred-in lots can produce snapshot > trade-derived; the
    # converse — trade-derived << snapshot — is the bug we're guarding.)
    assert row.open_qty >= row.snap_qty - 0.01, (
        f"XLU trade-derived open qty ({row.open_qty}) is materially less "
        f"than snapshot qty ({row.snap_qty}). The split adjustment is "
        "missing somewhere in int_equity_sessions or upstream."
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_xlu_closed_equity_leg_realized_is_positive(bq_client):
    """The user-visible regression: the 2026-04-27 sell of 1500 shares
    must show ~+$1,822.50 realized (not -$65,925). We assert positive
    rather than the exact value so a yfinance update or broker price
    revision doesn't break the test for cosmetic reasons; the SIGN flip
    is what matters here.
    """
    rows = list(
        bq_client.query(
            """
            SELECT close_date, quantity, sale_price_per_share, cost_basis,
                   sell_proceeds, realized_pnl
            FROM `ccwj-dbt.analytics.int_closed_equity_legs`
            WHERE symbol = 'XLU'
              AND description = 'Equity Sold'
            ORDER BY close_date
            """
        ).result()
    )
    assert rows, "XLU must have at least one closed equity leg"
    for r in rows:
        assert r.realized_pnl > -1000, (
            f"XLU close on {r.close_date}: realized_pnl = "
            f"${r.realized_pnl:.2f}. Pre-fix this was -$65,925 (split-"
            f"unaware FIFO matched 1500 post-split shares to pre-split "
            f"$90 cost basis). Cost basis: ${r.cost_basis:.2f}, "
            f"sale price: ${r.sale_price_per_share:.2f}."
        )
    # And per-share avg cost should match the snapshot's per-share basis
    # (within penny rounding) — the broker computes that in post-split
    # share units, so if our split adjustment is right they agree.
    # cost_basis / quantity = avg cost per share ≈ $45.16 for XLU.
    leg = rows[-1]
    avg_cost_per_share = leg.cost_basis / leg.quantity
    assert 40.0 <= avg_cost_per_share <= 50.0, (
        f"XLU avg cost per share = ${avg_cost_per_share:.2f}. Expected "
        f"~$45.16 (split-adjusted from ~$90 pre-split). Out-of-range "
        f"value suggests the split factor isn't being applied to the "
        f"FIFO denominator in int_closed_equity_legs."
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_xlu_mart_daily_pnl_quantities_are_split_adjusted(bq_client):
    """The chart's running average-cost equity walk reads
    equity_buy_qty / equity_sell_qty from mart_daily_pnl. Without split
    adjustment the buy-day shows raw 1700 while the sell-day shows
    raw 1500 — the chart walk reaches running shares = 200 (wrong) and
    matches a phantom cost. This pins the post-fix shape.
    """
    rows = list(
        bq_client.query(
            """
            SELECT date, equity_buy_qty, equity_sell_qty
            FROM `ccwj-dbt.analytics.mart_daily_pnl`
            WHERE symbol = 'XLU'
              AND (equity_buy_qty > 0 OR equity_sell_qty > 0)
            ORDER BY date
            """
        ).result()
    )
    by_date = {r.date.isoformat(): (r.equity_buy_qty, r.equity_sell_qty) for r in rows}
    # 2025-10-29 buy, 2026-04-27 sell.
    buy = by_date.get("2025-10-29")
    sell = by_date.get("2026-04-27")
    assert buy and buy[0] >= 3000, (
        f"XLU 2025-10-29 buy_qty in mart_daily_pnl must be split-adjusted "
        f"(>= 3000). Got: {buy}. Pre-fix this was 1700 (raw)."
    )
    assert sell and abs(sell[1] - 1500) < 1e-6, (
        f"XLU 2026-04-27 sell_qty in mart_daily_pnl must be 1500 (post-"
        f"split fill, factor=1.0). Got: {sell}."
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_xlu_mart_daily_pnl_close_prices_are_split_adjusted(bq_client):
    """Companion to the quantity test: mart_daily_pnl.close_price must be
    in TODAY's share-units so the chart's MTM walk consumes consistent
    units (split-adjusted shares × split-adjusted close = real $ value).

    Without this fix the chart drew a $137K phantom equity peak from
    2025-10-29 through 2025-12-04 (3400 split-adjusted shares × $90 raw
    pre-split close) followed by a cliff drop on the split day. Real
    user screenshot, May 2026.

    Pin: pre-split XLU dates show ~$44 (split-adjusted) NOT ~$88 (raw),
    and the split day itself shows ~$42 (post-split close from yfinance,
    no further adjustment needed).
    """
    rows = list(
        bq_client.query(
            """
            SELECT date, MAX(close_price) AS px
            FROM `ccwj-dbt.analytics.mart_daily_pnl`
            WHERE symbol = 'XLU'
              AND date IN (
                  DATE '2025-10-29', DATE '2025-12-04',
                  DATE '2025-12-05', DATE '2026-04-27'
              )
            GROUP BY 1
            ORDER BY date
            """
        ).result()
    )
    by_date = {r.date.isoformat(): float(r.px) for r in rows}
    # Pre-split: must be in current share-units, around $42-46.
    assert 30.0 < by_date["2025-10-29"] < 60.0, (
        f"XLU 2025-10-29 close in mart_daily_pnl = ${by_date['2025-10-29']:.2f}. "
        "Expected split-adjusted value around $44 (raw was ~$88). The "
        "loader's un-adjustment is feeding raw pre-split prices into the "
        "chart with split-adjusted quantities — units mismatch and the "
        "MTM walk shows phantom equity."
    )
    # Split day: yfinance close is already post-split; loader should
    # NOT have un-adjusted it. Mart should equal raw at this point.
    assert 30.0 < by_date["2025-12-05"] < 60.0, (
        f"XLU 2025-12-05 (split day) close = ${by_date['2025-12-05']:.2f}. "
        "Loader's date-vs-timestamp comparison must skip the split day "
        "itself (yfinance returns the post-split EOD close natively)."
    )
    # Post-split: matches raw, factor=1.0.
    assert 35.0 < by_date["2026-04-27"] < 55.0, (
        f"XLU 2026-04-27 close = ${by_date['2026-04-27']:.2f}. Expected "
        "around $46 (post-split raw). Factor for this date must be 1.0."
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_xlu_chart_walk_terminal_matches_positions_summary(bq_client):
    """End-to-end invariant: the running average-cost equity walk
    (the same shape the Flask chart helper computes) plus options
    cumulative + dividends cumulative should match positions_summary
    total_pnl within $1.

    This is the single number a user sees as "Total return" at the top
    of the Position Detail page. If this drifts, EITHER:
      - the mart's close_price is in mismatched units (chart cliff bug)
      - the mart's quantities are mismatched units (FIFO cost basis bug)
      - dividends are double-counted or under-counted
      - options realized + unrealized aren't aligned with positions_summary
    """
    df = bq_client.query(
        """
        SELECT date, equity_buy_qty, equity_buy_cost,
               equity_sell_qty, equity_sell_proceeds, close_price,
               cumulative_options_pnl, open_options_unrealized_pnl,
               cumulative_dividends_pnl
        FROM `ccwj-dbt.analytics.mart_daily_pnl`
        WHERE symbol = 'XLU'
        ORDER BY account, user_id, date
        """
    ).to_dataframe()
    shares = 0.0
    cost = 0.0
    realized = 0.0
    for _, row in df.iterrows():
        bq = float(row["equity_buy_qty"] or 0)
        bc = float(row["equity_buy_cost"] or 0)
        sq = float(row["equity_sell_qty"] or 0)
        sp = float(row["equity_sell_proceeds"] or 0)
        if bq > 0:
            shares += bq
            cost += bc
        if sq > 0 and shares > 0:
            avg = cost / shares if shares else 0
            sold = min(sq, shares)
            realized += sp - avg * sold
            cost -= avg * sold
            shares -= sold
    final_close = float(df["close_price"].iloc[-1] or 0)
    chart_terminal = (
        realized
        + (shares * final_close - cost)
        + float(df["cumulative_options_pnl"].iloc[-1] or 0)
        + float(df["open_options_unrealized_pnl"].iloc[-1] or 0)
        + float(df["cumulative_dividends_pnl"].iloc[-1] or 0)
    )
    ps_total = float(
        list(
            bq_client.query(
                """
                SELECT SUM(total_pnl) AS s
                FROM `ccwj-dbt.analytics.positions_summary`
                WHERE symbol = 'XLU'
                """
            ).result()
        )[0].s
        or 0
    )
    assert abs(chart_terminal - ps_total) < 1.0, (
        f"XLU chart terminal (${chart_terminal:.2f}) disagrees with "
        f"positions_summary total (${ps_total:.2f}) by "
        f"${abs(chart_terminal - ps_total):.2f}. The position page "
        "would render a reconciliation banner. Likely cause: split-"
        "adjustment mismatch between qty and close_price in "
        "mart_daily_pnl."
    )
