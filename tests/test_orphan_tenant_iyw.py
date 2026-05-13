"""Regression tests for the IYW Emmory Investment failure mode (May 2026).

Two stacked bugs produced a Position Detail page with:
  * Two phantom "Leg 1" pills (one Closed -$1,957, one Open $387)
  * A "Dividend Closed -$1,956.91" strategy row
  * Hero $396.67 vs chart terminal $-1,957 (reconciliation invariant
    tripped)

Root causes:

1) FLOAT-PRECISION SESSION BOUNDARY.  `int_equity_sessions` and
   `int_closed_equity_legs` detect new sessions via the predicate
   `prev_running_qty = 0 AND running_qty > 0`. With FLOAT64 share
   counts, a perfectly closed round-trip can leave running_qty at
   `-0.000000000000000000` (≈ -1e-17), not exactly 0. The strict
   equality fails, the next buy doesn't open a new session, and an
   entire 2025 round-trip + a fresh 2025-12-30 lot get fused into
   `session_id = 1` with closed-loss math even though the 12/30 lot
   is genuinely open. Fix: epsilon-tolerant zero check (1e-9).

2) STALE-UID ORPHAN-TENANT SPLIT.  Trade history landed under an old
   user_id (uid=2 in this case) while the broker's current snapshot
   landed under the canonical user_id (uid=9). Marts partition by
   (account, user_id), so sells and snapshot lived in different
   partitions. Fix: cross-reference `stg_current` ∪
   `stg_account_balances` from `stg_history` to re-stamp non-NULL
   stale uids in trade history. (The existing rule only handled the
   NULL → populated direction.)

These tests run against live BigQuery (skipped by default; set
RUN_BQ_TESTS=1 to enable). They pin the post-fix shape so a
regression on either bug surfaces the IYW symptom we already burned
hours on.
"""
from __future__ import annotations

import os

import pytest


_SKIP_REASON = (
    "Orphan-tenant integration tests against live BigQuery. "
    "Set RUN_BQ_TESTS=1 to enable."
)


@pytest.fixture(scope="module")
def bq_client():
    if not os.environ.get("RUN_BQ_TESTS"):
        pytest.skip(_SKIP_REASON)
    from google.cloud import bigquery

    return bigquery.Client(project="ccwj-dbt")


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_iyw_emmory_has_exactly_two_sessions(bq_client):
    """Float-precision regression: a fully-closed 2.0023-share round-trip
    in 2025 followed by a fresh 10-share buy on 2025-12-30 must split
    into TWO sessions in `int_equity_sessions`, not one fused session.

    Pre-fix the strict ``prev_running_qty = 0`` check failed on the
    -1e-17 IEEE 754 artifact and both halves landed in session_id=1.
    Post-fix the epsilon zero check (1e-9) lets the next buy start
    session_id=2.
    """
    rows = list(
        bq_client.query(
            """
            SELECT session_id, status, open_date, last_trade_date,
                   max_quantity_held, num_trades, total_pnl
            FROM `ccwj-dbt.analytics.int_equity_sessions`
            WHERE account = 'Emmory Investment'
              AND symbol = 'IYW'
            ORDER BY session_id
            """
        ).result()
    )

    assert len(rows) == 2, (
        f"Expected exactly 2 sessions for Emmory IYW (1 closed round-trip "
        f"+ 1 open lot), got {len(rows)}: {[(r.session_id, r.status) for r in rows]}"
    )

    closed = next(r for r in rows if r.status == "Closed")
    open_ = next(r for r in rows if r.status == "Open")

    assert closed.session_id == 1
    assert open_.session_id == 2, (
        f"The 2025-12-30 buy must start session_id=2, got "
        f"{open_.session_id} (float-precision regression?)"
    )
    assert closed.last_trade_date.isoformat() == "2025-12-29", (
        f"Closed session must end on the last sell date (2025-12-29), "
        f"not later. Got {closed.last_trade_date}. "
        f"If this is 2025-12-30 the 12/30 buy is being fused into the "
        f"closed session — float-precision bug regressed."
    )
    assert open_.open_date.isoformat() == "2025-12-30"


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_iyw_emmory_has_one_strategy_row(bq_client):
    """Stale-uid regression: positions_summary must show exactly ONE
    strategy row for Emmory IYW (Buy and Hold, Open). Pre-fix it
    showed two — a "Dividend Closed -$1,957" row from uid=2's
    isolated trade history plus a "Buy and Hold Open" row from
    uid=9's isolated snapshot.

    The phantom -$1,957 was the entire 12-share buy cost minus the
    12/29 sell proceeds, computed as if the position had transferred
    out (broker no longer holds it). With the canonical-uid backfill,
    both halves merge into uid=9's partition and the closed round-trip
    correctly reports ~$62 realized.
    """
    rows = list(
        bq_client.query(
            """
            SELECT user_id, strategy, status, total_pnl, realized_pnl
            FROM `ccwj-dbt.analytics.positions_summary`
            WHERE account = 'Emmory Investment'
              AND symbol = 'IYW'
            """
        ).result()
    )

    assert len(rows) == 1, (
        f"Expected exactly 1 strategy row for Emmory IYW after stale-uid "
        f"reconciliation, got {len(rows)}: "
        f"{[(r.user_id, r.strategy, r.status, float(r.total_pnl)) for r in rows]}. "
        f"A Dividend/Closed row with large negative realized would mean the "
        f"closed-equity-legs phantom write-off slipped through, OR uid splits "
        f"created two partitions."
    )
    only = rows[0]
    assert only.strategy == "Buy and Hold"
    assert only.status == "Open"
    assert only.realized_pnl > -10, (
        f"Realized P&L for Emmory IYW Buy and Hold should be small "
        f"positive (~$62 from the 12/29 sells), got "
        f"{only.realized_pnl}. If it's a large negative, the phantom "
        f"write-off bug (closed-leg cost-attribution on still-held "
        f"shares) is back."
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_emmory_trade_history_is_under_canonical_uid(bq_client):
    """Stale-uid regression: every Emmory Investment trade-history row
    must be stamped with the canonical user_id derived from
    `stg_current` / `stg_account_balances`. Pre-fix `trade_history.csv`
    rows landed under uid=2 (a stale user record) while the broker's
    current snapshot was under uid=9 → mart partitions split.

    The dbt test `no_stale_user_id_in_history` enforces this for the
    whole warehouse; this test is the symbol-anchored version that
    fails LOUDLY for the exact account that triggered the original
    incident.
    """
    rows = list(
        bq_client.query(
            """
            SELECT DISTINCT user_id
            FROM `ccwj-dbt.analytics.stg_history`
            WHERE account = 'Emmory Investment'
            """
        ).result()
    )

    uids = sorted([r.user_id for r in rows if r.user_id is not None])
    assert uids == [9], (
        f"Emmory Investment trade history must collapse to the canonical "
        f"uid=9 (broker-confirmed owner). Got: {uids}. "
        f"If uid=2 reappears, the canonical-account-owner backfill in "
        f"stg_history regressed (or the seed got re-stamped backward)."
    )
