"""DRIP (dividend reinvestment) detection at the intermediate layer.

Schwab Connect emits DRIPs as plain `Buy` rows in trade_history.csv with
the same description as a real buy. Without separation:
  * Position Detail's Raw Transaction Log shows a noisy stream of tiny
    "equity_buy" fills the user never explicitly placed
  * Position Legs surfaces fractional 0-share rows
  * The user can't tell "I deployed cash" from "Schwab reinvested my
    dividend"

Detection lives in `int_drip_fills` (downstream of `stg_daily_prices`,
NOT in `stg_history` — keeping `stg_history` independent of the daily
prices model preserves the CI workflow's two-pass build invariant).

`int_drip_fills` feeds:
  * `int_dividend_events.source = 'drip'` so the broker-actual amount
    overrides the yfinance synthetic estimate when the user has DRIP
  * `app/routes.py POSITION_TRADES_QUERY` joins so the Raw Transaction
    Log shows DRIPs with their own action badge

The integration tests below pin the contract against live BigQuery.
They're skipped by default (set RUN_BQ_TESTS=1 to enable).
"""
from __future__ import annotations

import os

import pytest


_SKIP_REASON = (
    "DRIP detection integration tests against live BigQuery. "
    "Set RUN_BQ_TESTS=1 to enable; requires ~/.dbt creds and network "
    "access. Skipped by default so the unit suite stays fast and "
    "offline."
)


@pytest.fixture(scope="module")
def bq_client():
    if not os.environ.get("RUN_BQ_TESTS"):
        pytest.skip(_SKIP_REASON)
    from google.cloud import bigquery

    return bigquery.Client(project="ccwj-dbt")


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_no_drip_flagged_for_symbols_without_yfinance_dividends(bq_client):
    """A row can only appear in `int_drip_fills` when the symbol has at
    least one `dividend > 0` row in `stg_daily_prices`. Otherwise the
    detection is matching against nothing — that would be a false positive.
    """
    rows = list(
        bq_client.query(
            """
            SELECT d.underlying_symbol AS sym, COUNT(*) AS n
            FROM `ccwj-dbt.analytics.int_drip_fills` d
            LEFT JOIN (
                SELECT DISTINCT symbol
                FROM `ccwj-dbt.analytics.stg_daily_prices`
                WHERE COALESCE(dividend, 0) > 0
            ) p ON p.symbol = d.underlying_symbol
            WHERE p.symbol IS NULL
            GROUP BY 1
            """
        ).result()
    )
    assert rows == [], (
        "DRIP rows for symbols with no yfinance ex-div dates: "
        f"{[(r.sym, r.n) for r in rows]}"
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_no_integer_quantity_buys_flagged_as_drip(bq_client):
    """Real buys (integer share count or fractional > 1 share) must
    NEVER appear in int_drip_fills — that would silently demote a
    real capital deployment to "Schwab reinvested my dividend" in
    the UI.
    """
    rows = list(
        bq_client.query(
            """
            SELECT account, underlying_symbol, trade_date, quantity, amount
            FROM `ccwj-dbt.analytics.int_drip_fills`
            WHERE quantity >= 1 OR quantity <= 0
            LIMIT 5
            """
        ).result()
    )
    assert rows == [], (
        "DRIP rows with quantity outside (0, 1): "
        f"{[(r.account, r.underlying_symbol, r.trade_date, r.quantity, r.amount) for r in rows]}"
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_drip_dividend_events_use_broker_actual_not_synthetic(bq_client):
    """For (account, user_id, symbol) with DRIP fills, the dividend
    event source MUST be 'drip' (broker-actual amount). Otherwise the
    yfinance synthetic estimate is double-counting against the broker
    truth — a regression of the precedence rule in
    `int_dividend_events`.
    """
    rows = list(
        bq_client.query(
            """
            WITH drip_tuples AS (
                SELECT DISTINCT account, user_id, underlying_symbol AS symbol
                FROM `ccwj-dbt.analytics.int_drip_fills`
            )
            SELECT
                d.account, d.user_id, d.symbol,
                ARRAY_AGG(DISTINCT e.source IGNORE NULLS) AS sources
            FROM drip_tuples d
            LEFT JOIN `ccwj-dbt.analytics.int_dividend_events` e
                ON e.account = d.account
               AND (e.user_id IS NOT DISTINCT FROM d.user_id)
               AND e.symbol = d.symbol
            GROUP BY 1, 2, 3
            HAVING ARRAY_LENGTH(sources) > 0
               AND NOT EXISTS (
                   SELECT 1 FROM UNNEST(sources) s WHERE s = 'drip'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM UNNEST(sources) s WHERE s = 'csv'
               )
            LIMIT 5
            """
        ).result()
    )
    assert rows == [], (
        "DRIP tuples that fell back to synthetic instead of broker actual: "
        f"{[(r.account, r.user_id, r.symbol, list(r.sources)) for r in rows]}"
    )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_iyw_emmory_drips_match_quarterly_ex_div(bq_client):
    """Concrete regression test: Emmory Investment IYW has 4 known
    DRIP fills (one per quarter in 2025). Each lands within 30 days
    after a yfinance ex-div date and corresponds to a real broker
    dividend payment in the $0.07-$0.12 range. Pinning the contract.
    """
    rows = list(
        bq_client.query(
            """
            SELECT trade_date, ROUND(drip_amount, 2) AS amt
            FROM `ccwj-dbt.analytics.int_drip_fills`
            WHERE account = 'Emmory Investment'
              AND underlying_symbol = 'IYW'
            ORDER BY trade_date
            """
        ).result()
    )
    assert len(rows) == 4, (
        f"expected 4 quarterly IYW DRIPs for Emmory Investment, got {len(rows)}: {rows}"
    )
    for r in rows:
        assert 0.05 <= r.amt <= 0.20, (
            f"IYW DRIP amount {r.amt} on {r.trade_date} outside expected "
            "quarterly $0.05-$0.20 band"
        )


@pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason=_SKIP_REASON)
def test_stg_history_is_not_downstream_of_stg_daily_prices(bq_client):
    """ARCHITECTURAL invariant: `stg_history` must NOT depend on
    `stg_daily_prices`, otherwise it falls into `stg_daily_prices+`
    and the CI workflow's two-pass build (Pass 1:
    `dbt build --exclude "stg_daily_prices+"`, Pass 2:
    `dbt build --select "stg_daily_prices+"`) skips stg_history (and
    effectively the whole warehouse) in Pass 1.

    DRIP detection joins to stg_daily_prices for the ex-div calendar,
    so it lives in `int_drip_fills` (intermediate/), NOT in
    `stg_history`. This test checks the BigQuery table's column list
    as a structural backstop — if anyone re-adds an
    `is_dividend_reinvestment` column to stg_history, this test
    fires and points at the rule.
    """
    rows = list(
        bq_client.query(
            """
            SELECT column_name
            FROM `ccwj-dbt.analytics.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'stg_history'
              AND LOWER(column_name) IN (
                  'is_dividend_reinvestment',
                  'is_drip',
                  'matched_ex_div_date'
              )
            """
        ).result()
    )
    cols = [r.column_name for r in rows]
    assert cols == [], (
        "stg_history must not carry DRIP detection columns "
        f"(found: {cols}). Detection lives in int_drip_fills so "
        "stg_history stays out of stg_daily_prices+ and the CI "
        "two-pass build keeps working."
    )
