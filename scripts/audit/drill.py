"""Drill into a specific (account, symbol) to see why list vs detail disagree.

Usage:  python scripts/audit/drill.py 'Schwab ••••0044' ABT
"""
from __future__ import annotations

import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

from app.bigquery_client import get_bigquery_client

DS = "`ccwj-dbt.analytics`"


def q(client, sql, **params):
    job = client.query(sql)
    return job.to_dataframe()


def main():
    if len(sys.argv) < 3:
        print("usage: drill.py <account> <symbol>")
        sys.exit(1)
    account = sys.argv[1]
    symbol = sys.argv[2]

    client = get_bigquery_client()

    print(f"\n========== {account} / {symbol} ==========\n")

    # 1) positions_summary rows (the basis for the list page)
    print("--- positions_summary rows (drives the list page) ---")
    df = q(client, f"""
        SELECT strategy, status,
               total_pnl, realized_pnl, unrealized_pnl, total_dividend_income,
               num_winners, num_losers
        FROM {DS}.positions_summary
        WHERE account = '{account}' AND symbol = '{symbol}'
        ORDER BY strategy
    """)
    print(df.to_string(index=False))
    print(f"  Σ realized_pnl = {df['realized_pnl'].sum():,.2f}")
    print(f"  Σ total_pnl    = {df['total_pnl'].sum():,.2f}")

    # 2) int_strategy_classification rows (one level deeper)
    print("\n--- int_strategy_classification (all rows) ---")
    df = q(client, f"""
        SELECT trade_group_type, strategy, status, total_pnl,
               trade_symbol, open_date, close_date, num_trades
        FROM {DS}.int_strategy_classification
        WHERE account = '{account}' AND symbol = '{symbol}'
        ORDER BY open_date, strategy
    """)
    if df.empty:
        print("  (no rows)")
    else:
        print(df.to_string(index=False))
        print(f"  Σ total_pnl (Closed) = {df['total_pnl'].sum():,.2f}")

    # 3) int_closed_equity_legs (what detail page uses)
    print("\n--- int_closed_equity_legs (detail-page source for equity) ---")
    schema = q(client, f"""
        SELECT column_name FROM `ccwj-dbt.analytics.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'int_closed_equity_legs'
    """)
    print("  cols:", schema["column_name"].tolist())
    df = q(client, f"""
        SELECT *
        FROM {DS}.int_closed_equity_legs
        WHERE account = '{account}' AND symbol = '{symbol}'
        LIMIT 30
    """)
    if df.empty:
        print("  (no rows)")
    else:
        print(df.to_string(index=False))
        print(f"  Σ realized_pnl = {df['realized_pnl'].sum():,.2f}")

    # 4) int_equity_sessions — another candidate of truth
    print("\n--- int_equity_sessions ---")
    try:
        df = q(client, f"""
            SELECT *
            FROM {DS}.int_equity_sessions
            WHERE account = '{account}' AND symbol = '{symbol}'
            ORDER BY session_id
        """)
        if df.empty:
            print("  (no rows)")
        else:
            print(df.to_string(index=False))
    except Exception as exc:
        print(f"  (skipped: {exc})")

    # 5) Raw trades to see what should be there
    print("\n--- raw stg_history for this symbol (instrument_type='Equity') ---")
    try:
        schema = q(client, f"""
            SELECT column_name FROM `ccwj-dbt.analytics.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'stg_history'
        """)
        print("  cols:", schema["column_name"].tolist())
        df = q(client, f"""
            SELECT trade_date, action, underlying_symbol,
                   quantity, price, amount
            FROM {DS}.stg_history
            WHERE account = '{account}'
              AND underlying_symbol = '{symbol}'
              AND instrument_type = 'Equity'
            ORDER BY trade_date, action
        """)
        if df.empty:
            print("  (no rows)")
        else:
            print(df.head(30).to_string(index=False))
            if len(df) > 30:
                print(f"  ... and {len(df) - 30} more rows")
            print(f"  Σ amount (raw cash flow) = {df['amount'].sum():,.2f}")
    except Exception as exc:
        print(f"  (skipped: {exc})")


if __name__ == "__main__":
    main()
