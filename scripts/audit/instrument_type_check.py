"""Audit instrument_type misclassification across stg_history."""
from __future__ import annotations

import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

from app.bigquery_client import get_bigquery_client
DS = "`ccwj-dbt.analytics`"


def q(client, sql):
    return client.query(sql).to_dataframe()


def main():
    client = get_bigquery_client()

    print("=" * 78)
    print("Sample raw stg_history rows with action='option_*' grouped by instrument_type")
    print("=" * 78)
    df = q(client, f"""
        SELECT instrument_type, action, COUNT(*) AS n
        FROM {DS}.stg_history
        WHERE STARTS_WITH(action, 'option_')
        GROUP BY instrument_type, action
        ORDER BY action, instrument_type
    """)
    print(df.to_string(index=False))

    print()
    print("=" * 78)
    print("Same for action='equity_*'")
    print("=" * 78)
    df = q(client, f"""
        SELECT instrument_type, action, COUNT(*) AS n
        FROM {DS}.stg_history
        WHERE STARTS_WITH(action, 'equity_')
        GROUP BY instrument_type, action
        ORDER BY action, instrument_type
    """)
    print(df.to_string(index=False))

    print()
    print("=" * 78)
    print("Sample mis-classified option rows (action='option_*' AND instrument_type='Equity')")
    print("=" * 78)
    df = q(client, f"""
        SELECT account, trade_date, trade_symbol, underlying_symbol, instrument_type,
               action, option_type, option_strike, option_expiry, quantity, amount
        FROM {DS}.stg_history
        WHERE STARTS_WITH(action, 'option_')
          AND instrument_type = 'Equity'
        ORDER BY trade_date
        LIMIT 20
    """)
    print(df.to_string(index=False))

    print()
    print("=" * 78)
    print("Total row counts by instrument_type")
    print("=" * 78)
    df = q(client, f"""
        SELECT instrument_type, COUNT(*) AS n
        FROM {DS}.stg_history
        GROUP BY instrument_type
        ORDER BY n DESC
    """)
    print(df.to_string(index=False))

    print()
    print("=" * 78)
    print("Source of stg_history (look at description schema)")
    print("=" * 78)
    df = q(client, f"""
        SELECT column_name, data_type
        FROM `ccwj-dbt.analytics.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'stg_history'
        ORDER BY ordinal_position
    """)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
