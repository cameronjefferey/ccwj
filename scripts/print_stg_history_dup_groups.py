#!/usr/bin/env python
"""Print stg_history duplicate-fill groups (warehouse-job diagnostic).

Runs the same grain as ``stg_history_no_duplicate_fills_per_tenant`` and
dumps member rows for the first groups so a red build shows WHY, not
just a count. Flask-free; uses the warehouse job's ADC.
"""
from __future__ import annotations

import os
import sys

import pandas as pd
from google.cloud import bigquery

PROJECT = os.environ.get("BQ_PROJECT", "ccwj-dbt").strip()
DATASET = (os.environ.get("BQ_DATASET") or "analytics").strip()
TABLE = f"{PROJECT}.{DATASET}.stg_history"


def _as_bq_date(value):
    """DATE query param: pandas NaT / 'NaT' → None (NULL date groups).

    Run 33141412571 printed the 41 NULL-date groups then crashed:
    ``Invalid date: 'NaT'``.
    """
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ("", "NaT", "nat", "None", "nan"):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date()
        except (ValueError, OverflowError):
            return None
    return value


def main() -> int:
    client = bigquery.Client(project=PROJECT)
    groups_sql = f"""
    with check1 as (
      select
        tenant_id, trade_date, action, trade_symbol, quantity, price, amount,
        count(*) as n_dupes, 'check1' as which
      from `{TABLE}`
      where tenant_id is not null
      group by tenant_id, trade_date, action, trade_symbol, quantity, price, amount
      having count(*) > 1
    ),
    check2 as (
      select
        tenant_id, trade_date, action, trade_symbol, quantity,
        round(price, 4) as price, cast(null as float64) as amount,
        count(*) as n_dupes, 'check2' as which
      from `{TABLE}`
      where tenant_id is not null
        and trade_symbol is not null
        and price is not null
      group by tenant_id, trade_date, action, trade_symbol, quantity, round(price, 4)
      having count(*) > 1
    )
    select * from check1
    union all
    select * from check2
    order by n_dupes desc, which, trade_date
    """
    groups = client.query(groups_sql).to_dataframe()
    print(f"{TABLE}: {len(groups)} duplicate groups")
    if groups.empty:
        return 0
    print(groups.head(20).to_string(index=False))
    print("--- member rows (first 5 groups) ---")
    for _, g in groups.head(5).iterrows():
        members_sql = f"""
        select tenant_id, trade_date, action_raw, action, trade_symbol,
               quantity, price, amount, description
        from `{TABLE}`
        where tenant_id = @tid
          and action = @action
          and (trade_date = @dte or (trade_date is null and @dte is null))
          and (trade_symbol = @sym or (trade_symbol is null and @sym is null))
        order by description, amount, price
        limit 8
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tid", "STRING", g["tenant_id"]),
                bigquery.ScalarQueryParameter("action", "STRING", g["action"]),
                bigquery.ScalarQueryParameter("dte", "DATE", _as_bq_date(g["trade_date"])),
                bigquery.ScalarQueryParameter("sym", "STRING", g["trade_symbol"]),
            ]
        )
        members = client.query(members_sql, job_config=job_config).to_dataframe()
        print(f"\n[{g['which']}] n={g['n_dupes']} {g['trade_date']} {g['action']} {g['trade_symbol']}")
        print(members.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
