#!/usr/bin/env python3
"""Read-only health flags for the private Vanguard account.

Prints PASS/FAIL lines only. Does not print balances, tickers, ids, or names.
Does not write to Postgres, BigQuery, or SnapTrade.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
import re
import sys
from datetime import date, datetime

PROJECT = "ccwj-dbt"
PROD_DATASET = "analytics"

_WRITE_RE = re.compile(
    r"\b(insert|update|delete|merge|create|drop|truncate|alter|grant|revoke|copy|call)\b",
    re.I,
)

_HIDDEN_VANGUARD_SQL = """
SELECT h.user_id, h.active
FROM admin_hidden_users h
JOIN snaptrade_accounts a ON a.user_id = h.user_id
WHERE strpos(lower(coalesce(a.broker_slug, '')), 'vanguard') > 0
   OR left(lower(coalesce(a.account_name, '')), 8) = 'vanguard'
"""

_ACCOUNTS_SQL = """
SELECT tenant_id, snaptrade_account_id, connection_broken_at,
       holdings_last_successful_sync, created_at, broker_slug, account_name
FROM snaptrade_accounts
WHERE user_id = %s
"""

# Vanguard partition is the account label's first token, the same rule as
# the per-broker staging models. Tenants are discovered here. No id is
# passed in, and the outer select returns only aggregates.
_WAREHOUSE_SQL = f"""
WITH labeled AS (
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_history`
  UNION ALL
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_current`
  UNION ALL
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
),
tenants AS (
  SELECT DISTINCT tenant_id
    FROM labeled
   WHERE tenant_id IS NOT NULL
     AND tenant_id != 'demo:demo-account'
     AND lower(split(trim(cast(account AS STRING)), ' ')[SAFE_OFFSET(0)]) = 'vanguard'
),
per_tenant AS (
  SELECT
    (SELECT COUNT(*)
       FROM `{PROJECT}.{PROD_DATASET}.stg_history` h
      WHERE h.tenant_id = t.tenant_id) AS history_n,
    (SELECT COUNT(*)
       FROM `{PROJECT}.{PROD_DATASET}.stg_current` c
      WHERE c.tenant_id = t.tenant_id) AS current_n,
    (SELECT COUNT(*)
       FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances` b
      WHERE b.tenant_id = t.tenant_id
        AND b.row_type = 'account_total'
        AND b.market_value IS NOT NULL) AS total_n,
    (SELECT SUM(c.market_value)
       FROM `{PROJECT}.{PROD_DATASET}.stg_current` c
      WHERE c.tenant_id = t.tenant_id) AS position_value,
    (SELECT SUM(b.market_value)
       FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances` b
      WHERE b.tenant_id = t.tenant_id
        AND b.row_type = 'cash') AS cash_value,
    (SELECT SUM(b.market_value)
       FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances` b
      WHERE b.tenant_id = t.tenant_id
        AND b.row_type = 'account_total') AS account_total
  FROM tenants t
)
SELECT
  COUNT(*) AS tenant_n,
  COUNTIF(
    history_n > 0
    AND current_n > 0
    AND total_n > 0
    AND position_value IS NOT NULL
    AND account_total IS NOT NULL
  ) AS complete_n,
  SUM(position_value + IFNULL(cash_value, 0)) AS staged_book,
  SUM(account_total) AS balance_book
FROM per_tenant
"""

_WAREHOUSE_LINE = re.compile(r"^(PASS|FAIL|FAIL off by about [1-9][0-9]*%)$")


def _code_only(sql: str) -> str:
    """Drop comments and quoted strings so a comment cannot hide a write."""
    out = []
    i = 0
    text = sql or ""
    n = len(text)
    while i < n:
        if text.startswith("--", i):
            nl = text.find("\n", i)
            if nl < 0:
                break
            i = nl + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:
                break
            i = end + 2
            continue
        if text[i] == "'":
            i += 1
            while i < n:
                if text[i] == "'":
                    if i + 1 < n and text[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


def statement_is_read(sql: str) -> bool:
    """True when the statement is a single read.

    Words inside comments or quotes do not count. ``updated_at`` does not
    count as a write. A ``WITH`` that then inserts does.
    """
    body = _code_only(sql).strip()
    if not body:
        return False
    body = body.rstrip().rstrip(";").strip()
    if ";" in body or _WRITE_RE.search(body):
        return False
    head = body.lstrip("(").lstrip().lower()
    return head.startswith(("select", "with", "show", "explain"))


def as_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def is_vanguard(row) -> bool:
    slug = (row.get("broker_slug") or "").lower()
    name = (row.get("account_name") or "").lower()
    return "vanguard" in slug or name.startswith("vanguard")


def holdings_is_fresh(*, last, created, today, threshold, unavailable=False) -> bool:
    """Match the product banner: unavailable or pre-connection stamps do not flag.

    A missing stamp is not fresh. ``threshold <= 0`` disables the backstop.
    """
    if threshold <= 0 or unavailable:
        return True
    if last is None:
        return False
    if created is not None and last < created:
        return True
    days = (today - last).days
    # A future stamp is not a stall. The product treats it the same way.
    if days < 0:
        return True
    return days < threshold


def connections_ok(rows, *, today, threshold, live) -> bool:
    if not rows:
        return False
    for row in rows:
        if row.get("connection_broken_at") is not None:
            return False
        info = (live or {}).get(row.get("snaptrade_account_id"))
        created = as_date(row.get("created_at"))
        stored_last = as_date(row.get("holdings_last_successful_sync"))
        if info and (info.get("last") is not None or info.get("unavailable")):
            fresh = holdings_is_fresh(
                last=info.get("last"),
                created=created,
                today=today,
                threshold=threshold,
                unavailable=bool(info.get("unavailable")),
            )
        else:
            fresh = holdings_is_fresh(
                last=stored_last,
                created=created,
                today=today,
                threshold=threshold,
                unavailable=False,
            )
        if not fresh:
            return False
    return True


def reconcile_line(staged, live) -> str:
    """PASS within one dollar, otherwise a coarse percent. Never a dollar amount."""
    if staged is None or live is None:
        return "FAIL"
    try:
        gap = abs(float(staged) - float(live))
        live_abs = abs(float(live))
        staged_abs = abs(float(staged))
    except (TypeError, ValueError):
        return "FAIL"
    if gap <= 1:
        return "PASS"
    base = live_abs if live_abs > 1 else staged_abs
    if base <= 0:
        return "FAIL"
    shown = int(round((gap / base) * 100))
    if shown < 1:
        shown = 1
    return f"off by about {shown}%"


def warehouse_status(*, complete: bool, staged, live) -> str:
    if not complete:
        return "FAIL"
    return reconcile_line(staged, live)


def warehouse_report_line(*, complete: bool, staged, balance) -> str:
    """PASS, or FAIL, or FAIL with a coarse percent. No dollar amount."""
    if not complete:
        return "FAIL"
    status = reconcile_line(staged, balance)
    if status == "PASS":
        return "PASS"
    if isinstance(status, str) and status.startswith("off by about "):
        return f"FAIL {status}"
    return "FAIL"


# Shares of (staged book − balance). Fractions are computed in the
# warehouse query; this function only turns those fractions into
# percents. It never sees a dollar amount, a ticker, or an id.
_GAP_ORDER = (
    "duplicate_or_missing_account",
    "money_market_both",
    "money_market_neither",
    "options_valuation",
    "fractional",
    "price_date",
)
_GAP_LOG_ORDER = (
    "price_date",
    "money_market_both",
    "money_market_neither",
    "fractional",
    "duplicate_or_missing_account",
    "options_valuation",
)
_GAP_LINE = re.compile(
    r"^(price_date|money_market_both|money_market_neither|fractional"
    r"|duplicate_or_missing_account|options_valuation)=[0-9]+%$"
)

# One aggregate row: a 0/1 flag and one fraction per cause. Inner
# amounts stay in the query. A cause is offered only when removing it
# moves the staged book toward the balance, and in the same direction.
_GAP_SQL = f"""
WITH tenants AS (
  SELECT DISTINCT tenant_id
    FROM (
      SELECT tenant_id, account
        FROM `{PROJECT}.{PROD_DATASET}.stg_history`
      UNION ALL
      SELECT tenant_id, account
        FROM `{PROJECT}.{PROD_DATASET}.stg_current`
      UNION ALL
      SELECT tenant_id, account
        FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
    )
   WHERE tenant_id IS NOT NULL
     AND tenant_id != 'demo:demo-account'
     AND lower(split(trim(cast(account AS STRING)), ' ')[SAFE_OFFSET(0)]) = 'vanguard'
),
held AS (
  SELECT
    c.market_value AS mv,
    c.quantity AS qty,
    c.current_price AS px,
    c.cost_basis AS cost,
    c.instrument_type AS kind,
    lower(coalesce(c.description, '')) AS descr,
    lower(coalesce(c.security_type_raw, '')) AS stype,
    c.trade_symbol AS symbol
  FROM `{PROJECT}.{PROD_DATASET}.stg_current` c
  JOIN tenants t ON t.tenant_id = c.tenant_id
),
closes AS (
  SELECT symbol, close_price
    FROM (
      SELECT
        trim(symbol) AS symbol,
        close_price,
        row_number() OVER (PARTITION BY trim(symbol) ORDER BY `date` DESC) AS rn
      FROM `{PROJECT}.{PROD_DATASET}.stg_daily_prices`
      WHERE trim(symbol) IN (
        SELECT DISTINCT symbol FROM held WHERE kind = 'Equity' AND symbol IS NOT NULL
      )
    )
   WHERE rn = 1
),
held_flag AS (
  SELECT
    h.mv,
    h.qty,
    h.px,
    cl.close_price AS cls,
    (
      h.kind NOT IN ('Call', 'Put')
      AND (
        strpos(h.descr, 'money market') > 0
        OR strpos(h.stype, 'money market') > 0
        OR (
          h.px IS NOT NULL
          AND abs(h.px - 1) <= 0.02
          AND (
            h.cost IS NULL
            OR h.qty IS NULL
            OR h.qty = 0
            OR abs(safe_divide(h.cost, h.qty) - 1) <= 0.02
          )
        )
      )
    ) AS is_mm,
    h.kind IN ('Call', 'Put') AS is_opt
  FROM held h
  LEFT JOIN closes cl ON cl.symbol = h.symbol
),
broker AS (
  SELECT
    safe_cast(trim(replace(replace(replace(
      cast(market_value AS STRING), '$', ''), ',', ''), ' ', '')) AS FLOAT64) AS mv,
    lower(trim(coalesce(cast(Symbol AS STRING), ''))) AS symbol,
    lower(trim(coalesce(cast(security_type AS STRING), ''))) AS stype
  FROM `{PROJECT}.{PROD_DATASET}.stg_broker_vanguard_current`
  WHERE tenant_id IN (SELECT tenant_id FROM tenants)
),
labels AS (
  SELECT tenant_id, lower(trim(cast(account AS STRING))) AS label
    FROM `{PROJECT}.{PROD_DATASET}.stg_current`
   WHERE tenant_id IN (SELECT tenant_id FROM tenants)
  UNION ALL
  SELECT tenant_id, lower(trim(cast(account AS STRING))) AS label
    FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
   WHERE tenant_id IN (SELECT tenant_id FROM tenants)
),
label_one AS (
  SELECT tenant_id, any_value(label) AS label
    FROM labels
   GROUP BY tenant_id
),
held_sums AS (
  SELECT tenant_id, SUM(market_value) AS held
    FROM `{PROJECT}.{PROD_DATASET}.stg_current`
   WHERE tenant_id IN (SELECT tenant_id FROM tenants)
   GROUP BY tenant_id
),
balance_sums AS (
  SELECT
    tenant_id,
    SUM(IF(row_type = 'cash', market_value, NULL)) AS cash,
    SUM(IF(row_type = 'account_total', market_value, NULL)) AS total
  FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
  WHERE tenant_id IN (SELECT tenant_id FROM tenants)
  GROUP BY tenant_id
),
books AS (
  SELECT
    t.tenant_id,
    l.label,
    h.held,
    b.cash,
    b.total
  FROM tenants t
  LEFT JOIN label_one l ON l.tenant_id = t.tenant_id
  LEFT JOIN held_sums h ON h.tenant_id = t.tenant_id
  LEFT JOIN balance_sums b ON b.tenant_id = t.tenant_id
),
ranked AS (
  SELECT
    ifnull(held, 0) + ifnull(cash, 0) AS staged,
    held,
    total,
    count(*) OVER (PARTITION BY coalesce(label, tenant_id)) AS label_n,
    row_number() OVER (
      PARTITION BY coalesce(label, tenant_id)
      ORDER BY abs(ifnull(total, 0)) DESC
    ) AS rn
  FROM books
),
measures AS (
  SELECT
    (SELECT ifnull(SUM(mv), 0) FROM held_flag) AS held_mv,
    (SELECT ifnull(SUM(cash), 0) FROM books) AS cash_mv,
    (SELECT SUM(total) FROM books) AS total_mv,
    (SELECT ifnull(SUM(IF(is_mm, mv, 0)), 0) FROM held_flag) AS mm_held,
    (SELECT ifnull(SUM(IF(
        symbol NOT IN ('account total', 'positions total')
        AND stype IN ('cash and money market', ''),
        mv, 0)), 0) FROM broker) AS missed_mv,
    (SELECT ifnull(SUM(IF(
        symbol NOT IN ('account total', 'positions total')
        AND stype NOT IN ('cash and money market', ''),
        mv, 0)), 0) FROM broker) AS raw_held,
    (SELECT ifnull(SUM(IF(is_opt, mv, 0)), 0) FROM held_flag) AS opt_stored,
    (SELECT ifnull(SUM(IF(is_opt, qty * px * 100, 0)), 0) FROM held_flag) AS opt_x100,
    (SELECT ifnull(SUM(IF(is_opt, qty * px, 0)), 0) FROM held_flag) AS opt_x1,
    (SELECT ifnull(SUM(IF(NOT is_opt AND NOT is_mm, mv - qty * px, 0)), 0)
       FROM held_flag) AS frac_mv,
    (SELECT ifnull(SUM(IF(
        NOT is_opt AND NOT is_mm AND cls IS NOT NULL,
        mv - qty * cls, 0)), 0) FROM held_flag) AS close_mv,
    (SELECT nullif(SUM(IF(symbol = 'account total', mv, 0)), 0) FROM broker) AS pos_total,
    (SELECT
        ifnull(SUM(IF(label_n > 1 AND rn > 1, staged - ifnull(total, 0), 0)), 0)
        + ifnull(SUM(IF(total IS NULL, staged, 0)), 0)
        + ifnull(SUM(IF(held IS NULL AND total IS NOT NULL, -total, 0)), 0)
       FROM ranked) AS account_mv
  FROM (SELECT 1 AS _one)
),
scored AS (
  SELECT
    held_mv + cash_mv - total_mv AS gap,
    IF(mm_held > 0 AND cash_mv + 1 >= mm_held, mm_held, 0) AS mm_both_mv,
    -missed_mv AS mm_neither_mv,
    opt_stored - opt_x100 AS opt_x100_mv,
    opt_stored - opt_x1 AS opt_x1_mv,
    frac_mv,
    close_mv,
    IF(pos_total IS NULL, NULL, pos_total - total_mv) AS snap_mv,
    account_mv + (held_mv - raw_held) AS dup_mv
  FROM measures
  WHERE total_mv IS NOT NULL
)
SELECT
  IF(gap IS NULL OR abs(gap) <= 1, 0, 1) AS has_gap,
  CASE
    WHEN gap IS NULL OR abs(gap) <= 1 OR dup_mv IS NULL THEN 0
    WHEN abs(gap - dup_mv) < abs(gap) AND dup_mv / gap > 0 THEN dup_mv / gap
    ELSE 0
  END AS duplicate_or_missing_account,
  CASE
    WHEN gap IS NULL OR abs(gap) <= 1 THEN 0
    WHEN abs(gap - mm_both_mv) < abs(gap) AND mm_both_mv / gap > 0 THEN mm_both_mv / gap
    ELSE 0
  END AS money_market_both,
  CASE
    WHEN gap IS NULL OR abs(gap) <= 1 THEN 0
    WHEN abs(gap - mm_neither_mv) < abs(gap) AND mm_neither_mv / gap > 0
      THEN mm_neither_mv / gap
    ELSE 0
  END AS money_market_neither,
  CASE
    WHEN gap IS NULL OR abs(gap) <= 1 THEN 0
    WHEN abs(gap - opt_x1_mv) <= abs(gap - opt_x100_mv)
         AND abs(gap - opt_x1_mv) < abs(gap)
         AND opt_x1_mv / gap > 0
      THEN opt_x1_mv / gap
    WHEN abs(gap - opt_x100_mv) < abs(gap) AND opt_x100_mv / gap > 0
      THEN opt_x100_mv / gap
    ELSE 0
  END AS options_valuation,
  CASE
    WHEN gap IS NULL OR abs(gap) <= 1 OR frac_mv IS NULL THEN 0
    WHEN abs(gap - frac_mv) < abs(gap) AND frac_mv / gap > 0 THEN frac_mv / gap
    ELSE 0
  END AS fractional,
  CASE
    WHEN gap IS NULL OR abs(gap) <= 1 THEN 0
    WHEN snap_mv IS NOT NULL
         AND abs(gap - snap_mv) < abs(gap)
         AND snap_mv / gap > 0
         AND (
           close_mv IS NULL
           OR NOT (abs(gap - close_mv) < abs(gap) AND close_mv / gap > 0)
           OR abs(gap - snap_mv) <= abs(gap - close_mv)
         )
      THEN snap_mv / gap
    WHEN close_mv IS NOT NULL
         AND abs(gap - close_mv) < abs(gap)
         AND close_mv / gap > 0
      THEN close_mv / gap
    ELSE 0
  END AS price_date
FROM scored
"""

# Shares of the same book gap (staged holdings + cash − account total).
# Account letters follow tenant_id order and are not names. A letter
# whose column is null is an account that does not exist. Holding
# shares are the dollar effect inside accounts that miss by more than
# one dollar, matched on a space-stripped symbol so an OCC pad is not
# a missing row and a duplicate. The balance date is whether the two
# raw tables were written on different New York dates. Pending cash is
# not summed: the loaded balance schema has no such column, so its
# share stays zero.
_NARROW_LETTERS = "ABCDEFGH"
_NARROW_CATS = (
    "holdings_missing_from_staging",
    "holdings_extra_in_staging",
    "qty_or_price_differs",
    "balance_other_vintage",
    "balance_other_date",
    "pending_cash",
)
_NARROW_ACCOUNT_LINE = re.compile(r"^account_[A-H]=-?[0-9]+%$")
_NARROW_CAT_LINE = re.compile(
    r"^(holdings_missing_from_staging|holdings_extra_in_staging|qty_or_price_differs"
    r"|balance_other_vintage|balance_other_date|pending_cash)=-?[0-9]+%$"
)
_NARROW_SQL = f"""
WITH labeled AS (
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_history`
  UNION ALL
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_current`
  UNION ALL
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
),
tenants AS (
  SELECT DISTINCT tenant_id
    FROM labeled
   WHERE tenant_id IS NOT NULL
     AND tenant_id != 'demo:demo-account'
     AND lower(split(trim(cast(account AS STRING)), ' ')[SAFE_OFFSET(0)]) = 'vanguard'
),
held AS (
  SELECT tenant_id, SUM(market_value) AS held_mv
    FROM `{PROJECT}.{PROD_DATASET}.stg_current`
   WHERE tenant_id IN (SELECT tenant_id FROM tenants)
   GROUP BY tenant_id
),
cash_rows AS (
  SELECT tenant_id, SUM(market_value) AS cash_mv
    FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
   WHERE tenant_id IN (SELECT tenant_id FROM tenants)
     AND row_type = 'cash'
   GROUP BY tenant_id
),
total_rows AS (
  SELECT tenant_id, SUM(market_value) AS account_total
    FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
   WHERE tenant_id IN (SELECT tenant_id FROM tenants)
     AND row_type = 'account_total'
   GROUP BY tenant_id
),
books AS (
  SELECT
    t.tenant_id,
    IFNULL(h.held_mv, 0) + IFNULL(c.cash_mv, 0) - IFNULL(b.account_total, 0) AS gap,
    b.account_total AS account_total
  FROM tenants t
  LEFT JOIN held h ON h.tenant_id = t.tenant_id
  LEFT JOIN cash_rows c ON c.tenant_id = t.tenant_id
  LEFT JOIN total_rows b ON b.tenant_id = t.tenant_id
),
overall AS (
  SELECT SUM(gap) AS gap FROM books
),
lettered AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY b.tenant_id) AS n,
    IF(
      o.gap IS NULL OR ABS(o.gap) <= 1,
      0,
      SAFE_DIVIDE(b.gap, o.gap)
    ) AS share
  FROM books b
  CROSS JOIN overall o
),
letters AS (
  SELECT
    MAX(IF(n = 1, share, NULL)) AS a,
    MAX(IF(n = 2, share, NULL)) AS b,
    MAX(IF(n = 3, share, NULL)) AS c,
    MAX(IF(n = 4, share, NULL)) AS d,
    MAX(IF(n = 5, share, NULL)) AS e,
    MAX(IF(n = 6, share, NULL)) AS f,
    MAX(IF(n = 7, share, NULL)) AS g,
    MAX(IF(n = 8, share, NULL)) AS h
  FROM lettered
),
failing AS (
  SELECT tenant_id FROM books WHERE ABS(gap) > 1
),
raw_rows AS (
  SELECT
    NULLIF(TRIM(tenant_id), '') AS tenant_id,
    REGEXP_REPLACE(UPPER(TRIM(CAST(Symbol AS STRING))), r'\\s+', '') AS sym_key,
    LOWER(TRIM(COALESCE(CAST(Symbol AS STRING), ''))) AS sym_label,
    LOWER(TRIM(COALESCE(CAST(security_type AS STRING), ''))) AS sec_type,
    SAFE_CAST(NULLIF(TRIM(CAST(Quantity AS STRING)), '') AS FLOAT64) AS qty,
    SAFE_CAST(NULLIF(TRIM(CAST(Price AS STRING)), '') AS FLOAT64) AS price,
    SAFE_CAST(TRIM(REPLACE(REPLACE(REPLACE(
      COALESCE(CAST(market_value AS STRING), ''), '$', ''), ',', ''), ' ', '')) AS FLOAT64) AS mv
  FROM `{PROJECT}.{PROD_DATASET}.stg_broker_vanguard_current`
  WHERE NULLIF(TRIM(tenant_id), '') IN (SELECT tenant_id FROM failing)
),
raw_hold AS (
  SELECT
    tenant_id,
    sym_key,
    SUM(qty) AS qty,
    SAFE_DIVIDE(SUM(qty * price), SUM(qty)) AS price,
    SUM(mv) AS mv
  FROM raw_rows
  WHERE sec_type NOT IN ('cash and money market', '')
    AND sym_label NOT IN ('account total', 'positions total')
    AND sym_key != ''
  GROUP BY tenant_id, sym_key
),
stg_hold AS (
  SELECT
    tenant_id,
    REGEXP_REPLACE(UPPER(TRIM(CAST(trade_symbol AS STRING))), r'\\s+', '') AS sym_key,
    SUM(quantity) AS qty,
    SAFE_DIVIDE(SUM(quantity * current_price), SUM(quantity)) AS price,
    SUM(market_value) AS mv
  FROM `{PROJECT}.{PROD_DATASET}.stg_current`
  WHERE tenant_id IN (SELECT tenant_id FROM failing)
  GROUP BY tenant_id, sym_key
),
paired AS (
  SELECT
    r.sym_key AS raw_key,
    s.sym_key AS stg_key,
    r.qty AS raw_qty,
    s.qty AS stg_qty,
    r.price AS raw_px,
    s.price AS stg_px,
    r.mv AS raw_mv,
    s.mv AS stg_mv
  FROM raw_hold r
  FULL OUTER JOIN stg_hold s
    ON r.tenant_id = s.tenant_id
   AND r.sym_key = s.sym_key
),
effects AS (
  SELECT
    SUM(IF(stg_key IS NULL, -IFNULL(raw_mv, 0), 0)) AS missing_dollars,
    SUM(IF(raw_key IS NULL, IFNULL(stg_mv, 0), 0)) AS extra_dollars,
    SUM(IF(
      raw_key IS NOT NULL AND stg_key IS NOT NULL AND (
        ABS(IFNULL(stg_qty, 0) - IFNULL(raw_qty, 0)) > 1e-4
        OR ABS(IFNULL(stg_px, 0) - IFNULL(raw_px, 0)) > 0.01
        OR ABS(IFNULL(stg_mv, 0) - IFNULL(raw_mv, 0)) > 1
      ),
      IFNULL(stg_mv, 0) - IFNULL(raw_mv, 0),
      0
    )) AS diff_dollars
  FROM paired
),
file_totals AS (
  SELECT tenant_id, SUM(mv) AS file_total
    FROM raw_rows
   WHERE sym_label = 'account total'
   GROUP BY tenant_id
),
vintage AS (
  SELECT SUM(
    IF(f.file_total IS NOT NULL, f.file_total - IFNULL(b.account_total, 0), 0)
  ) AS dollars
  FROM books b
  LEFT JOIN file_totals f ON f.tenant_id = b.tenant_id
  WHERE ABS(b.gap) > 1
)
SELECT
  letters.a AS a,
  letters.b AS b,
  letters.c AS c,
  letters.d AS d,
  letters.e AS e,
  letters.f AS f,
  letters.g AS g,
  letters.h AS h,
  IF(o.gap IS NULL OR ABS(o.gap) <= 1, 0, SAFE_DIVIDE(effects.missing_dollars, o.gap))
    AS holdings_missing_from_staging,
  IF(o.gap IS NULL OR ABS(o.gap) <= 1, 0, SAFE_DIVIDE(effects.extra_dollars, o.gap))
    AS holdings_extra_in_staging,
  IF(o.gap IS NULL OR ABS(o.gap) <= 1, 0, SAFE_DIVIDE(effects.diff_dollars, o.gap))
    AS qty_or_price_differs,
  IF(o.gap IS NULL OR ABS(o.gap) <= 1, 0, SAFE_DIVIDE(vintage.dollars, o.gap))
    AS balance_other_vintage,
  (
    SELECT IF(COUNT(DISTINCT d) > 1, 1.0, 0.0)
      FROM (
        SELECT DATE(TIMESTAMP_MILLIS(last_modified_time), 'America/New_York') AS d
          FROM `{PROJECT}.analytics_raw.__TABLES__`
         WHERE table_id IN ('current_positions', 'account_balances')
           AND last_modified_time IS NOT NULL
      )
  ) AS balance_other_date,
  (
    SELECT IF(
      COUNTIF(REGEXP_CONTAINS(LOWER(column_name), r'pending|unsettled')) > 0,
      1.0,
      0.0
    )
      FROM `{PROJECT}.analytics_raw.INFORMATION_SCHEMA.COLUMNS`
     WHERE table_name IN ('current_positions', 'account_balances')
  ) AS pending_cash
FROM letters
CROSS JOIN effects
CROSS JOIN overall o
CROSS JOIN vintage
"""


def _safe_hint(message: str) -> str:
    """A query-planner phrase with quoted text and amounts removed."""
    if "$" in (message or ""):
        return ""
    line = (message or "").split("\n", 1)[0]
    line = re.sub(r"'[^']*'", "", line)
    line = re.sub(r'"[^"]*"', "", line)
    line = re.sub(r"\b\d+\.\d+\b", "", line)
    line = re.sub(r"\s+", " ", line).strip()
    if not line or len(line) > 140:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9_ :,.\[\]()\-]+", line):
        return ""
    return line


def _finite(value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def attribute_fractions(fracs, *, has_gap: bool) -> dict:
    """Signed-free percents of the gap. They sum to 100 when a gap exists.

    Each fraction is the share of the gap that cause would remove.
    The closest fit is taken first. A remainder no measured cause
    absorbs stays unassigned, so a zero price-date fraction is not
    relabeled as timing.
    """
    if not has_gap:
        return {name: 0 for name in _GAP_ORDER}
    remaining = 1.0
    assigned = {name: 0.0 for name in _GAP_ORDER}
    unused = {name: _finite((fracs or {}).get(name)) for name in _GAP_ORDER}
    for _ in range(len(_GAP_ORDER)):
        if remaining <= 0.02:
            break
        scored = []
        for index, name in enumerate(_GAP_ORDER):
            fraction = unused[name]
            if fraction <= 0:
                continue
            if fraction > remaining * 1.5 and abs(fraction - remaining) > 0.5 * remaining:
                unused[name] = 0.0
                continue
            scored.append((abs(fraction - remaining), index, name, fraction))
        if not scored:
            break
        scored.sort()
        _dist, _index, name, fraction = scored[0]
        if abs(fraction - remaining) <= max(0.02, 0.15 * remaining) or (
            remaining < fraction <= remaining * 1.5
        ):
            take = remaining
        elif fraction < remaining:
            take = fraction
        else:
            unused[name] = 0.0
            continue
        assigned[name] += take
        remaining -= take
        unused[name] = 0.0
    raw = {name: assigned[name] * 100.0 for name in _GAP_ORDER}
    rounded = {name: int(round(raw[name])) for name in _GAP_ORDER}
    # Only absorb a rounding point. An unexplained remainder stays unassigned.
    drift = 100 - sum(rounded.values())
    if drift and abs(drift) <= 2 and any(rounded.values()):
        host = max(_GAP_ORDER, key=lambda name: (abs(rounded[name]), abs(raw[name])))
        rounded[host] += drift
    return rounded


def gap_lines(percents) -> list[str]:
    return [f"{name}={int(percents[name])}%" for name in _GAP_LOG_ORDER]


def _pct(value) -> int:
    return int(round(_finite(value) * 100.0))


def narrow_lines(values) -> list[str]:
    """Account letters plus category shares. Null letters are omitted."""
    values = values or {}
    lines = []
    for letter in _NARROW_LETTERS:
        key = letter.lower()
        if values.get(key) is None:
            continue
        lines.append(f"account_{letter}={_pct(values.get(key))}%")
    for name in _NARROW_CATS:
        lines.append(f"{name}={_pct(values.get(name))}%")
    return lines


def narrow_lines_ok(lines) -> bool:
    if not lines:
        return False
    accounts = []
    categories = []
    for line in lines:
        if line.startswith("account_"):
            if categories or not _NARROW_ACCOUNT_LINE.match(line):
                return False
            accounts.append(line)
        elif not _NARROW_CAT_LINE.match(line):
            return False
        else:
            categories.append(line)
    letters = [line.split("=", 1)[0].split("_", 1)[1] for line in accounts]
    if letters != list(_NARROW_LETTERS[: len(letters)]):
        return False
    return [line.split("=", 1)[0] for line in categories] == list(_NARROW_CATS)


def format_report(report) -> list[str]:
    def flag(ok) -> str:
        return "PASS" if ok else "FAIL"

    warehouse = report.get("warehouse")
    if warehouse == "PASS" or (
        isinstance(warehouse, str) and warehouse.startswith("off by about ")
    ):
        warehouse_line = f"warehouse={warehouse}"
    else:
        warehouse_line = "warehouse=FAIL"
    return [
        f"connections={flag(report.get('connections'))}",
        f"privacy={flag(report.get('privacy'))}",
        warehouse_line,
        f"positions={flag(report.get('positions'))}",
        f"overview={flag(report.get('overview'))}",
        f"settings={flag(report.get('settings'))}",
    ]


def _blank_report():
    return {
        "connections": False,
        "privacy": False,
        "warehouse": "FAIL",
        "positions": False,
        "overview": False,
        "settings": False,
    }


def _install_read_only(dbmod) -> None:
    original = dbmod._connect

    def _connect():
        conn = original()
        previous = conn.autocommit
        conn.autocommit = True
        conn.execute("SET default_transaction_read_only = on")
        shown = conn.execute("SHOW default_transaction_read_only").fetchone()
        flag = shown["default_transaction_read_only"] if isinstance(shown, dict) else shown[0]
        conn.autocommit = previous
        if str(flag).lower() != "on":
            raise RuntimeError("read-only")
        real_cursor = conn.cursor
        real_execute_conn = getattr(conn, "execute", None)

        def cursor(*args, **kwargs):
            cur = real_cursor(*args, **kwargs)
            real_execute = cur.execute

            def execute(query, *exec_args, **exec_kwargs):
                sql = query.decode("utf-8", "replace") if isinstance(query, bytes) else str(query)
                if not statement_is_read(sql):
                    raise RuntimeError("read-only")
                return real_execute(query, *exec_args, **exec_kwargs)

            cur.execute = execute
            return cur

        def commit():
            conn.rollback()

        conn.cursor = cursor
        if real_execute_conn is not None:
            def execute_conn(query, *exec_args, **exec_kwargs):
                sql = query.decode("utf-8", "replace") if isinstance(query, bytes) else str(query)
                if not statement_is_read(sql):
                    raise RuntimeError("read-only")
                return real_execute_conn(query, *exec_args, **exec_kwargs)

            try:
                conn.execute = execute_conn
            except Exception:
                pass
        try:
            conn.commit = commit
        except Exception:
            # The session is already read-only. A failed shadow still
            # cannot persist a write when the server enforces that.
            pass
        return conn

    dbmod._connect = _connect


def _install_bq_guard() -> None:
    from google.cloud import bigquery

    original = bigquery.Client.query

    def query(self, query_text, *args, **kwargs):
        sql = query_text if isinstance(query_text, str) else str(query_text)
        if not statement_is_read(sql):
            raise RuntimeError("read-only")
        job_config = kwargs.get("job_config")
        if job_config is None and args:
            job_config = args[0]
        if job_config is not None and getattr(job_config, "destination", None) is not None:
            raise RuntimeError("read-only")
        return original(self, query_text, *args, **kwargs)

    def _refuse(self, *args, **kwargs):
        raise RuntimeError("read-only")

    bigquery.Client.query = query
    for name in (
        "load_table_from_dataframe",
        "load_table_from_json",
        "load_table_from_file",
        "insert_rows",
        "insert_rows_json",
        "create_table",
        "delete_table",
        "update_table",
        "copy_table",
    ):
        if hasattr(bigquery.Client, name):
            setattr(bigquery.Client, name, _refuse)


def _prepare_runtime():
    os.environ["HAPPYTRADER_SKIP_DB_INIT"] = "1"
    os.environ.setdefault("SECRET_KEY", "read-only-health-check")
    os.environ["WTF_CSRF_ENABLED"] = "false"
    os.environ["RATELIMIT_ENABLED"] = "false"
    os.environ.pop("BQ_DATASET", None)
    os.environ.pop("QUERY_CACHE_REDIS_URL", None)
    os.environ.pop("SENTRY_DSN", None)
    logging.disable(logging.CRITICAL)
    _install_bq_guard()
    import app.db as dbmod
    from app import app as flask_app

    _install_read_only(dbmod)
    flask_app.testing = True
    flask_app.config["PROPAGATE_EXCEPTIONS"] = True
    flask_app.config["RATELIMIT_ENABLED"] = False
    flask_app.config["SESSION_IDLE_TIMEOUT_MINUTES"] = 0
    flask_app.logger.disabled = True
    logging.getLogger("happytrader.timing").disabled = True
    logging.getLogger("happytrader.cost").disabled = True
    import app.admin_overview as overview
    import app.cost_tracking as cost
    import app.funnel as funnel

    overview.record_page_view = lambda response: None
    funnel.note_internal_visit = lambda reason: None
    funnel.log_event = lambda *args, **kwargs: None
    cost.log_cost_event = lambda *args, **kwargs: None
    _block_production_writes()
    for handler in logging.getLogger("happytrader.timing").handlers:
        handler.setStream(io.StringIO())
    return flask_app, dbmod


def _block_production_writes() -> None:
    """Page renders must not start a sync, seed write, or warehouse rebuild."""
    import app.snaptrade as snap
    import app.upload as upload

    def _no(*args, **kwargs):
        return None

    snap.queue_account_read_sync = _no
    snap._sync_one_connection = _no
    snap._kick_post_connect_sync = _no
    upload.merge_and_push_seeds = lambda *args, **kwargs: (False, "read-only", None, None, None, True)
    upload.merge_and_push_seeds_batch = lambda *args, **kwargs: (False, "read-only", None, [])
    if hasattr(upload, "_dispatch_warehouse_rebuild"):
        upload._dispatch_warehouse_rebuild = _no
    import sys
    paper = sys.modules.get("app.paper_practice")
    if paper is not None:
        paper.queue_account_read_sync = _no


_SECRET_MANAGER_MISSING = False
_LAST_SECRET_STATUS = "skip"


def _status_from_error(exc: BaseException) -> str:
    """Coarse Secret Manager result. The exception text is never used."""
    if type(exc).__name__ == "NotFound":
        return "miss"
    return "denied"


def _secret_text(name: str) -> str:
    """Read one allow-listed secret. Empty when Secret Manager is unavailable."""
    global _SECRET_MANAGER_MISSING, _LAST_SECRET_STATUS
    if _LAST_SECRET_STATUS == "denied" or _SECRET_MANAGER_MISSING:
        if _SECRET_MANAGER_MISSING:
            _LAST_SECRET_STATUS = "unavailable"
        return ""
    try:
        from google.cloud import secretmanager
    except Exception:
        _SECRET_MANAGER_MISSING = True
        _LAST_SECRET_STATUS = "unavailable"
        return ""
    project = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
        or PROJECT
    ).strip()
    if not project or not name:
        _LAST_SECRET_STATUS = "unavailable"
        return ""
    try:
        client = secretmanager.SecretManagerServiceClient()
        path = f"projects/{project}/secrets/{name}/versions/latest"
        resp = client.access_secret_version(name=path)
        text = (resp.payload.data or b"").decode("utf-8").strip()
        _LAST_SECRET_STATUS = "hit" if text else "miss"
        return text
    except Exception as exc:
        _LAST_SECRET_STATUS = _status_from_error(exc)
        return ""


def _hydrate_env() -> None:
    """Fill connection env from Action secrets or an allow-listed secret name.

    Never prints the value. Missing names stay empty.
    """
    aliases = {
        "DATABASE_URL": (
            "DATABASE_URL",
            "PROD_DATABASE_URL",
            "database-url",
            "prod-database-url",
            "happytrader-database-url",
            "ccwj-database-url",
        ),
        "SNAPTRADE_CLIENT_ID": (
            "SNAPTRADE_CLIENT_ID",
            "snaptrade-client-id",
            "SNAPTRADE_CLIENT_SECRET",
        ),
        "SNAPTRADE_CONSUMER_KEY": (
            "SNAPTRADE_CONSUMER_KEY",
            "snaptrade-consumer-key",
            "snaptrade-client-secret",
        ),
    }
    for target, names in aliases.items():
        current = ""
        for name in names:
            current = (os.environ.get(name) or "").strip()
            if current:
                break
        if not current:
            for name in names:
                current = _secret_text(name)
                if current:
                    break
        if current and not (os.environ.get(target) or "").strip():
            os.environ[target] = current


def _database_url() -> str:
    _hydrate_env()
    value = (os.environ.get("DATABASE_URL") or "").strip()
    return value


def _live_reads(accounts):
    """Read SnapTrade details and balances. Returns (live_map, vanguard_total or None)."""
    from app.models import get_snaptrade_user
    from app.snaptrade import (
        SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS,
        _fetch_account_summary,
        _fetch_balances,
        _get_snaptrade_client,
        _holdings_last_successful_sync,
        _holdings_unavailable,
        snaptrade_sync_timeout,
    )
    from app.snaptrade_normalize import _reported_cash_and_total

    del SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS
    user_ids = {row.get("user_id") for row in accounts}
    if len(user_ids) != 1:
        return {}, None
    user_id = next(iter(user_ids))
    creds = get_snaptrade_user(user_id)
    client = _get_snaptrade_client()
    if not creds or client is None:
        return {}, None
    snap_user = creds.get("snaptrade_user_id")
    snap_secret = creds.get("snaptrade_secret")
    if not snap_user or not snap_secret:
        return {}, None
    live = {}
    totals = []
    balance_missing = False
    saw_vanguard = False
    for row in accounts:
        account_id = row.get("snaptrade_account_id")
        vanguard = is_vanguard(row)
        if vanguard:
            saw_vanguard = True
        try:
            with snaptrade_sync_timeout():
                summary = _fetch_account_summary(client, snap_user, snap_secret, account_id)
                balances = _fetch_balances(client, snap_user, snap_secret, account_id)
        except Exception:
            if vanguard:
                balance_missing = True
            continue
        live[account_id] = {
            "last": _holdings_last_successful_sync(summary),
            "unavailable": _holdings_unavailable(summary),
        }
        if not vanguard:
            continue
        _cash, total = _reported_cash_and_total(balances, summary)
        if total is None:
            balance_missing = True
            continue
        totals.append(float(total))
    if balance_missing or not saw_vanguard:
        return live, None
    return live, sum(totals)


def _staged_total(tenants):
    from google.cloud import bigquery

    if not tenants or not statement_is_read(_WAREHOUSE_SQL):
        return None
    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tenants", "STRING", list(tenants))]
    )
    rows = list(client.query(_WAREHOUSE_SQL, job_config=job_config, location="US").result())
    if len(rows) != 1:
        return None
    return rows[0]


def _render_pages(flask_app, user_id) -> dict:
    results = {"positions": False, "overview": False, "settings": False}
    expected = {
        "positions": "/positions",
        "overview": "/overview",
        "settings": "/profile",
    }
    client = flask_app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user_id)
        sess["_fresh"] = True
    for name, path in (
        ("positions", "/positions"),
        ("overview", "/overview"),
        ("settings", "/settings"),
    ):
        try:
            resp = client.get(path, headers={"X-HT-Full": "1"}, follow_redirects=True)
            final = getattr(resp.request, "path", "")
            results[name] = resp.status_code == 200 and final == expected[name]
        except Exception:
            results[name] = False
    return results


def run() -> dict:
    report = _blank_report()
    try:
        _fill_report(report)
    except Exception:
        return report
    return report


def _fill_report(report) -> None:
    if os.environ.get("HAPPYTRADER_SKIP_DB_INIT") != "1":
        os.environ["HAPPYTRADER_SKIP_DB_INIT"] = "1"
    if not _database_url():
        return
    if not statement_is_read(_HIDDEN_VANGUARD_SQL) or not statement_is_read(_ACCOUNTS_SQL):
        return
    flask_app, dbmod = _prepare_runtime()
    from app.snaptrade import SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS

    hidden = dbmod.fetch_all(_HIDDEN_VANGUARD_SQL)
    user_ids = {row.get("user_id") for row in hidden}
    if len(user_ids) != 1:
        return
    user_id = next(iter(user_ids))
    report["privacy"] = all(bool(row.get("active")) for row in hidden)
    accounts = dbmod.fetch_all(_ACCOUNTS_SQL, (user_id,))
    for row in accounts:
        row["user_id"] = user_id
    vanguard_rows = [row for row in accounts if is_vanguard(row)]
    tenants = []
    seen = set()
    for row in vanguard_rows:
        tenant = (row.get("tenant_id") or "").strip()
        if not tenant or tenant in seen:
            if not tenant:
                tenants = []
                break
            continue
        seen.add(tenant)
        tenants.append(tenant)
    live, snap_total = _live_reads(accounts)
    today = date.today()
    report["connections"] = connections_ok(
        accounts,
        today=today,
        threshold=SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS,
        live=live,
    )
    staged_row = None
    if tenants:
        try:
            staged_row = _staged_total(tenants)
        except Exception:
            staged_row = None
    if staged_row is None or not tenants:
        report["warehouse"] = "FAIL"
    else:
        expected = len(tenants)
        complete = (
            int(staged_row["history_n"] or 0) == expected
            and int(staged_row["current_n"] or 0) == expected
            and int(staged_row["balance_n"] or 0) == expected
        )
        report["warehouse"] = warehouse_status(
            complete=complete,
            staged=staged_row["staged_total"],
            live=snap_total,
        )
    rendered = _render_pages(flask_app, user_id)
    report.update(rendered)


def run_narrow_lines() -> list[str]:
    """Read prod analytics only. One share line per account letter and cause."""
    os.environ.pop("BQ_DATASET", None)
    if not statement_is_read(_NARROW_SQL):
        raise RuntimeError("read-only")
    _install_bq_guard()
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(_NARROW_SQL, location="US").result())
    if len(rows) != 1:
        raise RuntimeError("read-only")
    row = rows[0]
    return narrow_lines({key: row[key] for key in row.keys()})


def _print_narrow() -> int:
    lines = None
    reason = ""
    hint = ""
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        try:
            lines = run_narrow_lines()
        except Exception as exc:
            errors = getattr(exc, "errors", None)
            hint = ""
            if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                reason = str(errors[0].get("reason") or "")
                hint = _safe_hint(str(errors[0].get("message") or ""))
            else:
                reason = type(exc).__name__
    if reason and not re.fullmatch(r"[A-Za-z0-9_]+", reason):
        reason = ""
    if not narrow_lines_ok(lines):
        suffix = " ".join(part for part in (reason, hint) if part)
        print(f"breakdown=FAIL{(' ' + suffix) if suffix else ''}", flush=True)
        return 1
    print("\n".join(lines), flush=True)
    return 0


def run_gap_lines() -> list[str]:
    """Read prod analytics only. Returns one percent line per cause."""
    os.environ.pop("BQ_DATASET", None)
    if not statement_is_read(_GAP_SQL):
        raise RuntimeError("read-only")
    _install_bq_guard()
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(_GAP_SQL, location="US").result())
    if len(rows) != 1:
        raise RuntimeError("read-only")
    row = rows[0]
    has_gap = int(row["has_gap"] or 0) == 1
    fracs = {name: _finite(row[name]) for name in _GAP_ORDER}
    return gap_lines(attribute_fractions(fracs, has_gap=has_gap))


def _print_gap() -> int:
    lines = None
    reason = ""
    hint = ""
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        try:
            lines = run_gap_lines()
        except Exception as exc:
            errors = getattr(exc, "errors", None)
            hint = ""
            if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                reason = str(errors[0].get("reason") or "")
                hint = _safe_hint(str(errors[0].get("message") or ""))
            else:
                reason = type(exc).__name__
    if reason and not re.fullmatch(r"[A-Za-z0-9_]+", reason):
        reason = ""
    if (
        not lines
        or len(lines) != len(_GAP_LOG_ORDER)
        or any(not _GAP_LINE.match(line) for line in lines)
    ):
        suffix = " ".join(part for part in (reason, hint) if part)
        print(f"breakdown=FAIL{(' ' + suffix) if suffix else ''}", flush=True)
        return 1
    print("\n".join(lines), flush=True)
    return 0


def run_warehouse() -> str:
    """Read prod analytics only. Returns one flag line."""
    os.environ.pop("BQ_DATASET", None)
    if not statement_is_read(_WAREHOUSE_SQL):
        return "FAIL"
    _install_bq_guard()
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(_WAREHOUSE_SQL, location="US").result())
    if len(rows) != 1:
        return "FAIL"
    row = rows[0]
    tenant_n = int(row["tenant_n"] or 0)
    complete_n = int(row["complete_n"] or 0)
    complete = tenant_n > 0 and complete_n == tenant_n
    line = warehouse_report_line(
        complete=complete,
        staged=row["staged_book"],
        balance=row["balance_book"],
    )
    if not _WAREHOUSE_LINE.match(line):
        return "FAIL"
    return line


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    sys.tracebacklimit = 0
    logging.disable(logging.CRITICAL)
    if "--narrow" in args:
        return _print_narrow()
    if "--gap" in args:
        return _print_gap()
    line = "FAIL"
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        try:
            line = run_warehouse()
        except Exception:
            line = "FAIL"
    if not _WAREHOUSE_LINE.match(line):
        line = "FAIL"
    print(line, flush=True)
    return 0 if line == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
