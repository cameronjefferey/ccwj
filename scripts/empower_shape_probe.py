#!/usr/bin/env python3
"""Read-only shape of Empower rows in the raw seed tables.

SELECT-only against ``analytics_raw``. Prints counts, safe action labels,
and pass/fail flags. Does not print tenant ids, account numbers, symbols,
descriptions, or dollar amounts.

  EMPOWER_PRINT_TOTALS=1   when exactly one Empower tenant is present,
                           also print that tenant's account total, cash,
                           and holdings sum. Leave unset in CI.

Compare with Schwab and Vanguard is presence and sign-pattern only.
"""

from __future__ import annotations

import os
import re
import sys

PROJECT = "ccwj-dbt"
RAW = "analytics_raw"
WAREHOUSE = "analytics"

_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-")
_ACTION_RE = re.compile(r"^[A-Za-z][A-Za-z /.'-]{0,48}$")
_SAFE_SLUG_RE = re.compile(r"^empower(?:-[a-z0-9]+)?$")

_KNOWN = ("schwab", "alpaca", "fidelity", "interactive", "vanguard")
_HINTS = (
    "empower", "personal", "great", "retirement", "adp", "principal",
    "voya", "tiaa", "alight", "nationwide", "prudential", "lincoln",
    "transamerica", "milliman", "hancock", "wealthfront", "betterment",
    "sofi", "etrade", "robinhood", "webull", "tastytrade", "coinbase",
    "chase", "merrill", "morgan", "ubs", "ameritrade", "firstrade",
    "tradier", "tradestation", "apex", "drivewealth", "ally",
)

_SECURITY_TYPES = {
    "equity", "option", "bond", "fund", "cfd", "other", "structured",
    "cryptocurrency", "cash and money market", "future", "tokenized asset",
    "mutual fund", "etf", "cash",
}

# Seed actions whose cash amount the normalizer forces negative / positive.
_CASH_OUT = {
    "buy", "buy to open", "buy to close", "margin interest", "adr mgmt fee",
    "withdrawal",
}
_CASH_IN = {
    "sell", "sell to open", "sell to close", "sell short",
    "cash dividend", "qualified dividend", "special dividend",
    "special qual div", "pr yr cash div", "credit interest", "deposit",
}


def _num(column: str) -> str:
    return (
        "SAFE_CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE("
        f"CAST({column} AS STRING), '$', ''), ',', ''), '%', ''), ' ', '')) "
        "AS FLOAT64)"
    )


def _slug(column: str) -> str:
    return (
        "LOWER(SPLIT(TRIM(CAST({col} AS STRING)), ' ')[SAFE_OFFSET(0)])"
    ).format(col=column)


def is_safe_action_label(value: str) -> bool:
    text = (value or "").strip()
    if not _ACTION_RE.fullmatch(text):
        return False
    if " " in text:
        return True
    return any(ch.islower() for ch in text)


def is_safe_slug(value: str) -> bool:
    return bool(_SAFE_SLUG_RE.fullmatch((value or "").strip()))


def _assert_select_only(sql: str) -> None:
    stripped = sql.strip().lstrip("(")
    head = stripped.upper()
    if not head.startswith("WITH") and not head.startswith("SELECT"):
        raise SystemExit("refusing non-select empower probe")
    if re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER|LOAD)\b",
        sql,
        re.I,
    ):
        raise SystemExit("refusing empower probe sql that writes")


def discovery_sql() -> str:
    hints = ", ".join(f"'{name}'" for name in _HINTS)
    known = ", ".join(f"'{name}'" for name in _KNOWN)
    return f"""
    WITH tokens AS (
      SELECT {_slug('Account')} AS slug, 'history' AS surface
      FROM `{PROJECT}.{RAW}.trade_history`
      UNION ALL
      SELECT {_slug('Account')}, 'current'
      FROM `{PROJECT}.{RAW}.current_positions`
      UNION ALL
      SELECT {_slug('account')}, 'balances'
      FROM `{PROJECT}.{RAW}.account_balances`
    )
    SELECT
      COUNTIF(slug = 'empower') AS empower_exact,
      COUNTIF(STARTS_WITH(slug, 'empower-')) AS empower_hyphen,
      COUNTIF(REGEXP_CONTAINS(slug, r'empower') AND NOT STARTS_WITH(slug, 'empower'))
        AS empower_not_prefix,
      COUNTIF(slug IN ({hints}) AND slug != 'empower') AS other_hint_rows,
      COUNTIF(slug NOT IN ({known}) AND slug NOT IN ({hints}) AND slug != '')
        AS unknown_slug_rows
    FROM tokens
    """


def hint_sql() -> str:
    hints = ", ".join(f"'{name}'" for name in _HINTS if name != "empower")
    return f"""
    WITH tokens AS (
      SELECT {_slug('Account')} AS slug, 'history' AS surface
      FROM `{PROJECT}.{RAW}.trade_history`
      UNION ALL
      SELECT {_slug('Account')}, 'current'
      FROM `{PROJECT}.{RAW}.current_positions`
      UNION ALL
      SELECT {_slug('account')}, 'balances'
      FROM `{PROJECT}.{RAW}.account_balances`
    )
    SELECT slug, surface, COUNT(*) AS n
    FROM tokens
    WHERE STARTS_WITH(slug, 'empower') OR slug IN ({hints})
    GROUP BY slug, surface
    ORDER BY slug, surface
    """


def history_sql() -> str:
    amount = _num("Amount")
    fee = _num("fees_and_comm")
    qty = _num("Quantity")
    return f"""
    WITH rows AS (
      SELECT
        {_slug('Account')} AS slug,
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        TRIM(CAST(Action AS STRING)) AS action,
        UPPER(TRIM(CAST(Symbol AS STRING))) AS symbol,
        LOWER(TRIM(CAST(Description AS STRING))) AS description,
        {amount} AS amount,
        {fee} AS fee,
        {qty} AS qty,
        TRIM(CAST(Date AS STRING)) AS trade_date
      FROM `{PROJECT}.{RAW}.trade_history`
    )
    SELECT
      COUNT(*) AS n,
      COUNT(DISTINCT tenant_id) AS tenants,
      COUNTIF(REGEXP_CONTAINS(symbol, r'\\d{{6}}[CP]\\d{{8}}')) AS occ_symbols,
      COUNTIF(symbol = '') AS blank_symbols,
      COUNTIF(STARTS_WITH(symbol, 'DESC-')) AS description_keys,
      COUNTIF(REGEXP_CONTAINS(symbol, r'^[A-Z0-9]{{9}}$')) AS cusip_like,
      COUNTIF(REGEXP_CONTAINS(symbol, r'^[A-Z]{{2}}[A-Z0-9]{{10}}$')
              AND NOT REGEXP_CONTAINS(symbol, r'^[A-Z0-9]{{9}}$')) AS figi_like,
      COUNTIF(REGEXP_CONTAINS(symbol, r'^[A-Z][A-Z0-9.-]{{0,6}}$')) AS ticker_like,
      COUNTIF(REGEXP_CONTAINS(trade_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}')) AS iso_dates,
      COUNTIF(REGEXP_CONTAINS(trade_date, r'^\\d{{1,2}}/\\d{{1,2}}/\\d{{2,4}}$')) AS mdy_dates,
      COUNTIF(trade_date = '') AS blank_dates,
      COUNTIF(qty < 0) AS negative_qty,
      COUNTIF(fee < 0) AS negative_fees,
      COUNTIF(fee > 0) AS positive_fees,
      COUNTIF(fee IS NULL OR fee = 0) AS blank_or_zero_fees,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(buy to open|sell to open|buy to close|sell to close)\\b'))
        AS desc_open_close,
      COUNTIF(REGEXP_CONTAINS(description, r'\\bdividend')) AS desc_dividend,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(contribution|payroll|deferral)\\b'))
        AS desc_contribution,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(rollover|transfer)\\b')) AS desc_transfer,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(reinvest|rei)\\b')) AS desc_reinvest,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(interest)\\b')) AS desc_interest,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(fee|commission)\\b')) AS desc_fee,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(withdraw|distribution)\\b')) AS desc_withdrawal,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(exchange|conversion)\\b')) AS desc_exchange,
      COUNTIF(REGEXP_CONTAINS(description, r'\\b(loan|forfeiture)\\b')) AS desc_loan
    FROM rows
    WHERE STARTS_WITH(slug, 'empower')
    """


def action_sql() -> str:
    amount = _num("Amount")
    return f"""
    WITH rows AS (
      SELECT
        {_slug('Account')} AS slug,
        TRIM(CAST(Action AS STRING)) AS action,
        {amount} AS amount,
        UPPER(TRIM(CAST(Symbol AS STRING))) AS symbol
      FROM `{PROJECT}.{RAW}.trade_history`
    )
    SELECT
      action,
      COUNT(*) AS n,
      COUNTIF(amount > 0) AS pos,
      COUNTIF(amount < 0) AS neg,
      COUNTIF(amount = 0) AS zero_amt,
      COUNTIF(amount IS NULL) AS null_amt,
      COUNTIF(REGEXP_CONTAINS(symbol, r'\\d{{6}}[CP]\\d{{8}}')) AS occ
    FROM rows
    WHERE STARTS_WITH(slug, 'empower')
    GROUP BY action
    """


def current_sql() -> str:
    mv = _num("market_value")
    cost = _num("cost_bases")
    qty = _num("Quantity")
    return f"""
    WITH rows AS (
      SELECT
        {_slug('Account')} AS slug,
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        LOWER(TRIM(CAST(security_type AS STRING))) AS security_type,
        UPPER(TRIM(CAST(Symbol AS STRING))) AS symbol,
        {mv} AS mv,
        {cost} AS cost,
        {qty} AS qty
      FROM `{PROJECT}.{RAW}.current_positions`
    )
    SELECT
      COUNT(*) AS n,
      COUNT(DISTINCT tenant_id) AS tenants,
      COUNTIF(security_type IN ('cash and money market', '')
              OR symbol IN ('ACCOUNT TOTAL', 'POSITIONS TOTAL')) AS not_holdings,
      COUNTIF(NOT (security_type IN ('cash and money market', '')
              OR symbol IN ('ACCOUNT TOTAL', 'POSITIONS TOTAL'))) AS holdings,
      COUNTIF(REGEXP_CONTAINS(symbol, r'\\d{{6}}[CP]\\d{{8}}')) AS occ_symbols,
      COUNTIF(STARTS_WITH(symbol, 'DESC-')) AS description_keys,
      COUNTIF(REGEXP_CONTAINS(symbol, r'^[A-Z0-9]{{9}}$')) AS cusip_like,
      COUNTIF(symbol = '' OR symbol IS NULL) AS blank_symbols,
      COUNTIF(cost IS NULL OR cost = 0) AS blank_or_zero_cost,
      COUNTIF(mv IS NULL) AS null_market_value,
      COUNTIF(qty < 0) AS negative_qty
    FROM rows
    WHERE STARTS_WITH(slug, 'empower')
    """


def security_type_sql() -> str:
    return f"""
    SELECT
      LOWER(TRIM(CAST(security_type AS STRING))) AS security_type,
      COUNT(*) AS n
    FROM `{PROJECT}.{RAW}.current_positions`
    WHERE STARTS_WITH({_slug('Account')}, 'empower')
    GROUP BY security_type
    """


def balance_sql() -> str:
    mv = _num("market_value")
    return f"""
    SELECT
      LOWER(TRIM(CAST(row_type AS STRING))) AS row_type,
      COUNT(*) AS n,
      COUNT(DISTINCT NULLIF(TRIM(CAST(tenant_id AS STRING)), '')) AS tenants,
      COUNTIF({mv} IS NULL) AS null_mv
    FROM `{PROJECT}.{RAW}.account_balances`
    WHERE STARTS_WITH({_slug('account')}, 'empower')
    GROUP BY row_type
    """


def reconcile_sql() -> str:
    """Per-tenant seed total vs cash + holdings. Flags only, no dollars."""
    bal_mv = _num("market_value")
    pos_mv = _num("market_value")
    return f"""
    WITH bal AS (
      SELECT
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        LOWER(TRIM(CAST(row_type AS STRING))) AS row_type,
        {bal_mv} AS mv
      FROM `{PROJECT}.{RAW}.account_balances`
      WHERE STARTS_WITH({_slug('account')}, 'empower')
    ),
    totals AS (
      SELECT tenant_id, MIN(mv) AS mv_min, MAX(mv) AS mv_max, COUNT(*) AS n
      FROM bal
      WHERE row_type = 'account_total' AND tenant_id IS NOT NULL
      GROUP BY tenant_id
    ),
    cash AS (
      SELECT tenant_id, SUM(mv) AS cash
      FROM bal
      WHERE row_type = 'cash' AND tenant_id IS NOT NULL
      GROUP BY tenant_id
    ),
    holds AS (
      SELECT
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        SUM({pos_mv}) AS holdings
      FROM `{PROJECT}.{RAW}.current_positions`
      WHERE STARTS_WITH({_slug('Account')}, 'empower')
        AND LOWER(TRIM(COALESCE(CAST(security_type AS STRING), '')))
            NOT IN ('cash and money market', '')
        AND UPPER(TRIM(COALESCE(CAST(Symbol AS STRING), '')))
            NOT IN ('ACCOUNT TOTAL', 'POSITIONS TOTAL')
      GROUP BY tenant_id
    ),
    paired AS (
      SELECT
        t.tenant_id,
        t.n AS total_rows,
        t.mv_min,
        t.mv_max,
        c.cash,
        h.holdings,
        ABS(COALESCE(t.mv_max, 0) - COALESCE(t.mv_min, 0)) AS total_span,
        ABS(COALESCE(t.mv_max, 0) - (COALESCE(c.cash, 0) + COALESCE(h.holdings, 0)))
          AS gap
      FROM totals t
      LEFT JOIN cash c USING (tenant_id)
      LEFT JOIN holds h USING (tenant_id)
    )
    SELECT
      COUNT(*) AS tenants_with_total,
      COUNTIF(total_span > 1 OR total_rows > 1 AND total_span > 1) AS tenants_total_disagrees,
      COUNTIF(mv_min IS NULL OR mv_max IS NULL) AS tenants_null_total,
      COUNTIF(gap > 1) AS tenants_cash_plus_holdings_gap,
      COUNTIF(holdings IS NULL) AS tenants_missing_holdings,
      COUNTIF(cash IS NULL) AS tenants_missing_cash
    FROM paired
    """


def totals_sql() -> str:
    """Dollar lines. Caller prints them only for a single tenant."""
    bal_mv = _num("market_value")
    pos_mv = _num("market_value")
    cost = _num("cost_bases")
    return f"""
    WITH bal AS (
      SELECT
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        LOWER(TRIM(CAST(row_type AS STRING))) AS row_type,
        {bal_mv} AS mv
      FROM `{PROJECT}.{RAW}.account_balances`
      WHERE STARTS_WITH({_slug('account')}, 'empower')
    ),
    holds AS (
      SELECT
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        SUM({pos_mv}) AS holdings,
        SUM(IF({cost} IS NULL OR {cost} = 0, 0, {cost})) AS cost_sum,
        COUNTIF({cost} IS NULL OR {cost} = 0) AS holdings_missing_cost
      FROM `{PROJECT}.{RAW}.current_positions`
      WHERE STARTS_WITH({_slug('Account')}, 'empower')
        AND LOWER(TRIM(COALESCE(CAST(security_type AS STRING), '')))
            NOT IN ('cash and money market', '')
        AND UPPER(TRIM(COALESCE(CAST(Symbol AS STRING), '')))
            NOT IN ('ACCOUNT TOTAL', 'POSITIONS TOTAL')
      GROUP BY tenant_id
    )
    SELECT
      (SELECT COUNT(DISTINCT tenant_id) FROM bal WHERE tenant_id IS NOT NULL)
        AS balance_tenants,
      (SELECT MIN(mv) FROM bal WHERE row_type = 'account_total') AS total_min,
      (SELECT MAX(mv) FROM bal WHERE row_type = 'account_total') AS total_max,
      (SELECT SUM(mv) FROM bal WHERE row_type = 'cash') AS cash_sum,
      (SELECT SUM(holdings) FROM holds) AS holdings_sum,
      (SELECT SUM(cost_sum) FROM holds) AS cost_sum,
      (SELECT SUM(holdings_missing_cost) FROM holds) AS holdings_missing_cost
    """


def broker_presence_sql() -> str:
    """Which mapped actions and option symbols Schwab / Vanguard / Empower have."""
    return f"""
    WITH rows AS (
      SELECT
        {_slug('Account')} AS slug,
        LOWER(TRIM(CAST(Action AS STRING))) AS action,
        REGEXP_CONTAINS(UPPER(TRIM(CAST(Symbol AS STRING))), r'\\d{{6}}[CP]\\d{{8}}')
          AS is_occ
      FROM `{PROJECT}.{RAW}.trade_history`
    )
    SELECT
      CASE
        WHEN slug = 'schwab' THEN 'schwab'
        WHEN slug = 'vanguard' THEN 'vanguard'
        WHEN STARTS_WITH(slug, 'empower') THEN 'empower'
      END AS broker,
      COUNTIF(action = 'buy') > 0 AS has_buy,
      COUNTIF(action = 'sell') > 0 AS has_sell,
      COUNTIF(action IN ('buy to open', 'sell to open', 'buy to close', 'sell to close')) > 0
        AS has_option_verb,
      COUNTIF(action IN ('expired', 'assigned', 'exchange or exercise')) > 0
        AS has_option_lifecycle,
      COUNTIF(action = 'cash dividend') > 0 AS has_dividend,
      COUNTIF(action IN ('deposit', 'withdrawal', 'cash transfer')) > 0 AS has_cash_move,
      COUNTIF(action = 'adr mgmt fee') > 0 AS has_fee,
      COUNTIF(action = 'credit interest') > 0 AS has_interest,
      COUNTIF(action = 'journal') > 0 AS has_journal,
      COUNTIF(is_occ) > 0 AS has_occ
    FROM rows
    WHERE slug IN ('schwab', 'vanguard') OR STARTS_WITH(slug, 'empower')
    GROUP BY broker
    """


def warehouse_sql() -> str:
    """Prod warehouse catch-all presence. SELECT only. Flags, no dollars."""
    return f"""
    WITH hist AS (
      SELECT COUNT(*) AS n
      FROM `{PROJECT}.{WAREHOUSE}.stg_history`
      WHERE STARTS_WITH({_slug('account')}, 'empower')
    ),
    cur AS (
      SELECT COUNT(*) AS n
      FROM `{PROJECT}.{WAREHOUSE}.stg_current`
      WHERE STARTS_WITH({_slug('account')}, 'empower')
    ),
    bal AS (
      SELECT COUNT(*) AS n
      FROM `{PROJECT}.{WAREHOUSE}.stg_account_balances`
      WHERE STARTS_WITH({_slug('account')}, 'empower')
        AND row_type = 'account_total'
    )
    SELECT
      (SELECT n FROM hist) AS warehouse_history_rows,
      (SELECT n FROM cur) AS warehouse_current_rows,
      (SELECT n FROM bal) AS warehouse_total_rows
    """


def _client():
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT)


def _query(sql: str):
    _assert_select_only(sql)
    return list(_client().query(sql, location="US").result())


def _print(key: str, value) -> None:
    rendered = "null" if value is None else str(value)
    if "$" in rendered or _UUID_RE.search(rendered):
        rendered = "redacted"
    print(f"{key}={rendered}")


def _print_actions(rows) -> list[str]:
    safe = []
    unsafe = 0
    sign_mismatch = []
    for row in rows:
        label = str(row["action"] or "").strip()
        if not is_safe_action_label(label):
            unsafe += 1
            continue
        safe.append(label)
        key = label.casefold()
        pos = int(row["pos"] or 0)
        neg = int(row["neg"] or 0)
        _print(
            f"action.{label.replace(' ', '_')}",
            f"n={int(row['n'])},pos={pos},neg={neg},"
            f"zero={int(row['zero_amt'] or 0)},null={int(row['null_amt'] or 0)},"
            f"occ={int(row['occ'] or 0)}",
        )
        if key in _CASH_OUT and pos > neg and pos > 0:
            sign_mismatch.append(label)
        if key in _CASH_IN and neg > pos and neg > 0:
            sign_mismatch.append(label)
    _print("unsafe_action_labels", unsafe)
    _print("sign_mismatch_actions", ",".join(sorted(sign_mismatch)) or "none")
    return safe


def _print_security_types(rows) -> None:
    other = 0
    for row in rows:
        label = str(row["security_type"] or "").strip()
        if label in _SECURITY_TYPES:
            shown = label if label else "blank"
            _print(f"security_type.{shown.replace(' ', '_')}", int(row["n"]))
        else:
            other += int(row["n"] or 0)
    _print("security_type_other_rows", other)


def run() -> int:
    print("empower_shape_probe=start")
    try:
        found = _query(discovery_sql())
    except Exception as exc:
        message = str(exc).splitlines()[0][:180]
        if "$" in message or _UUID_RE.search(message):
            message = "query failed"
        print("empower_shape_probe=fail")
        print(f"empower_shape_error={message}")
        return 1
    if len(found) != 1:
        print("empower_shape_probe=fail")
        return 1
    disc = found[0]
    for key in (
        "empower_exact", "empower_hyphen", "empower_not_prefix",
        "other_hint_rows", "unknown_slug_rows",
    ):
        _print(key, int(disc[key] or 0))

    empower_rows = int(disc["empower_exact"] or 0) + int(disc["empower_hyphen"] or 0)
    try:
        for row in _query(hint_sql()):
            slug = str(row["slug"] or "")
            if not is_safe_slug(slug) and slug not in _HINTS:
                continue
            # Other brokers' volumes stay out of the log once Empower is found.
            if empower_rows and not str(slug).startswith("empower"):
                continue
            _print(f"hint.{slug}.{row['surface']}", int(row["n"]))
    except Exception:
        _print("hint_query", "unavailable")
    if empower_rows == 0:
        print("empower_shape_probe=absent")
        return 0

    sections = (
        ("history", history_sql()),
        ("current", current_sql()),
    )
    for name, sql in sections:
        try:
            rows = _query(sql)
        except Exception:
            _print(name, "unavailable")
            continue
        if len(rows) != 1:
            _print(name, "fail")
            continue
        row = rows[0]
        for key in row.keys():
            _print(f"{name}.{key}", row[key])

    try:
        _print_actions(_query(action_sql()))
    except Exception:
        _print("actions", "unavailable")

    try:
        _print_security_types(_query(security_type_sql()))
    except Exception:
        _print("security_types", "unavailable")

    try:
        for row in _query(balance_sql()):
            kind = str(row["row_type"] or "").strip()
            if kind not in {"cash", "account_total"}:
                _print("balance.other_row_type_rows", int(row["n"]))
                continue
            _print(
                f"balance.{kind}",
                f"n={int(row['n'])},tenants={int(row['tenants'] or 0)},"
                f"null_mv={int(row['null_mv'] or 0)}",
            )
    except Exception:
        _print("balances", "unavailable")

    try:
        rows = _query(reconcile_sql())
        if len(rows) == 1:
            for key in rows[0].keys():
                _print(f"reconcile.{key}", int(rows[0][key] or 0))
    except Exception:
        _print("reconcile", "unavailable")

    try:
        for row in _query(broker_presence_sql()):
            broker = str(row["broker"] or "")
            if broker not in {"schwab", "vanguard", "empower"}:
                continue
            flags = ",".join(
                f"{key}={str(row[key]).lower()}"
                for key in row.keys()
                if key != "broker"
            )
            _print(f"presence.{broker}", flags)
    except Exception:
        _print("presence", "unavailable")

    try:
        rows = _query(warehouse_sql())
        if len(rows) == 1:
            for key in rows[0].keys():
                _print(key, int(rows[0][key] or 0))
    except Exception:
        _print("warehouse", "unavailable")

    if os.environ.get("EMPOWER_PRINT_TOTALS") == "1":
        _print_totals()

    print("empower_shape_probe=done")
    return 0


def _print_totals() -> None:
    try:
        rows = _query(totals_sql())
    except Exception:
        _print("totals", "unavailable")
        return
    if len(rows) != 1:
        _print("totals", "unavailable")
        return
    row = rows[0]
    tenants = int(row["balance_tenants"] or 0)
    _print("totals.balance_tenants", tenants)
    _print("totals.holdings_missing_cost", int(row["holdings_missing_cost"] or 0))
    if tenants != 1:
        _print("totals.dollars", "withheld")
        return
    for key in ("total_min", "total_max", "cash_sum", "holdings_sum", "cost_sum"):
        value = row[key]
        if value is None:
            _print(f"totals.{key}", "null")
        else:
            _print(f"totals.{key}", f"{float(value):.2f}")


def main(argv: list[str] | None = None) -> int:
    del argv
    return run()


if __name__ == "__main__":
    sys.exit(main())
