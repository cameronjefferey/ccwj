#!/usr/bin/env python3
"""Select-only summary of Empower rows in the raw seed tables.

Prints counts per activity type, instrument type, and cash-sign
pattern. Does not print tenant ids, symbols, descriptions, or dollar
amounts.

Intended for a later ``dev_mirror`` run. Tests call the SQL builders
only. Do not execute this script from a unit test.
"""

from __future__ import annotations

import re
import sys

PROJECT = "ccwj-dbt"
RAW = "analytics_raw"

_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-")
_ACTION_RE = re.compile(r"^[A-Za-z][A-Za-z /.'-]{0,48}$")
_SECURITY_TYPES = {
    "equity", "option", "bond", "fund", "cfd", "other", "structured",
    "cryptocurrency", "cash and money market", "future", "tokenized asset",
    "mutual fund", "etf", "cash",
}
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


def _assert_select_only(sql: str) -> None:
    stripped = sql.strip().lstrip("(")
    head = stripped.upper()
    if not head.startswith("WITH") and not head.startswith("SELECT"):
        raise SystemExit("refusing non-select empower summary")
    if re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER|LOAD)\b",
        sql,
        re.I,
    ):
        raise SystemExit("refusing empower summary sql that writes")


def _empower_where(column: str) -> str:
    return f"STARTS_WITH({_slug(column)}, 'empower')"


def activity_sql() -> str:
    """One row per seed Action: counts and cash-sign buckets. No dollars."""
    amount = _num("Amount")
    return f"""
    WITH rows AS (
      SELECT
        TRIM(CAST(Action AS STRING)) AS action,
        {amount} AS amount,
        REGEXP_CONTAINS(
          UPPER(TRIM(CAST(Symbol AS STRING))), r'\\d{{6}}[CP]\\d{{8}}'
        ) AS is_occ
      FROM `{PROJECT}.{RAW}.trade_history`
      WHERE {_empower_where('Account')}
    )
    SELECT
      action,
      COUNT(*) AS n,
      COUNTIF(amount > 0) AS pos,
      COUNTIF(amount < 0) AS neg,
      COUNTIF(amount = 0) AS zero_amt,
      COUNTIF(amount IS NULL) AS null_amt,
      COUNTIF(is_occ) AS occ,
      COUNTIF(NOT is_occ) AS not_occ
    FROM rows
    GROUP BY action
    ORDER BY n DESC
    """


def instrument_sql() -> str:
    """Current-position security_type counts. No symbols."""
    return f"""
    SELECT
      LOWER(TRIM(CAST(security_type AS STRING))) AS security_type,
      COUNT(*) AS n
    FROM `{PROJECT}.{RAW}.current_positions`
    WHERE {_empower_where('Account')}
    GROUP BY security_type
    ORDER BY n DESC
    """


def presence_sql() -> str:
    """Row counts on the three raw surfaces. No dollars."""
    return f"""
    SELECT
      (SELECT COUNT(*) FROM `{PROJECT}.{RAW}.trade_history`
        WHERE {_empower_where('Account')}) AS history_rows,
      (SELECT COUNT(*) FROM `{PROJECT}.{RAW}.current_positions`
        WHERE {_empower_where('Account')}) AS current_rows,
      (SELECT COUNT(*) FROM `{PROJECT}.{RAW}.account_balances`
        WHERE {_empower_where('account')}) AS balance_rows
    """


def _print(key: str, value) -> None:
    rendered = "null" if value is None else str(value)
    if "$" in rendered or _UUID_RE.search(rendered):
        rendered = "redacted"
    print(f"{key}={rendered}")


def _client():
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT)


def _query(sql: str):
    _assert_select_only(sql)
    return list(_client().query(sql, location="US").result())


def _print_actions(rows) -> None:
    unsafe = 0
    mismatch = []
    for row in rows:
        label = str(row["action"] or "").strip()
        if not is_safe_action_label(label):
            unsafe += int(row["n"] or 0)
            continue
        pos = int(row["pos"] or 0)
        neg = int(row["neg"] or 0)
        _print(
            f"action.{label.replace(' ', '_')}",
            f"n={int(row['n'])},pos={pos},neg={neg},"
            f"zero={int(row['zero_amt'] or 0)},null={int(row['null_amt'] or 0)},"
            f"occ={int(row['occ'] or 0)},not_occ={int(row['not_occ'] or 0)}",
        )
        key = label.casefold()
        if key in _CASH_OUT and pos > neg and pos > 0:
            mismatch.append(label)
        if key in _CASH_IN and neg > pos and neg > 0:
            mismatch.append(label)
    _print("unsafe_action_rows", unsafe)
    _print("sign_mismatch_actions", ",".join(sorted(mismatch)) or "none")


def _print_instruments(rows) -> None:
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
    print("empower_raw_activity_summary=start")
    try:
        found = _query(presence_sql())
    except Exception as exc:
        message = str(exc).splitlines()[0][:180]
        if "$" in message or _UUID_RE.search(message):
            message = "query failed"
        print("empower_raw_activity_summary=fail")
        print(f"empower_raw_activity_error={message}")
        return 1
    if len(found) != 1:
        print("empower_raw_activity_summary=fail")
        return 1
    row = found[0]
    for key in ("history_rows", "current_rows", "balance_rows"):
        _print(key, int(row[key] or 0))
    if int(row["history_rows"] or 0) + int(row["current_rows"] or 0) == 0:
        print("empower_raw_activity_summary=absent")
        return 0
    try:
        _print_actions(_query(activity_sql()))
    except Exception:
        _print("actions", "unavailable")
    try:
        _print_instruments(_query(instrument_sql()))
    except Exception:
        _print("security_types", "unavailable")
    print("empower_raw_activity_summary=done")
    return 0


if __name__ == "__main__":
    sys.exit(run())
