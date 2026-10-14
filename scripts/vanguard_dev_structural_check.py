#!/usr/bin/env python3
"""Structural checks for a Vanguard dbt build in analytics_dev.

SELECT-only. Prints pass/fail flags and null rates. Does not print
balances, symbols, quantities, tenant ids, or dollar amounts.

  --check-profile PATH     refuse unless the dbt profile dataset is analytics_dev
  --check-debug-log PATH   refuse unless `dbt debug` reports schema analytics_dev
  --scrub-dbt-log PATH     print a redacted dbt summary (no row values)
  --unknown-actions        print Vanguard seed Action labels missing from the allow-list
  (no flag)                row-count, null-rate, and account-total reconcile flags
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT = "ccwj-dbt"
DEV_DATASET = "analytics_dev"
RAW_DATASET = "analytics_raw"
FORBIDDEN_DATASETS = frozenset({"analytics", "analytics_raw", "analytics_backups"})

_DATASET_RE = re.compile(r"(?m)^[ \t]*dataset:[ \t]*([A-Za-z0-9_]+)[ \t]*$")
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_DEBUG_STAMP_RE = re.compile(r"^\s*\d{2}:\d{2}:\d{2}\s+")
_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-")
_KEEP_RE = re.compile(
    r"(Done\.|Failure in |Error in |Database Error|Completed with |"
    r"PASS=|ERROR=|SKIP=|WARN=|Got \d+ result)"
)
_ACTION_RE = re.compile(r"^[A-Za-z][A-Za-z /.'-]{0,48}$")
_SLUG = (
    "lower(split(trim(cast({col} as string)), ' ')[safe_offset(0)]) = 'vanguard'"
)


def profile_dataset(text: str) -> str:
    found = _DATASET_RE.findall(text)
    if len(found) != 1:
        raise SystemExit("refusing profile: expected exactly one dataset")
    return found[0]


def assert_dev_profile(text: str) -> None:
    dataset = profile_dataset(text)
    if dataset != DEV_DATASET or dataset in FORBIDDEN_DATASETS:
        raise SystemExit("refusing profile: dbt target is not analytics_dev")


def assert_debug_schema(text: str) -> None:
    """Read the target schema from `dbt debug`.

    dbt 1.9 prefixes every line with a timestamp and an ANSI reset, so a
    raw ``^schema:`` search misses ``schema: analytics_dev`` and would
    refuse a dev build that is already pointed at the right dataset.
    """
    cleaned = _ANSI_RE.sub("", text)
    schemas = []
    for line in cleaned.splitlines():
        body = _DEBUG_STAMP_RE.sub("", line).strip()
        match = re.match(r"schema:\s*([A-Za-z0-9_]+)\s*$", body)
        if match:
            schemas.append(match.group(1))
    if DEV_DATASET not in schemas:
        raise SystemExit("refusing dbt debug: schema analytics_dev was not reported")
    if any(name in FORBIDDEN_DATASETS for name in schemas):
        raise SystemExit("refusing dbt debug: a prod schema was reported")


def is_safe_action_label(value: str) -> bool:
    """Action phrases are safe to print. A one-word all-caps token is not.

    Seed Action values are title case ("Buy", "Cash Dividend"). A ticker
    that landed in that column is a short all-caps token and must stay
    out of the log.
    """
    text = value.strip()
    if not _ACTION_RE.fullmatch(text):
        return False
    if " " in text:
        return True
    return any(ch.islower() for ch in text)


def scrub_dbt_line(line: str) -> str | None:
    if "$" in line or _UUID_RE.search(line):
        return None
    if not _KEEP_RE.search(line):
        return None
    redacted = re.sub(r"'[^']{21,}'", "'…'", line)
    redacted = re.sub(r'"[^"]{21,}"', '"…"', redacted)
    return redacted.strip()[:240]


def allowed_actions(yml_text: str) -> list[str]:
    """Action accepted_values from the Vanguard history schema test."""
    marker = yml_text.find("name: Action")
    if marker < 0:
        raise SystemExit("vanguard schema yml has no Action column")
    rest = yml_text[marker:]
    end = rest.find("\n  - name: stg_broker_vanguard_current")
    block = rest if end < 0 else rest[:end]
    values_at = block.find("values:")
    if values_at < 0:
        raise SystemExit("vanguard schema yml has no Action accepted_values")
    labels = []
    for line in block[values_at:].splitlines()[1:]:
        match = re.match(r"\s+-\s+(.+?)\s*$", line)
        if not match:
            if labels:
                break
            continue
        labels.append(match.group(1).strip().strip("'\""))
    if not labels:
        raise SystemExit("vanguard Action allow-list is empty")
    return labels


def _null_rate(column: str) -> str:
    """Share of rows whose string form is blank. Rounded in SQL."""
    return (
        "ROUND(SAFE_DIVIDE(COUNTIF("
        f"NULLIF(TRIM(CAST({column} AS STRING)), '') IS NULL"
        "), NULLIF(COUNT(*), 0)), 3)"
    )


def _structural_sql() -> str:
    slug_account = _SLUG.format(col="account")
    history = f"`{PROJECT}.{DEV_DATASET}.stg_broker_vanguard_history`"
    current = f"`{PROJECT}.{DEV_DATASET}.stg_broker_vanguard_current`"
    balances = f"`{PROJECT}.{DEV_DATASET}.stg_broker_vanguard_balances`"
    stg_history = f"`{PROJECT}.{DEV_DATASET}.stg_history`"
    stg_current = f"`{PROJECT}.{DEV_DATASET}.stg_current`"
    stg_balances = f"`{PROJECT}.{DEV_DATASET}.stg_account_balances`"
    wealth = f"`{PROJECT}.{DEV_DATASET}.mart_wealth_daily`"
    raw_balances = f"`{PROJECT}.{RAW_DATASET}.account_balances`"
    seed_mv = (
        "SAFE_CAST(TRIM(REPLACE(REPLACE(REPLACE("
        "CAST(market_value AS STRING), '$', ''), ',', ''), ' ', '')) AS FLOAT64)"
    )
    return f"""
    WITH history AS (SELECT * FROM {history}),
    current_pos AS (SELECT * FROM {current}),
    balances AS (SELECT * FROM {balances}),
    seed AS (
      SELECT
        NULLIF(TRIM(CAST(tenant_id AS STRING)), '') AS tenant_id,
        {seed_mv} AS mv
      FROM {raw_balances}
      WHERE {_SLUG.format(col="account")}
        AND LOWER(TRIM(CAST(row_type AS STRING))) = 'account_total'
    ),
    seed_by_tenant AS (
      SELECT tenant_id, MIN(mv) AS mv_min, MAX(mv) AS mv_max
      FROM seed
      WHERE tenant_id IS NOT NULL
      GROUP BY tenant_id
    ),
    stg_totals AS (
      SELECT tenant_id, MIN(market_value) AS mv_min, MAX(market_value) AS mv_max
      FROM {stg_balances}
      WHERE {slug_account}
        AND row_type = 'account_total'
        AND tenant_id IS NOT NULL
      GROUP BY tenant_id
    ),
    wealth_latest AS (
      SELECT tenant_id, account_value
      FROM {wealth}
      WHERE {slug_account}
      QUALIFY date = MAX(date) OVER (PARTITION BY tenant_id)
    ),
    wealth_by_tenant AS (
      SELECT tenant_id, MIN(account_value) AS mv_min, MAX(account_value) AS mv_max
      FROM wealth_latest
      WHERE tenant_id IS NOT NULL
      GROUP BY tenant_id
    ),
    staging_pairs AS (
      SELECT
        s.tenant_id AS seed_tenant,
        t.tenant_id AS other_tenant,
        s.mv_min AS seed_min,
        s.mv_max AS seed_max,
        t.mv_min AS other_min,
        t.mv_max AS other_max
      FROM seed_by_tenant s
      FULL OUTER JOIN stg_totals t
        ON s.tenant_id = t.tenant_id
    ),
    wealth_pairs AS (
      SELECT
        s.tenant_id AS seed_tenant,
        w.tenant_id AS other_tenant,
        s.mv_min AS seed_min,
        s.mv_max AS seed_max,
        w.mv_min AS other_min,
        w.mv_max AS other_max
      FROM seed_by_tenant s
      FULL OUTER JOIN wealth_by_tenant w
        ON s.tenant_id = w.tenant_id
    )
    SELECT
      IF((SELECT COUNT(*) FROM history) > 0, 'pass', 'fail') AS history_rows_gt_0,
      IF((SELECT COUNT(*) FROM current_pos) > 0, 'pass', 'fail') AS current_rows_gt_0,
      IF((SELECT COUNT(*) FROM balances) > 0, 'pass', 'fail') AS balances_rows_gt_0,
      IF((SELECT COUNT(*) FROM {stg_history} WHERE {slug_account}) > 0, 'pass', 'fail')
        AS union_history_rows_gt_0,
      IF((SELECT COUNT(*) FROM {stg_current} WHERE {slug_account}) > 0, 'pass', 'fail')
        AS union_current_rows_gt_0,
      IF((SELECT COUNT(*) FROM {stg_balances} WHERE {slug_account}) > 0, 'pass', 'fail')
        AS union_balances_rows_gt_0,
      (SELECT {_null_rate('tenant_id')} FROM history) AS history_null_tenant_id,
      (SELECT {_null_rate('Action')} FROM history) AS history_null_action,
      (SELECT {_null_rate('Date')} FROM history) AS history_null_date,
      (SELECT {_null_rate('Symbol')} FROM history) AS history_null_symbol,
      (SELECT {_null_rate('Amount')} FROM history) AS history_null_amount,
      (SELECT {_null_rate('tenant_id')} FROM current_pos) AS current_null_tenant_id,
      (SELECT {_null_rate('Symbol')} FROM current_pos) AS current_null_symbol,
      (SELECT {_null_rate('Quantity')} FROM current_pos) AS current_null_quantity,
      (SELECT {_null_rate('market_value')} FROM current_pos) AS current_null_market_value,
      (SELECT {_null_rate('cost_bases')} FROM current_pos) AS current_null_cost_bases,
      (SELECT {_null_rate('security_type')} FROM current_pos) AS current_null_security_type,
      (SELECT {_null_rate('tenant_id')} FROM balances) AS balances_null_tenant_id,
      (SELECT {_null_rate('row_type')} FROM balances) AS balances_null_row_type,
      (SELECT {_null_rate('market_value')} FROM balances) AS balances_null_market_value,
      (SELECT {_null_rate('src_priority')} FROM balances) AS balances_null_src_priority,
      IF(
        (SELECT COUNT(*) FROM seed) > 0
        AND (SELECT COUNTIF(tenant_id IS NULL) FROM seed) = 0
        AND (SELECT COUNT(*) FROM staging_pairs) > 0
        AND (
          SELECT COUNTIF(
            seed_tenant IS NULL
            OR other_tenant IS NULL
            OR seed_min IS NULL
            OR seed_max IS NULL
            OR other_min IS NULL
            OR other_max IS NULL
            OR ABS(seed_max - seed_min) > 1
            OR ABS(other_max - other_min) > 1
            OR ABS(other_min - seed_min) > 1
            OR ABS(other_max - seed_max) > 1
          ) FROM staging_pairs
        ) = 0,
        'pass',
        'fail'
      ) AS staging_account_total_vs_snaptrade,
      IF(
        (SELECT COUNT(*) FROM seed) > 0
        AND (SELECT COUNTIF(tenant_id IS NULL) FROM seed) = 0
        AND (SELECT COUNT(*) FROM wealth_pairs) > 0
        AND (
          SELECT COUNTIF(
            seed_tenant IS NULL
            OR other_tenant IS NULL
            OR seed_min IS NULL
            OR seed_max IS NULL
            OR other_min IS NULL
            OR other_max IS NULL
            OR ABS(seed_max - seed_min) > 1
            OR ABS(other_max - other_min) > 1
            OR ABS(other_min - seed_min) > 1
            OR ABS(other_max - seed_max) > 1
          ) FROM wealth_pairs
        ) = 0,
        'pass',
        'fail'
      ) AS warehouse_account_value_vs_snaptrade
    """


_PRINT_KEYS = (
    "history_rows_gt_0",
    "current_rows_gt_0",
    "balances_rows_gt_0",
    "union_history_rows_gt_0",
    "union_current_rows_gt_0",
    "union_balances_rows_gt_0",
    "history_null_tenant_id",
    "history_null_action",
    "history_null_date",
    "history_null_symbol",
    "history_null_amount",
    "current_null_tenant_id",
    "current_null_symbol",
    "current_null_quantity",
    "current_null_market_value",
    "current_null_cost_bases",
    "current_null_security_type",
    "balances_null_tenant_id",
    "balances_null_row_type",
    "balances_null_market_value",
    "balances_null_src_priority",
    "staging_account_total_vs_snaptrade",
    "warehouse_account_value_vs_snaptrade",
)


def _assert_select_only(sql: str) -> None:
    stripped = sql.strip().lstrip("(")
    if not stripped.upper().startswith("WITH") and not stripped.upper().startswith("SELECT"):
        raise SystemExit("refusing non-select structural sql")
    if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|ALTER)\b", sql, re.I):
        raise SystemExit("refusing structural sql that writes")


def _client():
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT)


def run_structural() -> int:
    sql = _structural_sql()
    _assert_select_only(sql)
    try:
        rows = list(_client().query(sql, location="US").result())
    except Exception as exc:  # noqa: BLE001 — surface a flag, not a traceback with row data
        message = str(exc).splitlines()[0][:180]
        if "$" in message or _UUID_RE.search(message):
            message = "query failed"
        print(f"structural_check=fail")
        print(f"structural_error={message}")
        return 0
    if len(rows) != 1:
        print("structural_check=fail")
        return 0
    row = rows[0]
    print("structural_check=pass")
    for key in _PRINT_KEYS:
        value = row[key]
        if value is None:
            rendered = "null"
        elif hasattr(value, "is_integer") or isinstance(value, float):
            rendered = f"{float(value):.3f}"
        else:
            rendered = str(value)
        if "$" in rendered or _UUID_RE.search(rendered):
            rendered = "redacted"
        print(f"{key}={rendered}")
    return 0


def run_unknown_actions() -> int:
    yml_path = (
        Path(__file__).resolve().parents[1]
        / "dbt/models/staging/brokers/_stg_broker_vanguard.yml"
    )
    allowed = {label.casefold() for label in allowed_actions(yml_path.read_text())}
    sql = f"""
    SELECT DISTINCT TRIM(CAST(Action AS STRING)) AS action
    FROM `{PROJECT}.{RAW_DATASET}.trade_history`
    WHERE {_SLUG.format(col="Account")}
      AND NULLIF(TRIM(CAST(Action AS STRING)), '') IS NOT NULL
    """
    _assert_select_only(sql)
    try:
        rows = list(_client().query(sql, location="US").result())
    except Exception:
        print("unknown_action_labels=unavailable")
        return 0
    unknown_safe = []
    unsafe = 0
    for row in rows:
        label = str(row["action"] or "").strip()
        if label.casefold() in allowed:
            continue
        if is_safe_action_label(label):
            unknown_safe.append(label)
        else:
            unsafe += 1
    if not unknown_safe and unsafe == 0:
        print("unknown_action_labels=none")
    else:
        print("unknown_action_labels=" + ",".join(sorted(set(unknown_safe))))
    print(f"unsafe_action_labels={unsafe}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-profile")
    parser.add_argument("--check-debug-log")
    parser.add_argument("--scrub-dbt-log")
    parser.add_argument("--unknown-actions", action="store_true")
    args = parser.parse_args(argv)
    if args.check_profile:
        assert_dev_profile(Path(args.check_profile).read_text())
        print("profile_dataset=analytics_dev")
        return 0
    if args.check_debug_log:
        assert_debug_schema(Path(args.check_debug_log).read_text())
        print("dbt_debug_schema=analytics_dev")
        return 0
    if args.scrub_dbt_log:
        log_path = Path(args.scrub_dbt_log)
        if not log_path.exists():
            print("dbt_log=missing")
            return 0
        for line in log_path.read_text(errors="replace").splitlines():
            kept = scrub_dbt_line(line)
            if kept:
                print(kept)
        return 0
    if args.unknown_actions:
        return run_unknown_actions()
    return run_structural()


if __name__ == "__main__":
    sys.exit(main())
