#!/usr/bin/env python
"""Collapse already-landed history fills that the merge key missed.

Warehouse run 33132317666 (2026-08-28) failed
``stg_history_no_duplicate_fills_per_tenant`` with 153 groups after a
Schwab CSV upload landed on a SnapTrade tenant. stg_history grew
6.9k → 10.0k. Root cause: SnapTrade writes zero-padded ``05/14/2024``
while Schwab's CSV export writes ``5/14/2024``. ``stg_history`` parses
both to the same DATE; ``_dedup_history_rows`` compared the raw strings
and kept both.

This script re-runs the same per-tenant fill grain as
``app.upload._dedup_history_rows`` (after the date-padding fix) against
the raw ``trade_history`` seed and WRITE_TRUNCATEs if anything changed.
The warehouse workflow calls it before the first dbt build so the test
sees a clean seed. No-op when already clean.

Flask-free: the warehouse job has BigQuery credentials but not
SECRET_KEY / Postgres. Helpers are inlined from ``app/upload.py`` and
pinned by ``tests/test_repair_history_fill_dedup.py``.

Usage:
    python scripts/repair_history_fill_dedup.py
    python scripts/repair_history_fill_dedup.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from google.cloud import bigquery

# Inlined from app.upload so this script can run in the warehouse job
# without booting Flask (SECRET_KEY / Postgres are not set there).
from datetime import date, datetime
import re

_DATE_MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_DATE_ISO_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_CROSS_SOURCE_PRICE_DP = 4

PROJECT = os.environ.get("BQ_RAW_PROJECT", "ccwj-dbt").strip()
DATASET = (os.environ.get("BQ_RAW_DATASET") or "analytics_raw").strip()
TABLE = f"{PROJECT}.{DATASET}.trade_history"
_ROW_SEQ = "_row_seq"
# Refuse to write if we'd drop more than this fraction — a grain bug
# that collapsed unrelated rows is worse than a red dbt test.
_MAX_DROP_FRACTION = 0.25

HISTORY_SEED_COLUMNS = [
    "Account", "user_id", "tenant_id",
    "Date", "Action", "Symbol", "Description",
    "Quantity", "Price", "fees_and_comm", "Amount",
]


def _canonicalize_seed_cell(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    s_num = s.replace(",", "").strip()
    if s_num.startswith("$"):
        s_num = s_num[1:].strip()
    if len(s_num) >= 2 and s_num[0] == "(" and s_num[-1] == ")":
        s_num = "-" + s_num[1:-1].replace("$", "").strip()
    try:
        f = float(s_num)
    except (TypeError, ValueError):
        return s
    if pd.isna(f):
        return ""
    out = f"{f:.6f}".rstrip("0").rstrip(".")
    if out in ("", "-"):
        return "0"
    if out == "-0":
        return "0"
    return out


def _canonicalize_date_mdy(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    m = _DATE_MDY_RE.search(s)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(year, month, day).strftime("%m/%d/%Y")
        except ValueError:
            return s
    m = _DATE_ISO_RE.search(s)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(year, month, day).strftime("%m/%d/%Y")
        except ValueError:
            return s
    return s


def _canonicalize_key_cell(col, value):
    if str(col).lower() == "date":
        return _canonicalize_date_mdy(value)
    return _canonicalize_seed_cell(value)


def _canonicalize_cross_source_price(value):
    base = _canonicalize_seed_cell(value)
    if base == "":
        return ""
    try:
        f = round(float(base), _CROSS_SOURCE_PRICE_DP)
    except (TypeError, ValueError):
        return base
    out = f"{f:.{_CROSS_SOURCE_PRICE_DP}f}".rstrip("0").rstrip(".")
    if out in ("", "-", "-0"):
        return "0"
    return out


def _dedup_history_rows(df, seed_columns):
    """Same grain as ``app.upload._dedup_history_rows`` (date-padded)."""
    if df is None or df.empty:
        return df
    key_cols = [
        c for c in seed_columns
        if str(c).lower() not in ("account", "user_id")
    ]
    if not key_cols:
        return df
    canon = df[key_cols].copy()
    for c in key_cols:
        if c in canon.columns:
            canon[c] = canon[c].map(lambda v, _c=c: _canonicalize_key_cell(_c, v))
    keep_mask = ~canon.duplicated(subset=key_cols, keep="last")
    df = df.loc[keep_mask].reset_index(drop=True)

    cross_key_lower = {"date", "action", "symbol", "quantity", "price"}
    cross_key_cols = [
        c for c in seed_columns
        if str(c).lower() in cross_key_lower
    ]
    if len(cross_key_cols) < len(cross_key_lower) or "Description" not in df.columns:
        return df

    df = df.reset_index(drop=True)
    sym_col = next((c for c in df.columns if str(c).lower() == "symbol"), None)
    price_col = next((c for c in cross_key_cols if str(c).lower() == "price"), None)
    sym_blank = df[sym_col].map(lambda v: _canonicalize_seed_cell(v) == "")
    price_blank = df[price_col].map(lambda v: _canonicalize_seed_cell(v) == "")
    eligible = ~(sym_blank | price_blank)

    canon2 = df[cross_key_cols].copy()
    for c in cross_key_cols:
        if c == price_col:
            canon2[c] = canon2[c].map(_canonicalize_cross_source_price)
        else:
            canon2[c] = canon2[c].map(lambda v, _c=c: _canonicalize_key_cell(_c, v))
    desc_lens = df["Description"].fillna("").astype(str).str.len()
    order = (-desc_lens.to_numpy()).argsort(kind="stable")

    seen: set = set()
    drop_positions: set = set()
    for pos in order:
        if not bool(eligible.iloc[pos]):
            continue
        key = tuple(canon2.iloc[pos][c] for c in cross_key_cols)
        if key in seen:
            drop_positions.add(pos)
        else:
            seen.add(key)
    if not drop_positions:
        return df
    keep_mask2 = [i not in drop_positions for i in range(len(df))]
    return df.loc[keep_mask2].reset_index(drop=True)


def dedup_history_by_tenant(df: pd.DataFrame) -> pd.DataFrame:
    """Run fill dedup independently per tenant_id (never across tenants)."""
    if df is None or df.empty:
        return df
    for col in HISTORY_SEED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    parts = []
    work = df.copy()
    work["__tid"] = work["tenant_id"].fillna("").astype(str)
    for _, grp in work.groupby("__tid", sort=False, dropna=False):
        grp = grp.drop(columns=["__tid"])
        cleaned = _dedup_history_rows(grp.copy(), HISTORY_SEED_COLUMNS)
        # Persist the padded Date so the next SnapTrade/CSV merge keys match.
        if "Date" in cleaned.columns:
            cleaned["Date"] = cleaned["Date"].map(
                lambda v: _canonicalize_date_mdy(v) or v
            )
        parts.append(cleaned)
    if not parts:
        return df
    return pd.concat(parts, ignore_index=True)[HISTORY_SEED_COLUMNS]


def _client():
    return bigquery.Client(project=PROJECT)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print counts and do not write.",
    )
    args = parser.parse_args(argv)

    client = _client()
    try:
        raw = client.query(
            f"SELECT * FROM `{TABLE}` ORDER BY {_ROW_SEQ}"
        ).to_dataframe()
    except Exception as exc:
        print(f"ERROR: failed to read {TABLE}: {exc}", file=sys.stderr)
        return 1

    if raw.empty:
        print(f"{TABLE} is empty — nothing to repair.")
        return 0

    if _ROW_SEQ in raw.columns:
        raw = raw.drop(columns=[_ROW_SEQ])
    raw = raw.astype(object).where(pd.notna(raw), "")

    before = len(raw)
    cleaned = dedup_history_by_tenant(raw)
    after = len(cleaned)
    dropped = before - after
    print(f"{TABLE}: {before} rows → {after} rows (dropped {dropped})")

    if dropped < 0:
        print("ERROR: dedup grew the table — aborting.", file=sys.stderr)
        return 1
    if before and dropped / before > _MAX_DROP_FRACTION:
        print(
            f"ERROR: would drop {dropped}/{before} rows "
            f"(> {_MAX_DROP_FRACTION:.0%} cap) — aborting.",
            file=sys.stderr,
        )
        return 1
    if dropped == 0:
        print("Already clean — no write.")
        return 0
    if args.dry_run:
        print("Dry run — not writing.")
        return 0

    cleaned = cleaned.astype(object).where(cleaned != "", None)
    cleaned[_ROW_SEQ] = range(len(cleaned))
    schema = [
        bigquery.SchemaField(col, "STRING")
        for col in cleaned.columns
        if col != _ROW_SEQ
    ] + [bigquery.SchemaField(_ROW_SEQ, "INT64")]
    job = client.load_table_from_dataframe(
        cleaned,
        TABLE,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    print(f"Wrote {after} rows to {TABLE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
