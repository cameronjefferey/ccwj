"""Read-only reproduction of the nightly batched seed fold.

Reads the three raw seed tables, refolds each tenant onto the stored CSV
the same way ``merge_and_push_seeds_batch`` does, and asks pyarrow to
convert the result. It never calls ``load_table_from_dataframe`` and
never dispatches a warehouse rebuild.

Logs are counts, seed column names, and a sanitized exception. No row
values, account labels, tenant ids, tickers, or dollar amounts.
"""
from __future__ import annotations

import re
import sys
import traceback
from io import StringIO

_SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$")


def _public_exception(exc, allowed_columns=()) -> str:
    """Exception class plus a scrubbed message. Column names may remain."""
    allowed = {str(col) for col in allowed_columns if _SAFE_NAME.match(str(col))}
    text = str(exc or "")
    text = re.sub(r"'[^']*'", "'…'", text)
    text = re.sub(r'"([^"]*)"', lambda m: f'"{m.group(1)}"' if m.group(1) in allowed else '"…"', text)
    text = re.sub(r"snaptrade:[A-Za-z0-9_-]+", "snaptrade:…", text, flags=re.I)
    text = re.sub(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "…",
        text,
        flags=re.I,
    )
    text = re.sub(r"\$[\d,.]+", "$…", text)
    text = re.sub(r"\b[\w.+-]+@[\w.-]+\b", "…@…", text)
    text = re.sub(r"(?m)^.*(?:,){3,}.*$", "[row omitted]", text)
    def _token(match):
        token = match.group(0)
        if token in allowed:
            return token
        if len(token) > 24:
            return "…"
        return token
    text = re.sub(r"[A-Za-z0-9_./:-]{8,}", _token, text)
    return f"{type(exc).__name__}: {text[:500]}"


def _install_write_tripwire():
    """Any table load or seed-store write aborts the process."""
    from google.cloud import bigquery

    def _refuse(*_args, **_kwargs):
        raise SystemExit("diagnostic refused a table write")

    bigquery.Client.load_table_from_dataframe = _refuse
    import app.seed_store as seed_store
    import app.upload as upload

    seed_store.write_seed_csvs = _refuse
    upload.write_seed_csvs = _refuse
    upload._commit_git_paths = _refuse
    upload._dispatch_warehouse_rebuild = _refuse
    return _refuse


def _report(exc, label, allowed_columns):
    print(f"FAIL {label}: {_public_exception(exc, allowed_columns)}", file=sys.stderr)
    # Traceback frames name functions, not row values. Still scrub the text.
    buf = StringIO()
    traceback.print_exception(type(exc), exc, exc.__traceback__, file=buf)
    print(_public_exception(RuntimeError(buf.getvalue()), allowed_columns), file=sys.stderr)


def _refold_table(path, csv_text, seed_columns, account_col):
    import pandas as pd

    from app.upload import _merge_seed_with_existing

    frame = pd.read_csv(StringIO(csv_text), dtype=str, keep_default_na=False)
    columns = [str(col) for col in frame.columns if _SAFE_NAME.match(str(col))]
    tenant_count = int(frame["tenant_id"].nunique()) if "tenant_id" in frame.columns else 0
    print(
        f"{path}: rows={len(frame)} tenants={tenant_count} columns={columns}"
    )
    empty_counts = {
        col: int((frame[col] == "").sum())
        for col in frame.columns
        if _SAFE_NAME.match(str(col))
    }
    print(f"{path}: empty_cells={empty_counts}")
    if "tenant_id" not in frame.columns or account_col not in frame.columns:
        print(f"{path}: missing tenant or account column; skip refold")
        return False

    content = csv_text
    failed = False
    for _tid, group in frame.groupby("tenant_id", sort=False):
        account_name = str(group[account_col].iloc[0])
        prepared = group.copy()
        try:
            content = _merge_seed_with_existing(
                path,
                account_name,
                prepared,
                list(seed_columns),
                tenant_id=str(group["tenant_id"].iloc[0]),
                existing_content=content,
            )
        except Exception as exc:
            _report(exc, f"refold {path}", columns)
            failed = True
            break
    if failed:
        return True

    try:
        from google.cloud import bigquery
        from google.cloud.bigquery._pandas_helpers import dataframe_to_arrow

        from app.seed_store import _string_seed_frame

        out = pd.read_csv(StringIO(content), dtype=str, keep_default_na=False)
        out = out.astype(object).where(out != "", None)
        out = _string_seed_frame(out)
        schema = [
            bigquery.SchemaField(str(col), "STRING")
            for col in out.columns
            if str(col) != "_row_seq"
        ]
        dataframe_to_arrow(out, schema)
        print(f"{path}: refold and arrow conversion ok rows={len(out)}")
    except Exception as exc:
        _report(exc, f"arrow {path}", columns)
        return True
    return False


def main() -> int:
    # Shape probe is select-only. A dispatch of this workflow from a
    # branch that has the probe file runs it even when the workflow
    # YAML is the one on the default branch.
    try:
        from empower_shape_probe import run as _empower_shape
        _empower_shape()
    except Exception:
        print("empower_shape_probe=unavailable")
    _install_write_tripwire()
    from app.seed_store import read_seed_csv
    from app.upload import (
        BALANCE_SEED_COLUMNS,
        BALANCE_SEED_PATH,
        CURRENT_PATH,
        CURRENT_SEED_COLUMNS,
        HISTORY_PATH,
        HISTORY_SEED_COLUMNS,
    )

    tables = (
        (HISTORY_PATH, HISTORY_SEED_COLUMNS, "Account"),
        (CURRENT_PATH, CURRENT_SEED_COLUMNS, "Account"),
        (BALANCE_SEED_PATH, BALANCE_SEED_COLUMNS, "account"),
    )
    failed = False
    for path, columns, account_col in tables:
        try:
            csv_text = read_seed_csv(path)
        except Exception as exc:
            _report(exc, f"read {path}", columns)
            failed = True
            continue
        if not csv_text:
            print(f"{path}: empty or missing")
            continue
        if _refold_table(path, csv_text, columns, account_col):
            failed = True
    print("NO_TABLE_WRITES")
    return 2 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
