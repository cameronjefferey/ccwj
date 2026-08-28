"""Pin the warehouse-job history-fill repair to the same grain as merge."""
import importlib.util
from pathlib import Path

import pandas as pd

_SPEC = importlib.util.spec_from_file_location(
    "repair_history_fill_dedup",
    Path(__file__).resolve().parents[1] / "scripts" / "repair_history_fill_dedup.py",
)
_repair = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_repair)
HISTORY_SEED_COLUMNS = _repair.HISTORY_SEED_COLUMNS
_canonicalize_date_mdy = _repair._canonicalize_date_mdy
dedup_history_by_tenant = _repair.dedup_history_by_tenant


def _row(account, date, action, symbol, qty, price, amount, *, tenant_id, desc=""):
    return {
        "Account": account,
        "user_id": "9",
        "tenant_id": tenant_id,
        "Date": date,
        "Action": action,
        "Symbol": symbol,
        "Description": desc,
        "Quantity": qty,
        "Price": price,
        "fees_and_comm": "",
        "Amount": amount,
    }


def test_repair_collapses_unpadded_csv_date_within_one_tenant():
    df = pd.DataFrame([
        _row("Emmory", "05/14/2024", "Buy", "IYW", "20", "131.96", "-2639.2",
             tenant_id="snaptrade:a", desc="ISHARES US TECHNOLOGY ETF"),
        _row("Emmory", "5/14/2024", "Buy", "IYW", "20", "131.96", "-2639.2",
             tenant_id="snaptrade:a", desc="IYW"),
    ], columns=HISTORY_SEED_COLUMNS)
    out = dedup_history_by_tenant(df)
    assert len(out) == 1
    assert _canonicalize_date_mdy(out.iloc[0]["Date"]) == "05/14/2024"
    assert out.iloc[0]["Description"] == "ISHARES US TECHNOLOGY ETF"


def test_repair_collapses_qualified_vs_cash_dividend_within_one_tenant():
    df = pd.DataFrame([
        _row("Emmory", "05/14/2024", "Cash Dividend", "JEPI", "", "", "42.5",
             tenant_id="snaptrade:a", desc="JPMorgan Equity Premium Income ETF"),
        _row("Emmory", "5/14/2024", "Qualified Dividend", "JEPI", "", "", "42.50",
             tenant_id="snaptrade:a", desc="JEPI"),
    ], columns=HISTORY_SEED_COLUMNS)
    out = dedup_history_by_tenant(df)
    assert len(out) == 1
    assert out.iloc[0]["Description"] == "JPMorgan Equity Premium Income ETF"


def test_repair_collapses_csv_dash_qty_vs_blank_on_dividend():
    df = pd.DataFrame([
        _row("Emmory", "05/14/2024", "Cash Dividend", "JEPI", "", "", "42.5",
             tenant_id="snaptrade:a", desc="JPMorgan Equity Premium Income ETF"),
        _row("Emmory", "5/14/2024", "Qualified Dividend", "JEPI", "--", "--", "42.50",
             tenant_id="snaptrade:a", desc="JEPI"),
    ], columns=HISTORY_SEED_COLUMNS)
    out = dedup_history_by_tenant(df)
    assert len(out) == 1
    assert out.iloc[0]["Description"] == "JPMorgan Equity Premium Income ETF"


def test_repair_never_collapses_across_tenants():
    df = pd.DataFrame([
        _row("Emmory", "05/14/2024", "Buy", "IYW", "20", "131.96", "-2639.2",
             tenant_id="snaptrade:parent", desc="parent"),
        _row("Emmory", "5/14/2024", "Buy", "IYW", "20", "131.96", "-2639.2",
             tenant_id="snaptrade:child", desc="child"),
    ], columns=HISTORY_SEED_COLUMNS)
    out = dedup_history_by_tenant(df)
    assert len(out) == 2
    assert set(out["tenant_id"]) == {"snaptrade:parent", "snaptrade:child"}
