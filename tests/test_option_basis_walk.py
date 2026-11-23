"""Run the option-basis window SQL against the dbt unit-test fixtures.

The warehouse unit tests are the source of the expected P&L. This check
executes the same close-basis SQL (from ``option_open_events`` through
``multi_close_realized``) on SQLite, which has the window functions and
can host ``safe_divide`` / ``least`` / ``greatest`` as UDFs. It does not
need BigQuery, so PR CI catches a regression in the non-recursive carry
before the nightly rebuild.
"""

from pathlib import Path
import sqlite3

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
_MODEL = ROOT / "dbt" / "models" / "intermediate" / "int_option_contract_daily_pnl.sql"
_FIXTURES = (
    ROOT / "dbt" / "models" / "intermediate" / "int_option_contract_daily_pnl.yml",
    ROOT / "dbt" / "models" / "intermediate" / "int_option_contract_daily_pnl_basis.yml",
)


def _sqlite_supports_basis_sql():
    con = sqlite3.connect(":memory:")
    try:
        con.execute(
            "select 1 is not distinct from null, ln(1.0), exp(0.0)"
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    return True


def _register(con):
    def least(a, b):
        if a is None or b is None:
            return None
        return a if a < b else b

    def greatest(a, b):
        if a is None or b is None:
            return None
        return a if a > b else b

    def safe_divide(a, b):
        if a is None or b is None or b == 0:
            return None
        return a / b

    con.create_function("least", 2, least)
    con.create_function("greatest", 2, greatest)
    con.create_function("safe_divide", 2, safe_divide)


def _basis_select_sql():
    text = _MODEL.read_text()
    start = text.index("option_open_events as (")
    end = text.index("\nrealized_close as (")
    body = text[start:end].strip().rstrip(",").strip()
    body = body.replace("{{ ref('stg_history') }}", "src_history")
    if "{{" in body:
        raise AssertionError("extracted basis SQL still contains jinja")
    return f"""
        with contracts as (select * from src_contracts),
        {body}
        select
            symbol,
            trade_symbol,
            date,
            pnl_today,
            is_realized_close
        from multi_close_realized
    """


def _cases():
    cases = []
    for path in _FIXTURES:
        doc = yaml.safe_load(path.read_text())
        cases.extend(doc["unit_tests"])
    return cases


def _load(con, cases):
    con.execute(
        """
        create table src_history (
            tenant_id text,
            account text,
            user_id integer,
            trade_symbol text,
            trade_date text,
            option_expiry text,
            quantity real,
            amount real,
            instrument_type text,
            action text
        )
        """
    )
    con.execute(
        """
        create table src_contracts (
            tenant_id text,
            account text,
            user_id integer,
            symbol text,
            trade_symbol text,
            open_date text,
            close_date text,
            status text,
            net_cash_flow real,
            contracts_sold_to_open real,
            contracts_bought_to_open real,
            contracts_closed real,
            realized_pnl real,
            realized_close_date text
        )
        """
    )
    for case in cases:
        for given in case["given"]:
            if given["input"] == "ref('stg_history')":
                for row in given["rows"]:
                    con.execute(
                        """
                        insert into src_history values
                            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["tenant_id"],
                            row["account"],
                            row["user_id"],
                            row["trade_symbol"],
                            str(row["trade_date"]),
                            str(row["option_expiry"]),
                            row["quantity"],
                            row["amount"],
                            row["instrument_type"],
                            row["action"],
                        ),
                    )
            elif given["input"] == "ref('int_option_contracts')":
                for row in given["rows"]:
                    con.execute(
                        """
                        insert into src_contracts values
                            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["tenant_id"],
                            row["account"],
                            row["user_id"],
                            row["underlying_symbol"],
                            row["trade_symbol"],
                            str(row["open_date"]),
                            str(row["close_date"]) if row.get("close_date") else None,
                            row["status"],
                            row["net_cash_flow"],
                            row["contracts_sold_to_open"],
                            row["contracts_bought_to_open"],
                            row["contracts_closed"],
                            row["realized_pnl"],
                            str(row["realized_close_date"]),
                        ),
                    )


def test_window_basis_matches_unit_test_realized_pnl():
    if not _sqlite_supports_basis_sql():
        pytest.skip("sqlite is missing ln/exp or IS NOT DISTINCT FROM")
    cases = _cases()
    expected = []
    for case in cases:
        for row in case["expect"]["rows"]:
            if not row["is_realized_close"]:
                continue
            expected.append(
                (
                    row["symbol"],
                    row["trade_symbol"],
                    str(row["date"]),
                    float(row["pnl_today"]),
                )
            )
    # The two unit tests name these dollars explicitly. Keep them visible
    # here so a fixture edit that changes the spec fails this sentence too.
    by_symbol = {}
    for symbol, _trade, _date, pnl in expected:
        by_symbol.setdefault(symbol, []).append(pnl)
    assert by_symbol["AAA"] == [50.0, 50.0]
    assert by_symbol["BBB"] == [150.0, 100.0]
    assert by_symbol["CCC"] == [50.0, 50.0]
    assert by_symbol["LONG"] == [60.0, 40.0, 80.0]
    assert by_symbol["SHORT"] == [-60.0, -40.0, -80.0]
    assert by_symbol["MISSING"] == [-50.0, -200.0]

    con = sqlite3.connect(":memory:")
    _register(con)
    _load(con, cases)
    try:
        rows = con.execute(_basis_select_sql()).fetchall()
    except sqlite3.OperationalError as exc:
        pytest.fail(f"basis SQL failed on sqlite: {exc}")
    actual = [
        (symbol, trade, str(date), float(pnl))
        for symbol, trade, date, pnl, flag in rows
        if flag in (1, True)
    ]
    assert len(actual) == len(expected)
    actual_sorted = sorted(actual)
    expected_sorted = sorted(expected)
    for got, want in zip(actual_sorted, expected_sorted):
        assert got == want, (got, want)
