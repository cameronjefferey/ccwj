"""Pin stg_history Date extract to the same grain as merge.

Python ``_canonicalize_date_mdy`` uses ``.search()`` (MDY anywhere) and
accepts two-digit years. A trailing ``$`` on the SQL extract left Schwab
CSV timestamps (``05/14/2024 as of 08:30 PM``) as NULL trade_date
(run 33141412571). After that was unanchored, run 33142404800 still had
40 NULL-date groups: raw Date on ``manual:manual:Schwab Account`` is
``1/20/23`` / ``11/18/22`` (two-digit year).
"""
import re
from pathlib import Path

_MACRO = (
    Path(__file__).resolve().parents[1]
    / "dbt" / "macros" / "parse_seed_date.sql"
)
_STG = (
    Path(__file__).resolve().parents[1]
    / "dbt" / "models" / "staging" / "stg_history.sql"
)
_MDY = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")
_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})")
_MDY_YY = re.compile(r"^(\d{1,2}/\d{1,2}/\d{2})(?:\s|$)")


def test_stg_history_uses_shared_parse_seed_date_macro():
    assert "parse_seed_date('date')" in _STG.read_text()


def test_settlement_dated_cte_is_comma_joined():
    """A missing comma after crypto_norm makes BigQuery reject ``dated``.

    Run 37089600141: Syntax error: Unexpected identifier "dated".
    """
    sql = _STG.read_text()
    assert re.search(
        r"crypto_norm as \([\s\S]*?\)\s*,\s*(?:--[^\n]*\n\s*)*dated as \(",
        sql,
    )


def test_expiry_date_cap_collapses_the_check2_fill_grain():
    """Capping a posting date onto expiry must not leave two fills.

    Run 37090194526: the same BE buy-to-close survived upload dedup
    (raw dates differ) and then shared a trade_date, so
    stg_history_no_duplicate_fills_per_tenant check 2 failed. Blank
    prices stay out of this collapse — distinct expiries have no Symbol.
    """
    sql = _STG.read_text()
    assert "fill_ranked as (" in sql
    assert "round(d.price, 4)" in sql
    # Window PARTITION BY cannot be FLOAT64 (run 37091190523).
    assert "cast(d.quantity as string)" in sql
    assert "cast(round(d.price, 4) as string)" in sql
    assert "fee_adjusted asc" in sql
    assert "drop_estimated_fee" in sql
    assert "est\\. fee" in sql or r"est\. fee" in sql
    assert "option_expiry < c.trade_date" in sql
    assert "where d.trade_symbol is not null" in sql
    assert "and d.price is not null" in sql
    assert "from history_rows" in sql


def test_parse_seed_date_covers_four_and_two_digit_mdy():
    sql = _MACRO.read_text()
    assert r"r'(\d{1,2}/\d{1,2}/\d{4})$'" not in sql
    assert r"r'(\d{1,2}/\d{1,2}/\d{4})'" in sql
    assert r"\bas of\s+" in sql
    assert r"%m/%d/%y" in sql
    assert r"^(\d{1,2}/\d{1,2}/\d{2})(?:\s|$)" in sql


def test_mdy_extract_reads_schwab_as_of_clock_and_two_digit_year():
    assert _MDY.search("05/14/2024 as of 08:30 PM").group(1) == "05/14/2024"
    assert _MDY.search("5/14/2024 12:00:00 AM").group(1) == "5/14/2024"
    assert _ISO.search("2024-05-14T00:00:00").group(1) == "2024-05-14"
    assert _MDY_YY.search("1/20/23").group(1) == "1/20/23"
    assert _MDY_YY.search("11/18/22 as of 08:30 PM").group(1) == "11/18/22"
    assert _MDY_YY.search("04/21/2025") is None
    # The warehouse macro takes the date AFTER "as of" when that token
    # is a calendar date. A clock suffix still uses the leading date.
    as_of = re.search(
        r"(?i)\bas of\s+(\d{1,2}/\d{1,2}/\d{4})",
        "10/02/2026 as of 10/01/2026",
    )
    assert as_of.group(1) == "10/01/2026"
    assert re.search(
        r"(?i)\bas of\s+(\d{1,2}/\d{1,2}/\d{4})",
        "05/14/2024 as of 08:30 PM",
    ) is None
