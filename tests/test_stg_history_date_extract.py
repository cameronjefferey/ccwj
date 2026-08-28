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


def test_parse_seed_date_covers_four_and_two_digit_mdy():
    sql = _MACRO.read_text()
    assert r"r'(\d{1,2}/\d{1,2}/\d{4})$'" not in sql
    assert r"r'(\d{1,2}/\d{1,2}/\d{4})'" in sql
    assert r"%m/%d/%y" in sql
    assert r"^(\d{1,2}/\d{1,2}/\d{2})(?:\s|$)" in sql


def test_mdy_extract_reads_schwab_as_of_clock_and_two_digit_year():
    assert _MDY.search("05/14/2024 as of 08:30 PM").group(1) == "05/14/2024"
    assert _MDY.search("5/14/2024 12:00:00 AM").group(1) == "5/14/2024"
    assert _ISO.search("2024-05-14T00:00:00").group(1) == "2024-05-14"
    assert _MDY_YY.search("1/20/23").group(1) == "1/20/23"
    assert _MDY_YY.search("11/18/22 as of 08:30 PM").group(1) == "11/18/22"
    assert _MDY_YY.search("04/21/2025") is None
