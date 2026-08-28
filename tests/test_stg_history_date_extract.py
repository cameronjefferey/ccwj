"""Pin stg_history Date extract to the same grain as merge.

Python ``_canonicalize_date_mdy`` uses ``.search()`` (MDY anywhere).
A trailing ``$`` on the SQL extract left Schwab CSV timestamps
(``05/14/2024 as of 08:30 PM``) as NULL trade_date, and CHECK 1 of
``stg_history_no_duplicate_fills_per_tenant`` fused every same-
(tenant, action, symbol, amount) row (run 33141412571: 41 groups).
"""
import re
from pathlib import Path

_STG = (
    Path(__file__).resolve().parents[1]
    / "dbt" / "models" / "staging" / "stg_history.sql"
)
_MDY = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")
_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})")


def test_stg_history_mdy_extract_is_not_end_anchored():
    sql = _STG.read_text()
    assert r"r'(\d{1,2}/\d{1,2}/\d{4})$'" not in sql
    assert r"r'(\d{1,2}/\d{1,2}/\d{4})'" in sql
    assert r"r'^(\d{4}-\d{2}-\d{2})'" not in sql
    assert r"r'(\d{4}-\d{2}-\d{2})'" in sql


def test_mdy_extract_reads_schwab_as_of_and_clock_suffix():
    assert _MDY.search("05/14/2024 as of 08:30 PM").group(1) == "05/14/2024"
    assert _MDY.search("5/14/2024 12:00:00 AM").group(1) == "5/14/2024"
    assert _ISO.search("2024-05-14T00:00:00").group(1) == "2024-05-14"
