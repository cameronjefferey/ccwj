"""CI guard: dbt SQL must not contain a recursive CTE.

BigQuery allows ``WITH RECURSIVE`` only at the top level of a statement.
dbt unit tests wrap each model in an outer query, so a recursive CTE fails
that unit test, skips every later test on the model, and fails the warehouse
rebuild. Macros are inlined into the model, so they have the same restriction.

The phrase is matched even inside comments. Say "recursive CTE" in a comment
if you need to warn the next reader; put the longer explanation in AGENTS.md.
"""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
_SQL_ROOTS = (ROOT / "dbt" / "models", ROOT / "dbt" / "macros")
# Keyword, not a column named ``recursive_flag`` or a CTE named ``retention``.
_RECURSIVE = re.compile(r"\bwith\s+recursive\b", re.IGNORECASE)


def test_recursive_cte_pattern_matches_keyword_variants():
    assert _RECURSIVE.search("WITH RECURSIVE walk AS (")
    assert _RECURSIVE.search("with\nrecursive walk as (")
    assert _RECURSIVE.search("with\trecursive walk as (")
    assert not _RECURSIVE.search("with retention as (")
    assert not _RECURSIVE.search("recursive_cte as (")
    assert not _RECURSIVE.search("starts_episode")


def test_dbt_sql_has_no_recursive_cte():
    hits = []
    for root in _SQL_ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.sql")):
            if _RECURSIVE.search(path.read_text()):
                hits.append(str(path.relative_to(ROOT)))
    assert not hits, (
        "BigQuery rejects a recursive CTE once dbt wraps the model for a "
        "unit test, and the warehouse rebuild fails. Rewrite the walk with "
        "window functions.\n" + "\n".join(hits)
    )
