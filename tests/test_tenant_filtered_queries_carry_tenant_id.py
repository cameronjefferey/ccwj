"""Every query whose DataFrame goes through filter_df_by_tenant_ids MUST
select tenant_id.

WHY (Aug 2026 regression): filter_df_by_tenant_ids fails CLOSED when the
frame has no ``tenant_id`` column (bigquery-tenant-isolation rule). That is
the right security posture — but it means a query that scopes tenants ONLY
in SQL and doesn't project the column gets its result silently emptied for
every non-admin user. That blanked the Position Detail symbol tab strip,
closed legs/equity tables, the earnings hero pill, the /symbols page, the
/accounts KPIs, sector/strategy-fit rollups and more, with nothing but an
ERROR log line ("column 'tenant_id' missing — failing closed").

This test statically pins the projection: for each (module, constant) whose
frame is DataFrame-filtered at request time, the OUTERMOST select list must
contain ``tenant_id``. If you add a new tenant-filtered query, add it here.
If a frame genuinely has no tenant column (public market data like
stg_earnings_calendar), do NOT run it through the filter — see the
earnings_df comment in app/position_detail.py.
"""

import re

import pytest

# (module path, constant name) pairs whose DataFrames are passed to
# filter_df_by_tenant_ids in a request handler.
FILTERED_QUERIES = [
    ("app.position_detail", "POSITION_SUMMARY_QUERY"),
    ("app.position_detail", "POSITION_TRADES_QUERY"),
    ("app.position_detail", "POSITION_CURRENT_QUERY"),
    ("app.position_detail", "POSITION_CLOSED_LEGS_QUERY"),
    ("app.position_detail", "POSITION_CLOSED_EQUITY_QUERY"),
    ("app.position_detail", "POSITION_MATRIX_QUERY"),
    ("app.position_detail", "POSITION_LEGS_QUERY"),
    ("app.position_detail", "POSITION_DIVIDENDS_QUERY"),
    ("app.position_detail", "SYMBOL_TABS_QUERY"),
    ("app.pnl_charts", "CHART_DATA_QUERY"),
    ("app.pnl_charts", "CHART_DATA_ALL_QUERY"),
    ("app.symbols_page", "TRADES_QUERY"),
    ("app.symbols_page", "CURRENT_POSITIONS_QUERY"),
    ("app.accounts_page", "ACCOUNT_BALANCES_QUERY"),
    ("app.accounts_page", "STRATEGY_CLASSIFICATION_QUERY"),
    ("app.accounts_page", "ACCOUNT_POSITIONS_SUMMARY_QUERY"),
    ("app.accounts_page", "ACCOUNT_LEGS_QUERY"),
    ("app.accounts_page", "NET_DEPOSITS_QUERY"),
    ("app.positions_page", "DEFAULT_QUERY"),
    ("app.positions_page", "POSITIONS_TAG_STRAT_QUERY"),
    ("app.sectors_page", "SECTORS_QUERY"),
    ("app.strategies", "STRATEGY_PERFORMANCE_QUERY"),
    ("app.strategies", "STRATEGY_TREND_QUERY"),
    ("app.strategies", "STRATEGY_POSITIONS_QUERY"),
    ("app.strategies", "STRATEGY_TYPE_BREAKDOWN_QUERY"),
    ("app.strategies", "DTE_MONEYNESS_QUERY"),
    ("app.strategy_fit", "STRATEGY_FIT_QUERY"),
    ("app.strategy_fit", "STRATEGY_FIT_OPTIONS_QUERY"),
    ("app.strategy_fit_insights", "STRATEGY_FIT_QUERY"),
    ("app.weekly_review", "OPEN_POSITIONS_QUERY"),
    ("app.weekly_review", "ACCOUNT_VALUE_QUERY"),
    ("app.weekly_review", "TODAY_SNAPSHOT_ENRICHED_QUERY"),
    ("app.weekly_review", "DAY_ACCOUNTS_QUERY"),
    ("app.weekly_review", "DAY_TRADES_QUERY"),
    ("app.insights", "BEHAVIOR_OBSERVATIONS_QUERY"),
    ("app.wealth", "WEALTH_DAILY_QUERY"),
    ("app.trader_story", "STORY_TRADES_QUERY"),
    ("app.trader_story", "STORY_DIVIDENDS_QUERY"),
    ("app.trader_story", "STORY_SUMMARY_QUERY"),
    ("app.execution_quality", "EXECUTION_REVIEW_QUERY"),
    ("app.execution_quality", "POSITION_EXECUTION_QUERY"),
    ("app.execution_quality", "OPEN_OPTION_RECORD_QUERY"),
]


def _outer_select_list(sql: str) -> str:
    """Text of the OUTERMOST select list (the last SELECT ... FROM pair —
    for CTE queries the final SELECT is the projection the app sees)."""
    matches = re.findall(r"\bSELECT\b(.*?)\bFROM\b", sql, re.S | re.I)
    assert matches, "query has no SELECT ... FROM"
    return matches[-1]


@pytest.mark.parametrize("module_path,const", FILTERED_QUERIES)
def test_tenant_filtered_query_projects_tenant_id(module_path, const):
    import importlib

    module = importlib.import_module(module_path)
    sql = getattr(module, const)
    outer = _outer_select_list(sql)
    assert "tenant_id" in outer or "*" == outer.strip() or "SELECT *" in sql, (
        f"{module_path}.{const} is DataFrame-filtered by tenant_id but its "
        "outer SELECT does not project the column — filter_df_by_tenant_ids "
        "will FAIL CLOSED and blank the page section for every non-admin "
        "user. Add tenant_id to the SELECT (and GROUP BY if aggregated)."
    )
