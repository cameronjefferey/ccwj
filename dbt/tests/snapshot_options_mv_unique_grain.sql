-- Regression: snapshot_options_market_values_daily MERGEs on
-- (tenant_grain, user_id, trade_symbol) where
-- tenant_grain = coalesce(nullif(trim(tenant_id),''), account).
--
-- If the options subset of stg_current has more than one row for that grain,
-- the dbt snapshot's MERGE dies with
--   "UPDATE/MERGE must match at most one source row for each target row"
-- which ABORTS the entire non-price `dbt build` (the `--exclude
-- stg_daily_prices+` step in bigquery_update.yml) and SILENTLY freezes
-- positions_summary + the weekly/benchmark/strategy marts at their last good
-- build, while the price cohort keeps refreshing via prices_refresh.yml. The
-- page then tells two stories: KPI cards / Strategy Breakdown (stale
-- positions_summary) vs Breakdown-by-Type / Legs / chart (fresh int_ models).
--
-- Real incident 2026-07-30..08-04: user 18 held JPM 260807C00355000 in TWO
-- physical accounts both labeled "Schwab Account" (distinct tenant_ids). The
-- old (account, user_id, trade_symbol) key collided; keying on tenant_grain
-- disambiguates. See snapshot_options_market_values_daily.sql and AGENTS.md
-- re-grain rule. This test keeps the snapshot grain unique so it can't recur.
select
    coalesce(nullif(trim(tenant_id), ''), account) as tenant_grain,
    coalesce(user_id, -1) as user_id,
    trade_symbol,
    count(*) as n
from {{ ref('stg_current') }}
where instrument_type in ('Call', 'Put')
group by 1, 2, 3
having count(*) > 1
