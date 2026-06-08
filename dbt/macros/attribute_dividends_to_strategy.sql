{#
    attribute_dividends_to_strategy

    Single source of truth for "which strategy gets credit for the cash
    dividends on this symbol?" Used by positions_summary.sql and mirrored
    (with a date-window filter on the source) by the runtime SQL in
    app/routes.py:DATE_FILTERED_QUERY. If you change this macro, the
    runtime mirror in routes.py MUST be updated to match — search for
    `ATTRIBUTION_INVARIANT` in that file.

    Why this lives in a macro:
      * The same dividend-attribution rules apply at multiple grains:
        full lifetime (positions_summary mart) and date-windowed views
        (the URL-parameterized runtime query in /positions). Putting the
        ranking + attribution + reclassification logic in one place
        prevents the two from drifting (one of the most common bug
        patterns in this codebase has been "fixed in mart, forgot to
        fix in runtime SQL" or vice versa).

    Inputs:
      strategy_summary_relation: an aggregated CTE/relation that has
        one row per (account, user_id, symbol, strategy) with at minimum
        these columns: account, user_id, symbol, strategy, status,
        total_pnl, realized_pnl, unrealized_pnl, total_premium_received,
        total_premium_paid, num_trade_groups, num_individual_trades,
        num_winners, num_losers, win_rate, avg_pnl_per_trade,
        avg_days_in_trade, first_trade_date, last_trade_date.

      dividends_relation: per (account, user_id, symbol) totals with
        columns total_dividend_income and dividend_count. For the mart
        path this is int_dividends; for the runtime path it's a CTE
        that windows int_dividend_events by trade_date.

    Output: SQL fragment that emits two CTEs (with_dividend_rank and
    with_attributed_dividends), ready to be selected from in a final
    SELECT. The downstream final SELECT must apply the Buy and Hold →
    Dividend reclassification described inline below.

    Output shape (with_attributed_dividends):
      account, user_id, symbol, strategy, status,
      total_pnl, realized_pnl, unrealized_pnl,
      total_premium_received, total_premium_paid,
      num_trade_groups, num_individual_trades,
      num_winners, num_losers, win_rate,
      avg_pnl_per_trade, avg_days_in_trade,
      first_trade_date, last_trade_date,
      dividend_rank,
      attributed_dividend_income, attributed_dividend_count

    Tenancy invariant: the row_number() PARTITION and the dividends join
    must include tenant_id (the canonical per-physical-account key) so
    dividend ranking can never leak across physical accounts that share a
    display label, AND so two such accounts holding the same symbol don't
    fuse their dividend attribution. Both input relations must therefore
    carry a tenant_id column (the routine DATE_FILTERED_QUERY mirror in
    app/routes.py must select it on both CTEs too — ATTRIBUTION_INVARIANT).
    See .cursor/rules/bigquery-tenant-isolation.mdc.
#}

{% macro attribute_dividends_to_strategy(strategy_summary_relation, dividends_relation) %}

with_dividend_rank as (
    select
        ss.*,
        -- Rank strategies for dividend attribution. Wheel > Covered Call
        -- > Buy and Hold > everything else. The trader is most likely to
        -- think of dividends as belonging to the most "active income" of
        -- their equity-backed strategies — a Wheel position dominates a
        -- Covered Call which dominates pure Buy and Hold. Other option-
        -- only strategies (Long Call, Naked Call, etc.) hold no shares
        -- so they can't earn dividends; they fall to rank 99.
        row_number() over (
            partition by ss.tenant_id, ss.account, ss.user_id, ss.symbol
            order by
                case ss.strategy
                    when 'Wheel'        then 1
                    when 'Covered Call' then 2
                    when 'Buy and Hold' then 3
                    else 99
                end
        ) as dividend_rank
    from {{ strategy_summary_relation }} ss
),

with_attributed_dividends as (
    select
        wdr.*,
        -- Only the rank-1 strategy gets credited with the dividends.
        -- Every other strategy on this (account, user_id, symbol) shows
        -- 0 to keep totals across strategies summing correctly.
        case when wdr.dividend_rank = 1
            then coalesce(d.total_dividend_income, 0)
            else 0
        end as attributed_dividend_income,
        case when wdr.dividend_rank = 1
            then coalesce(d.dividend_count, 0)
            else 0
        end as attributed_dividend_count
    from with_dividend_rank wdr
    left join {{ dividends_relation }} d
        on wdr.account = d.account
        and (wdr.user_id is not distinct from d.user_id)
        and (wdr.tenant_id is not distinct from d.tenant_id)
        and wdr.symbol = d.symbol
)

{% endmacro %}
