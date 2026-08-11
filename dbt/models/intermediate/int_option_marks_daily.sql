{{
    config(
        materialized='table'
    )
}}
/*
    Daily option marks, unfolded from the accumulating SCD2 snapshot.
    Materialized as a table so the SCD2 unfold is paid once per build,
    not on every downstream read (int_option_contract_daily_pnl,
    int_option_pnl_series, and the runtime consumers above them).

    ``snapshot_options_market_values_daily`` captures every change to the
    live option snapshot (check strategy on market_value / quantity /
    cost_basis / current_price), but until this model existed NOTHING in
    the DAG consumed that history: ``int_option_contract_daily_pnl`` and
    ``int_option_pnl_series`` read only the live ``stg_current`` (today's
    mark), so every historical day contributed $0 MTM and the charts fell
    back to realize-on-close steps. Daily option values over time are the
    product's core differentiator — this model is what turns the
    accumulated captures into a usable per-day series.

    OUTPUT GRAIN: one row per (tenant_id, account, user_id, trade_symbol,
    date) for every PAST day (date < current_date()) on which a snapshot
    version was alive. Today is deliberately excluded — the live
    ``stg_current`` read is fresher and owns today in every consumer, so
    the union is disjoint by construction.

    SCD2 → daily unfold: each version covers [date(dbt_valid_from),
    date(dbt_valid_to or now)]; when several versions touch the same day
    (intraday syncs), the LATEST valid_from wins — the end-of-day truth.
    Hard deletes (contract left the account) close the version, so days
    after the drop produce no rows. Gaps (weekends, missed syncs) are
    left absent here; the dense-spine carry-forward in
    int_option_contract_daily_pnl fills them downstream.

    KEY HYGIENE: the snapshot stores ``coalesce(user_id, -1)`` (MERGE
    sentinel) and raw ``tenant_id`` — map the sentinel back to NULL and
    blank tenant_id to NULL so joins against int_option_contracts'
    natural keys behave.

    HISTORY NOTE: the SCD2 table began accumulating 2026-08-04 (the
    prior generation was lost to the dataset-expiration incident — see
    AGENTS.md). Coverage grows one trading day at a time from there;
    consumers gate marks-based claims on observed density, so surfaces
    strengthen automatically as data accrues.
*/

with versions as (
    select
        nullif(trim(tenant_id), '')       as tenant_id,
        account,
        nullif(user_id, -1)               as user_id,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snapshot_options_market_values_daily') }}
    where trade_symbol is not null
      and underlying_symbol is not null
      and trim(underlying_symbol) != ''

    union all

    -- Demo = relabeled MIRROR of the source tenant's option-mark history,
    -- matching the staging mirror (stg_demo_current) and the balance mirror
    -- in mart_account_equity_daily. Without this the demo tenant would have
    -- no snapshot history of its own, every past day would contribute $0
    -- MTM, and the demo's option leg would degrade to flat
    -- realize-on-close steps — hiding the product's headline feature on the
    -- one page prospects actually look at.
    select
        'demo:demo-account' as tenant_id,
        'Demo Account'      as account,
        cast(null as int64) as user_id,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snapshot_options_market_values_daily') }}
    where trade_symbol is not null
      and underlying_symbol is not null
      and trim(underlying_symbol) != ''
      and nullif(trim(tenant_id), '') = '{{ var("demo_source_tenant_id", "") }}'
      and '{{ var("demo_source_tenant_id", "") }}' != ''
),

unfolded as (
    select
        v.*,
        d as date
    from versions v
    cross join unnest(
        generate_date_array(
            date(v.dbt_valid_from),
            least(
                date(coalesce(v.dbt_valid_to, current_timestamp())),
                date_sub(current_date(), interval 1 day)
            )
        )
    ) as d
),

ranked as (
    select
        *,
        row_number() over (
            partition by tenant_id, account, user_id, trade_symbol, date
            order by dbt_valid_from desc
        ) as rn
    from unfolded
)

select
    tenant_id,
    account,
    user_id,
    trade_symbol,
    underlying_symbol,
    option_expiry,
    option_strike,
    option_type,
    quantity,
    current_price,
    market_value,
    cost_basis,
    unrealized_pnl,
    date,
    -- Sign-corrected MTM, same unified formula as stg_current.cleaned /
    -- int_option_contract_daily_pnl.snapshots / short_aware_unrealized_pnl
    -- in app/upload.py: shorts carry negative market_value (cost-to-close)
    -- and positive cost_basis (premium received).
    case
        when quantity is null or quantity = 0
        then coalesce(market_value, 0) - coalesce(cost_basis, 0)
        when quantity < 0
        then coalesce(market_value, 0) + coalesce(cost_basis, 0)
        else coalesce(market_value, 0) - coalesce(cost_basis, 0)
    end as mtm_unrealized_pnl
from ranked
where rn = 1
