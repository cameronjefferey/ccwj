{{ config(materialized='table') }}

/*
    Daily wealth view backing the /wealth page.

    One row per (account, user_id, date) with:
      - account_value, cash_value, equity_value, option_value
        (forwarded from mart_account_equity_daily so the page reads
         from a single mart instead of joining two).
      - account_value_delta — day-over-day account_value change for
        the same account/user.
      - dividend_today, interest_net_today, fees_today — cash flows
        recorded in stg_history on this trade date for this account.
        ``interest_net`` is ``credit_interest + margin_interest`` so the
        sign on margin (already negative) cancels naturally.
        Fees are ``adr_fee`` (also signed-negative). All three are 0
        on days with no matching history rows.
      - cumulative_dividends, cumulative_interest_net, cumulative_fees
        — running totals from the start of each account's snapshot
        history. Lets the page render "where the growth came from"
        without a second BQ round-trip.
      - net_deposit_today, cumulative_net_deposits — external cash the
        trader moved IN (+) or OUT (−) of the account on/through this day.

        Two modes, never stacked on the same tenant:

          1. ITEMIZED — the tenant has at least one ``cash_transfer``
             dated on or before the first snapshot (a CSV backfill of
             Schwab ``Funds Received`` / ``MoneyLink Transfer`` / cash
             ``Journal``, or a broker feed that actually sent deposits). Cumulative is
             the running sum of ALL those rows ASOF onto the snapshot
             spine. Day-1 exclude line is
             ``account_value − Σ deposits through day 1`` (pre-snapshot
             trading P&L stays). Do NOT also add ``int_opening_cash``
             — that lump already contains those deposits.
          2. FALLBACK — no pre-snapshot cash_transfer rows. Opening
             cash (``int_opening_cash`` = first snapshot account_value)
             is the missing deposit, plus explicit transfers AFTER
             day 1. Day-1 cumulative equals account_value (exclude
             line starts at $0). This is every account funded before
             capture that has no CSV cash history.

        ``cumulative_net_deposits`` ASOF-joins onto the spine so a
        weekend withdrawal still counts on the next snapshot.
        ``net_deposit_today`` is the day-over-day difference of that
        cumulative.

    The /wealth page can answer:
      - "How much do I have today and how is it allocated?" — top row
        of equity_value / option_value / cash_value, summing to
        account_value.
      - "How has it changed?" — day-over-day account_value_delta or
        the delta between start-of-range and end-of-range
        account_value.
      - "Where did the growth come from?" — cumulative_dividends +
        cumulative_interest_net + cumulative_fees vs the residual
        change in account_value over the same window.

    Tenant safety: every join, partition, and group-by is keyed on
    (account, user_id) and uses ``IS NOT DISTINCT FROM`` so a user_id
    NULL on the demo path doesn't get double-counted. See
    docs/USER_ID_TENANCY.md and .cursor/rules/bigquery-tenant-isolation.mdc.
*/

with equity as (
    select
        account,
        user_id,
        tenant_id,
        date,
        account_value,
        cash_value,
        equity_value,
        option_value
    from {{ ref('mart_account_equity_daily') }}
),

opening as (
    select
        account,
        user_id,
        tenant_id,
        first_date,
        opening_deposit
    from {{ ref('int_opening_cash') }}
),

-- Daily history aggregates. Bucketed by trade_date so the join below
-- is a tight equi-join; rows where no history exists for the day stay
-- NULL and get coalesced to 0 at the final select.
-- ``stg_history.amount`` is the signed-by-action cash flow ("negative =
-- cash out, positive = cash in" — see the ``amount_signed`` CTE in
-- stg_history.sql). credit_interest is positive and margin_interest is
-- negative there, so summing them gives a true net interest figure.
-- adr_fee is also negative, so summing it directly preserves the sign.
history_by_day as (
    select
        account,
        user_id,
        tenant_id,
        trade_date as date,
        sum(case when action = 'dividend'        then amount else 0 end) as dividend_today,
        sum(case when action = 'credit_interest' then amount else 0 end)
            + sum(case when action = 'margin_interest' then amount else 0 end)
            as interest_net_today,
        sum(case when action = 'adr_fee'         then amount else 0 end) as fees_today
    from {{ ref('stg_history') }}
    where action in ('dividend', 'credit_interest', 'margin_interest', 'adr_fee')
    group by 1, 2, 3, 4
),

-- Every explicit cash_transfer, including those dated before the first
-- snapshot. Used when a CSV/broker backfill actually itemized deposits.
transfers_all as (
    select
        account,
        user_id,
        tenant_id,
        trade_date as date,
        sum(amount) as net_deposit_today
    from {{ ref('stg_history') }}
    where action = 'cash_transfer'
    group by 1, 2, 3, 4
),

transfer_running_all as (
    select
        account,
        user_id,
        tenant_id,
        date,
        net_deposit_today,
        sum(net_deposit_today) over (
            partition by tenant_id, account, user_id
            order by date
            rows between unbounded preceding and current row
        ) as itemized_cum
    from transfers_all
),

itemized_tenants as (
    select distinct
        o.tenant_id,
        o.account,
        o.user_id
    from opening o
    inner join {{ ref('stg_history') }} t
      on (o.tenant_id is not distinct from t.tenant_id)
     and o.account = t.account
     and (o.user_id is not distinct from t.user_id)
     and t.action = 'cash_transfer'
     and t.trade_date <= o.first_date
),

-- Fallback path: transfers AFTER the opening snapshot only.
transfers_after_opening as (
    select
        t.account,
        t.user_id,
        t.tenant_id,
        t.trade_date as date,
        sum(t.amount) as net_deposit_today
    from {{ ref('stg_history') }} t
    inner join opening o
      on o.account = t.account
     and (o.user_id is not distinct from t.user_id)
     and (o.tenant_id is not distinct from t.tenant_id)
     and t.trade_date > o.first_date
    where t.action = 'cash_transfer'
    group by 1, 2, 3, 4
),

transfer_running as (
    select
        account,
        user_id,
        tenant_id,
        date,
        net_deposit_today,
        sum(net_deposit_today) over (
            partition by tenant_id, account, user_id
            order by date
            rows between unbounded preceding and current row
        ) as post_opening_cum
    from transfers_after_opening
),

equity_with_itemized as (
    select
        e.*,
        coalesce(ar.itemized_cum, 0) as itemized_cum
    from equity e
    left join transfer_running_all ar
      on ar.account = e.account
     and (ar.user_id is not distinct from e.user_id)
     and (ar.tenant_id is not distinct from e.tenant_id)
     and ar.date <= e.date
    qualify row_number() over (
        partition by e.tenant_id, e.account, e.user_id, e.date
        order by ar.date desc nulls last
    ) = 1
),

joined as (
    select
        e.account,
        e.user_id,
        e.tenant_id,
        e.date,
        e.account_value,
        e.cash_value,
        e.equity_value,
        e.option_value,
        coalesce(h.dividend_today, 0)      as dividend_today,
        coalesce(h.interest_net_today, 0)  as interest_net_today,
        coalesce(h.fees_today, 0)          as fees_today,
        case
            when i.tenant_id is not null then e.itemized_cum
            else coalesce(o.opening_deposit, 0)
                + coalesce(tr.post_opening_cum, 0)
        end as cumulative_net_deposits
    from equity_with_itemized e
    left join opening o
      on o.account = e.account
     and (o.user_id is not distinct from e.user_id)
     and (o.tenant_id is not distinct from e.tenant_id)
    left join history_by_day h
      on h.account = e.account
     and (h.user_id is not distinct from e.user_id)
     and (h.tenant_id is not distinct from e.tenant_id)
     and h.date    = e.date
    left join transfer_running tr
      on tr.account = e.account
     and (tr.user_id is not distinct from e.user_id)
     and (tr.tenant_id is not distinct from e.tenant_id)
     and tr.date <= e.date
    left join itemized_tenants i
      on (i.tenant_id is not distinct from e.tenant_id)
     and i.account = e.account
     and (i.user_id is not distinct from e.user_id)
    qualify row_number() over (
        partition by e.tenant_id, e.account, e.user_id, e.date
        order by tr.date desc nulls last
    ) = 1
)

select
    account,
    user_id,
    tenant_id,
    date,
    account_value,
    cash_value,
    equity_value,
    option_value,

    -- Day-over-day account-value change. NULL on the first day per
    -- (tenant_id, account, user_id) so charts can render a gap rather than
    -- pretending the first observation was a delta from zero.
    account_value - lag(account_value) over (
        partition by tenant_id, account, user_id
        order by date
    ) as account_value_delta,

    dividend_today,
    interest_net_today,
    fees_today,

    -- Day-1: either the inferred opening lump (fallback) or the
    -- running sum of itemized transfers through that date.
    coalesce(
        cumulative_net_deposits - lag(cumulative_net_deposits) over (
            partition by tenant_id, account, user_id
            order by date
        ),
        cumulative_net_deposits
    ) as net_deposit_today,

    -- Running totals scoped to (tenant_id, account, user_id) so two
    -- physical accounts sharing a display label never have their tallies
    -- merged.
    sum(dividend_today) over (
        partition by tenant_id, account, user_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_dividends,

    cumulative_net_deposits,

    sum(interest_net_today) over (
        partition by tenant_id, account, user_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_interest_net,

    sum(fees_today) over (
        partition by tenant_id, account, user_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_fees
from joined
order by tenant_id, account, user_id, date
