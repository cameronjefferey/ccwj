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

    The /wealth page can answer:
      - "How much do I have today and how is it allocated?" — top row
        of equity_value / option_value / cash_value, summing to
        account_value.
      - "How has it changed?" — day-over-day account_value_delta or
        the delta between start-of-range and end-of-range
        account_value.
      - "Where did the growth come from?" — cumulative_dividends +
        cumulative_interest_net + cumulative_fees vs the residual
        change in account_value over the same window. The residual
        is *not* a clean "deposits" number because Schwab API sync
        only emits TRADE rows (see app/schwab.py
        _schwab_trade_rows) and the export-side actions taxonomy
        (see stg_history) doesn't tag deposits/withdrawals
        explicitly — anything that isn't a trade, dividend,
        interest, or fee lands in `action='other'` or never reaches
        history at all. Treat the residual as "everything else"
        rather than as a precise deposit metric.

    Tenant safety: every join, partition, and group-by is keyed on
    (account, user_id) and uses ``IS NOT DISTINCT FROM`` so a user_id
    NULL on the demo path doesn't get double-counted. See
    docs/USER_ID_TENANCY.md and .cursor/rules/bigquery-tenant-isolation.mdc.
*/

with equity as (
    select
        account,
        user_id,
        date,
        account_value,
        cash_value,
        equity_value,
        option_value
    from {{ ref('mart_account_equity_daily') }}
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
        trade_date as date,
        sum(case when action = 'dividend'        then amount else 0 end) as dividend_today,
        sum(case when action = 'credit_interest' then amount else 0 end)
            + sum(case when action = 'margin_interest' then amount else 0 end)
            as interest_net_today,
        sum(case when action = 'adr_fee'         then amount else 0 end) as fees_today
    from {{ ref('stg_history') }}
    where action in ('dividend', 'credit_interest', 'margin_interest', 'adr_fee')
    group by 1, 2, 3
),

joined as (
    select
        e.account,
        e.user_id,
        e.date,
        e.account_value,
        e.cash_value,
        e.equity_value,
        e.option_value,
        coalesce(h.dividend_today, 0)      as dividend_today,
        coalesce(h.interest_net_today, 0)  as interest_net_today,
        coalesce(h.fees_today, 0)          as fees_today
    from equity e
    left join history_by_day h
      on h.account = e.account
     and (h.user_id is not distinct from e.user_id)
     and h.date    = e.date
)

select
    account,
    user_id,
    date,
    account_value,
    cash_value,
    equity_value,
    option_value,

    -- Day-over-day account-value change. NULL on the first day per
    -- (account, user_id) so charts can render a gap rather than
    -- pretending the first observation was a delta from zero.
    account_value - lag(account_value) over (
        partition by account, user_id
        order by date
    ) as account_value_delta,

    dividend_today,
    interest_net_today,
    fees_today,

    -- Running totals scoped to the (account, user_id) so two users
    -- sharing an account label never have their tallies merged.
    sum(dividend_today) over (
        partition by account, user_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_dividends,

    sum(interest_net_today) over (
        partition by account, user_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_interest_net,

    sum(fees_today) over (
        partition by account, user_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_fees
from joined
order by account, user_id, date
