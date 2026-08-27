/*
    Opening cash is the first snapshot's account_value.

    Day-1 cumulative_net_deposits MUST equal that day's account_value
    so "Exclude deposits & withdrawals" starts the adjusted line at $0
    (the money already in the account is treated as a deposit). Later
    explicit cash_transfers (trade_date > first snapshot) stack on top.

    Fails if the first wealth row's cumulative disagrees with
    account_value, OR if the latest row disagrees with
    opening + sum(cash_transfers after first_date).
*/

with first_wealth as (
    select
        tenant_id,
        account,
        user_id,
        min(date) as first_date
    from {{ ref('mart_wealth_daily') }}
    where tenant_id is not null
    group by 1, 2, 3
),

first_row as (
    select
        w.tenant_id,
        w.account,
        w.user_id,
        w.date as first_date,
        w.account_value,
        w.cumulative_net_deposits
    from {{ ref('mart_wealth_daily') }} w
    inner join first_wealth f
      on f.tenant_id is not distinct from w.tenant_id
     and f.account = w.account
     and (f.user_id is not distinct from w.user_id)
     and f.first_date = w.date
),

last_row as (
    select * except (rn)
    from (
        select
            w.tenant_id,
            w.account,
            w.user_id,
            w.date as last_date,
            w.cumulative_net_deposits,
            row_number() over (
                partition by w.tenant_id, w.account, w.user_id
                order by w.date desc
            ) as rn
        from {{ ref('mart_wealth_daily') }} w
        where w.tenant_id is not null
    )
    where rn = 1
),

post_opening_transfers as (
    select
        f.tenant_id,
        f.account,
        f.user_id,
        coalesce(sum(h.amount), 0) as post_opening
    from first_row f
    left join {{ ref('stg_history') }} h
      on h.tenant_id is not distinct from f.tenant_id
     and h.account = f.account
     and (h.user_id is not distinct from f.user_id)
     and h.action = 'cash_transfer'
     and h.trade_date > f.first_date
    group by 1, 2, 3
)

select
    'first_day' as check_name,
    f.tenant_id,
    f.account,
    f.first_date as as_of,
    f.account_value as expected_cum,
    f.cumulative_net_deposits as actual_cum
from first_row f
where abs(f.cumulative_net_deposits - f.account_value) > 0.01

union all

select
    'latest_day' as check_name,
    l.tenant_id,
    l.account,
    l.last_date as as_of,
    f.account_value + p.post_opening as expected_cum,
    l.cumulative_net_deposits as actual_cum
from last_row l
inner join first_row f
  on f.tenant_id is not distinct from l.tenant_id
 and f.account = l.account
 and (f.user_id is not distinct from l.user_id)
inner join post_opening_transfers p
  on p.tenant_id is not distinct from l.tenant_id
 and p.account = l.account
 and (p.user_id is not distinct from l.user_id)
where abs(l.cumulative_net_deposits - (f.account_value + p.post_opening)) > 0.01
