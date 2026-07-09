-- Singular test: an iron condor must read as ONE strategy, not two.
--
-- A DAL iron condor (short 93C / long 95C call spread + short 86P / long
-- 83P put spread, legged in together on 2026-07-08) used to render as a
-- 'Call Spread' row AND a 'Put Spread' row in Strategy Breakdown. The
-- int_strategy_classification 'Iron Condor' branch collapses the four legs
-- into one label.
--
-- Invariant: for any (tenant, account, user, symbol, expiry) whose option
-- legs form BOTH a call spread (>= 2 call legs) AND a put spread (>= 2 put
-- legs) opened within a 7-day window, none of those legs may still carry
-- the generic 'Call Spread' / 'Put Spread' label — they must all be
-- 'Iron Condor'. A call spread and put spread opened MORE than 7 days apart
-- on the same expiry are intentionally NOT fused (the window guard below
-- mirrors the classification model), so they don't trip this test.
with legs as (
    select
        tenant_id, account, user_id, symbol, option_expiry,
        trade_symbol, option_type, open_date, strategy
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
      and strategy in ('Call Spread', 'Put Spread', 'Iron Condor')
),

condor_groups as (
    select tenant_id, account, user_id, symbol, option_expiry
    from legs
    group by 1, 2, 3, 4, 5
    having count(distinct case when option_type = 'C' then trade_symbol end) >= 2
       and count(distinct case when option_type = 'P' then trade_symbol end) >= 2
       and date_diff(max(open_date), min(open_date), day) <= 7
)

select l.*
from legs l
join condor_groups g
    on (l.tenant_id     is not distinct from g.tenant_id)
    and (l.user_id      is not distinct from g.user_id)
    and l.account       = g.account
    and l.symbol        = g.symbol
    and (l.option_expiry is not distinct from g.option_expiry)
where l.strategy in ('Call Spread', 'Put Spread')
