/*
    Closed equity sessions in int_strategy_classification must take
    realized_pnl from int_closed_equity_legs, not int_equity_sessions.total_pnl.

    Those two formulas disagree when a session is Closed only because a
    later snapshot / opening-balance session owns the leftover shares:
    session.total_pnl still writes leftover cost as realized, while the
    legs suppress that writeoff (symbol still held on the account).
    Reconcile CHECK 2 (run 33301622998) failed on DXCM / PL / ARCC /
    NVO / PEAK for 12 days on exactly that split.
*/

with class_closed as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        -- trade_symbol is '{symbol}_session_{id}'
        safe_cast(regexp_extract(trade_symbol, r'_session_(-?[0-9]+)$') as int64)
            as session_id,
        round(sum(realized_pnl), 2) as class_realized
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'equity_session'
      and status = 'Closed'
    group by 1, 2, 3, 4, 5
),

legs as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        session_id,
        round(sum(realized_pnl), 2) as legs_realized
    from {{ ref('int_closed_equity_legs') }}
    group by 1, 2, 3, 4, 5
)

select
    c.tenant_id,
    c.account,
    c.user_id,
    c.symbol,
    c.session_id,
    c.class_realized,
    coalesce(l.legs_realized, 0) as legs_realized
from class_closed c
left join legs l
    on (c.tenant_id is not distinct from l.tenant_id)
    and c.account = l.account
    and (c.user_id is not distinct from l.user_id)
    and c.symbol = l.symbol
    and (c.session_id is not distinct from l.session_id)
where abs(c.class_realized - coalesce(l.legs_realized, 0)) > 0.02
