{#
    Price-tracker fold regression (2026-07-14).

    A nominal equity lot (<= 1 share) held alongside options on the same
    underlying is a "so I can watch the ticker" position, not a standalone
    Buy and Hold. It must be FOLDED into that underlying's option strategy
    in int_strategy_classification — otherwise a pure long-call position
    reads as "mixed" and the Strategy Breakdown shows a spurious Buy and
    Hold row for a single tracking share (real case: SEI, user 9, 1 share +
    a long call).

    Fails (returns rows) if any equity session with <= 1 share that has at
    least one overlapping option contract is still classified 'Buy and Hold'
    (or 'Crypto' — a tracker is neither). Standalone tiny lots with NO
    options are out of scope (they legitimately stay Buy and Hold).
#}

with equity_strat as (
    select
        tenant_id, account, user_id, symbol, trade_symbol, strategy
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'equity_session'
      and strategy in ('Buy and Hold', 'Crypto')
),

-- Session grain rebuilt from the session id embedded in trade_symbol
-- (``<symbol>_session_<id>``) so we can read max_quantity_held.
sessions as (
    select
        tenant_id, account, user_id, symbol, session_id, status,
        max_quantity_held, open_date, last_trade_date
    from {{ ref('int_equity_sessions') }}
    where coalesce(max_quantity_held, 0) <= 1
),

overlapping_options as (
    select distinct
        s.tenant_id, s.account, s.user_id, s.symbol, s.session_id
    from sessions s
    join {{ ref('int_option_contracts') }} oc
        on s.account = oc.account
        and (s.user_id is not distinct from oc.user_id)
        and (s.tenant_id is not distinct from oc.tenant_id)
        and s.symbol = oc.underlying_symbol
        and oc.open_date >= s.open_date
        and oc.open_date <= case when s.status = 'Open' then current_date() else s.last_trade_date end
)

select
    es.tenant_id,
    es.account,
    es.user_id,
    es.symbol,
    es.trade_symbol,
    es.strategy
from equity_strat es
join sessions s
    on es.account = s.account
    and (es.user_id is not distinct from s.user_id)
    and (es.tenant_id is not distinct from s.tenant_id)
    and es.trade_symbol = concat(s.symbol, '_session_', cast(s.session_id as string))
join overlapping_options oo
    on s.account = oo.account
    and (s.user_id is not distinct from oo.user_id)
    and (s.tenant_id is not distinct from oo.tenant_id)
    and s.symbol = oo.symbol
    and s.session_id = oo.session_id
