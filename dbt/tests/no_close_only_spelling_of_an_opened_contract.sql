/*
    One option contract, one trade_symbol.

    Compact OCC, space-padded OSI, and Schwab long form of the same
    contract must not survive as two rows. The failure mode: the close
    stays on one spelling with no opening fill (opened_before_history,
    dropped from int_strategy_classification and from the coach) while
    the open stays on the other spelling. The close is then invisible
    to strategy tags and to CHECK 12, and the open is a different
    contract. CHECK 12, run 38531276167, was this shape until the
    symbol id was unified; the test still catches a split.

    A genuine pre-window close — no opening fill on any spelling — is
    not this bug and is not returned.
*/

select
    c.tenant_id,
    c.account,
    c.trade_symbol as close_only_symbol,
    o.trade_symbol as opened_symbol
from {{ ref('int_option_contracts') }} c
join {{ ref('int_option_contracts') }} o
    on c.account = o.account
    and (c.user_id is not distinct from o.user_id)
    and (c.tenant_id is not distinct from o.tenant_id)
    and upper(trim(c.underlying_symbol)) = upper(trim(o.underlying_symbol))
    and c.option_expiry = o.option_expiry
    and c.option_strike is not null
    and abs(c.option_strike - o.option_strike) < 0.001
    and upper(trim(c.option_type)) = upper(trim(o.option_type))
    and c.trade_symbol != o.trade_symbol
where coalesce(c.opened_before_history, false)
  and not coalesce(o.opened_before_history, false)
  and c.status = 'Closed'
  and c.close_date is not null
