/*
    A Closed option contract must have a close_date. int_option_exit_analysis
    (and therefore mart_coaching_signals.total_closed) filters
    `close_date is not null`, while int_strategy_classification counts
    status='Closed'. A snapshot-dropped contract with NULL close_date
    made CHECK 12 fail (run 33301622998): coach lagged classification
    by 1–2 Covered Call / Put Spread contracts.
*/

select
    tenant_id,
    account,
    user_id,
    trade_symbol,
    status,
    close_date,
    open_date,
    option_expiry
from {{ ref('int_option_contracts') }}
where status = 'Closed'
  and close_date is null
