with equity_open_date as (
    select 
        account,
        symbol,
        min(transaction_date) as equity_open_date 
    from {{ ref('history_and_current_combined') }}
    where security_type = 'Equity'
        and is_current_position_establishement_1_0 = 1
        and position_establishment_order = 1
    group by account, symbol
)

select 
    history_and_current_combined.account,
    history_and_current_combined.symbol,
    history_and_current_combined.transaction_date,
    history_and_current_combined.security_type,
    history_and_current_combined.quantity,
    history_and_current_combined.trade_symbol,
    history_and_current_combined.amount,
    history_and_current_combined.action,
    history_and_current_combined.next_position_amount,
    round(history_and_current_combined.amount + history_and_current_combined.next_position_amount,2) as position_gain_or_loss,
from {{ ref('history_and_current_combined')}}
    join equity_open_date 
        on history_and_current_combined.account = equity_open_date.account
        and history_and_current_combined.symbol = equity_open_date.symbol
        and history_and_current_combined.transaction_date >= equity_open_date.equity_open_date
where history_and_current_combined.action in ('buy','buy to open','sell to open','pr yr cash div','cash dividend','special dividend','special qual div','qualified dividend')
