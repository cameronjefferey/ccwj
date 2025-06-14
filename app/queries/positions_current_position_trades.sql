select 
    account,
    symbol,
    transaction_date,
    security_type,
    quantity,
    trade_symbol,
    amount,
    next_position_amount,
    position_gain_or_loss,
from `ccwj-dbt.analytics.positions_current_equity`