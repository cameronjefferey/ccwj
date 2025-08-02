select 
    account,
    symbol,
    security_type,
    trade_symbol,
    open_equity_date,
    close_equity_date
    equity_quantity,
    equity_gain_or_loss,
from `ccwj-dbt.analytics.positions_current_equity`