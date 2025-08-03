select 
    account,
    symbol,
    security_type,
    trade_symbol,
    open_position_date,
    close_position_date,
    cast(position_quantity as int64) as position_quantity,
    round(position_gain_or_loss,2) as position_gain_or_loss,
from `ccwj-dbt.analytics.positions_current_equity`