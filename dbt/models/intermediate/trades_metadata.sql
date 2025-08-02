with trades_metadata as (
select 
    account,
    symbol,
    trade_symbol,
    security_type,
    action,
    --Quantities
    --Only need to know how many were opened since open will always equal closed
    quantity as trade_quantity,

    --Dates
    transaction_date as trade_date,
    
    --Amounts
    amount as trade_amount,

--Security Trade Order 
    row_number() over (partition by account, symbol, security_type order by transaction_date, action desc) as position_trade_order,

    --Equity Trade Order 
    case when security_type = 'Equity' then rank() over (partition by account, symbol, security_type order by transaction_date, action desc) end as equity_trade_order,
    CASE WHEN security_type = 'Equity' AND action = 'buy' THEN
      COUNTIF(action = 'buy') OVER (
        PARTITION BY account, symbol
        ORDER BY transaction_date, action DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    END AS equity_trade_buys_order,
    CASE WHEN security_type = 'Equity' AND action IN ('sell','holding') THEN
      COUNTIF(action IN ('sell','holding')) OVER (
        PARTITION BY account, symbol
        ORDER BY transaction_date, action DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    END AS equity_trade_sells_order
    
from {{ ref('history_and_current_combined')}}
where 1=1
)
select 
    trades_metadata.*,
    
from trades_metadata
where 1=1