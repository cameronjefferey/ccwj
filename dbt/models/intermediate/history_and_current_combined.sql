with combined as (
select 
    account,
    transaction_date,
    trade_symbol,
    action,
    symbol,
    case when security_type = 'ETFs & Closed End Funds' then 'Equity' else security_type end as security_type,
    option_expiration_date,
    option_expiration_price,
    option_security_type,
    quantity,
    price,
    0 as fees_and_comm,
    market_value as amount,
    cost_basis,
    cost_per_share,

from {{ ref('current')}}

UNION ALL 

select 
    account,
    transaction_date,
    trade_symbol,
    action,
    symbol,
    security_type,
    option_expiration_date,
    option_expiration_price,
    option_security_type,
    quantity,
    price,
    fees_and_comm,
    amount,
    0 as cost_basis,
    cost_per_share,
    


from {{ ref('history')}}
)
, position_establishments as (
select 
    account,
    trade_symbol,
    security_type,
    action,
    transaction_date,
    amount,
    sum(case 
        when action = 'buy' then quantity
        when action = 'sell' then -quantity
    end) over (partition by account, trade_symbol order by transaction_date) as position_quantity,
from combined 
{# where action in ('buy','sell','holding','sell to open','buy to open') #}
)
, prior_position_quantities as (
select
    account,
    trade_symbol,
    transaction_date,
    security_type,
    lead(transaction_date) over (partition by account, trade_symbol, security_type order by transaction_date) as next_position_transaction_date,
    action,
    lead(action) over (partition by account, trade_symbol, security_type order by transaction_date) as next_position_action,
    position_quantity,
    --In order to establish the "first" position establishment, the previous holdings should be 0 and the current position
    --action should be "holding"
    coalesce(lag(position_quantity) over (partition by account, trade_symbol, security_type order by transaction_date),0) as prior_position_quantity,
    lead(position_quantity) over (partition by account, trade_symbol, security_type order by transaction_date) as next_position_quantity,
    amount,
    lead(amount) over (partition by account, trade_symbol, security_type order by transaction_date) as next_position_amount
from position_establishments
)
, current_position_establishement as (
select 
    prior_position_quantities.*,
    case 
        when prior_position_quantity = 0
            and prior_position_quantities.security_type = 'Equity'
        then 1
        else 0 
    end as is_current_position_establishement_1_0,
    case 
        when prior_position_quantities.prior_position_quantity > 0 
            and prior_position_quantities.action = 'buy'
        then 1 
        else 0 
    end as is_buying_more_shares_in_current_position_1_0
from prior_position_quantities
    left join combined 
        on prior_position_quantities.account = combined.account
            and prior_position_quantities.trade_symbol = combined.trade_symbol
            and combined.action = 'holding'
            and combined.security_type = 'Equity'
)
, final as (
select 
    combined.*,
    current_position_establishement.next_position_transaction_date,
    current_position_establishement.prior_position_quantity,
    current_position_establishement.position_quantity,
    current_position_establishement.next_position_quantity,
    current_position_establishement.next_position_action,
    current_position_establishement.next_position_amount,
    current_position_establishement.is_current_position_establishement_1_0,
    CASE
    WHEN is_current_position_establishement_1_0 = 1
    THEN
      SUM(is_current_position_establishement_1_0) 
        OVER (
          PARTITION BY account,symbol
          ORDER BY transaction_date DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  
        )
  END AS position_establishment_order,
  is_buying_more_shares_in_current_position_1_0
from combined
    left join current_position_establishement using (account, trade_symbol,transaction_date)
)
select *
from final  
where 1=1
