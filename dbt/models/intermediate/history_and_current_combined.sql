with combined as (
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
    action,
    transaction_date,
    sum(case 
        when action = 'buy' then quantity
        when action = 'sell' then -quantity
    end) over (partition by account, trade_symbol order by transaction_date) as position_quantity,
from combined 
where action in ('buy','sell','holding')
)
, prior_position_quantities as (
select
    account,
    trade_symbol,
    transaction_date,
    action,
    lag(position_quantity) over (partition by account, trade_symbol order by transaction_date) as prior_position_quantity,
    position_quantity,
    lead(position_quantity) over (partition by account, trade_symbol order by transaction_date) as next_position_quantity,
    lead(action) over (partition by account, trade_symbol order by transaction_date) as next_position_action,
from position_establishments
)
, current_position_establishement as (
select 
    *,
    prior_position_quantity,
    position_quantity,
    next_position_quantity,
    next_position_action,
    case 
        when coalesce(prior_position_quantity,0) = 0
            and position_quantity > 0
            and next_position_quantity = position_quantity 
            and next_position_action = 'holding'
        then 1
        else 0 
    end as is_current_position_establishement_1_0
from prior_position_quantities
)
select 
    combined.*,
    current_position_establishement.is_current_position_establishement_1_0
from combined
    left join current_position_establishement using (account, trade_symbol,transaction_date)