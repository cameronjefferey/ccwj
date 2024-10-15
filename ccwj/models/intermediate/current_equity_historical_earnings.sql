with current_holdings as (
select 
   distinct symbol
from {{ ref('current')}}
)
, holdings_earnings_historical as (
select 
   date,
   current_holdings.symbol,
   history.security_type,
   sum(amount) as gain_or_loss,
from current_holdings 
   join {{ ref('history')}} 
      on current_holdings.symbol = history.symbol
group by 1,2,3
)
, running_earnings_historical as (
select 
   date,
   symbol,
   security_type,
   gain_or_loss,
   sum(gain_or_loss) over (partition by symbol,security_type order by date asc) as running_earnings_historical
from holdings_earnings_historical
where 1=1
   and security_type = 'Equity'
)
, daily_holdings_value as (
select 
    symbol,
    row_number() over (partition by symbol,action,option_security_type order by date) as stock_action_order,
    date,
    coalesce(lead(date) over (partition by symbol, option_security_type order by date)-1,current_date()) as next_equity_date,
    action,
    case when action = 'Sell' then quantity*-1 else quantity end as quantity,
    sum(case when action = 'Sell' then quantity*-1 else quantity end) over (partition by symbol,option_security_type order by date) as stock_action_quantity,
from {{ ref('history')}}
where symbol = 'CFLT'
    and action in ('Buy','Sell')
order by date
)
, final as (
select 
   calendar_symbol_dates.day,
   calendar_symbol_dates.symbol,
   daily_holdings_value.stock_action_quantity,
   running_earnings_historical.gain_or_loss,
   sum(running_earnings_historical.gain_or_loss) over (partition by calendar_symbol_dates.symbol order by calendar_symbol_dates.day asc) as running_earnings_historical,
   cflt_prices.close as closing_price
from {{ ref('calendar_symbol_dates')}} 
   left join running_earnings_historical
      on running_earnings_historical.date = calendar_symbol_dates.day 
         and running_earnings_historical.symbol = calendar_symbol_dates.symbol 
   left join daily_holdings_value 
      on daily_holdings_value.symbol = calendar_symbol_dates.symbol 
         and daily_holdings_value.date <= calendar_symbol_dates.day 
         and daily_holdings_value.next_equity_date >= calendar_symbol_dates.day 
   left join {{ ref('cflt_prices')}}
      on lower(cflt_prices.symbol) = lower(calendar_symbol_dates.symbol)
         and date(cflt_prices.date) = date(calendar_symbol_dates.day)
where 1=1
   

   and calendar_symbol_dates.symbol = 'CFLT'
)
select 
   day,
   symbol,
   running_earnings_historical,
   stock_action_quantity*closing_price as daily_holdings_value,
   round(running_earnings_historical+(stock_action_quantity*closing_price),2) as historical_equity_holdings
from final 
where 1=1
   and day BETWEEN '2022-01-01' and current_date()
   and closing_price is not null 
order by day desc 