with fix as (
select 
    account,
    symbol as trade_symbol,
    split(symbol," ")[0] as symbol,
    split(symbol," ")[SAFE_OFFSET(1)] as option_expiration_date,
    split(symbol," ")[SAFE_OFFSET(2)] as option_expiration_price,
    split(symbol," ")[SAFE_OFFSET(3)] as option_security_type,
    Description,
    Quantity,
    Price,
    price_change_percent,
    CAST(REGEXP_REPLACE(case 
       when cast(price_change_dollar as  string) LIKE '($%)' THEN CONCAT('-', REPLACE(REPLACE(cast(price_change_dollar as  string), ')', ''), '($', ''))
       when cast(price_change_dollar as  string) LIKE '$%' THEN REPLACE(REPLACE(cast(price_change_dollar as  string), ')', ''), '$', '')
       else cast(price_change_dollar as  string)
    end, r'\$|,', '') as FLOAT64) as price_change_dollar,
   CAST(REGEXP_REPLACE(case 
       when cast(market_value as string) LIKE '($%)' THEN CONCAT('-', REPLACE(REPLACE(cast(market_value as string), ')', ''), '($', ''))
       when cast(market_value as string) LIKE '$%' THEN REPLACE(REPLACE(cast(market_value as string), ')', ''), '$', '')
       else cast(market_value as string)
    end, r'\$|,', '') as FLOAT64) as market_value,
    day_change_percent,
    CAST(REGEXP_REPLACE(case 
       when cast(day_change_dollar as string) LIKE '($%)' THEN CONCAT('-', REPLACE(REPLACE(cast(day_change_dollar as string), ')', ''), '($', ''))
       when cast(day_change_dollar as string) LIKE '$%' THEN REPLACE(REPLACE(cast(day_change_dollar as string), ')', ''), '$', '')
       else cast(day_change_dollar as string)
    end, r'\$|,', '') as FLOAT64) as day_change_dollar,
    CAST(REGEXP_REPLACE(case 
       when cast(cost_bases as string) LIKE '($%)' THEN CONCAT('-', REPLACE(REPLACE(cast(cost_bases as string), ')', ''), '($', ''))
       when cast(cost_bases as string) LIKE '$%' THEN REPLACE(REPLACE(cast(cost_bases as string), ')', ''), '$', '')
       else cast(cost_bases as string)
    end, r'\$|,', '') as FLOAT64) as cost_basis,
    gain_or_loss_percent,
    CAST(REGEXP_REPLACE(case 
       when cast(gain_or_loss_dollat as string) LIKE '($%)' THEN CONCAT('-', REPLACE(REPLACE(cast(gain_or_loss_dollat as string), ')', ''), '($', ''))
       when cast(gain_or_loss_dollat as string) LIKE '$%' THEN REPLACE(REPLACE(cast(gain_or_loss_dollat as string), ')', ''), '$', '')
       else cast(gain_or_loss_dollat as string)
    end, r'\$|,', '') as FLOAT64) as gain_or_loss_dollar,
    rating,
    divident_reinvestment,
    is_capital_gain,
    percent_of_account,
    expiration_date,
    cost_per_share,
    last_earnings_date,
    dividend_yield,
    last_dividend,
    ex_dividend_date,
    pe_ratio,
    annual_week_low,
    annual_week_high,
    volume,
    intrinsic_value,
    in_the_money,
    security_type,
    margin_requirement,
    
from {{ ref('0417_current')}}
)
select 
    *
from fix 

