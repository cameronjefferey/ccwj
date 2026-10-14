-- Per-broker current-positions adapter: Vanguard (via SnapTrade).
-- Unioned into stg_current. Same exclusivity rules as
-- stg_broker_vanguard_history: first token "vanguard" only.
--
-- Explicit projection (broker_current_rows column order):
--   user_id, tenant_id, Account, Symbol, Description, Quantity, Price,
--   price_change_dollar, price_change_percent, market_value,
--   day_change_dollar, day_change_percent, cost_bases,
--   gain_or_loss_dollat, gain_or_loss_percent, rating,
--   divident_reinvestment, is_capital_gain, percent_of_account,
--   expiration_date, cost_per_share, last_earnings_date, dividend_yield,
--   last_dividend, ex_dividend_date, pe_ratio, annual_week_low,
--   annual_week_high, volume, intrinsic_value, in_the_money,
--   security_type, margin_requirement
--
-- security_type "Cash and Money Market" is a settlement sweep. stg_current
-- drops it from holdings; stg_broker_vanguard_balances can still read it
-- as a priority-2 cash line when the balances seed has no cash row.

with vanguard_current as (
    {{ broker_current_rows('vanguard') }}
)

select
    user_id,
    tenant_id,
    Account,
    Symbol,
    Description,
    Quantity,
    Price,
    price_change_dollar,
    price_change_percent,
    market_value,
    day_change_dollar,
    day_change_percent,
    cost_bases,
    gain_or_loss_dollat,
    gain_or_loss_percent,
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
    margin_requirement
from vanguard_current
