-- Per-broker current-positions adapter: Fidelity (via SnapTrade).
-- Unioned into stg_current. Same exclusivity rules as
-- stg_broker_fidelity_history: first token "fidelity" only.
-- A Schwab or Vanguard label cannot land in this model.
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
-- Mutual funds (FXAIX and the like) stay Equity. security_type
-- "Cash and Money Market" is a core-position sweep (SPAXX / FDRXX when
-- SnapTrade sets cash_equivalent). stg_current drops that type from
-- holdings; stg_broker_fidelity_balances can still read it as a
-- priority-2 cash line when the balances seed has no cash row.

with fidelity_current as (
    {{ broker_current_rows('fidelity') }}
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
from fidelity_current
