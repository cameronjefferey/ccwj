/*
    underlying_symbol is the equity root the price loader fetches from
    Yahoo. A compact OCC contract (AAPL240119C00150000) is not a ticker.
    Run 38499020109: 303 of those counted as failed price fetches and
    tripped PRICES_GUARD_TRIPPED at 67.5% coverage. The root belongs in
    underlying_symbol; the contract stays on trade_symbol.
*/

select 'stg_history' as src, underlying_symbol
from {{ ref('stg_history') }}
where regexp_contains(
    replace(upper(trim(underlying_symbol)), ' ', ''),
    r'^[A-Z][A-Z0-9.-]{0,5}\d{6}[CP]\d{8}$'
)

union all

select 'stg_current' as src, underlying_symbol
from {{ ref('stg_current') }}
where regexp_contains(
    replace(upper(trim(underlying_symbol)), ' ', ''),
    r'^[A-Z][A-Z0-9.-]{0,5}\d{6}[CP]\d{8}$'
)
