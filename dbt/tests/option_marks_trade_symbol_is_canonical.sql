/*
    Historical option marks must use the same compact contract id as
    stg_current and int_option_contracts. The SCD2 snapshot keeps legacy
    padded and long-form spellings forever, so int_option_marks_daily owns
    the read-time migration.
*/

select *
from {{ ref('int_option_marks_daily') }}
where trade_symbol != {{ canonical_option_trade_symbol(
    'trade_symbol',
    'underlying_symbol',
    'option_expiry',
    'option_strike',
    'option_type'
) }}
