-- Singular test: every option row in `stg_current` must be typed as
-- 'Call' or 'Put', never 'Other'.
--
-- Anchor: the trade_symbol carries an OSI option slice (`\d{6}[CP]\d{8}`,
-- e.g. "DAL   260717C00093000"). If a symbol has that slice it IS an
-- option contract and MUST classify as Call/Put so downstream option
-- logic attaches to it.
--
-- This would have caught the 2026-07-08 bug the day it shipped: for a
-- 3-character root ("DAL") the OSI string is space-padded to 6 chars, so
-- `split(symbol, ' ')[safe_offset(3)]` landed on the OSI tail instead of a
-- C/P token and the unguarded coalesce shadowed the correct osi_cp='C',
-- typing every 3-char-root option as instrument_type='Other'. That dropped
-- them from the `int_option_contracts` snapshot join (which filters
-- instrument_type in ('Call','Put')), so open-option P&L fell back to net
-- premium instead of mark-to-market — a DAL iron condor read $1,483 net
-- credit as "unrealized P&L" vs the true $73.34 MTM.
select
    tenant_id,
    account,
    user_id,
    trade_symbol,
    instrument_type,
    option_type
from {{ ref('stg_current') }}
where regexp_contains(upper(trim(coalesce(trade_symbol, ''))), r'\d{6}[CP]\d{8}')
  and instrument_type not in ('Call', 'Put')
