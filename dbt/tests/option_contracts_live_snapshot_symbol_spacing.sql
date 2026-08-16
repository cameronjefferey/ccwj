/*
    A live unexpired option must stay Open when history and holdings use
    different whitespace around the OSI symbol.

    SnapTrade can emit e.g. ``FN    260814C00120000`` in history and
    ``FN 260814C00120000`` in current holdings. The option lifecycle uses
    snapshot absence to infer closure for contracts opened before today, so
    an exact-text-only join falsely realizes the position and removes it from
    int_enriched_current. Match the stable OSI core plus underlying instead.
*/

with live_snapshot_matches as (
    select
        c.tenant_id,
        c.account,
        c.user_id,
        c.trade_symbol as history_trade_symbol,
        cur.trade_symbol as current_trade_symbol,
        c.underlying_symbol,
        c.option_expiry,
        c.open_date,
        c.close_type,
        c.status
    from {{ ref('int_option_contracts') }} c
    join {{ ref('stg_current') }} cur
        on (c.tenant_id is not distinct from cur.tenant_id)
        and c.account = cur.account
        and (c.user_id is not distinct from cur.user_id)
        and upper(trim(coalesce(c.underlying_symbol, '')))
            = upper(trim(coalesce(cur.underlying_symbol, '')))
        and regexp_extract(
                upper(trim(coalesce(c.trade_symbol, ''))),
                r'(\d{6}[CP]\d{8})'
            ) is not null
        and regexp_extract(
                upper(trim(coalesce(c.trade_symbol, ''))),
                r'(\d{6}[CP]\d{8})'
            ) = regexp_extract(
                upper(trim(coalesce(cur.trade_symbol, ''))),
                r'(\d{6}[CP]\d{8})'
            )
        and cur.instrument_type in ('Call', 'Put')
    where c.trade_symbol != cur.trade_symbol
      and c.option_expiry >= current_date()
      and not exists (
          select 1
          from {{ ref('stg_history') }} h
          where (h.tenant_id is not distinct from c.tenant_id)
            and h.account = c.account
            and (h.user_id is not distinct from c.user_id)
            and h.trade_symbol = c.trade_symbol
            and h.action in (
                'option_buy_to_close', 'option_sell_to_close',
                'option_expired', 'option_assigned', 'option_exercised'
            )
      )
)

select *
from live_snapshot_matches
where status != 'Open'
