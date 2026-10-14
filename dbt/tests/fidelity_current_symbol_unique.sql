/*
    One Fidelity snapshot row per (tenant_id, symbol).

    Two rows for the same symbol in one account would double market
    value and cost basis when holdings are summed. Blank symbols are
    out of scope (they are not a holding).
*/

select
    tenant_id,
    symbol,
    count(*) as n
from {{ ref('stg_broker_fidelity_current') }}
where trim(coalesce(cast(symbol as string), '')) != ''
group by tenant_id, symbol
having count(*) > 1
