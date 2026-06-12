/*
    ``mart_sector_movers`` MUST have at most one row per ``symbol``.

    The model is SYMBOL-grain market data (one recent-move row per symbol,
    deduped from the per-tenant price rows). The Earnings Watch page joins
    the current user's held sectors to this table; a duplicated symbol would
    double-count a mover in the "Movers in your sectors" list. The dedup
    (group by symbol, day → latest/prior pick) enforces this by construction;
    this test is the backstop.
*/

select
    symbol,
    count(*) as n
from {{ ref('mart_sector_movers') }}
group by symbol
having count(*) > 1
