/*
    mart_daily_pnl's natural grain must stay one row per
    (tenant_id, account, user_id, symbol, date).

    Regression (Aug 2026): synthetic inferred-opening rows were UNION ALLed
    with real daily history. When the inferred opening date already contained
    an option or other symbol event, downstream joins emitted duplicate daily
    rows and cumulative windows counted that day's dividend twice (a real
    position was overstated by $274 in the nightly reconcile audit).
*/

select
    tenant_id,
    account,
    user_id,
    symbol,
    date,
    count(*) as row_count
from {{ ref('mart_daily_pnl') }}
group by 1, 2, 3, 4, 5
having count(*) != 1
