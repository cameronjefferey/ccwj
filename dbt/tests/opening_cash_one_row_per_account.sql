/*
    int_opening_cash is one row per (tenant_id, account, user_id) —
    the first snapshot date and that day's account_value. A duplicate
    would double-count opening capital in mart_wealth_daily.
*/

select
    tenant_id,
    account,
    user_id,
    count(*) as n
from {{ ref('int_opening_cash') }}
group by 1, 2, 3
having count(*) > 1
