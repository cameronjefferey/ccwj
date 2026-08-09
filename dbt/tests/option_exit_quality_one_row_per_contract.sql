-- int_option_exit_quality must stay one row per closed contract.
-- The rolls / exit-analysis joins are both deduped at the source, but a
-- fanout here would silently double every execution-review dollar figure
-- (early-close deltas, premium kept, roll counts) on the Trader Profile
-- card and the Position review sentences.

select
    tenant_id,
    account,
    user_id,
    trade_symbol,
    count(*) as n
from {{ ref('int_option_exit_quality') }}
group by 1, 2, 3, 4
having count(*) > 1
