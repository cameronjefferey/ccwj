/*
    ``mart_account_snapshots_enriched`` MUST only carry trading-day rows
    (Mon-Fri), and its ``base_1d_date`` MUST also be a weekday.

    Why: the upstream mart_account_equity_daily forward-fills a row for every
    calendar day so the equity chart has no gaps. If those weekend rows leaked
    into this mart, the Daily Review's "vs yesterday" would compare a weekend
    against the prior weekend and always read $0 — and a Monday's "yesterday"
    would be Sunday instead of the previous Friday. The model filters to
    weekdays; this test is the backstop.

    BigQuery DAYOFWEEK: 1 = Sunday, 7 = Saturday.
*/

select
    tenant_id,
    account,
    date,
    base_1d_date
from {{ ref('mart_account_snapshots_enriched') }}
where extract(dayofweek from date) in (1, 7)
   or (base_1d_date is not null
       and extract(dayofweek from base_1d_date) in (1, 7))
