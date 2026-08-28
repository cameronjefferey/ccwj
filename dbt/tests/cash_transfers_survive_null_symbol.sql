/*
    Deposit / withdrawal survival (2026-08).

    Raw seed rows with Action in ('Deposit', 'Withdrawal', 'Cash Transfer')
    MUST land in stg_history as action='cash_transfer'. They often ship
    with a NULL Symbol (IBKR DISBURSEMENT withdrawals). The final WHERE
    in stg_history used to drop them because ``NULL != 'CURRENCY_USD'``
    is UNKNOWN. That left mart_wealth_daily.cumulative_net_deposits at 0
    for every tenant and made the "exclude deposits & withdrawals"
    toggle a no-op.

    Fails (returns rows) for any parseable raw cash-movement row that
    has no matching stg_history cash_transfer on (tenant_id, trade_date,
    amount). Same-day identical amounts that the seed already fused
    would still match one staging row.
*/

with raw_cash as (
    select
        nullif(trim(cast(tenant_id as string)), '') as tenant_id,
        {{ parse_seed_date('Date') }} as trade_date,
        coalesce(safe_cast(Amount as float64), 0) as amount,
        trim(cast(Action as string)) as action_raw
    from {{ source('raw_broker', 'trade_history') }}
    where lower(trim(cast(Action as string))) in ('deposit', 'withdrawal', 'cash transfer')
),

stg_cash as (
    select
        tenant_id,
        trade_date,
        amount
    from {{ ref('stg_history') }}
    where action = 'cash_transfer'
)

select
    r.tenant_id,
    r.trade_date,
    r.amount,
    r.action_raw
from raw_cash r
left join stg_cash s
  on s.tenant_id is not distinct from r.tenant_id
 and s.trade_date = r.trade_date
 and abs(s.amount - r.amount) < 0.01
where r.trade_date is not null
  and s.trade_date is null
