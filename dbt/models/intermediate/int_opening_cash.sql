{{
    config(
        materialized='table'
    )
}}

/*
    Inferred opening CASH — one row per (tenant, account, user).

    SnapTrade only backfills a short activity window, and cash movements
    were dropped entirely until deposit capture shipped. So the first
    ``mart_account_equity_daily`` snapshot is almost always mid-life:
    the account already holds money that never landed as
    ``stg_history.action = 'cash_transfer'``.

    That day-1 account value IS the missing deposit (net of any
    withdrawals that happened before we were watching) WHEN the
    tenant has no ``cash_transfer`` rows on or before that date.
    If a CSV backfill itemized Schwab Funds Received / MoneyLink
    (or the broker sent deposits), ``mart_wealth_daily`` uses those
    rows instead — adding this lump on top would double-count.

    Without this fallback, ``mart_wealth_daily.cumulative_net_deposits``
    starts at 0 and the /accounts "Exclude deposits & withdrawals"
    toggle is a no-op on every account funded before capture that
    has no CSV cash history.

    Sibling of ``int_opening_balances`` (inferred opening SHARE counts
    for positions whose buys predate the window). This model is account-
    grain cash, not symbol-grain equity. It is NOT written into
    ``stg_history`` — a synthetic Deposit fill would show up as a trade
    on Daily Review / Position Detail. Only wealth / net-deposits
    surfaces read this.

    opening_deposit = account_value on the first snapshot date.
    Fallback path only: later explicit cash_transfers (trade_date >
    first_date) stack on top in mart_wealth_daily.
*/

select
    tenant_id,
    account,
    user_id,
    date as first_date,
    account_value as opening_deposit
from {{ ref('mart_account_equity_daily') }}
qualify row_number() over (
    partition by tenant_id, account, user_id
    order by date
) = 1
