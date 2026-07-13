{#
    Regression: an OPEN option contract with NO broker snapshot yet must
    contribute $0 to total_pnl — NOT the raw premium (net_cash_flow).

    A contract opened via the real-time ORDERS feed (intraday poll) has a
    fill in stg_history but no stg_current row: the broker holdings
    snapshot lags, and the intraday sync is history-only by design (see
    broker-sync-safety skill 2026-07-10). Before the fix,
    int_option_contracts.total_pnl fell through to ``net_cash_flow`` for
    these, so a freshly BOUGHT call read as a full -premium unrealized
    LOSS (real case 2026-07-13: SEI 260821C00070000 → -$9,200 total
    return on a call worth ~what was paid) and a freshly SOLD call read
    as a phantom +premium gain. That violates realize-on-close
    (AGENTS "Option P&L Attribution" #3: "$0 contribution while open if
    the contract has NEVER been snapshotted — defer the credit to
    close_date") AND disagrees with int_option_contract_daily_pnl, whose
    lifetime spine for a never-snapshotted open contract is just
    [open_date] with mtm=0 → chart terminal $0.

    "Never snapshotted" == no matching row in stg_current, which is
    exactly the ``else`` branch of int_option_contracts.total_pnl
    (cur.trade_symbol IS NULL). The real mark-to-market lands once the
    daily holdings sync writes a stg_current snapshot row.

    Fails (returns rows) if any open, never-snapshotted contract has a
    non-zero total_pnl.
#}

with open_contracts as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        status,
        net_cash_flow,
        total_pnl
    from {{ ref('int_option_contracts') }}
    where status = 'Open'
),

snapshotted as (
    select distinct
        tenant_id,
        account,
        user_id,
        trade_symbol
    from {{ ref('stg_current') }}
    where instrument_type in ('Call', 'Put')
      and trim(coalesce(trade_symbol, '')) != ''
)

select
    oc.tenant_id,
    oc.account,
    oc.user_id,
    oc.trade_symbol,
    oc.net_cash_flow,
    oc.total_pnl
from open_contracts oc
left join snapshotted s
    on oc.account = s.account
    and (oc.user_id is not distinct from s.user_id)
    and (oc.tenant_id is not distinct from s.tenant_id)
    and oc.trade_symbol = s.trade_symbol
where s.trade_symbol is null           -- never snapshotted
  and abs(oc.total_pnl) > 0.01
