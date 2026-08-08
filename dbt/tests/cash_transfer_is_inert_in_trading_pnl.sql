{#
    Deposit / withdrawal containment (2026-08).

    External cash movements (the trader adding/removing their own money) are
    ingested as ``stg_history.action = 'cash_transfer'`` so the /wealth +
    /accounts "exclude deposits & withdrawals" toggle can net them out. They
    are NOT trading P&L and MUST stay inert in every trade / session /
    dividend / option model — all of which filter to Equity / Call / Put /
    dividend. The one catch-all consumer (``mart_daily_pnl.other_amount``,
    which feeds the account + position P&L charts) has an explicit
    ``action <> 'cash_transfer'`` guard.

    The load-bearing invariant is that a cash_transfer row is classified as
    ``instrument_type = 'Cash Event'`` (never Equity/Call/Put/Dividend). If a
    future edit adds a cash-movement mapping to the ``action`` CASE in
    stg_history.sql but forgets the matching ``instrument_type`` CASE branch,
    the deposit would be typed ``Equity`` and silently leak into equity
    sessions, FIFO cost basis, and the P&L charts as a phantom trade.

    Fails (returns rows) if any cash_transfer row is not a Cash Event.
    See mart_wealth_daily (net_deposit_today) + AGENTS.md "Wealth".
#}

select
    account,
    user_id,
    tenant_id,
    trade_date,
    action,
    instrument_type,
    amount
from {{ ref('stg_history') }}
where action = 'cash_transfer'
  and instrument_type != 'Cash Event'
