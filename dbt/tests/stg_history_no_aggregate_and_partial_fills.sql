/*
    No equity trade group may retain BOTH an orders-feed AGGREGATE row and
    its activities-feed PARTIAL-FILL rows for the same
    (tenant_id, trade_date, underlying_symbol, action).

    Why this exists (2026-07-16). Alpaca (via SnapTrade) reports the SAME
    order at two granularities:
      * recent_orders -> ONE aggregate row at filled_quantity, Description =
        company name ("Kaiser Aluminum Corporation").
      * activities    -> N per-execution partial fills, Description like
        "KALU BUY FILL at 162.12" / "... PARTIAL_FILL at 162.14".
    The shared cross-source dedup in app/upload._dedup_history_rows keys on
    (Date, Action, Symbol, Quantity, Price); the aggregate's quantity equals
    the SUM of the partials, not any single partial, so the dedup can't
    collapse them and BOTH survive — doubling the position's cost basis and
    producing a phantom unrealized loss (cameronbot Alpaca paper: total
    return rendered -$77k vs the real ~-$15k on a $100k paper account).

    The fix (dbt/models/staging/brokers/stg_broker_alpaca_history.sql) drops
    the activities partials when an aggregate exists. This test is the
    warehouse backstop: if a future broker/regression lets both survive, the
    build fails here instead of a user finding it on the position page.

    Non-Alpaca brokers don't emit the "... FILL at ..." activities wording,
    so their equity groups have zero partial-fill rows and never trip this.

    See ~/.cursor/skills/broker-sync-safety/SKILL.md "Bugs we've shipped"
    2026-07-16.
*/

with flagged as (
    select
        tenant_id,
        trade_date,
        underlying_symbol,
        action,
        regexp_contains(description, r'(?i) (PARTIAL_)?FILL at ') as is_partial_fill
    from {{ ref('stg_history') }}
    where tenant_id is not null
      and instrument_type = 'Equity'
      and action in ('equity_buy', 'equity_sell')
)

select
    tenant_id,
    trade_date,
    underlying_symbol,
    action,
    countif(is_partial_fill) as n_partial_fill,
    countif(not is_partial_fill) as n_aggregate
from flagged
group by tenant_id, trade_date, underlying_symbol, action
having countif(is_partial_fill) > 0
   and countif(not is_partial_fill) > 0
