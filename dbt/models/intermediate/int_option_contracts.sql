/*
    Option contract lifecycle.

    Groups every trade on the same option contract (account + trade_symbol)
    into a single row with:
      - direction (Sold / Bought)
      - premiums collected / paid
      - closing info (Expired, Assigned, Closed, Exercised)
      - total P&L including unrealised component for open contracts
*/

with option_trades as (
    select
        account,
        user_id,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        trade_date,
        action,
        quantity,
        amount,
        fees
    from {{ ref('stg_history') }}
    where instrument_type in ('Call', 'Put')
),

-- Predominant direction per contract (for signing expired / assigned quantities).
-- Keyed on (account, user_id, trade_symbol) so two users with the same
-- account label and the same option contract symbol don't get their
-- direction collapsed together.
direction_lookup as (
    select
        account,
        user_id,
        trade_symbol,
        sum(case when action = 'option_sell_to_open' then quantity else 0 end) as total_sto_qty,
        sum(case when action = 'option_buy_to_open'  then quantity else 0 end) as total_bto_qty,
        case
            when sum(case when action = 'option_sell_to_open' then quantity else 0 end)
              >= sum(case when action = 'option_buy_to_open'  then quantity else 0 end)
            then 'Sold'
            else 'Bought'
        end as direction
    from option_trades
    group by 1, 2, 3
),

contract_summary as (
    select
        o.account,
        o.user_id,
        o.trade_symbol,
        o.underlying_symbol,
        max(o.option_expiry)  as option_expiry,
        max(o.option_strike)  as option_strike,
        max(o.option_type)    as option_type,
        d.direction,

        -- Dates
        min(o.trade_date)  as open_date,
        max(o.trade_date)  as close_date,

        -- Quantities
        sum(case when o.action = 'option_sell_to_open' then o.quantity else 0 end) as contracts_sold_to_open,
        sum(case when o.action = 'option_buy_to_open'  then o.quantity else 0 end) as contracts_bought_to_open,
        sum(case when o.action in (
            'option_buy_to_close', 'option_sell_to_close',
            'option_expired', 'option_assigned', 'option_exercised'
        ) then o.quantity else 0 end) as contracts_closed,

        -- Cash flows
        sum(case when o.action = 'option_sell_to_open'  then o.amount else 0 end) as premium_received,
        sum(case when o.action = 'option_buy_to_open'   then o.amount else 0 end) as premium_paid,
        sum(case when o.action = 'option_buy_to_close'  then o.amount else 0 end) as cost_to_close,
        sum(case when o.action = 'option_sell_to_close' then o.amount else 0 end) as proceeds_from_close,
        sum(o.amount) as net_cash_flow,
        sum(o.fees)   as total_fees,

        -- How the contract was closed (highest-priority terminal event wins)
        max(case
            when o.action = 'option_assigned'  then 'Assigned'
            when o.action = 'option_exercised' then 'Exercised'
            when o.action = 'option_expired'   then 'Expired'
            when o.action in ('option_buy_to_close', 'option_sell_to_close') then 'Closed'
        end) as close_type,

        count(*) as num_trades

    from option_trades o
    join direction_lookup d
        on o.account = d.account
        and (o.user_id is not distinct from d.user_id)
        and o.trade_symbol = d.trade_symbol
    group by o.account, o.user_id, o.trade_symbol, o.underlying_symbol, d.direction
),

-- Open options that appear in stg_current (e.g. Schwab snapshot) but have no
-- matching rows in trade history yet — otherwise positions_summary stays empty.
snapshot_only_options as (
    select
        c.account,
        c.user_id,
        c.trade_symbol,
        c.underlying_symbol,
        c.option_expiry,
        c.option_strike,
        c.option_type,
        case when coalesce(c.quantity, 0) < 0 then 'Sold' else 'Bought' end as direction,

        coalesce(c.snapshot_date, current_date()) as open_date,
        cast(null as date) as close_date,

        0.0 as contracts_sold_to_open,
        0.0 as contracts_bought_to_open,
        0.0 as contracts_closed,

        0.0 as premium_received,
        0.0 as premium_paid,
        0.0 as cost_to_close,
        0.0 as proceeds_from_close,

        safe_subtract(
            coalesce(c.unrealized_pnl, safe_subtract(c.market_value, c.cost_basis)),
            coalesce(c.market_value, 0)
        ) as net_cash_flow,

        0.0 as total_fees,
        cast(null as string) as close_type,
        0 as num_trades

    from {{ ref('stg_current') }} c
    where c.instrument_type in ('Call', 'Put')
      and trim(coalesce(c.trade_symbol, '')) != ''
      and not exists (
          select 1
          from contract_summary x
          where x.account = c.account
            and (x.user_id is not distinct from c.user_id)
            and x.trade_symbol = c.trade_symbol
      )
),

all_contracts as (
    select * from contract_summary
    union all
    select * from snapshot_only_options
)

select
    c.*,

    -- Status
    --
    -- Order matters. Past-expiry MUST be checked BEFORE
    -- "snapshot-implies-open" because Schwab's snapshot lags actual
    -- expiry processing by 1-2 trading days. Real example (May 2026):
    -- BE 290C 5/8 expired Friday OTM, but Schwab's Monday snapshot still
    -- carried the contract with quantity=-2 and market_value=-$2 (a
    -- bookkeeping artifact, not a real cost-to-close — the contract no
    -- longer trades). Pre-fix the position page rendered the leg as
    -- "Open" until the next snapshot dropped the row a day or two later.
    -- The trader's view: from the moment the bell rings on expiry
    -- Friday, the position is realized. Calendar wins over snapshot.
    --
    -- close_type from history (Assigned / Exercised / Expired explicit
    -- event) still wins above the calendar fallback because it's the
    -- highest-precision signal we have.
    case
        when c.close_type is not null         then 'Closed'
        when c.option_expiry < current_date() then 'Closed'
        when cur.trade_symbol is not null     then 'Open'
        else 'Open'
    end as status,

    -- Current market data for open contracts
    coalesce(cur.market_value, 0)    as current_market_value,
    coalesce(cur.unrealized_pnl, 0)  as current_unrealized_pnl,

    -- Total P&L for open vs closed contracts:
    --
    -- CLOSED: c.net_cash_flow is the only truth (sum of fills from
    -- stg_history). No snapshot to mix in.
    --
    -- OPEN:   trust the broker snapshot's full-precision unrealized_pnl
    -- directly. The naive `net_cash_flow + market_value` combines rounded
    -- $0.01 fill prices from stg_history with full-precision snapshot
    -- market_value, accumulating ~$1-2 of rounding drift per contract.
    -- Worked example: Sara/BE 290C 5/8 sold-to-open at fill price
    -- $15.01305. Schwab's CSV rounds the price column to $15.02 → seed
    -- amount = 200 × $15.02 = $3,004 (history). Schwab's cost_basis
    -- preserves $3,002.61 (snapshot). True open unrealized = $3,000.61
    -- (snapshot's market_value + cost_basis with sign flip). The naive
    -- formula gave $3,002 ($3,004 history − $2 mark), which propagates a
    -- $1.39 drift into positions_summary.total_pnl and trips the
    -- page-level reconciliation invariant card on otherwise-healthy
    -- positions. Snapshot wins for open marks.
    --
    -- Falls back to the old formula when snapshot is missing (covers the
    -- pre-snapshot warm-up window for newly opened contracts).
    case
        when cur.trade_symbol is not null
             and cur.unrealized_pnl is not null
        then cur.unrealized_pnl
        when cur.trade_symbol is not null
        then c.net_cash_flow + coalesce(cur.market_value, 0)
        else c.net_cash_flow
    end as total_pnl,

    -- Duration
    date_diff(
        case
            when cur.trade_symbol is not null then current_date()
            else coalesce(c.close_date, current_date())
        end,
        c.open_date,
        day
    ) as days_in_trade

from all_contracts c
left join {{ ref('stg_current') }} cur
    on c.account = cur.account
    and (c.user_id is not distinct from cur.user_id)
    and c.trade_symbol = cur.trade_symbol
    and cur.instrument_type in ('Call', 'Put')
