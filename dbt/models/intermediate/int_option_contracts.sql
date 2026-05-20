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
        --
        -- ``close_date`` = when the position effectively ENDED, not the
        -- last fill date. This matters for OTM expiries: Schwab does not
        -- ship an explicit ``option_expired`` event, so for a sold call
        -- that just expires worthless the only fill in stg_history is
        -- the original STO. Pre-fix close_date = STO date, which both
        --   (a) made days_in_trade = 0 for every OTM expiry, and
        --   (b) made every realize-on-close P&L attribution land on the
        --       OPEN date instead of the close date — defeating the
        --       whole purpose of int_option_contract_daily_pnl.
        -- Precedence:
        --   1. last fill date among closing actions (BTC / STC /
        --      explicit option_expired / option_assigned / option_exercised)
        --   2. option_expiry, if past current_date()
        --   3. NULL (still open with no terminal event)
        min(o.trade_date)  as open_date,
        -- ``close_date`` precedence:
        --   1. last fill date among closing actions
        --   2. option_expiry, if past current_date()
        --   3. NULL (still open with no terminal event)
        -- Defensive guard for broker-error fills that record an STO
        -- AFTER the option's own expiry (real example May 2026: PLTR
        -- 5/8 expiry, sync registered an STO on 5/12). The calendar-
        -- expiry branch would yield close_date < open_date —
        -- nonsensical. Coerce to open_date so the contract is treated
        -- as same-day-closed (zero days_in_trade) and its P&L still
        -- realizes — better than NULL (which would defer the realized
        -- credit forever). Wrapped in a CASE so we keep NULL for
        -- genuinely-open contracts (no closing action AND not past
        -- expiry yet); ``greatest(NULL, anything)`` is NULL in BQ,
        -- which would silently mark every open contract as
        -- close-date=open_date — a much worse failure mode.
        case
            when coalesce(
                    max(case
                            when o.action in (
                                'option_buy_to_close', 'option_sell_to_close',
                                'option_expired', 'option_assigned', 'option_exercised'
                            )
                            then o.trade_date
                        end),
                    case
                        when max(o.option_expiry) < current_date()
                        then max(o.option_expiry)
                    end
                ) is null then null
            else greatest(
                coalesce(
                    max(case
                            when o.action in (
                                'option_buy_to_close', 'option_sell_to_close',
                                'option_expired', 'option_assigned', 'option_exercised'
                            )
                            then o.trade_date
                        end),
                    case
                        when max(o.option_expiry) < current_date()
                        then max(o.option_expiry)
                    end
                ),
                min(o.trade_date)
            )
        end as close_date,

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
        -- Snapshot-only contracts have no fills in stg_history, so
        -- they have no closing-action date. But the same calendar-
        -- truth rule still applies: if option_expiry is in the past,
        -- the position is realized regardless of what the broker's
        -- stale snapshot says. Without this branch, snapshot-only
        -- past-expiry contracts (e.g. broker-error STO recorded for
        -- an expired contract) would have status='Closed' but
        -- close_date=NULL, and int_option_contract_daily_pnl would
        -- silently drop their realized P&L. Mirrors the close_date
        -- precedence in contract_summary.
        case
            when c.option_expiry < current_date()
            then greatest(
                c.option_expiry,
                coalesce(c.snapshot_date, current_date())
            )
            else cast(null as date)
        end as close_date,

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
),

-- OTM-at-expiry inference (worthless-expiry auto-close).
--
-- The existing calendar-truth rule (``option_expiry < current_date()``
-- below) realizes a contract the FIRST DAY AFTER expiry — but on
-- expiry day itself it still reads as Open until BigQuery's
-- ``current_date()`` advances past expiry. That gap matters: a trader
-- whose Friday-expiry short call closes OTM at 4:00 PM ET sees the
-- broker snapshot's stale cost-to-close (e.g. -$183) all evening and
-- weekend long — even though the contract is unambiguously worthless
-- and the premium is fully realized. The Monday broker sync ships an
-- explicit ``option_expired`` action and the existing close_type
-- precedence then fires, but we shouldn't have to wait two calendar
-- days for the page to be honest about something Friday's closing
-- print already determined.
--
-- The fix: when the underlying's daily close on the expiry date is
-- strictly OTM relative to the strike, infer that the contract
-- expired worthless and realize at ``net_cash_flow`` immediately.
-- Strict OTM only (close < strike for calls; close > strike for
-- puts) — at-the-money or ITM expiries are left as Open because the
-- broker still has discretion (auto-exercise threshold) and the
-- realized number would differ between assignment vs. exercise.
-- For ITM, wait for the broker's explicit action.
--
-- The yfinance daily close for the expiry day lives in
-- ``stg_daily_prices`` and lands via the price loader after market
-- close (Render cron at ~21:30 UTC weekdays). The CI dbt build then
-- picks it up. Anyone hitting the page over the weekend sees the
-- realized credit; the Monday broker sync still ships
-- ``option_expired`` and the existing close_type precedence
-- harmlessly takes over with the same ``net_cash_flow``.
--
-- Why this is safe to do BEFORE the broker confirms:
--   net_cash_flow is the sum of explicit fills only. For an OTM
--   expiry there is no closing fill (the option just dies), so
--   net_cash_flow = premium received (or paid). That's exactly
--   what the broker's ``option_expired`` event with amount=$0 will
--   crystallize too. No double-counting, no risk of disagreement.
expiry_close_lookup as (
    select
        account,
        user_id,
        symbol     as underlying_symbol,
        date       as expiry_date,
        close_price
    from {{ ref('stg_daily_prices') }}
    where date        is not null
      and close_price is not null
),

otm_at_expiry as (
    select
        c.account,
        c.user_id,
        c.trade_symbol,
        case
            -- Strict OTM call: underlying closed BELOW the strike.
            when c.option_expiry = current_date()
                 and c.option_strike is not null
                 and c.option_type   = 'C'
                 and e.close_price is not null
                 and e.close_price < c.option_strike
            then true
            -- Strict OTM put: underlying closed ABOVE the strike.
            when c.option_expiry = current_date()
                 and c.option_strike is not null
                 and c.option_type   = 'P'
                 and e.close_price is not null
                 and e.close_price > c.option_strike
            then true
            else false
        end as inferred_otm_today
    from all_contracts c
    left join expiry_close_lookup e
        on c.account            = e.account
        and (c.user_id is not distinct from e.user_id)
        and c.underlying_symbol = e.underlying_symbol
        and c.option_expiry     = e.expiry_date
)

select
    c.account,
    c.user_id,
    -- Stage 2 broker_account_id passthrough (see docs/BROKER_ACCOUNT_ID_MIGRATION.md).
    dba.broker_account_id,
    c.trade_symbol,
    c.underlying_symbol,
    c.option_expiry,
    c.option_strike,
    c.option_type,
    c.direction,
    c.open_date,

    -- Effective close_date: original (history closing-action OR past-
    -- expiry calendar branch) is the strongest signal. When neither
    -- has fired yet but the same-day OTM inference is true, fall
    -- back to the option_expiry date so downstream
    -- (int_option_contract_daily_pnl realized_close branch) emits
    -- the realized credit on the right calendar day.
    coalesce(
        c.close_date,
        case when iotm.inferred_otm_today then c.option_expiry end
    ) as close_date,

    c.contracts_sold_to_open,
    c.contracts_bought_to_open,
    c.contracts_closed,
    c.premium_received,
    c.premium_paid,
    c.cost_to_close,
    c.proceeds_from_close,
    c.net_cash_flow,
    c.total_fees,

    -- close_type: preserve broker-confirmed values when present.
    -- ``ExpiredOTM`` is reserved for the inferred-from-yfinance branch
    -- so admin debugging can distinguish "we deduced this" from
    -- "broker confirmed this." When the Monday sync ships an explicit
    -- ``option_expired`` event, ``c.close_type`` becomes 'Expired'
    -- and overrides this value in the next build (same realized
    -- credit either way — net_cash_flow doesn't change).
    case
        when c.close_type is not null then c.close_type
        when iotm.inferred_otm_today  then 'ExpiredOTM'
        else c.close_type
    end as close_type,

    c.num_trades,

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
    --
    -- The ``inferred_otm_today`` branch handles expiry day itself:
    -- when the underlying closed strictly OTM, realize before the
    -- broker confirms on Monday. See ``otm_at_expiry`` CTE header.
    case
        when c.close_type is not null         then 'Closed'
        when c.option_expiry < current_date() then 'Closed'
        when iotm.inferred_otm_today          then 'Closed'
        when cur.trade_symbol is not null     then 'Open'
        else 'Open'
    end as status,

    -- Current market data for open contracts
    coalesce(cur.market_value, 0)    as current_market_value,
    coalesce(cur.unrealized_pnl, 0)  as current_unrealized_pnl,

    -- Total P&L for open vs closed contracts.
    --
    -- Calendar truth wins over snapshot presence: a contract whose
    -- ``status`` says Closed (because ``close_type`` is set OR
    -- ``option_expiry`` is past OR ``inferred_otm_today`` fired) MUST
    -- realize via ``net_cash_flow`` regardless of whether Schwab's
    -- stale snapshot still carries it. Pre-fix the case branch keyed
    -- off ``cur.trade_symbol is not null`` and silently used
    -- ``cur.unrealized_pnl`` for expired-but-still-snapshotted
    -- contracts (real example May 2026: NVDA 6/5 230C closed via
    -- assignment 4/24, snapshot stale at mv=-1375 → total_pnl
    -- rendered as -$546 instead of the true realized +$838). This
    -- made the Strategy Breakdown / chart / positions_summary
    -- disagree with int_option_contract_daily_pnl (which correctly
    -- emits realized at close_date).
    --
    -- CLOSED: ``c.net_cash_flow`` is the only truth (sum of all
    -- fills from stg_history). The matching status logic at the
    -- top of this SELECT already reserved 'Closed' for these.
    --
    -- OPEN:   trust the broker snapshot's full-precision
    -- ``unrealized_pnl`` directly. The naive
    -- ``net_cash_flow + market_value`` combines rounded $0.01 fill
    -- prices from stg_history with full-precision snapshot
    -- market_value, accumulating ~$1-2 of rounding drift per
    -- contract (Sara/BE 290C 5/8 STO at fill price $15.01305 →
    -- seed amount $3,004 vs snapshot cost_basis $3,002.61 → $1.39
    -- drift, trips the page-level reconciliation invariant). Falls
    -- back to ``net_cash_flow`` when the snapshot is missing
    -- (pre-snapshot warm-up window for newly opened contracts).
    case
        when c.close_type is not null         then c.net_cash_flow
        when c.option_expiry < current_date() then c.net_cash_flow
        when iotm.inferred_otm_today          then c.net_cash_flow
        when cur.trade_symbol is not null
             and cur.unrealized_pnl is not null
        then cur.unrealized_pnl
        when cur.trade_symbol is not null
        then c.net_cash_flow + coalesce(cur.market_value, 0)
        else c.net_cash_flow
    end as total_pnl,

    -- Duration
    --
    -- For closed contracts (close_date set) use the close_date even if
    -- the broker's stale snapshot still carries the contract — calendar
    -- truth wins, same precedence as the ``status`` column above. The
    -- coalesce mirrors the effective close_date above so the duration
    -- of an inferred-OTM contract reflects open → expiry, not
    -- open → today.
    date_diff(
        coalesce(
            c.close_date,
            case when iotm.inferred_otm_today then c.option_expiry end,
            current_date()
        ),
        c.open_date,
        day
    ) as days_in_trade

from all_contracts c
left join otm_at_expiry iotm
    on c.account = iotm.account
    and (c.user_id is not distinct from iotm.user_id)
    and c.trade_symbol = iotm.trade_symbol
left join {{ ref('stg_current') }} cur
    on c.account = cur.account
    and (c.user_id is not distinct from cur.user_id)
    and c.trade_symbol = cur.trade_symbol
    and cur.instrument_type in ('Call', 'Put')
left join {{ ref('dim_broker_accounts') }} dba
    on c.account = dba.account_name
    and (c.user_id is not distinct from dba.user_id)
