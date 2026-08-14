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
        tenant_id,
        account,
        user_id,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        trade_date,
        action,
        description,
        quantity,
        amount,
        fees
    from {{ ref('stg_history') }}
    where instrument_type in ('Call', 'Put')
),

-- Predominant direction per contract (for signing expired / assigned quantities).
-- Keyed on (tenant_id, account, user_id, trade_symbol) so two physical
-- accounts sharing a display label (e.g. multiple "Schwab Account"s) and
-- the same option contract symbol don't get their direction collapsed.
direction_lookup as (
    select
        tenant_id,
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
    group by 1, 2, 3, 4
),

contract_summary as (
    select
        o.tenant_id,
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
        --   1. last fill date among closing actions — BUT settlement events
        --      (option_expired / option_assigned / option_exercised) are
        --      CAPPED at option_expiry. Schwab books these 1-2 trading days
        --      LATE: a Friday 6/26 OTM expiry posts as ``option_expired`` on
        --      Monday 6/29. The position really ended on the expiry date, so
        --      crediting the broker's late booking date would (a) inflate
        --      days_in_trade and (b) push the trade into the WRONG ISO week —
        --      making an option that expired last week reappear in "Trades
        --      this week" days later. ``least(trade_date, option_expiry)``
        --      keeps the real date for genuine EARLY assignment/exercise
        --      (trade_date < expiry) while pulling late-booked expiries back
        --      to the expiry date. Active closes (BTC / STC) are booked
        --      same-day and keep their trade_date. This makes close_date
        --      STABLE across the Monday sync (matches the otm_at_expiry
        --      inference, which already dates worthless expiries to expiry).
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
                            -- Settlement events booked late → cap at expiry
                            -- (least() preserves genuine early exercise).
                            when o.action in (
                                'option_expired', 'option_assigned', 'option_exercised'
                            )
                            then least(o.trade_date, coalesce(o.option_expiry, o.trade_date))
                            -- Active closes are booked same-day → trust them.
                            when o.action in (
                                'option_buy_to_close', 'option_sell_to_close'
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
                            -- Settlement events booked late → cap at expiry
                            -- (least() preserves genuine early exercise).
                            when o.action in (
                                'option_expired', 'option_assigned', 'option_exercised'
                            )
                            then least(o.trade_date, coalesce(o.option_expiry, o.trade_date))
                            -- Active closes are booked same-day → trust them.
                            when o.action in (
                                'option_buy_to_close', 'option_sell_to_close'
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

        count(*) as num_trades,

        -- Offset round-trip with no explicit close action. Alpaca
        -- activities omit open/close metadata (both legs land as "to
        -- Open"); Schwab descriptions via SnapTrade are often just
        -- "CALL FABRINET $X EXP …" with the same default-to-open
        -- failure (ORCL 2026-06 phantom Naked Call). When buy qty
        -- exactly offsets sell qty and the live snapshot no longer
        -- carries the contract, the position ended — don't wait for
        -- expiry. The final SELECT still requires cur_trade_symbol
        -- IS NULL so a live remainder stays Open.
        case
            when countif(o.action in (
                    'option_buy_to_close', 'option_sell_to_close',
                    'option_expired', 'option_assigned', 'option_exercised'
                 )) = 0
             and sum(case
                    when o.action in ('option_buy_to_open', 'option_buy_to_close')
                    then o.quantity else 0
                 end) > 0
             and sum(case
                    when o.action in ('option_sell_to_open', 'option_sell_to_close')
                    then o.quantity else 0
                 end) > 0
             and abs(
                    sum(case
                        when o.action in ('option_buy_to_open', 'option_buy_to_close')
                        then o.quantity else 0
                    end)
                    - sum(case
                        when o.action in ('option_sell_to_open', 'option_sell_to_close')
                        then o.quantity else 0
                    end)
                 ) < 1e-9
            then max(o.trade_date)
        end as _activity_flat_close_date

    from option_trades o
    join direction_lookup d
        on o.account = d.account
        and (o.user_id is not distinct from d.user_id)
        and (o.tenant_id is not distinct from d.tenant_id)
        and o.trade_symbol = d.trade_symbol
    group by o.tenant_id, o.account, o.user_id, o.trade_symbol, o.underlying_symbol, d.direction
),

-- Open options that appear in stg_current (e.g. Schwab snapshot) but have no
-- matching rows in trade history yet — otherwise positions_summary stays empty.
snapshot_only_options as (
    select
        c.tenant_id,
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
        0 as num_trades,
        cast(null as date) as _activity_flat_close_date

    from {{ ref('stg_current') }} c
    where c.instrument_type in ('Call', 'Put')
      and trim(coalesce(c.trade_symbol, '')) != ''
      and not exists (
          select 1
          from contract_summary x
          where x.account = c.account
            and (x.user_id is not distinct from c.user_id)
            and (x.tenant_id is not distinct from c.tenant_id)
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
-- The expiry-day close is universal market data (same for every tenant),
-- and stg_daily_prices carries no tenant_id. Dedup to one row per
-- (underlying_symbol, expiry_date) and join on symbol+date only — joining
-- on the (account, user_id) label would fan out across physical accounts
-- that share a display label and duplicate every option contract.
expiry_close_lookup as (
    select
        symbol     as underlying_symbol,
        date       as expiry_date,
        any_value(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    where date        is not null
      and close_price is not null
    group by 1, 2
),

otm_at_expiry as (
    select
        c.tenant_id,
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
        on c.underlying_symbol = e.underlying_symbol
        and c.option_expiry     = e.expiry_date
),

-- Join the live snapshot + OTM-at-expiry inference once, then derive the
-- status / P&L flags in a single place so the partial-close logic stays
-- readable (this used to be one giant final SELECT).
joined as (
    select
        c.*,
        iotm.inferred_otm_today,
        cur.trade_symbol   as cur_trade_symbol,
        cur.market_value   as cur_market_value,
        cur.unrealized_pnl as cur_unrealized_pnl
    from all_contracts c
    left join otm_at_expiry iotm
        on c.account = iotm.account
        and (c.user_id is not distinct from iotm.user_id)
        and (c.tenant_id is not distinct from iotm.tenant_id)
        and c.trade_symbol = iotm.trade_symbol
    left join {{ ref('stg_current') }} cur
        on c.account = cur.account
        and (c.user_id is not distinct from cur.user_id)
        and (c.tenant_id is not distinct from cur.tenant_id)
        and c.trade_symbol = cur.trade_symbol
        and cur.instrument_type in ('Call', 'Put')
),

flagged as (
    select
        *,
        -- Contracts still open = everything opened minus everything closed
        -- (BTC/STC/expired/assigned/exercised). > 0 means a live remainder.
        (coalesce(contracts_sold_to_open, 0)
         + coalesce(contracts_bought_to_open, 0)
         - coalesce(contracts_closed, 0)) as remaining_open_qty,
        -- Effective realization date for the CLOSED portion (unchanged
        -- precedence from the old final SELECT): history closing-action date
        -- (capped at expiry for late-booked settlements) → past-expiry
        -- calendar → OTM-at-expiry inference. NULL only when nothing has
        -- closed and the contract has not expired.
        coalesce(
            case when cur_trade_symbol is null then _activity_flat_close_date end,
            close_date,
            case when inferred_otm_today then option_expiry end
        ) as eff_close_date
    from joined
),

flagged2 as (
    select
        *,
        -- PARTIAL CLOSE: some contracts were closed (BTC/STC/etc.) but the
        -- broker snapshot still carries a live remainder AND the contract
        -- has not expired / gone worthless. This is a genuinely OPEN
        -- position that must NOT be flipped to 'Closed' just because a
        -- closing fill exists. Real case CRWV 260814C00074000 (Aug 2026):
        -- bought 25, sold 10, 15 still held — pre-fix rendered as fully
        -- Closed with total_pnl = net_cash_flow (-$523.62) instead of the
        -- true +$10,736 realized on the 10 sold plus +$20,465 unrealized on
        -- the 15 held. remaining_open_qty > 0 is the from-history signal;
        -- cur_trade_symbol is not null confirms the broker still holds it.
        (cur_trade_symbol is not null
         and remaining_open_qty > 1e-6
         and coalesce(contracts_closed, 0) > 1e-6
         and coalesce(option_expiry >= current_date(), true)
         and not coalesce(inferred_otm_today, false)) as is_partial_open
    from flagged
)

select
    account,
    user_id,
    -- v2 tenant_id carried natively from staging through the contract grain.
    tenant_id,
    trade_symbol,
    underlying_symbol,
    option_expiry,
    option_strike,
    option_type,
    direction,
    open_date,

    -- Output close_date: NULL for a partial close (the position is still
    -- open, so days_in_trade and the int_option_contract_daily_pnl lifetime
    -- spine treat it as ongoing and keep marking the remainder to market).
    -- The realized credit for the CLOSED portion attributes on
    -- realized_close_date instead. For fully-closed contracts this is the
    -- same effective close_date as before.
    case when is_partial_open then null else eff_close_date end as close_date,

    -- Date to attribute the realized P&L of the closed portion. Equals the
    -- effective close date for fully-closed contracts and the last closing
    -- fill date for partial closes; NULL when nothing has closed. Read by
    -- int_option_contract_daily_pnl's realized branch.
    eff_close_date as realized_close_date,

    contracts_sold_to_open,
    contracts_bought_to_open,
    contracts_closed,
    premium_received,
    premium_paid,
    cost_to_close,
    proceeds_from_close,
    net_cash_flow,
    total_fees,

    -- close_type: preserve broker-confirmed values when present.
    -- ``ExpiredOTM`` is reserved for the inferred-from-yfinance branch
    -- so admin debugging can distinguish "we deduced this" from
    -- "broker confirmed this." When the Monday sync ships an explicit
    -- ``option_expired`` event, ``close_type`` becomes 'Expired'
    -- and overrides this value in the next build (same realized
    -- credit either way — net_cash_flow doesn't change).
    case
        when close_type is not null then close_type
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null then 'Closed'
        when inferred_otm_today  then 'ExpiredOTM'
        else close_type
    end as close_type,

    num_trades,

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
    -- highest-precision signal we have — EXCEPT when only PART of the
    -- opened quantity was closed (is_partial_open). A partial close keeps
    -- the contract Open; the realized portion is credited separately.
    --
    -- The ``inferred_otm_today`` branch handles expiry day itself:
    -- when the underlying closed strictly OTM, realize before the
    -- broker confirms on Monday. See ``otm_at_expiry`` CTE header.
    case
        when close_type is not null and not is_partial_open then 'Closed'
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null     then 'Closed'
        when option_expiry < current_date()   then 'Closed'
        when inferred_otm_today               then 'Closed'
        when cur_trade_symbol is not null      then 'Open'
        -- Opened before today and the live snapshot no longer carries
        -- the contract: the broker does not hold it. Same-day opens can
        -- beat the snapshot by minutes, so those stay Open.
        when open_date < current_date('America/New_York') then 'Closed'
        else 'Open'
    end as status,

    -- Current market data for open contracts
    coalesce(cur_market_value, 0)    as current_market_value,
    coalesce(cur_unrealized_pnl, 0)  as current_unrealized_pnl,

    -- Total P&L = realized (closed portion) + unrealized (open portion).
    --
    -- Calendar truth wins over snapshot presence: a fully-closed contract
    -- (close_type set / past-expiry / OTM-inferred, and no live remainder)
    -- realizes via ``net_cash_flow`` regardless of whether Schwab's stale
    -- snapshot still carries it (real example May 2026: NVDA 6/5 230C closed
    -- via assignment 4/24, snapshot stale at mv=-1375 → would render -$546
    -- instead of the true realized +$838).
    --
    -- FULLY CLOSED: ``net_cash_flow`` is the only truth (sum of all fills).
    --
    -- PARTIAL CLOSE (still open): ``net_cash_flow + current_market_value``.
    -- The identity ``net_cash_flow + market_value == realized_on_closed +
    -- unrealized_on_open`` holds for both longs and shorts (market_value
    -- carries the sign). Using market_value — not unrealized_pnl — is what
    -- folds the closing proceeds back in. CRWV: -523.62 + 31,725 = +31,201.
    --
    -- FULLY OPEN: trust the snapshot's full-precision ``unrealized_pnl``
    -- (the naive net_cash_flow + market_value accumulates ~$1-2 of rounded-
    -- fill drift and trips the page reconciliation invariant).
    --
    -- FULLY OPEN + NEVER SNAPSHOTTED: contribute $0, not net_cash_flow —
    -- defer the credit to close (AGENTS "Option P&L Attribution" #3).
    case
        when close_type is not null and not is_partial_open then net_cash_flow
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null     then net_cash_flow
        when option_expiry < current_date()   then net_cash_flow
        when inferred_otm_today               then net_cash_flow
        when is_partial_open
            then net_cash_flow + coalesce(cur_market_value, 0)
        when cur_trade_symbol is not null
             and cur_unrealized_pnl is not null then cur_unrealized_pnl
        when cur_trade_symbol is not null
            then net_cash_flow + coalesce(cur_market_value, 0)
        else 0.0
    end as total_pnl,

    -- Realized P&L on the CLOSED portion only (0 while fully open). For a
    -- partial close this is total − unrealized = (net_cash_flow +
    -- market_value) − unrealized_pnl = net_cash_flow + the sign-adjusted
    -- remaining cost basis. Downstream reads this so the realized wedge of
    -- a partial close lands consistently in int_option_contract_daily_pnl
    -- (realized branch), int_position_legs (closed-options P&L) and
    -- int_strategy_classification (realized/unrealized split). For a fully-
    -- closed contract it equals net_cash_flow (== total_pnl), so those
    -- consumers are byte-for-byte unchanged for the non-partial case.
    case
        when close_type is not null and not is_partial_open then net_cash_flow
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null     then net_cash_flow
        when option_expiry < current_date()   then net_cash_flow
        when inferred_otm_today               then net_cash_flow
        when is_partial_open
            then net_cash_flow
                 + coalesce(cur_market_value, 0)
                 - coalesce(cur_unrealized_pnl, 0)
        else 0.0
    end as realized_pnl,

    -- Duration. A partial close uses the output close_date (NULL) → today,
    -- so days_in_trade reflects the still-open position; fully-closed keeps
    -- the effective close date; open contracts run open → today.
    date_diff(
        coalesce(
            case when is_partial_open then null else eff_close_date end,
            current_date()
        ),
        open_date,
        day
    ) as days_in_trade

from flagged2
