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
            -- An opening fill exists. Majority of opening quantity wins.
            -- sell-to-open >= buy-to-open is NOT safe when both are 0: a
            -- closing-only contract (buy-to-close / assignment with no STO
            -- in the broker window) would be labeled Sold, then Naked Call
            -- or Cash-Secured Put, and the close proceeds would book as a
            -- 100% win. KLAC 800C, NVO, AMZN, DJT 30P.
            when sum(case when action = 'option_sell_to_open' then quantity else 0 end)
               + sum(case when action = 'option_buy_to_open'  then quantity else 0 end)
               > 1e-9
            then case
                when sum(case when action = 'option_sell_to_open' then quantity else 0 end)
                  >= sum(case when action = 'option_buy_to_open'  then quantity else 0 end)
                then 'Sold'
                else 'Bought'
            end
            -- No opening fill. Infer the side the close is undoing.
            -- Buying to close or an assignment closes a short. Selling to
            -- close closes a long. A lone option_exercised stays Unknown:
            -- Schwab uses that action for both a long exercise and a short
            -- assignment, and the split-link below closes the pre-split
            -- contract from the adjusted symbol.
            when sum(case when action = 'option_buy_to_close' then quantity else 0 end)
               + sum(case when action = 'option_assigned' then quantity else 0 end)
               > sum(case when action = 'option_sell_to_close' then quantity else 0 end)
            then 'Sold'
            when sum(case when action = 'option_sell_to_close' then quantity else 0 end)
               > sum(case when action = 'option_buy_to_close' then quantity else 0 end)
               + sum(case when action = 'option_assigned' then quantity else 0 end)
            then 'Bought'
            when sum(case when action = 'option_buy_to_close' then quantity else 0 end)
               + sum(case when action = 'option_assigned' then quantity else 0 end)
               > 1e-9
            then 'Sold'
            else 'Unknown'
        end as direction,
        -- Closing activity with no opening fill: the contract was opened
        -- before the broker history window. Realized P&L is unknown
        -- (cost basis is not in the file) and must not be booked.
        (
            sum(case when action = 'option_sell_to_open' then quantity else 0 end)
          + sum(case when action = 'option_buy_to_open'  then quantity else 0 end)
          <= 1e-9
          and sum(case
                when action in (
                    'option_buy_to_close', 'option_sell_to_close',
                    'option_assigned', 'option_exercised', 'option_expired'
                ) then quantity else 0 end) > 1e-9
        ) as opened_before_history
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
        d.opened_before_history,

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
    group by o.tenant_id, o.account, o.user_id, o.trade_symbol, o.underlying_symbol, d.direction, d.opened_before_history
),

-- Open options that appear in stg_current (e.g. Schwab snapshot) but have no
-- matching rows in trade history yet — otherwise positions_summary stays empty.
-- The existence check uses the same OSI-core identity as the live join below;
-- exact-text checking would create a second snapshot-only row whenever history
-- and holdings differ only in root padding.
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
        false as opened_before_history,

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

        -- The live snapshot quantity is the position. Leaving both
        -- open-counts at 0 rendered "QTY 0" on an estimated expiry
        -- (BE Oct 2 $297.50C) even though the broker still held the contracts.
        case
            when coalesce(c.quantity, 0) < 0 then abs(c.quantity)
            else 0.0
        end as contracts_sold_to_open,
        case
            when coalesce(c.quantity, 0) > 0 then abs(c.quantity)
            else 0.0
        end as contracts_bought_to_open,
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
            and (
                x.trade_symbol = c.trade_symbol
                or (
                    regexp_extract(
                        upper(trim(coalesce(x.trade_symbol, ''))),
                        r'(\d{6}[CP]\d{8})'
                    ) is not null
                    and regexp_extract(
                        upper(trim(coalesce(x.trade_symbol, ''))),
                        r'(\d{6}[CP]\d{8})'
                    ) = regexp_extract(
                        upper(trim(coalesce(c.trade_symbol, ''))),
                        r'(\d{6}[CP]\d{8})'
                    )
                    and upper(trim(coalesce(x.underlying_symbol, '')))
                        = upper(trim(coalesce(c.underlying_symbol, '')))
                )
                -- History "BE 10/02/2026 297.50 C" and the snapshot OCC
                -- "BE   261002C00297500" are one contract. Exact text and
                -- the OSI core both miss the long form, which minted a
                -- second row: qty 0, open date = today, no premium.
                or (
                    c.option_expiry is not null
                    and c.option_strike is not null
                    and nullif(trim(coalesce(c.option_type, '')), '') is not null
                    and x.option_expiry = c.option_expiry
                    and abs(coalesce(x.option_strike, 0) - c.option_strike) < 0.001
                    and upper(trim(coalesce(x.option_type, '')))
                        = upper(trim(c.option_type))
                    and upper(trim(coalesce(x.underlying_symbol, '')))
                        = upper(trim(coalesce(c.underlying_symbol, '')))
                )
            )
      )
),

all_contracts as (
    select * from contract_summary
    union all
    select * from snapshot_only_options
),

-- A split changes the OCC symbol. SCHD's covered 85C became 3x 28.33C
-- after the 3:1 split, and the broker shipped only an Exercised row on
-- the adjusted symbol. That row has no opening fill (opened_before_history)
-- while the original 85C never receives a close, so it stays open and the
-- adjusted symbol books as a qty-0 loss. When strike and quantity scale
-- by the split factor between the two dates, copy the adjusted close onto
-- the original contract. cumulative_split_factor(d) is the product of
-- split_ratio for splits strictly after d, so a pre-split open of 85
-- divided by 3 lands on 28.33.
split_links as (
    select
        orig.tenant_id,
        orig.account,
        orig.user_id,
        orig.trade_symbol as orig_trade_symbol,
        adj.close_date as mapped_close_date,
        adj.close_type as mapped_close_type
    from all_contracts adj
    join all_contracts orig
        on (adj.tenant_id is not distinct from orig.tenant_id)
        and adj.account = orig.account
        and (adj.user_id is not distinct from orig.user_id)
        and upper(trim(coalesce(adj.underlying_symbol, '')))
            = upper(trim(coalesce(orig.underlying_symbol, '')))
        and adj.option_type = orig.option_type
        and adj.trade_symbol != orig.trade_symbol
        and coalesce(adj.opened_before_history, false)
        and not coalesce(orig.opened_before_history, false)
        and coalesce(orig.contracts_sold_to_open, 0)
            + coalesce(orig.contracts_bought_to_open, 0) > 1e-9
        and orig.close_type is null
        and adj.option_strike is not null
        and orig.option_strike is not null
    left join {{ ref('int_split_factors') }} fo
        on fo.symbol = orig.underlying_symbol
        and fo.trade_date = orig.open_date
    left join {{ ref('int_split_factors') }} fa
        on fa.symbol = adj.underlying_symbol
        and fa.trade_date = coalesce(adj.close_date, adj.open_date)
    where coalesce(fo.cumulative_split_factor, 1)
            > coalesce(fa.cumulative_split_factor, 1) + 1e-9
      and abs(
            orig.option_strike
            / (coalesce(fo.cumulative_split_factor, 1)
               / coalesce(fa.cumulative_split_factor, 1))
            - adj.option_strike
          ) < 0.05
      and abs(
            (coalesce(orig.contracts_sold_to_open, 0)
             + coalesce(orig.contracts_bought_to_open, 0))
            * (coalesce(fo.cumulative_split_factor, 1)
               / coalesce(fa.cumulative_split_factor, 1))
            - greatest(
                coalesce(adj.contracts_closed, 0),
                coalesce(adj.contracts_sold_to_open, 0)
                    + coalesce(adj.contracts_bought_to_open, 0)
              )
          ) < 0.05
    qualify row_number() over (
        partition by orig.tenant_id, orig.account, orig.user_id, orig.trade_symbol
        order by abs(
            orig.option_strike
            / (coalesce(fo.cumulative_split_factor, 1)
               / coalesce(fa.cumulative_split_factor, 1))
            - adj.option_strike
        )
    ) = 1
),

-- Expiry settlement from the official close (app/expiry_settlement.py).
--
-- Do not wait for the broker's expired / as-of / cash-settlement line.
-- Once the expiry session is over and stg_daily_prices has the
-- underlying's close on the expiry date, a contract with no closing
-- activity realizes immediately:
--
--   * OTM or ATM — settlement cash $0, so realized P&L is the opening
--     fills (short keeps the premium, long loses the debit).
--   * ITM cash-settled index (SPX/SPXW/XSP/NDX/RUT, plus NDXP/RUTW and
--     VIX/DJX/OEX/XEO/RVX so those are not treated as a share
--     assignment) — add intrinsic, strike vs close × 100 × opened
--     contracts. A short pays it; a long receives it.
--   * ITM equity — option P&L stays the fill cash. The shares are the
--     equity line, same as an option_assigned row. No synthetic share fill.
--
-- The row is close_type 'Settled at expiry (est.)' until a broker close
-- exists (contracts_closed > 0 or an action close_type). That close's
-- cash is already in net_cash_flow, so the estimate adds nothing.
--
-- On the expiry date itself the session is over at 16:00 ET, or 16:15 ET
-- for those index roots (a daily bar can print before the index close).
-- After that New York date, the session is over. No official close stays
-- Open through that session so the opening credit is not booked as a
-- worthless win. Equity still missing a close on the next weekday
-- (Friday → Monday) falls back to the old calendar close: expired at $0,
-- labeled 'Settled at expiry (est.)', close_date = option_expiry. A later
-- price or broker line replaces that estimate; the date stays the expiry
-- so the realized dollar does not move to the day the fallback fired.
-- Cash-settled index roots never take that $0 fallback. With no official
-- print they are close_type 'Settlement pending', realized and total $0,
-- so an ITM SPXW spread is not a worthless win when ^GSPC was not loaded.
--
-- Price: exact underlying, else the parent (SPXW→SPX, NDXP→NDX, RUTW→RUT)
-- only when the exact symbol has no row — a join that can match both
-- would fan out. One close per (symbol, date); stg_daily_prices has no
-- tenant grain. Joining on (account, user_id) would duplicate contracts
-- that share a display label.
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
        coalesce(exact.close_price, parent.close_price) as expiry_close,
        (
            c.option_strike is not null
            and coalesce(exact.close_price, parent.close_price) is not null
            and (
                (c.option_type = 'C'
                    and coalesce(exact.close_price, parent.close_price) < c.option_strike)
                or (c.option_type = 'P'
                    and coalesce(exact.close_price, parent.close_price) > c.option_strike)
            )
        ) as strictly_otm,
        -- Same-day worthless expiry, only after the bell. Keeps a partial
        -- close from staying "open" once the remainder is strictly OTM.
        (
            c.option_expiry = current_date('America/New_York')
            and c.option_strike is not null
            and coalesce(exact.close_price, parent.close_price) is not null
            and (
                (c.option_type = 'C'
                    and coalesce(exact.close_price, parent.close_price) < c.option_strike)
                or (c.option_type = 'P'
                    and coalesce(exact.close_price, parent.close_price) > c.option_strike)
            )
            and time(current_datetime('America/New_York')) >= (
                case
                    when upper(trim(coalesce(c.underlying_symbol, ''))) in (
                        'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                        'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
                    )
                    then time '16:15:00'
                    else time '16:00:00'
                end
            )
        ) as inferred_otm_today,
        (
            c.option_expiry is not null
            and (
                c.option_expiry < current_date('America/New_York')
                or (
                    c.option_expiry = current_date('America/New_York')
                    and time(current_datetime('America/New_York')) >= (
                        case
                            when upper(trim(coalesce(c.underlying_symbol, ''))) in (
                                'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                                'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
                            )
                            then time '16:15:00'
                            else time '16:00:00'
                        end
                    )
                )
            )
        ) as expiry_session_over,
        -- Next weekday after expiry (Friday → Monday). Once that New York
        -- date has started and the official close is still missing, equity
        -- takes the $0 calendar fallback. Cash index roots stay pending.
        (
            c.option_expiry is not null
            and current_date('America/New_York') >= case extract(dayofweek from c.option_expiry)
                when 6 then date_add(c.option_expiry, interval 3 day)
                when 7 then date_add(c.option_expiry, interval 2 day)
                else date_add(c.option_expiry, interval 1 day)
            end
        ) as unpriced_fallback_due
    from all_contracts c
    left join expiry_close_lookup exact
        on upper(trim(c.underlying_symbol)) = upper(trim(exact.underlying_symbol))
        and c.option_expiry = exact.expiry_date
    left join expiry_close_lookup parent
        on exact.close_price is null
        and c.option_expiry = parent.expiry_date
        and upper(trim(parent.underlying_symbol)) = case upper(trim(c.underlying_symbol))
            when 'SPXW' then 'SPX'
            when 'NDXP' then 'NDX'
            when 'RUTW' then 'RUT'
        end
),

-- Join the live snapshot + OTM-at-expiry inference once, then derive the
-- status / P&L flags in a single place so the partial-close logic stays
-- readable (this used to be one giant final SELECT).
--
-- Contract symbols are joined by their OSI core as well as exact text.
-- SnapTrade can vary the root padding between history and holdings
-- (``FN    260814C00120000`` vs ``FN 260814C00120000``). Treating that
-- formatting difference as "missing from the live snapshot" falsely
-- realizes every such unexpired contract opened before today.
joined as (
    select
        c.*,
        iotm.inferred_otm_today,
        iotm.expiry_close,
        iotm.strictly_otm,
        iotm.expiry_session_over,
        iotm.unpriced_fallback_due,
        cur.trade_symbol   as cur_trade_symbol,
        cur.market_value   as cur_market_value,
        cur.unrealized_pnl as cur_unrealized_pnl,
        sl.mapped_close_date,
        sl.mapped_close_type
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
        and (
            c.trade_symbol = cur.trade_symbol
            or (
                regexp_extract(
                    upper(trim(coalesce(c.trade_symbol, ''))),
                    r'(\d{6}[CP]\d{8})'
                ) is not null
                and regexp_extract(
                    upper(trim(coalesce(c.trade_symbol, ''))),
                    r'(\d{6}[CP]\d{8})'
                ) = regexp_extract(
                    upper(trim(coalesce(cur.trade_symbol, ''))),
                    r'(\d{6}[CP]\d{8})'
                )
                and upper(trim(coalesce(c.underlying_symbol, '')))
                    = upper(trim(coalesce(cur.underlying_symbol, '')))
            )
        )
        and cur.instrument_type in ('Call', 'Put')
    left join split_links sl
        on (c.tenant_id is not distinct from sl.tenant_id)
        and c.account = sl.account
        and (c.user_id is not distinct from sl.user_id)
        and c.trade_symbol = sl.orig_trade_symbol
),

flagged as (
    select
        * except (close_date, close_type),
        coalesce(close_date, mapped_close_date) as close_date,
        coalesce(close_type, mapped_close_type) as close_type,
        -- Contracts still open = everything opened minus everything closed
        -- (BTC/STC/expired/assigned/exercised). > 0 means a live remainder.
        (coalesce(contracts_sold_to_open, 0)
         + coalesce(contracts_bought_to_open, 0)
         - coalesce(contracts_closed, 0)) as remaining_open_qty,
        -- Effective realization date for the CLOSED portion (unchanged
        -- precedence from the old final SELECT): history closing-action date
        -- (capped at expiry for late-booked settlements) → past-expiry
        -- calendar → OTM-at-expiry inference → snapshot-drop close.
        -- NULL only when the contract is still Open (live snapshot, or a
        -- same-day open that beat the snapshot).
        --
        -- Snapshot-drop close (Aug 2026 CHECK 12): status already flips
        -- to Closed when the broker drops a pre-today contract
        -- (`open_date < current_date` + no snapshot). Leaving
        -- close_date NULL made int_option_exit_analysis skip the row
        -- (`close_date is not null`) so mart_coaching_signals.total_closed
        -- lagged classification Closed counts (run 33301622998).
        -- Date it to today — the moment we accepted snapshot truth —
        -- until a real settlement fill / expiry date arrives.
        coalesce(
            case when cur_trade_symbol is null then _activity_flat_close_date end,
            close_date,
            case when inferred_otm_today then option_expiry end,
            case
                when cur_trade_symbol is null
                 and open_date < current_date('America/New_York')
                then current_date('America/New_York')
            end
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
         and not coalesce(inferred_otm_today, false)) as is_partial_open,
        -- No closing activity, session over, official close in hand.
        -- The label says it is still an estimate. Equity with no close
        -- uses the same estimate once the next weekday has started
        -- ($0 settlement, because expiry_close is null). Cash index
        -- roots are excluded from that branch — see settlement_pending.
        (
            coalesce(close_type, '') = ''
            and _activity_flat_close_date is null
            and coalesce(contracts_closed, 0) < 1e-6
            and option_expiry is not null
            and coalesce(expiry_session_over, false)
            and (
                (
                    option_strike is not null
                    and expiry_close is not null
                )
                or (
                    expiry_close is null
                    and coalesce(unpriced_fallback_due, false)
                    and upper(trim(coalesce(underlying_symbol, ''))) not in (
                        'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                        'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
                    )
                )
            )
        ) as expiry_settled_est,
        -- Cash index, session over, next weekday started, still no
        -- official close. Do not book the opening credit. Status stays
        -- Closed so the legs table can show the label; realized is $0.
        (
            coalesce(close_type, '') = ''
            and _activity_flat_close_date is null
            and coalesce(contracts_closed, 0) < 1e-6
            and option_expiry is not null
            and coalesce(expiry_session_over, false)
            and expiry_close is null
            and coalesce(unpriced_fallback_due, false)
            and upper(trim(coalesce(underlying_symbol, ''))) in (
                'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
            )
        ) as settlement_pending,
        -- Session over, still no close, and the next weekday has not
        -- started. Stay open. Booking net_cash_flow here is the
        -- worthless-ITM bug.
        (
            coalesce(close_type, '') = ''
            and _activity_flat_close_date is null
            and coalesce(contracts_closed, 0) < 1e-6
            and option_expiry is not null
            and coalesce(expiry_session_over, false)
            and expiry_close is null
            and not coalesce(unpriced_fallback_due, false)
        ) as awaiting_expiry_close,
        -- Intrinsic for an ITM cash-settled index. Equity and OTM/ATM
        -- contribute 0; a later broker close contributes 0 because
        -- expiry_settled_est is false once contracts_closed > 0.
        case
            when coalesce(close_type, '') != ''
              or _activity_flat_close_date is not null
              or coalesce(contracts_closed, 0) >= 1e-6
              or not coalesce(expiry_session_over, false)
              or expiry_close is null
              or option_strike is null
              or upper(trim(coalesce(underlying_symbol, ''))) not in (
                    'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                    'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
                )
            then 0.0
            else round(
                (case when direction = 'Sold' then -1.0 else 1.0 end)
                * greatest(
                    case
                        when option_type = 'C' then expiry_close - option_strike
                        when option_type = 'P' then option_strike - expiry_close
                        else 0.0
                    end,
                    0.0
                )
                * 100.0
                * coalesce(
                    case
                        when direction = 'Sold' then contracts_sold_to_open
                        else contracts_bought_to_open
                    end,
                    0.0
                ),
                2
            )
        end as est_settlement_cash
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
    coalesce(opened_before_history, false) as opened_before_history,
    open_date,

    -- Output close_date: NULL for a partial close (the position is still
    -- open, so days_in_trade and the int_option_contract_daily_pnl lifetime
    -- spine treat it as ongoing and keep marking the remainder to market).
    -- The realized credit for the CLOSED portion attributes on
    -- realized_close_date instead. For fully-closed contracts this is the
    -- same effective close_date as before.
    case
        when settlement_pending then cast(null as date)
        when expiry_settled_est then option_expiry
        when awaiting_expiry_close or is_partial_open then cast(null as date)
        else eff_close_date
    end as close_date,

    -- Date to attribute the realized P&L of the closed portion. Equals the
    -- effective close date for fully-closed contracts and the last closing
    -- fill date for partial closes; NULL when nothing has closed. Read by
    -- int_option_contract_daily_pnl's realized branch.
    case
        when settlement_pending then cast(null as date)
        when expiry_settled_est then option_expiry
        when awaiting_expiry_close then cast(null as date)
        else eff_close_date
    end as realized_close_date,

    contracts_sold_to_open,
    contracts_bought_to_open,
    contracts_closed,
    premium_received,
    premium_paid,
    cost_to_close,
    proceeds_from_close,
    net_cash_flow,
    total_fees,

    -- close_type: a broker action wins. The estimate label is only for
    -- a session that is over, with an official close, and no closing
    -- fill yet. The next build that sees option_expired / as-of /
    -- assigned replaces it (expiry_settled_est is then false, and
    -- net_cash_flow already includes the broker cash).
    case
        when settlement_pending then 'Settlement pending'
        when expiry_settled_est then 'Settled at expiry (est.)'
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
    -- The estimate realizes on the expiry date once the bell has rung
    -- and the official close is in. Equity with no close stays Open
    -- (awaiting) until the next weekday, then expires at $0 under the
    -- same estimate. A cash index with no close is Settlement pending:
    -- Closed so the row stays on the legs table, with realized $0.
    case
        when settlement_pending                    then 'Closed'
        when expiry_settled_est                    then 'Closed'
        when close_type is not null and not is_partial_open then 'Closed'
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null     then 'Closed'
        when awaiting_expiry_close            then 'Open'
        when inferred_otm_today               then 'Closed'
        when cur_trade_symbol is not null      then 'Open'
        -- Opened before today and the live snapshot no longer carries
        -- the contract: the broker does not hold it. Same-day opens can
        -- beat the snapshot by minutes, so those stay Open.
        when open_date < current_date('America/New_York') then 'Closed'
        else 'Open'
    end as status,

    -- Current market data for open contracts
    case
        when coalesce(opened_before_history, false) then 0.0
        else coalesce(cur_market_value, 0)
    end as current_market_value,
    case
        when coalesce(opened_before_history, false) then 0.0
        else coalesce(cur_unrealized_pnl, 0)
    end as current_unrealized_pnl,

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
        -- Cost basis is not in the file. Booking the close proceeds
        -- alone is a phantom profit (or a phantom loss on an exercised
        -- split-adjusted symbol).
        when coalesce(opened_before_history, false) then 0.0
        -- Unpriced cash index: not the opening credit.
        when settlement_pending then 0.0
        -- Opening fills plus index intrinsic. Equity ITM adds $0
        -- (assignment lives on the stock line). OTM adds $0.
        when expiry_settled_est
            then net_cash_flow + est_settlement_cash
        when close_type is not null and not is_partial_open then net_cash_flow
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null     then net_cash_flow
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
    -- closed contract it equals net_cash_flow plus any estimated index
    -- intrinsic (== total_pnl). Broker closes add no second cash.
    case
        when coalesce(opened_before_history, false) then 0.0
        when settlement_pending then 0.0
        when expiry_settled_est
            then net_cash_flow + est_settlement_cash
        when close_type is not null and not is_partial_open then net_cash_flow
        when _activity_flat_close_date is not null
             and cur_trade_symbol is null     then net_cash_flow
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
            case
                when settlement_pending then null
                when expiry_settled_est then option_expiry
                when awaiting_expiry_close or is_partial_open then null
                else eff_close_date
            end,
            current_date()
        ),
        open_date,
        day
    ) as days_in_trade

from flagged2
