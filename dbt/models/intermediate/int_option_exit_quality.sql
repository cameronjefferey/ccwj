{{
    config(
        materialized='table'
    )
}}
/*
    Execution grading per resolved option contract — the decision, judged
    (table, not view: this is read at REQUEST TIME by the Trader Profile
    and Position Detail pages — as a view the rolls/prices/exit-analysis
    joins recomputed per read and cost ~4.5s, the slowest query in the
    page's parallel wave)
    against everything the warehouse knows about what happened next.

    This is the analytical core of the "execution review" surfaces
    (Trader Profile card, Position review sentences, day-row verdicts).
    Other platforms show the trade log; we grade the trade against the
    market's subsequent record. Two independent evidence layers:

    1. EXPIRY COUNTERFACTUAL (needs no snapshot history — computable for
       the entire trade record from stg_daily_prices). For every contract
       closed EARLY via an explicit BTC/STC, compare the actual closing
       cash against what settling at expiry would have paid:

           intrinsic_at_expiry = max(0, S_expiry − K)  for calls
                                 max(0, K − S_expiry)  for puts
           early_close_vs_expiry_delta
               = closing_cash − settlement_cash
           where settlement_cash = −intrinsic_total (short)
                                   +intrinsic_total (long)

       delta < 0 → closing early cost money vs holding to expiry
       (canonical case: paid $180 to buy back a call that went on to
       expire worthless). delta > 0 → closing early beat holding
       (bought back before the strike went $820 in the money). The
       delta is NULL — deliberately ungraded — for assignments,
       exercises, expiries, contracts whose expiry hasn't passed, and
       contracts with no price row at expiry. Presentation stays
       neutral evidence ("N of M buybacks were on contracts that went
       on to expire worthless"), never advice: holding to expiry also
       carried risk the trader chose not to take.

    2. MARKS RECORD (strengthens as snapshot history accumulates —
       int_option_marks_daily since 2026-08-04). Peak / giveback /
       density come from int_option_exit_analysis; consumers gate on
       data_reliable so these claims switch on per-contract once
       coverage is real. This is the "after X days of data" gate the
       product promises: expiry counterfactuals are live immediately,
       marks-based grading earns its way in.

    ROLL NECESSITY rides on layer 1: a rolled-away contract IS the
    closed contract, so its own intrinsic at its ORIGINAL expiry answers
    "did the strike you rolled away from ever get run over?" —
    expired_worthless = the roll's buyback leg was insurance that wasn't
    needed at expiry; intrinsic > 0 = the roll genuinely sidestepped
    that much settlement value.

    GRAIN: one row per (tenant_id, account, user_id, trade_symbol),
    status = 'Closed' contracts only. Pinned by
    dbt/tests/option_exit_quality_one_row_per_contract.sql.
*/

with closed_contracts as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol as symbol,
        trade_symbol,
        option_type,
        option_strike,
        option_expiry,
        direction,
        open_date,
        close_date,
        close_type,
        days_in_trade as days_held,
        contracts_sold_to_open,
        contracts_bought_to_open,
        premium_received,
        premium_paid,
        cost_to_close,
        proceeds_from_close,
        net_cash_flow as realized_pnl,
        -- Opened quantity scales the intrinsic counterfactual. Zero for
        -- snapshot-only contracts (no fills in history) — those are
        -- excluded from grading below.
        coalesce(contracts_sold_to_open, 0)
            + coalesce(contracts_bought_to_open, 0) as contracts
    from {{ ref('int_option_contracts') }}
    where status = 'Closed'
      and open_date is not null
      and close_date is not null
),

-- Universal market data: one close per (symbol, expiry date). Same dedup
-- rationale as expiry_close_lookup in int_option_contracts — joining
-- stg_daily_prices on its stamped (account, user_id) grain fans out
-- across tenants sharing a display label.
expiry_close_lookup as (
    select
        symbol,
        date as expiry_date,
        any_value(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    where date is not null
      and close_price is not null
    group by 1, 2
),

-- Was this contract the CLOSED leg of a roll? int_option_rolls is
-- already deduped to the single best match per closed leg
-- (match_rank = 1); the group-by here is belt-and-braces so a future
-- relaxation of that filter can never fan this model out.
rolls as (
    select
        tenant_id,
        account,
        user_id,
        old_trade_symbol as trade_symbol,
        any_value(new_trade_symbol) as roll_new_trade_symbol,
        any_value(new_strike)       as roll_new_strike,
        any_value(new_expiry)       as roll_new_expiry,
        any_value(net_roll_credit)  as net_roll_credit
    from {{ ref('int_option_rolls') }}
    group by 1, 2, 3, 4
),

-- Marks-based exit metrics (peak / giveback / density). LEFT JOIN: the
-- expiry counterfactual must not depend on snapshot coverage.
exit_analysis as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        peak_unrealized_pnl,
        peak_date,
        snapshot_count,
        snapshot_density,
        data_reliable,
        pnl_given_back,
        giveback_pct,
        days_held_past_peak
    from {{ ref('int_option_exit_analysis') }}
),

graded as (
    select
        c.*,
        e.close_price as underlying_close_at_expiry,

        -- Per-share intrinsic value at expiry. option_type is 'C'/'P'
        -- in this layer (see otm_at_expiry in int_option_contracts);
        -- upper(left(...)) tolerates 'Call'/'Put' defensively.
        case
            when e.close_price is null or c.option_strike is null then null
            when upper(left(c.option_type, 1)) = 'C'
                then greatest(e.close_price - c.option_strike, 0)
            when upper(left(c.option_type, 1)) = 'P'
                then greatest(c.option_strike - e.close_price, 0)
        end as intrinsic_at_expiry
    from closed_contracts c
    left join expiry_close_lookup e
        on c.symbol = e.symbol
        and c.option_expiry = e.expiry_date
)

select
    g.account,
    g.user_id,
    -- v2 tenant_id carried natively from staging through the contract grain.
    g.tenant_id,
    g.symbol,
    g.trade_symbol,
    g.option_type,
    g.option_strike,
    g.option_expiry,
    g.direction,
    g.open_date,
    g.close_date,
    g.close_type,
    g.days_held,
    date_diff(g.option_expiry, g.close_date, day) as dte_at_close,
    g.contracts,
    g.premium_received,
    g.premium_paid,
    g.cost_to_close,
    g.proceeds_from_close,
    g.realized_pnl,

    g.underlying_close_at_expiry,
    g.intrinsic_at_expiry,
    round(coalesce(g.intrinsic_at_expiry, 0) * 100 * g.contracts, 2)
        as expiry_settlement_value,

    -- TRUE when the contract would have (or did) expire worthless.
    -- NULL when the expiry outcome is unknowable (no price at expiry).
    case
        when g.intrinsic_at_expiry is null then null
        else g.intrinsic_at_expiry <= 0
    end as expired_worthless,

    -- Gradeable early close: an explicit BTC/STC before expiry with a
    -- known expiry outcome and real opened quantity.
    (g.close_type = 'Closed'
     and g.close_date < g.option_expiry
     and g.intrinsic_at_expiry is not null
     and g.contracts > 0) as gradeable_early_close,

    -- What closing early gained (+) or gave up (−) vs settling at
    -- expiry. closing_cash = cost_to_close + proceeds_from_close (BTC
    -- amounts are negative, STC positive); settlement_cash is the
    -- signed intrinsic. See model header for the derivation.
    case
        when g.close_type = 'Closed'
         and g.close_date < g.option_expiry
         and g.intrinsic_at_expiry is not null
         and g.contracts > 0
        then round(
            (coalesce(g.cost_to_close, 0) + coalesce(g.proceeds_from_close, 0))
            - (case
                   when g.direction = 'Bought'
                   then g.intrinsic_at_expiry * 100 * g.contracts
                   else -(g.intrinsic_at_expiry * 100 * g.contracts)
               end),
            2)
    end as early_close_vs_expiry_delta,

    -- Roll linkage (this contract = the closed leg).
    (r.trade_symbol is not null) as was_rolled,
    r.roll_new_strike,
    r.roll_new_expiry,
    r.net_roll_credit,

    -- Marks record (NULL-safe; gate on data_reliable downstream).
    x.peak_unrealized_pnl,
    x.peak_date,
    coalesce(x.snapshot_count, 0) as snapshot_count,
    coalesce(x.snapshot_density, 0) as snapshot_density,
    coalesce(x.data_reliable, false) as data_reliable,
    coalesce(x.pnl_given_back, 0) as pnl_given_back,
    coalesce(x.giveback_pct, 0) as giveback_pct,
    x.days_held_past_peak

from graded g
left join rolls r
    on g.account = r.account
    and (g.user_id is not distinct from r.user_id)
    and (g.tenant_id is not distinct from r.tenant_id)
    and g.trade_symbol = r.trade_symbol
left join exit_analysis x
    on g.account = x.account
    and (g.user_id is not distinct from x.user_id)
    and (g.tenant_id is not distinct from x.tenant_id)
    and g.trade_symbol = x.trade_symbol
