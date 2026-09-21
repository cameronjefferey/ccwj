{{
    config(
        materialized='table'
    )
}}
/*
    Exit timing analysis per closed option contract.

    Table (was view, Aug 2026): read at REQUEST TIME by insights and
    weekly_review, and its upstream int_option_pnl_series now unfolds the
    SCD2 marks history — recomputing that per page read is wasted work.

    For each closed contract with snapshot history, computes:
      - peak_unrealized_pnl: the best P&L observed during the hold
      - peak_date: when the peak occurred
      - pnl_given_back: how much profit was surrendered by not closing at peak
      - giveback_pct: pnl_given_back as a % of peak
      - held_past_peak_days: how many days you held after the optimal exit
      - snapshot_density: ratio of snapshots to hold days (1.0 = every day).
        Same-day round-trips (days_in_trade = 0) use 1 as the hold length so
        intra-day marks count as full coverage of that session.
      - data_reliable: true when density >= 40% and at least 2 snapshots.
        A 1-day hold with open+close marks is a complete daily curve; the
        old floor of 3 snapshots silently dropped overnight round-trips.
      - in_marks_window: close_date is on/after the first captured option
        mark (SCD2 history began 2026-08-04). Contracts outside this window
        can never be data_reliable — coverage % must use this denominator,
        not lifetime closed, or a long trade history reads as "3% coverage."

    Contracts with sparse snapshots are flagged so downstream consumers
    can exclude or de-weight them — the peak might have been missed.
*/

with contracts as (
    select
        oc.tenant_id,
        oc.account,
        oc.user_id,
        oc.trade_symbol,
        oc.underlying_symbol,
        oc.direction,
        oc.option_type,
        oc.option_expiry,
        oc.option_strike,
        oc.open_date,
        oc.close_date,
        oc.close_type,
        oc.days_in_trade,
        oc.total_pnl  as actual_pnl,
        oc.premium_received,
        oc.premium_paid,
        coalesce(s.strategy, 'Other Option') as strategy,
        tk.dte_at_open,
        tk.dte_bucket
    from {{ ref('int_option_contracts') }} oc
    left join {{ ref('int_strategy_classification') }} s
        on oc.account = s.account
        and (oc.user_id is not distinct from s.user_id)
        and (oc.tenant_id is not distinct from s.tenant_id)
        and oc.trade_symbol = s.trade_symbol
        and s.trade_group_type = 'option_contract'
    left join {{ ref('int_option_trade_kinds') }} tk
        on oc.account = tk.account
        and (oc.user_id is not distinct from tk.user_id)
        and (oc.tenant_id is not distinct from tk.tenant_id)
        and oc.trade_symbol = tk.trade_symbol
    where oc.status = 'Closed'
      and oc.close_date is not null
),

peak_stats as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        max(unrealized_pnl)  as peak_unrealized_pnl,
        min(unrealized_pnl)  as trough_unrealized_pnl,
        count(*)             as snapshot_count,
        min(snapshot_date)   as first_snapshot,
        max(snapshot_date)   as last_snapshot
    from {{ ref('int_option_pnl_series') }}
    group by 1, 2, 3, 4
),

peak_dates as (
    select
        ps.tenant_id,
        ps.account,
        ps.user_id,
        ps.trade_symbol,
        min(pnl.snapshot_date) as peak_date
    from peak_stats ps
    join {{ ref('int_option_pnl_series') }} pnl
        on ps.account = pnl.account
        and (ps.user_id is not distinct from pnl.user_id)
        and (ps.tenant_id is not distinct from pnl.tenant_id)
        and ps.trade_symbol = pnl.trade_symbol
        and pnl.unrealized_pnl = ps.peak_unrealized_pnl
    group by 1, 2, 3, 4
)

select
    c.account,
    c.user_id,
    -- v2 tenant_id carried natively from staging through the contract grain.
    c.tenant_id,
    c.trade_symbol,
    c.underlying_symbol,
    c.strategy,
    c.direction,
    c.option_type,
    c.option_expiry,
    c.option_strike,
    c.open_date,
    c.close_date,
    c.close_type,
    c.days_in_trade,
    c.dte_at_open,
    c.dte_bucket,
    c.actual_pnl,
    c.premium_received,
    c.premium_paid,

    coalesce(ps.peak_unrealized_pnl, 0)  as peak_unrealized_pnl,
    pd.peak_date,
    coalesce(ps.snapshot_count, 0)        as snapshot_count,

    -- Snapshot density: what fraction of hold days have a snapshot.
    -- greatest(days_in_trade, 1) so same-day (0 DTE) expiries with
    -- intra-day marks are density 4.0, not 0.
    case
        when ps.snapshot_count is not null
        then round(ps.snapshot_count / greatest(c.days_in_trade, 1), 2)
        else 0
    end as snapshot_density,

    -- Reliable = dense enough curve to trust the peak. Two snapshots
    -- cover a 1-day hold; a 30-day hold with 2 snaps is density 0.07
    -- and stays unreliable. Same-day holds with >=2 intra-day marks pass.
    case
        when ps.snapshot_count >= 2
             and (ps.snapshot_count / greatest(c.days_in_trade, 1)) >= 0.4
        then true
        else false
    end as data_reliable,

    -- Eligible for marks-based scoring (coverage denominator).
    (c.close_date >= mw.marks_start) as in_marks_window,

    date_diff(pd.peak_date, c.open_date, day) as days_open_to_peak,
    date_diff(c.close_date, pd.peak_date, day) as days_held_past_peak,

    case
        when ps.peak_unrealized_pnl is not null
             and ps.peak_unrealized_pnl > c.actual_pnl
        then round(ps.peak_unrealized_pnl - c.actual_pnl, 2)
        else 0
    end as pnl_given_back,

    case
        when ps.peak_unrealized_pnl is not null
             and ps.peak_unrealized_pnl > 0
             and ps.peak_unrealized_pnl > c.actual_pnl
        then round(
            (ps.peak_unrealized_pnl - c.actual_pnl)
            / ps.peak_unrealized_pnl * 100, 1
        )
        else 0
    end as giveback_pct,

    case
        when c.direction = 'Sold' and abs(c.premium_received) > 0
        then round(c.actual_pnl / abs(c.premium_received) * 100, 1)
        else null
    end as pct_of_premium_captured,

    case
        when pd.peak_date is not null
             and date_diff(c.close_date, pd.peak_date, day) <= 2
        then true
        else false
    end as optimal_exit

from contracts c
cross join (
    -- First captured option mark; older contracts can never be reliable.
    select min(date) as marks_start from {{ ref('int_option_marks_daily') }}
) mw
left join peak_stats ps
    on c.account = ps.account
    and (c.user_id is not distinct from ps.user_id)
    and (c.tenant_id is not distinct from ps.tenant_id)
    and c.trade_symbol = ps.trade_symbol
left join peak_dates pd
    on c.account = pd.account
    and (c.user_id is not distinct from pd.user_id)
    and (c.tenant_id is not distinct from pd.tenant_id)
    and c.trade_symbol = pd.trade_symbol
