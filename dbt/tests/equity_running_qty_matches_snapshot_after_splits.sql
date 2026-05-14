{#
    Invariant: for every symbol that has at least one split during the
    user's trade window, the trade-derived open quantity (sum of
    split-adjusted max_quantity_held from int_equity_sessions) must
    match the snapshot quantity within an integer-multiple of the
    cumulative split factor.

    What this catches:

      - A new consumer of stg_history.quantity that forgets to multiply
        by int_split_factors.cumulative_split_factor (the May 2026
        XLU regression, when 1700 raw buy + 1500 raw sell + 2:1 split
        = trade-derived 200 vs snapshot 1900 = $66K phantom realized
        loss). Without the fix, snapshot/trade ratio is exactly 2.0
        (or 3.0, 4.0, …) — the canonical split-factor signature.

      - A regression in int_split_factors itself (cumulative LN/EXP
        product breaks for a new split shape — e.g. a forward split
        followed by a reverse on the same symbol).

      - A mis-applied factor where signed_quantity uses raw qty but
        running_qty uses adjusted, or vice versa.

    What this does NOT catch (deliberately scoped to splits only):

      - Partial-snapshot mismatches on non-split symbols (transferred-
        in lots, pre-history holdings, DRIPs missing from sync). Those
        are a different bug class — see the snapshot_equity_sessions
        guard in int_equity_sessions and AMZN-class mismatches that
        long preceded the split work.

      - Symbols whose splits all happened BEFORE the user's first
        trade (e.g. AMZN's 2022-06-06 20:1 split on a user who first
        bought AMZN in 2025). cumulative_split_factor is 1.0
        everywhere for those, so they're correctly excluded.

    Failure mode the test catches: a position whose snapshot qty is
    EXACTLY (within rounding) snap_qty = trade_qty × split_factor for
    SOME split that's actually known on this symbol — meaning the
    consumer is using raw qty and missing the multiply.

    Tolerance: 1% of trade qty. Looser than the share-epsilon used by
    int_equity_sessions itself (1e-9) because we're checking a higher-
    level invariant across multiple sessions.
#}

with split_symbols as (
    -- Only check symbols that have AT LEAST ONE split DURING the
    -- user's trade window (between first trade and today). Splits
    -- before the user's history are no-ops by construction.
    select distinct
        d.symbol,
        max(sf.cumulative_split_factor) as max_factor
    from {{ ref('stg_split_events') }} d
    join {{ ref('int_split_factors') }} sf
      on sf.symbol = d.symbol
    where sf.cumulative_split_factor > 1.0 + 1e-9  -- some pre-split fill
       or sf.cumulative_split_factor < 1.0 - 1e-9  -- (or reverse split)
    group by d.symbol
    having max(sf.cumulative_split_factor) >= 1.5
        or min(sf.cumulative_split_factor) <= 0.667
),

snapshot_open as (
    select
        trim(account)             as account,
        user_id,
        trim(underlying_symbol)   as symbol,
        sum(quantity)             as snap_qty
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
      and coalesce(quantity, 0) > 0
      and trim(coalesce(underlying_symbol, '')) != ''
    group by 1, 2, 3
),

trade_derived_open as (
    select
        account,
        user_id,
        symbol,
        sum(case when status = 'Open' then max_quantity_held else 0 end)
            as trade_open_qty
    from {{ ref('int_equity_sessions') }}
    group by 1, 2, 3
),

joined as (
    select
        ss.symbol,
        s.account,
        s.user_id,
        s.snap_qty,
        coalesce(t.trade_open_qty, 0) as trade_open_qty,
        ss.max_factor,
        s.snap_qty - coalesce(t.trade_open_qty, 0) as gap
    from split_symbols ss
    join snapshot_open s
        on s.symbol = ss.symbol
    left join trade_derived_open t
        on  s.account = t.account
        and (s.user_id is not distinct from t.user_id)
        and s.symbol  = t.symbol
)

-- Fail when the snapshot has materially MORE shares than trade history
-- explains AND the gap looks like a split shape on a symbol that
-- actually has a known split. The split-unaware bug always manifests
-- this way (snapshot has the post-split count; trade history has the
-- pre-split count and they're off by exactly the cumulative factor).
select *
from joined
where gap > greatest(0.01, trade_open_qty * 0.01)
