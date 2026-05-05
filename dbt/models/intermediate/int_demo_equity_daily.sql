{{
    config(
        materialized='table'
    )
}}
/*
    Synthetic daily account value for Demo Account so Weekly Review's
    "Today's Snapshot" and weekly returns have a full history (no empty
    cells) when a visitor opens the demo cold.

    History (read this before tweaking):
      - V1: literal straight line (82k -> 185k linear + tiny jitter).
        Investors / power users could spot it immediately, and the
        endpoint drifted ~$70k above the demo snapshot.
      - V2: saturating growth + sin(t/57)*8000 regime + cos(t/9.5)*3500
        chop + ±$1.2k noise. Looked "real" but the cycles happened to
        roll the chart into a -5.4% drawdown vs 1 Month right at the
        time of an investor demo. The hero rendered:
          "vs 1 Month  -$6,906   -5.46%"
        — exactly the shape we don't want a fundraising visitor to see
        first.

    V3 (this version) targets:
      - Today close to $120k.
      - Hero tiles for the demo open positive across vs Yesterday /
        vs Week start / vs 1 Week / vs 1 Month, regardless of where
        the underlying cycle phase happens to land.
      - Chart still "breathes": visible historical drawdowns and
        recoveries, no perfect ramp.
      - Fully deterministic via farm_fingerprint so re-runs are
        byte-identical.

    The "recent rally" term is the load-bearing change: it adds an
    exponentially-decaying bump anchored on `current_date()`, so the
    last ~45 days always trend up into the present even when the
    slow-regime sin happens to be in a dip. Decay is gentle enough
    that the chart doesn't show a kink where the rally starts.

    Output: one row per (Demo Account, date) from 2022-01-01 through
    current_date. Only for the literal account string 'Demo Account' —
    mart_account_equity_daily explicitly excludes that account from
    the snapshot-driven path before unioning this in, so this never
    touches real-user data.
*/
with date_series as (
    select date_day as date
    from unnest(
        generate_date_array(date('2022-01-01'), current_date(), interval 1 day)
    ) as date_day
),

-- Cap t at 5 years so the saturating growth curve plateaus instead of
-- continuing to climb forever. Without the cap, the demo silently
-- drifts away from a realistic snapshot value as more time passes
-- between dbt runs.
modeled as (
    select
        date,
        least(date_diff(date, date('2022-01-01'), day), 1825) as t,
        date_diff(current_date(), date, day) as days_back
    from date_series
),

components as (
    select
        date,
        t,
        -- Smooth saturating growth: $80k toward ~$118k via (1 - exp(-t/k)).
        -- Slower saturation (k=1100) than V2 so the curve is still gently
        -- rising at t=1581 instead of fully plateaued.
        80000 + 53000 * (1 - exp(-t / 1100.0)) as growth,
        -- Slow regime ±$2.5k, period ~180 days. Smaller than V2's $8k so
        -- the cycles can't whip the hero number into a multi-thousand-dollar
        -- drawdown by themselves.
        sin(t * 6.283185 / 180.0) * 2500 as regime,
        -- Monthly chop ±$800, period ~28 days — gives the chart visible
        -- "weekly variance" without dominating month-over-month numbers.
        sin(t * 6.283185 / 28.0) * 800 as chop,
        -- Deterministic daily noise ±$400. Tighter than V2 (±$1,200) so
        -- daily-change tiles look like options-trader noise, not crypto.
        cast(mod(cast(farm_fingerprint(cast(date as string)) as int64), 800) - 400 as int64) as noise,
        -- "Recent rally": exponentially-decaying bump anchored on today.
        -- Forces the last ~45 days to trend up into the present, so the
        -- demo's hero numbers (vs Yesterday / Week / Month) always read
        -- positive even when the regime sin happens to be in a dip.
        -- Decays smoothly (no kink visible on the chart).
        case
            when days_back is null or days_back < 0 then 0.0
            else 3500.0 * exp(-cast(days_back as float64) / 22.0)
        end as rally
    from modeled
),

shaped as (
    select
        date,
        -- Floor at $50k so a deep drawdown can never produce a negative
        -- account_value (chart code asserts positive values).
        greatest(50000, growth + regime + chop + noise + rally) as account_value_raw
    from components
)

select
    'Demo Account' as account,
    -- Demo synthetic data has no Postgres user_id (the demo user's id varies
    -- per environment). Stage 3's app code special-cases the demo user to
    -- filter by ``account = 'Demo Account'`` rather than ``user_id``, so the
    -- NULL here is intentional — see docs/USER_ID_TENANCY.md.
    cast(null as int64)                                  as user_id,
    date,
    cast(account_value_raw * (1 - 0.05 - 0.12) as int64) as equity_value,
    cast(account_value_raw * 0.12 as int64)              as option_value,
    cast(account_value_raw * 0.05 as int64)              as cash_value,
    cast(account_value_raw as int64)                     as account_value
from shaped
order by date
