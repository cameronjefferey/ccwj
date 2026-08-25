{#
    Dividend labels must be a real yield, not an incidental coupon.

    positions_summary reclassifies Buy and Hold → Dividend only when
    dividends are ≥ 2.5% of invested AND ≥ 15% of the P&L story, or when
    dividends are ≥ 40% of (|price P&L| + dividends). Both paths imply
    share-of-story ≥ 15%. A row labeled Dividend with a smaller share
    is the old `divs > greatest(price_pnl, 0)` bug returning — that
    rule tagged every underwater stock that paid a coupon (UFO −$6,571
    with a $17 dividend).

    Fails (returns rows) if any Dividend row has
    divs / (|trade_only_pnl| + divs) < 0.15, or has no dividend income.
#}

select
    tenant_id,
    account,
    user_id,
    symbol,
    strategy,
    status,
    trade_only_pnl,
    total_dividend_income,
    safe_divide(
        total_dividend_income,
        abs(trade_only_pnl) + total_dividend_income
    ) as div_share_of_story
from {{ ref('positions_summary') }}
where strategy = 'Dividend'
  and (
      coalesce(total_dividend_income, 0) <= 0
      or safe_divide(
             total_dividend_income,
             abs(coalesce(trade_only_pnl, 0)) + total_dividend_income
         ) < 0.15
  )
