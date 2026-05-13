/*
    Per-event dividend rows for every (account, user_id, symbol, trade_date).

    Produced from three sources:

    1) `csv` source — explicit dividend rows from `stg_history.action = 'dividend'`.
       Manual Schwab CSV exports include "Cash Dividend" / "Qualified Dividend"
       lines; we trust them when present.

    2) `drip` source — fractional `equity_buy` rows that Schwab Connect emits
       on ex-dividend dates. These are dividend reinvestments where the broker
       takes the cash dividend and immediately buys fractional shares; the
       absolute dollar amount of the buy IS the dividend the user received.
       Surfaces as a real, broker-confirmed dividend event (more accurate
       than synthetic for accounts on DRIP). Detected in
       `int_drip_fills` (sibling intermediate model).

    3) `synthetic` source — yfinance ex-div per-share dividend
       (`stg_daily_prices.dividend`) × the user's running share count on the
       ex-div date, derived from `stg_history` equity events. Fallback for
       accounts with neither CSV dividend rows nor DRIP fills.

       Schwab Connect (OAuth sync) doesn't return dividends cleanly, and most
       users have never run a manual CSV upload. JEPI / JEPQ / SCHD / VYM
       holders saw $0 dividends on /position even when they clearly owned
       thousands of shares for years. yfinance carries the per-share dividend
       on each ex-div date, so we can reconstruct what the user actually
       received without depending on the broker pipeline.

    Precedence (per (account, user_id, symbol) tuple):
      - if any CSV `dividend` row exists, keep ONLY CSV rows
      - else if any DRIP fill exists, keep ONLY DRIP rows
      - else fall back to synthetic
    Avoids double-counting between yfinance and the broker.

    Holdings clip: yfinance dividends extend through today even after the
    user no longer holds a symbol. For accounts where the symbol no longer
    appears in `stg_current` (fully closed, or transferred to another
    account via a Schwab Journal entry), we clip ex-div dates after the
    account's last equity event in `stg_history` so post-close dividends
    don't keep accruing in the wrong account.

    Downstream consumers:
      - `int_dividends` (lifetime aggregate per tuple)
      - `mart_daily_pnl.dividends_amount` (per (account, symbol, day))
      - `app/routes.py DATE_FILTERED_QUERY` (via int_dividends)
*/

with equity_events as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_date,
        case
            when action = 'equity_buy'                          then  quantity
            when action in ('equity_sell', 'equity_sell_short') then -quantity
            else 0
        end as signed_qty
    from {{ ref('stg_history') }}
    where instrument_type = 'Equity'
),

ex_divs as (
    select
        symbol,
        date as ex_div_date,
        max(dividend) as div_per_share
    from {{ ref('stg_daily_prices') }}
    where coalesce(dividend, 0) > 0
    group by 1, 2
),

holdings_keys as (
    select distinct
        account,
        user_id,
        underlying_symbol as symbol
    from {{ ref('stg_history') }}
    where instrument_type = 'Equity'
),

holdings_meta as (
    select
        h.account,
        h.user_id,
        h.symbol,
        max(e.trade_date) as last_trade_date,
        max(case when c.quantity > 0 then 1 else 0 end) as has_current_shares
    from holdings_keys h
    left join equity_events e
        on  e.account = h.account
        and (e.user_id is not distinct from h.user_id)
        and e.symbol  = h.symbol
    left join {{ ref('stg_current') }} c
        on  c.account            = h.account
        and (c.user_id is not distinct from h.user_id)
        and c.underlying_symbol  = h.symbol
        and c.instrument_type    = 'Equity'
    group by 1, 2, 3
),

csv_dividend_keys as (
    select distinct
        account,
        user_id,
        underlying_symbol as symbol
    from {{ ref('stg_history') }}
    where action = 'dividend'
),

drip_keys as (
    select distinct
        account,
        user_id,
        underlying_symbol as symbol
    from {{ ref('int_drip_fills') }}
),

shares_on_exdiv as (
    select
        hm.account,
        hm.user_id,
        hm.symbol,
        edd.ex_div_date,
        edd.div_per_share,
        coalesce(
            sum(
                case
                    when ee.trade_date <= edd.ex_div_date then ee.signed_qty
                    else 0
                end
            ),
            0
        ) as shares_held,
        hm.has_current_shares,
        hm.last_trade_date
    from holdings_meta hm
    join ex_divs edd
        on hm.symbol = edd.symbol
    left join equity_events ee
        on  ee.account = hm.account
        and (ee.user_id is not distinct from hm.user_id)
        and ee.symbol  = hm.symbol
    group by 1, 2, 3, 4, 5, 7, 8
),

synthetic_events as (
    select
        s.account,
        s.user_id,
        s.symbol,
        s.ex_div_date as trade_date,
        round(s.shares_held * s.div_per_share, 2) as amount,
        'synthetic' as source
    from shares_on_exdiv s
    left join csv_dividend_keys cdk
        on  cdk.account = s.account
        and (cdk.user_id is not distinct from s.user_id)
        and cdk.symbol  = s.symbol
    left join drip_keys dk
        on  dk.account = s.account
        and (dk.user_id is not distinct from s.user_id)
        and dk.symbol  = s.symbol
    where s.shares_held > 0
      and cdk.account is null
      and dk.account   is null
      and (
          s.has_current_shares = 1
          or s.ex_div_date <= s.last_trade_date
      )
),

csv_events as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_date,
        amount,
        'csv' as source
    from {{ ref('stg_history') }}
    where action = 'dividend'
),

drip_events as (
    -- Per (account, user_id, symbol): keep DRIPs only when there's no
    -- CSV dividend stream for the same tuple (CSV always wins).
    select
        d.account,
        d.user_id,
        d.underlying_symbol as symbol,
        d.trade_date,
        round(d.drip_amount, 2) as amount,
        'drip' as source
    from {{ ref('int_drip_fills') }} d
    left join csv_dividend_keys cdk
        on  cdk.account = d.account
        and (cdk.user_id is not distinct from d.user_id)
        and cdk.symbol  = d.underlying_symbol
    where cdk.account is null
)

select * from csv_events
union all
select * from drip_events
union all
select * from synthetic_events
