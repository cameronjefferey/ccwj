/*
    Tax lot classification for closed trade groups.
    Adds gain_type (Short-Term / Long-Term), tax_year, and wash sale flags.
    One row per closed trade group from int_strategy_classification.
*/

with closed_trades as (
    select
        account,
        symbol,
        trade_symbol,
        strategy,
        trade_group_type,
        status,
        open_date,
        close_date,
        days_in_trade,
        total_pnl,
        num_trades,

        case
            when days_in_trade > 365 then 'Long-Term'
            else 'Short-Term'
        end as gain_type,

        extract(year from close_date) as tax_year

    from {{ ref('int_strategy_classification') }}
    where status = 'Closed'
      and close_date is not null
),

/*
    Wash sale detection: a closed loss where the same symbol was traded
    (opened) within 30 days before or after the loss close date.
*/
wash_sale_matches as (
    select distinct
        l.account,
        l.symbol,
        l.close_date as loss_close_date,
        r.open_date  as repurchase_open_date,
        r.strategy    as repurchase_strategy,
        date_diff(r.open_date, l.close_date, day) as days_between
    from closed_trades l
    join {{ ref('int_strategy_classification') }} r
        on  l.account  = r.account
        and l.symbol    = r.symbol
        and l.trade_symbol != r.trade_symbol
        and abs(date_diff(r.open_date, l.close_date, day)) <= 30
    where l.total_pnl < 0
),

wash_flags as (
    select
        account,
        symbol,
        loss_close_date,
        min(repurchase_open_date)  as first_repurchase_date,
        min(repurchase_strategy)   as repurchase_strategy,
        min(days_between)          as days_between
    from wash_sale_matches
    group by 1, 2, 3
)

select
    ct.account,
    ct.symbol,
    ct.trade_symbol,
    ct.strategy,
    ct.trade_group_type,
    ct.open_date,
    ct.close_date,
    ct.days_in_trade,
    ct.total_pnl,
    ct.num_trades,
    ct.gain_type,
    ct.tax_year,

    case when wf.loss_close_date is not null then true else false end
        as is_potential_wash_sale,

    wf.first_repurchase_date as wash_repurchase_date,
    wf.repurchase_strategy   as wash_repurchase_strategy,
    wf.days_between          as wash_days_between

from closed_trades ct
left join wash_flags wf
    on  ct.account    = wf.account
    and ct.symbol     = wf.symbol
    and ct.close_date = wf.loss_close_date
