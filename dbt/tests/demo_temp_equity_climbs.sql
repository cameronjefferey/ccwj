{{ config(enabled=var('demo_temp_seed', false)) }}

/*
    Temporary demo book: the account curve must rise, and it must cover
    more than a couple of months. Fails the warehouse build if the seed
    is wired up but the curve is flat. Remove with the seed — see
    dbt/seeds/DEMO_TEMP_SEED.md.
*/

with ordered as (
    select
        as_of,
        account_value,
        row_number() over (order by as_of asc) as rn_asc,
        row_number() over (order by as_of desc) as rn_desc
    from {{ ref('int_demo_temp_equity_daily') }}
),

ends as (
    select
        (select account_value from ordered where rn_asc = 1) as start_value,
        (select account_value from ordered where rn_desc = 1) as end_value,
        (select count(*) from ordered) as n_days
)

select start_value, end_value, n_days
from ends
where end_value < start_value + 1000
   or n_days < 60
