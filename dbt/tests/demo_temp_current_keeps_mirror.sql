{{ config(enabled=demo_temp_seed_on()) }}

/*
    Flag on: each demo position's quantity is the bot mirror's quantity
    plus the seed file's quantity for that symbol. A shared ticker is
    summed, not dropped. Disabled when DEMO_TEMP_SEED is off.
*/

with demo as (
    select upper(trim(trade_symbol)) as symbol, sum(quantity) as qty
    from {{ ref('stg_current') }}
    where tenant_id = 'demo:demo-account'
    group by 1
),

mirror as (
    select upper(trim(trade_symbol)) as symbol, sum(quantity) as qty
    from {{ ref('stg_current') }}
    where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
    group by 1
),

seed as (
    select
        upper(trim(cast(Symbol as string))) as symbol,
        sum({{ parse_seed_number('Quantity') }}) as qty
    from {{ ref('demo_temp_current') }}
    group by 1
),

keys as (
    select symbol from demo
    union distinct
    select symbol from mirror
    union distinct
    select symbol from seed
)

select
    k.symbol,
    d.qty as demo_qty,
    m.qty as mirror_qty,
    s.qty as seed_qty
from keys k
left join demo d using (symbol)
left join mirror m using (symbol)
left join seed s using (symbol)
where abs(
    coalesce(d.qty, 0) - coalesce(m.qty, 0) - coalesce(s.qty, 0)
) > 0.01
