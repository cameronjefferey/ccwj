/*
    Alpaca lifecycle events are not recent-orders aggregates.

    A same-day short option can produce an activities SELL FILL plus an
    Expired/Assigned event on the same date.  When no recent-orders row exists,
    the fill is the only source of its premium and must survive the Alpaca
    adapter.  Terminal rows have no Buy/Sell side and must never cause the
    activities fill to be deduplicated away.
*/

with raw_alpaca as (
    {{ broker_history_rows('alpaca') }}
),

flagged as (
    select
        *,
        coalesce(
            regexp_contains(upper(Symbol), r'\d{6}[CP]\d{8}'),
            false
        ) as is_option,
        coalesce(
            regexp_contains(Description, r'(?i) (PARTIAL_)?FILL at '),
            false
        ) as is_activity_fill,
        case
            when starts_with(Action, 'Buy') then 'buy'
            when starts_with(Action, 'Sell') then 'sell'
        end as trade_side,
        Action in ('Expired', 'Assigned', 'Exchange or Exercise')
            as is_terminal
    from raw_alpaca
),

activities_only_same_day_fills as (
    select f.*
    from flagged f
    where f.is_option
      and f.is_activity_fill
      and f.trade_side is not null
      and exists (
          select 1
          from flagged terminal
          where terminal.tenant_id is not distinct from f.tenant_id
            and terminal.Date = f.Date
            and terminal.Symbol = f.Symbol
            and terminal.is_terminal
      )
      and not exists (
          select 1
          from flagged aggregate_row
          where aggregate_row.tenant_id is not distinct from f.tenant_id
            and aggregate_row.Date = f.Date
            and aggregate_row.Symbol = f.Symbol
            and aggregate_row.trade_side = f.trade_side
            and aggregate_row.is_option
            and not aggregate_row.is_activity_fill
      )
)

select
    f.tenant_id,
    f.Date,
    f.Action,
    f.Symbol,
    f.Description,
    f.Quantity,
    f.Price
from activities_only_same_day_fills f
left join {{ ref('stg_broker_alpaca_history') }} staged
    on staged.tenant_id is not distinct from f.tenant_id
    and staged.Date = f.Date
    and staged.Action = f.Action
    and staged.Symbol = f.Symbol
    and staged.Description = f.Description
    and staged.Quantity = f.Quantity
    and staged.Price = f.Price
where staged.Symbol is null
