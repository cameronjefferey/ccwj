{% macro option_close_basis_walk() %}
{#-
  Unrolled average-cost walk over allocatable_close_state.

  Step 1 is the recursive base term. Each later step joins the next
  basis_event_num to the prior step only. The formulas are the same
  expressions the self-referential CTE used. Do not switch this back
  to a recursive CTE: dbt wraps model SQL in parentheses, and BigQuery
  rejects that. Depth is var('option_close_basis_max_steps').
-#}
{% set max_steps = var('option_close_basis_max_steps') | int %}
close_basis_step_1 as (
    select
        cs.*,
        cs.opened_qty_to_date as opened_qty_seen,
        cs.opening_cash_to_date as opening_cash_seen,
        greatest(cs.opened_qty_to_date - cs.closed_qty, 0.0)
            as remaining_open_qty,
        cs.opening_cash_to_date * (
            1.0 - least(
                coalesce(
                    safe_divide(cs.closed_qty, cs.opened_qty_to_date),
                    0
                ),
                1.0
            )
        ) as remaining_opening_cash,
        cs.opening_cash_to_date * least(
            coalesce(
                safe_divide(cs.closed_qty, cs.opened_qty_to_date),
                0
            ),
            1.0
        ) as allocated_opening_cash
    from allocatable_close_state cs
    where cs.basis_event_num = 1
),
{% for i in range(2, max_steps + 1) %}
close_basis_step_{{ i }} as (
    select * from close_basis_step_{{ i - 1 }}
    union all
    select
        cs.*,
        cs.opened_qty_to_date as opened_qty_seen,
        cs.opening_cash_to_date as opening_cash_seen,
        greatest(
            greatest(
                prev.remaining_open_qty
                + cs.opened_qty_to_date
                - prev.opened_qty_seen,
                0.0
            ) - cs.closed_qty,
            0.0
        ) as remaining_open_qty,
        (
            prev.remaining_opening_cash
            + cs.opening_cash_to_date
            - prev.opening_cash_seen
        ) * (
            1.0 - least(
                coalesce(
                    safe_divide(
                        cs.closed_qty,
                        greatest(
                            prev.remaining_open_qty
                            + cs.opened_qty_to_date
                            - prev.opened_qty_seen,
                            0.0
                        )
                    ),
                    0
                ),
                1.0
            )
        ) as remaining_opening_cash,
        (
            prev.remaining_opening_cash
            + cs.opening_cash_to_date
            - prev.opening_cash_seen
        ) * least(
            coalesce(
                safe_divide(
                    cs.closed_qty,
                    greatest(
                        prev.remaining_open_qty
                        + cs.opened_qty_to_date
                        - prev.opened_qty_seen,
                        0.0
                    )
                ),
                0
            ),
            1.0
        ) as allocated_opening_cash
    from allocatable_close_state cs
    join close_basis_step_{{ i - 1 }} prev
      on (cs.tenant_id is not distinct from prev.tenant_id)
     and cs.account = prev.account
     and (cs.user_id is not distinct from prev.user_id)
     and cs.trade_symbol = prev.trade_symbol
     and prev.basis_event_num = {{ i - 1 }}
     and cs.basis_event_num = {{ i }}
),
{% endfor %}
close_basis_walk as (
    select * from close_basis_step_{{ max_steps }}
),
{% endmacro %}
