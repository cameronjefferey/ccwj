{#
    Per-broker staging row emitters.

    Each macro returns the column-guarded, string-cast rows for ONE broker
    from the relevant RAW seed(s), filtered by broker_row_filter(). The
    output column shape exactly matches the first CTE the corresponding
    base model used to build directly from the seed, so the base model just
    UNIONs the per-broker models (+ demo) and runs its existing parse
    unchanged.

    These are thin adapters: today they are pure passthrough. The point of
    one model per broker is that broker-specific quirks (a broker that
    ships a weird date format, sign convention, duplicate-fill pattern,
    etc.) get fixed INSIDE that broker's model where it is isolated and
    independently testable — never by special-casing the shared parse.

    The user_id / tenant_id existence guards (adapter.get_columns_in_relation)
    live here once instead of in every per-broker model, so each model file
    stays a one-line macro call.
#}

{#- ---------------------------------------------------------------------
    History rows  ->  shape of stg_history's `trade_history_as_strings`
--------------------------------------------------------------------- -#}
{% macro broker_history_rows(broker_slug, is_catch_all=false) %}
{% if execute %}
    {%- set _cols = adapter.get_columns_in_relation(ref('trade_history')) | map(attribute='name') | list -%}
{% else %}
    {%- set _cols = [] -%}
{% endif %}
{%- set _user_id_expr = "cast(user_id as string)" if 'user_id' in _cols else "cast(null as string)" -%}
{%- set _tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _cols else "cast(null as string)" -%}
    select
        cast(Account as string) as Account,
        {{ _user_id_expr }} as user_id,
        {{ _tenant_id_expr }} as tenant_id,
        cast(Date as string) as Date,
        cast(Action as string) as Action,
        cast(Symbol as string) as Symbol,
        cast(Description as string) as Description,
        cast(Quantity as string) as Quantity,
        cast(Price as string) as Price,
        cast(fees_and_comm as string) as fees_and_comm,
        cast(Amount as string) as Amount
    from {{ ref('trade_history') }}
    where {{ broker_row_filter('Account', broker_slug, is_catch_all) }}
{% endmacro %}


{#- ---------------------------------------------------------------------
    Current rows  ->  shape of stg_current's `current_as_strings`
    (user_id, tenant_id, then the common string columns)
--------------------------------------------------------------------- -#}
{% macro broker_current_rows(broker_slug, is_catch_all=false) %}
{% if execute %}
    {%- set _cols = adapter.get_columns_in_relation(ref('current_positions')) | map(attribute='name') | list -%}
{% else %}
    {%- set _cols = [] -%}
{% endif %}
{%- set _user_id_expr = "cast(user_id as string)" if 'user_id' in _cols else "cast(null as string)" -%}
{%- set _tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _cols else "cast(null as string)" -%}
    select
        {{ _user_id_expr }} as user_id,
        {{ _tenant_id_expr }} as tenant_id,
        cast(Account as string) as Account,
        cast(Symbol as string) as Symbol,
        cast(Description as string) as Description,
        cast(Quantity as string) as Quantity,
        cast(Price as string) as Price,
        cast(price_change_dollar as string) as price_change_dollar,
        cast(price_change_percent as string) as price_change_percent,
        cast(market_value as string) as market_value,
        cast(day_change_dollar as string) as day_change_dollar,
        cast(day_change_percent as string) as day_change_percent,
        cast(cost_bases as string) as cost_bases,
        cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string) as gain_or_loss_percent,
        cast(rating as string) as rating,
        cast(divident_reinvestment as string) as divident_reinvestment,
        cast(is_capital_gain as string) as is_capital_gain,
        cast(percent_of_account as string) as percent_of_account,
        cast(expiration_date as string) as expiration_date,
        cast(cost_per_share as string) as cost_per_share,
        cast(last_earnings_date as string) as last_earnings_date,
        cast(dividend_yield as string) as dividend_yield,
        cast(last_dividend as string) as last_dividend,
        cast(ex_dividend_date as string) as ex_dividend_date,
        cast(pe_ratio as string) as pe_ratio,
        cast(annual_week_low as string) as annual_week_low,
        cast(annual_week_high as string) as annual_week_high,
        cast(volume as string) as volume,
        cast(intrinsic_value as string) as intrinsic_value,
        cast(in_the_money as string) as in_the_money,
        cast(security_type as string) as security_type,
        cast(margin_requirement as string) as margin_requirement
    from {{ ref('current_positions') }}
    where {{ broker_row_filter('Account', broker_slug, is_catch_all) }}
{% endmacro %}


{#- ---------------------------------------------------------------------
    Balance rows  ->  shape of stg_account_balances's `unioned` CTE.

    Dual-source: this broker's cash + account_total rows come from BOTH
    the broker-sync `account_balances` seed (src_priority 1) AND the legacy
    `current_positions` export (cash-and-money-market + account/positions
    total rows, src_priority 2). Demo rows are NOT emitted here — the base
    model adds demo separately because demo is not a broker. The base
    model's dedup on (coalesce(tenant_id, account), row_type) is unchanged.
--------------------------------------------------------------------- -#}
{% macro broker_balances_rows(broker_slug, is_catch_all=false) %}
{% if execute %}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions')) | map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('account_balances')) | map(attribute='name') | list -%}
{% else %}
    {%- set _curr_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{% endif %}
{%- set _curr_user_id_expr = "cast(user_id as string)" if 'user_id' in _curr_cols else "cast(null as string)" -%}
{%- set _curr_tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _curr_cols else "cast(null as string)" -%}
{%- set _bal_user_id_expr  = "cast(user_id as string)" if 'user_id' in _bal_cols  else "cast(null as string)" -%}
{%- set _bal_tenant_id_expr  = "cast(tenant_id as string)" if 'tenant_id' in _bal_cols  else "cast(null as string)" -%}

    -- Broker-sync balances seed (authoritative; src_priority 1)
    select
        trim(cast(account as string)) as account,
        safe_cast(safe_cast(nullif(trim({{ _bal_user_id_expr }}), '') as float64) as int64) as user_id,
        nullif(trim({{ _bal_tenant_id_expr }}), '') as tenant_id,
        case lower(trim(cast(row_type as string)))
            when 'cash' then 'cash'
            when 'account_total' then 'account_total'
        end as row_type,
        safe_cast(trim(replace(replace(replace(cast(market_value as string), '$', ''), ',', ''), ' ', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(replace(cast(cost_basis as string), '$', ''), ',', ''), ' ', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl as string), '$', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl_pct as string), '%', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(replace(cast(percent_of_account as string), '%', ''), ',', '')) as float64) as percent_of_account,
        1 as src_priority
    from {{ ref('account_balances') }}
    where trim(coalesce(cast(account as string), '')) != ''
      and lower(trim(coalesce(cast(row_type as string), ''))) in ('cash', 'account_total')
      and {{ broker_row_filter('account', broker_slug, is_catch_all) }}

    union all

    -- Legacy current_positions cash rows (src_priority 2)
    select
        trim(cast(Account as string)) as account,
        safe_cast(safe_cast(nullif(trim({{ _curr_user_id_expr }}), '') as float64) as int64) as user_id,
        nullif(trim({{ _curr_tenant_id_expr }}), '') as tenant_id,
        'cash' as row_type,
        safe_cast(trim(replace(replace(cast(market_value as string), '$', ''), ',', '')) as float64) as market_value,
        cast(null as float64) as cost_basis,
        cast(null as float64) as unrealized_pnl,
        cast(null as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(cast(percent_of_account as string), '%', '')) as float64) as percent_of_account,
        2 as src_priority
    from {{ ref('current_positions') }}
    where lower(trim(coalesce(cast(security_type as string), ''))) = 'cash and money market'
      and {{ broker_row_filter('Account', broker_slug, is_catch_all) }}

    union all

    -- Legacy current_positions account/positions total rows (src_priority 2)
    select
        trim(cast(Account as string)) as account,
        safe_cast(safe_cast(nullif(trim({{ _curr_user_id_expr }}), '') as float64) as int64) as user_id,
        nullif(trim({{ _curr_tenant_id_expr }}), '') as tenant_id,
        'account_total' as row_type,
        safe_cast(trim(replace(replace(cast(market_value as string), '$', ''), ',', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(cast(cost_bases as string), '$', ''), ',', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(cast(gain_or_loss_dollat as string), '$', ''), ',', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(cast(gain_or_loss_percent as string), '%', '')) as float64) as unrealized_pnl_pct,
        cast(null as float64) as percent_of_account,
        2 as src_priority
    from {{ ref('current_positions') }}
    where lower(trim(coalesce(cast(symbol as string), ''))) in ('account total', 'positions total')
      and {{ broker_row_filter('Account', broker_slug, is_catch_all) }}
{% endmacro %}
