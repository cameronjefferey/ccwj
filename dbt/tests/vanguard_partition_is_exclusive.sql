/*
    Vanguard rows must sit in the vanguard adapters only.

    broker_slug_from_account() is the first token of the display label.
    known_brokers() includes vanguard, so the catch-all excludes it, and
    the named Schwab / Alpaca / Fidelity / IBKR adapters require their
    own slug. A row here means a label was routed into two partitions
    or the vanguard models accepted a non-Vanguard label.

    Returns one row per offending model. Zero rows is a pass.
*/

with offenders as (
    select 'schwab_history' as model
    from {{ ref('stg_broker_schwab_history') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'alpaca_history'
    from {{ ref('stg_broker_alpaca_history') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'fidelity_history'
    from {{ ref('stg_broker_fidelity_history') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'interactive_history'
    from {{ ref('stg_broker_interactive_history') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'empower_history'
    from {{ ref('stg_broker_empower_history') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'other_history'
    from {{ ref('stg_broker_other_history') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'vanguard_history_foreign'
    from {{ ref('stg_broker_vanguard_history') }}
    where coalesce({{ broker_slug_from_account('Account') }}, '') != 'vanguard'

    union all
    select 'schwab_current'
    from {{ ref('stg_broker_schwab_current') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'alpaca_current'
    from {{ ref('stg_broker_alpaca_current') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'fidelity_current'
    from {{ ref('stg_broker_fidelity_current') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'interactive_current'
    from {{ ref('stg_broker_interactive_current') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'empower_current'
    from {{ ref('stg_broker_empower_current') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'other_current'
    from {{ ref('stg_broker_other_current') }}
    where {{ broker_slug_from_account('Account') }} = 'vanguard'
    union all
    select 'vanguard_current_foreign'
    from {{ ref('stg_broker_vanguard_current') }}
    where coalesce({{ broker_slug_from_account('Account') }}, '') != 'vanguard'

    union all
    select 'schwab_balances'
    from {{ ref('stg_broker_schwab_balances') }}
    where {{ broker_slug_from_account('account') }} = 'vanguard'
    union all
    select 'alpaca_balances'
    from {{ ref('stg_broker_alpaca_balances') }}
    where {{ broker_slug_from_account('account') }} = 'vanguard'
    union all
    select 'fidelity_balances'
    from {{ ref('stg_broker_fidelity_balances') }}
    where {{ broker_slug_from_account('account') }} = 'vanguard'
    union all
    select 'interactive_balances'
    from {{ ref('stg_broker_interactive_balances') }}
    where {{ broker_slug_from_account('account') }} = 'vanguard'
    union all
    select 'empower_balances'
    from {{ ref('stg_broker_empower_balances') }}
    where {{ broker_slug_from_account('account') }} = 'vanguard'
    union all
    select 'other_balances'
    from {{ ref('stg_broker_other_balances') }}
    where {{ broker_slug_from_account('account') }} = 'vanguard'
    union all
    select 'vanguard_balances_foreign'
    from {{ ref('stg_broker_vanguard_balances') }}
    where coalesce({{ broker_slug_from_account('account') }}, '') != 'vanguard'
)

select model, count(*) as n
from offenders
group by model
