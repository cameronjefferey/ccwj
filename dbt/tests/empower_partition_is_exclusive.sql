/*
    Empower rows must sit in the empower adapters only.

    broker_slug_from_account() is the first token of the display label.
    known_brokers() includes empower, so the catch-all excludes it, and
    the named Schwab / Vanguard / Fidelity / Alpaca / IBKR adapters
    require their own slug. A row here means a label was routed into
    two partitions or the empower models accepted a non-Empower label.

    Returns one row per offending model. Zero rows is a pass.
*/

with offenders as (
    select 'schwab_history' as model
    from {{ ref('stg_broker_schwab_history') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'alpaca_history'
    from {{ ref('stg_broker_alpaca_history') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'fidelity_history'
    from {{ ref('stg_broker_fidelity_history') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'interactive_history'
    from {{ ref('stg_broker_interactive_history') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'vanguard_history'
    from {{ ref('stg_broker_vanguard_history') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'other_history'
    from {{ ref('stg_broker_other_history') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'empower_history_foreign'
    from {{ ref('stg_broker_empower_history') }}
    where coalesce({{ broker_slug_from_account('Account') }}, '') != 'empower'

    union all
    select 'schwab_current'
    from {{ ref('stg_broker_schwab_current') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'alpaca_current'
    from {{ ref('stg_broker_alpaca_current') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'fidelity_current'
    from {{ ref('stg_broker_fidelity_current') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'interactive_current'
    from {{ ref('stg_broker_interactive_current') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'vanguard_current'
    from {{ ref('stg_broker_vanguard_current') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'other_current'
    from {{ ref('stg_broker_other_current') }}
    where {{ broker_slug_from_account('Account') }} = 'empower'
    union all
    select 'empower_current_foreign'
    from {{ ref('stg_broker_empower_current') }}
    where coalesce({{ broker_slug_from_account('Account') }}, '') != 'empower'

    union all
    select 'schwab_balances'
    from {{ ref('stg_broker_schwab_balances') }}
    where {{ broker_slug_from_account('account') }} = 'empower'
    union all
    select 'alpaca_balances'
    from {{ ref('stg_broker_alpaca_balances') }}
    where {{ broker_slug_from_account('account') }} = 'empower'
    union all
    select 'fidelity_balances'
    from {{ ref('stg_broker_fidelity_balances') }}
    where {{ broker_slug_from_account('account') }} = 'empower'
    union all
    select 'interactive_balances'
    from {{ ref('stg_broker_interactive_balances') }}
    where {{ broker_slug_from_account('account') }} = 'empower'
    union all
    select 'vanguard_balances'
    from {{ ref('stg_broker_vanguard_balances') }}
    where {{ broker_slug_from_account('account') }} = 'empower'
    union all
    select 'other_balances'
    from {{ ref('stg_broker_other_balances') }}
    where {{ broker_slug_from_account('account') }} = 'empower'
    union all
    select 'empower_balances_foreign'
    from {{ ref('stg_broker_empower_balances') }}
    where coalesce({{ broker_slug_from_account('account') }}, '') != 'empower'
)

select model, count(*) as n
from offenders
group by model
