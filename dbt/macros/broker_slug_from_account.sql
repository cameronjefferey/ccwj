{#
    Broker classification — single source of truth.

    Under v2 (docs/V2_TENANT_KEY_DESIGN.md) every SnapTrade tenant carries
    tenant_id = "snaptrade:<uuid>", so the literal broker (schwab / alpaca /
    fidelity / ...) is NOT recoverable from tenant_id — it is the SAME slug
    for every brokerage. The only broker hint that survives into the
    warehouse today is the broker-name prefix of the display ``Account``
    label that SnapTrade ships ("Schwab Account", "Alpaca Paper Account",
    "Schwab ••••6342"). These macros centralize that derivation so the
    per-broker staging models, the catch-all model, and dim_broker_tenants
    all classify rows the same way.

    This is a DISPLAY-derived classification, NOT a tenancy boundary.
    Tenant isolation is still on tenant_id everywhere (see
    .cursor/rules/bigquery-tenant-isolation.mdc) — splitting by broker_slug
    only partitions the same already-tenant-stamped rows for per-broker
    visibility / quirk handling. Never use broker_slug to scope a
    user-facing read.

    To add a brokerage (slug = lowercased first token of the account label,
    e.g. "Interactive Brokers …" -> 'interactive'):
      1. Add its slug to ``known_brokers()`` below.
      2. Add stg_broker_<slug>_history / _current / _balances models
         (one-line macro calls — see dbt/models/staging/brokers/).
      3. Add those models to the UNION in stg_history / stg_current /
         stg_account_balances.
      4. Add those models to the per-surface unions in
         dbt/tests/broker_split_preserves_all_rows.sql (else their rows
         leave the catch-all and the parity count drops).
    The ``_other_`` catch-all keeps any not-yet-modeled broker's rows
    flowing through in the meantime, so steps 2-4 never lose data.
    Modeled today: schwab, alpaca, fidelity, interactive (IBKR).
#}

{#-
    broker_slug_from_account(account_col)

    SQL expression -> lowercased first whitespace token of the trimmed
    account label. "Schwab Account" -> 'schwab', "Alpaca Paper Account"
    -> 'alpaca', "Schwab ••••6342" -> 'schwab'. Empty/NULL -> '' (which
    falls into the catch-all, never dropped).
-#}
{% macro broker_slug_from_account(account_col) -%}
    lower(split(trim(cast({{ account_col }} as string)), ' ')[safe_offset(0)])
{%- endmacro %}


{#-
    known_brokers() -> the list of broker slugs that have their own
    dedicated per-broker staging models. The catch-all (_other_) model
    matches every real row whose slug is NOT in this list.
-#}
{% macro known_brokers() -%}
    {#- Slugs are the lowercased first token of the account label, so IBKR
        (label "Interactive Brokers ...") classifies as 'interactive'. -#}
    {{ return(['schwab', 'alpaca', 'fidelity', 'interactive']) }}
{%- endmacro %}


{#-
    broker_row_filter(account_col, broker_slug, is_catch_all)

    Emits the WHERE predicate that selects one broker's rows from a raw
    seed. For a named broker: slug = '<broker_slug>'. For the catch-all:
    slug NOT IN known_brokers().
-#}
{% macro broker_row_filter(account_col, broker_slug, is_catch_all=false) -%}
    {%- if is_catch_all -%}
        {{ broker_slug_from_account(account_col) }} not in (
            {%- for b in known_brokers() -%}'{{ b }}'{%- if not loop.last -%}, {% endif -%}{%- endfor -%}
        )
    {%- else -%}
        {{ broker_slug_from_account(account_col) }} = '{{ broker_slug }}'
    {%- endif -%}
{%- endmacro %}
