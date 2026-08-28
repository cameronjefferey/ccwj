{#
    Parse a raw seed numeric cell the way Schwab CSVs actually write them.

    ``safe_cast(amount as float64)`` is NULL for ``$1,150.00`` / ``($26.99)``.
    ``stg_history`` then ``coalesce(..., 0)`` so every unparseable Amount
    becomes 0 and CHECK 1 of ``stg_history_no_duplicate_fills_per_tenant``
    fuses unrelated CSV rows that share (date, action, symbol, qty).

    Warehouse runs 33139304912 / 33140422151: 153 groups after the
    merge-key repair dropped 1 of 11274 raw rows. The remaining collisions
    are created at this cast, not in the raw seed grain.

    Mirrors ``app.upload._canonicalize_seed_cell`` ($ / commas / accounting
    negatives) so staging and the merge key agree.
#}
{% macro parse_seed_number(expr) -%}
safe_cast(
    nullif(
        regexp_replace(
            regexp_replace(
                replace(trim(cast({{ expr }} as string)), ',', ''),
                r'^\$',
                ''
            ),
            r'^\((.*)\)$',
            r'-\1'
        ),
        ''
    ) as float64
)
{%- endmacro %}
