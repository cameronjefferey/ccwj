{#
    One trade_symbol for every spelling of an option contract.

    Compact OCC (``ABCD240119C00150000``), space-padded OSI
    (``ABCD  240119C00150000``), and Schwab long form
    (``ABCD 01/19/2024 150.00 C``) are the same contract.
    ``occ_underlying_symbol()`` stores the stock root on all three, so
    ``est_fee_matched`` — which keys root + expiry + strike + type, not
    the raw symbol text — started treating an estimated-fee open on one
    spelling as the shadow of a statement open on another. Dropping
    that open left the close on the first spelling with no cost basis.
    Classification omits ``opened_before_history`` contracts. The coach
    still counts the close as ``Other Option``. That is CHECK 12
    (run 38531276167): coach closed-counts higher than classified
    option contracts, only on Other Option.

    Rebuild the compact OCC symbol before that match. A row with no
    expiry, strike, or call/put flag keeps its original symbol, so
    equities are unchanged.
#}
{% macro canonical_option_trade_symbol(trade_symbol, underlying, expiry, strike, cp) -%}
case
    when {{ expiry }} is not null
     and {{ strike }} is not null
     and upper(trim({{ cp }})) in ('C', 'P')
     and regexp_contains(
            upper(trim({{ underlying }})),
            r'^[A-Z][A-Z0-9.-]{0,5}$'
         )
    then concat(
        upper(trim({{ underlying }})),
        format_date('%y%m%d', {{ expiry }}),
        upper(trim({{ cp }})),
        lpad(
            cast(cast(round({{ strike }} * 1000) as int64) as string),
            8,
            '0'
        )
    )
    else {{ trade_symbol }}
end
{%- endmacro %}


{#
    Same compact id, parsed from a symbol string that has not been
    through stg_history / stg_current yet.

    Demo seed rows are still Schwab long form (``ABCD 01/19/2024 150.00 C``)
    while the bot mirror is already OCC. stg_current rewrites both to
    one trade_symbol and then keeps a single row per (tenant, symbol).
    Summing or comparing on the raw text drops one lot, or fails the
    mirror-plus-seed check, once those spellings share an id.

    The date / strike / call-put extraction matches cleaned_raw in
    stg_current.sql. Equities have no expiry and come back uppercased
    and trimmed.
#}
{% macro canonical_option_symbol_text(symbol_expr) -%}
{%- set sym = "upper(trim(cast(" ~ symbol_expr ~ " as string)))" -%}
{%- set osi = "regexp_extract(" ~ sym ~ ", r'(\\d{6}[CP]\\d{8})')" -%}
{{ canonical_option_trade_symbol(
    sym,
    occ_underlying_symbol(sym, sym),
    "coalesce(
        safe.parse_date('%m/%d/%Y', nullif(split(" ~ sym ~ ", ' ')[safe_offset(1)], '')),
        case
            when " ~ osi ~ " is not null
            then date(
                2000 + cast(substr(" ~ osi ~ ", 1, 2) as int64),
                cast(substr(" ~ osi ~ ", 3, 2) as int64),
                cast(substr(" ~ osi ~ ", 5, 2) as int64)
            )
        end
    )",
    "coalesce(
        safe_cast(split(" ~ sym ~ ", ' ')[safe_offset(2)] as float64),
        safe_cast(safe_divide(safe_cast(substr(" ~ osi ~ ", 8, 8) as int64), 1000) as float64)
    )",
    "coalesce(
        case
            when nullif(split(" ~ sym ~ ", ' ')[safe_offset(3)], '') in ('C', 'P')
            then nullif(split(" ~ sym ~ ", ' ')[safe_offset(3)], '')
        end,
        substr(" ~ osi ~ ", 7, 1)
    )"
) }}
{%- endmacro %}
