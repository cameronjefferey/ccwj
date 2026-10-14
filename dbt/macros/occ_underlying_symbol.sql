{#
    Underlying root for a seed Symbol cell.

    Space-split is correct for Schwab long form
    (``AAPL 05/12/2023 175.00 C``) and for space-padded OSI
    (``AAPL  240119C00150000`` → first token ``AAPL``). Compact OCC
    has no space (``AAPL240119C00150000``, and any 6-character root
    from ``snaptrade_symbol_to_osi``, which pads to 6 and then has
    nothing left to pad). The first token is the whole contract.
    The price loader fetches ``underlying_symbol`` from Yahoo, so
    those contracts count as missing equities and trip
    ``PRICES_GUARD_TRIPPED`` (run 38499020109: 303 of 328 failures
    were compact OCC; the prior run on the same SHA was 95.7%).

    When the space-stripped symbol is a full OCC contract, the root
    is the prefix before the ``YYMMDD[CP]strike`` tail. Otherwise
    keep the first space-separated token.
#}
{% macro occ_underlying_symbol(sym_trim, sym_upper) -%}
case
    when regexp_contains(
        replace({{ sym_upper }}, ' ', ''),
        r'^[A-Z][A-Z0-9.-]{0,5}\d{6}[CP]\d{8}$'
    )
    then regexp_extract(
        replace({{ sym_upper }}, ' ', ''),
        r'^([A-Z][A-Z0-9.-]{0,5})\d{6}[CP]\d{8}$'
    )
    else trim(split({{ sym_trim }}, ' ')[safe_offset(0)])
end
{%- endmacro %}
