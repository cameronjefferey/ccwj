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
