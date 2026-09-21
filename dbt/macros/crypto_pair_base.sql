{#
    Base ticker for a crypto pair quote.

    Coinbase (and other crypto brokers) ship activities as BTC-USD,
    BTC/USD, or BTCUSD while the position snapshot is BTC. History then
    never joins the holding. Returns the candidate base; the caller
    joins ``stg_crypto_symbols`` and only rewrites when that base is on
    the whitelist, so USDC, equities, and option OSI roots stay put.
    Longer quotes (USDT, USDC) are stripped before USD.
#}
{% macro crypto_pair_base_expr(symbol_sql) -%}
regexp_replace(
    regexp_replace(upper(trim({{ symbol_sql }})), r'[-/](USDT|USDC|USD)$', ''),
    r'(USDT|USDC|USD)$',
    ''
)
{%- endmacro %}
