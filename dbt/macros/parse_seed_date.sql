{#
    Parse a raw seed Date cell into a DATE.

    SnapTrade writes zero-padded ``MM/DD/YYYY``. Schwab's web CSV omits
    the leading zero and often appends a clock / "as of" suffix
    (``5/14/2024 12:00:00 AM``, ``05/14/2024 as of 08:30 PM``). pandas
    ``read_csv`` rewrites Date as ISO ``YYYY-MM-DD``. The long-tenured
    manual CSV tenant writes two-digit years (``1/20/23``).

    Warehouse run 33142404800 (after #70/#71): 40 leftover CHECK 1 groups
    were still ``trade_date IS NULL`` on ``manual:manual:Schwab Account``.
    The dump of raw Date was ``1/20/23``, ``11/18/22``, ``12/30/22`` —
    ``%m/%d/%Y`` does not read a two-digit year. ``_canonicalize_date_mdy``
    already accepts ``M/D/YY``; staging must too.
#}
{% macro parse_seed_date(expr) -%}
coalesce(
    safe.parse_date(
        '%m/%d/%Y',
        regexp_extract(trim(cast({{ expr }} as string)), r'(\d{1,2}/\d{1,2}/\d{4})')
    ),
    safe.parse_date(
        '%Y-%m-%d',
        regexp_extract(trim(cast({{ expr }} as string)), r'(\d{4}-\d{2}-\d{2})')
    ),
    safe.parse_date(
        '%m/%d/%y',
        regexp_extract(trim(cast({{ expr }} as string)), r'^(\d{1,2}/\d{1,2}/\d{2})(?:\s|$)')
    )
)
{%- endmacro %}
