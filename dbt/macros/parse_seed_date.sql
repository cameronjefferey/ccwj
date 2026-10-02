{#
    Parse a raw seed Date cell into a DATE.

    SnapTrade writes zero-padded ``MM/DD/YYYY``. Schwab's web CSV omits
    the leading zero and often appends a clock / "as of" suffix
    (``5/14/2024 12:00:00 AM``, ``05/14/2024 as of 08:30 PM``). A cash-
    settled index option posts the next morning as
    ``10/02/2026 as of 10/01/2026``: the first date is the post date,
    the date after "as of" is the trade date. pandas ``read_csv``
    rewrites Date as ISO ``YYYY-MM-DD``. The long-tenured manual CSV
    tenant writes two-digit years (``1/20/23``).

    Warehouse run 33142404800 (after #70/#71): 40 leftover CHECK 1 groups
    were still ``trade_date IS NULL`` on ``manual:manual:Schwab Account``.
    The dump of raw Date was ``1/20/23``, ``11/18/22``, ``12/30/22`` —
    ``%m/%d/%Y`` does not read a two-digit year. ``_canonicalize_date_mdy``
    already accepts ``M/D/YY``; staging must too.
#}
{% macro parse_seed_date(expr) -%}
coalesce(
    -- "10/02/2026 as of 10/01/2026" → 10/01 (trade date). A clock
    -- suffix ("as of 08:30 PM") does not match, so it falls through
    -- to the leading date below.
    safe.parse_date(
        '%m/%d/%Y',
        regexp_extract(
            trim(cast({{ expr }} as string)),
            r'(?i)\bas of\s+(\d{1,2}/\d{1,2}/\d{4})'
        )
    ),
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
