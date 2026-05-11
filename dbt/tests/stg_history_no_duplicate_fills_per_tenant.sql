/*
    A single trade fill should appear EXACTLY ONCE per tenant
    (user_id, account). Duplicates here cascade into every downstream
    metric: doubled trade counts, doubled equity sessions, phantom
    "Cost Written Off" closed legs, doubled cumulative P&L.

    Production regression (May 2026): user_id=7, account='Schwab ••••5989'
    landed with 213 rows / 158 unique trades (55 dupes). Sample seed:

      Schwab ••••5989,7.0,11/14/2024,Sell to Open,CFLT  241220C00030000,
        CONFLUENT INC 12/20/2024 $30 Call,40.0,1.15,,4600.0   (x5)
      Schwab ••••5989,7.0,12/04/2024,Buy,CURRENCY_USD,USD currency,
        26.990000000000002,,,-26.990000000000002              (x3, drift)

    Two failure modes the dedup in app/upload._merge_seed_with_existing
    tries to prevent:
        1. Byte-identical re-landing across multiple sync cycles.
        2. Float-precision drift across syncs (26.99 vs 26.990000000000002)
           — same trade, different float serialization.

    See tests/test_upload_merge.py::test_canonicalize_seed_cell_collapses_known_drift_forms
    for the merge-side dedup helper. This dbt test is the warehouse-side
    backstop: if a future sync regression sneaks dupes past the merge
    helper, this test fails on the next dbt build instead of the user
    discovering it on the position page.

    Tenant grain is (user_id, account, trade_date, action, trade_symbol,
    quantity, price, amount). fees and description are excluded — fees
    can drift between sync runs (broker recomputes them) and description
    is just human-readable text that mirrors symbol.

    user_id=NULL rows (legacy unowned, pre-tenancy seed) are excluded so
    the test doesn't flag historical noise we already know about. Once
    the tenancy backfill lands (Stage 4 in docs/USER_ID_TENANCY.md) this
    `where user_id is not null` carve-out can drop and every row becomes
    in-scope.
*/

select
    user_id,
    account,
    trade_date,
    action,
    trade_symbol,
    quantity,
    price,
    amount,
    count(*) as n_dupes
from {{ ref('stg_history') }}
where user_id is not null
group by user_id, account, trade_date, action, trade_symbol,
         quantity, price, amount
having count(*) > 1
