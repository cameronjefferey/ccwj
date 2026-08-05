/*
    A single trade fill should appear EXACTLY ONCE per tenant
    (tenant_id). Duplicates here cascade into every downstream metric:
    doubled trade counts, doubled equity sessions, phantom "Cost Written
    Off" closed legs, doubled cumulative P&L.

    Production regression (May 2026, v1): user_id=7, account='Schwab ••••5989'
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

    TWO grains are checked (UNION ALL — any row = failure):

    CHECK 1 — full grain (tenant_id, trade_date, action, trade_symbol,
    quantity, price, amount). Catches byte-identical re-lands and
    ≤6-decimal float-serialization drift (26.99 vs 26.990000000000002).
    fees/description excluded (fees drift; description mirrors symbol).

    CHECK 2 — cross-source fill grain (tenant_id, trade_date, action,
    trade_symbol, round(price,4)) with amount OMITTED, scoped to real
    fills (trade_symbol AND price non-null). SnapTrade's two feeds report
    the SAME fill at different precision — orders derives Price at full
    float precision (131.960622, 0.486667) while activities carries the
    broker's 4-decimal Price (131.9606, 0.4867) — and Amount also drifts
    (activities is net of fees; orders is gross). CHECK 1 (raw price +
    amount) let both survive → doubled shares (AAOI Aug 2026: 225-share
    fill counted twice → +$29.7k phantom equity). Rounding Price to 4dp
    and dropping Amount mirrors the cross-source dedup key in
    app/upload._dedup_history_rows so a feed-precision regression that
    sneaks past the merge helper fails the build here. 4dp is coarse
    enough to fuse the two feeds, fine enough to keep genuinely distinct
    sub-penny option fills apart. Non-fill events (dividends/fees/interest,
    blank Symbol/Price) are excluded here — CHECK 1's Amount grain covers
    them.

    Under v2 every legitimate row carries a tenant_id; NULL is filtered
    so demo/legacy rows don't trip the test.
*/

select
    tenant_id,
    trade_date,
    action,
    trade_symbol,
    quantity,
    price,
    amount,
    count(*) as n_dupes
from {{ ref('stg_history') }}
where tenant_id is not null
group by tenant_id, trade_date, action, trade_symbol,
         quantity, price, amount
having count(*) > 1

union all

select
    tenant_id,
    trade_date,
    action,
    trade_symbol,
    quantity,
    round(price, 4) as price,
    cast(null as float64) as amount,
    count(*) as n_dupes
from {{ ref('stg_history') }}
where tenant_id is not null
  and trade_symbol is not null
  and price is not null
group by tenant_id, trade_date, action, trade_symbol,
         quantity, round(price, 4)
having count(*) > 1
