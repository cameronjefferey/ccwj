# Fidelity via SnapTrade — pre-onboard notes

Read-only audit for a new private Fidelity connection. No customer figures.
Example dollars below are made up.

This work sits on PR #285 (Vanguard's own staging partition and the shared
SnapTrade normalize fixes). Until that PR merges, this branch contains those
commits.

## How a Fidelity account is routed

The warehouse label is built from SnapTrade's institution name, not the
nickname. `_stable_account_name` title-cases that name and, when the account
number has digits, appends the last four (`Fidelity ••••5678`). Otherwise the
label is `Fidelity Account`.

`broker_slug_from_account()` keeps the first word, lowercased. These labels
all land in the Fidelity adapters:

- `Fidelity ••••5678`
- `Fidelity Account`
- `Fidelity Investments ••••7766`
- `Fidelity NetBenefits ••••2222`

`tenant_id` stays `snaptrade:<uuid>`. The first word is display routing only.
A Fidelity row cannot also sit in the Schwab, Vanguard, Alpaca, IBKR, or
catch-all adapters (`dbt/tests/fidelity_partition_is_exclusive.sql`).

Fidelity is already a modeled broker, so the early-broker six-month Pro note
does not apply.

## What SnapTrade sends

Sources: SnapTrade account activities, the positions endpoint this app calls
(`get_user_account_positions`), and the holdings notes on cash equivalents.
The app does not call the newer positions API that uses `instrument.kind`.

**Account label.** Institution name is a string such as `Fidelity` or
`Fidelity Investments`. The first word is the route. A name that does not
start with Fidelity (the example to watch for is `National Financial
Services`) would miss this partition and land in the catch-all. Confirm the
live `institution_name` on the first connect.

**Symbols.** Documented as an object or null. A bare string is also accepted
(the same coerce used for Vanguard). A mutual fund such as FXAIX is Equity.
Options arrive as an `option_symbol` object (underlying, expiry, strike,
call/put). Long-form option text is promoted to OCC. `units` is the only
share count. SnapTrade's `fractional_units` is ignored completely, including
when `units` is null. Example: units 12 and fractional_units 0.25 stores 12.
units 40 and fractional_units 1.44 stores 40. The same number in both fields
stays that number because only `units` is read.

**Core position.** Money-market funds are included in cash and also returned
as positions with `cash_equivalent: true`. SnapTrade warns that adding those
positions on top of cash double-counts the account. SPAXX and FDRXX are the
usual Fidelity names, but the code does not hardcode those tickers. When the
flag is true the snapshot security type is `Cash and Money Market`.
`stg_current` drops that type, so it is not a holding. Share buys, sells, and
reinvestment companion buys of that symbol are not written on a sync that
still sees the flagged position. Dividends and interest on it are kept.
Buys already stored from an earlier sync are not deleted (the dedup key
does not remove rows).

An unflagged money-market fund (type text, or a price within two cents of
$1) is dropped the same way only when the broker cash balance already
includes it. Example: cash $5,200 already includes a $1,000 fund, stock
market value is $8,000, and the account total is $13,200. That total matches
cash plus the stock, so the fund is not a second holding. Example the other
way: cash $4,200 does not include the $1,000 fund, stock is $8,000, and the
same $13,200 total only matches if the fund stays a holding. A missing
account total keeps the fund as a holding. A market-priced mutual fund
(example FXAIX at $200) is never treated as cash.

**Activities the sync keeps.**

| SnapTrade type | Seed action |
| --- | --- |
| BUY / SELL | Buy / Sell (options become Buy to Open and the other open/close labels when the order says so) |
| DIVIDEND, SUBSTITUTE_DIVIDEND | Cash Dividend. `units` is often the share balance, not shares bought, so it does not become a Buy |
| REI, STOCK_DIVIDEND | Cash Dividend, plus a companion Buy when units × price matches the cash and the symbol is not a flagged sweep |
| INTEREST | Credit Interest |
| FEE, TAX | ADR Mgmt Fee (the label stays, because the dedup key includes Action) |
| CONTRIBUTION, DEPOSIT, INTERNAL_CASH_TRANSFER_IN | Deposit |
| WITHDRAWAL, INTERNAL_CASH_TRANSFER_OUT | Withdrawal |
| OPTIONEXPIRATION / OPTIONASSIGNMENT / OPTIONEXERCISE | Expired / Assigned / Exchange or Exercise |
| DISTRIBUTION with a ticker | Cash Dividend. No companion Buy |
| DISTRIBUTION with no ticker | Withdrawal |

**Activities the sync drops.** TRANSFER, share JOURNAL, SPLIT,
EXTERNAL_ASSET_TRANSFER_IN / OUT, and any type SnapTrade has not mapped.
Fidelity orders that 403 are ignored. That 403 does not mark the connection
broken. A disabled brokerage authorization is the broken signal.

**Cash vs margin.** Fidelity coverage for `cash_or_margin` is limited and
often `unknown`. That field is not a routing key.

## Empty book and a brand-new connection

A holdings timestamp from before the connection was created is not a stall
(the same fix as #278). `holdings_unavailable` does not raise a reconnect.
A young connection that was flagged only for that old timestamp shows
"Sync will retry", not "Reconnect needed". A connection that has not finished
its first sync shows "Waiting for the first sync".

A real stall still flags: the holdings clock stopped advancing after the
account was connected.

A columnless empty positions frame does not 500. Overview, Positions,
Strategies, and Settings render for a linked Fidelity account whose warehouse
frames are empty.

## Structural check

`scripts/vanguard_dev_structural_check.py --broker fidelity` reads
`analytics_dev` and `analytics_raw` with SELECT only. It prints pass, fail,
or absent, plus null rates. It does not print dollars, symbols, or tenant
ids. Zero raw Fidelity rows print `absent`. Raw rows that missed the Fidelity
adapter or the union print `fail`. A core-sweep-only snapshot does not fail
the holdings union, because `stg_current` drops `Cash and Money Market`.

The one-shot build on 2026-10-14
([run 38590557268](https://github.com/cameronjefferey/ccwj/actions/runs/38590557268),
commit `97ab3a63`) wrote `analytics_dev` only. dbt finished with one warning,
`option_close_qty_does_not_exceed_open`, the same warn-severity test as the
Vanguard dev build, and zero errors. Fidelity history and balances were
present, routed into the adapters and the unions, and the account-total
check passed. Every Fidelity schema test passed, including the action
allow-list and the partition exclusivity test. Current positions were
`absent`: the raw seed has no Fidelity holdings rows, so a live snapshot
(mutual funds, a flagged core position, options, fractional shares) has not
been seen in production data yet.

## Decisions Cameron made

All dollars are examples, not a customer's book. The mapping lives on PR
#285 and this branch is rebased onto that commit.

1. **DISTRIBUTION.** A ticker means income (Cash Dividend). No ticker means
   a cash withdrawal. Example: a $40 DISTRIBUTION on FXAIX is a dividend of
   +$40. A $40 DISTRIBUTION with no symbol is a withdrawal of −$40. Neither
   is dropped, and neither becomes a Buy.

2. **Share transfers stay off the tape.** TRANSFER and
   EXTERNAL_ASSET_TRANSFER_IN are not fills. Shares that are still held are
   priced at the broker's cost (`int_opening_balances`). Example: 10 shares
   still held, broker cost $500, inferred opening cash −$500. A transfer that
   arrived and was fully sold before the first snapshot can still be missing
   from realized P&L until a CSV fills the buy.

3. **`fractional_units` is ignored.** Only `units` is stored. Example: units
   12 and fractional_units 0.25 stores 12. units 40 and fractional_units 1.44
   stores 40. A null `units` does not fall through to the deprecated field.

4. **Unflagged money-market fund.** If the broker cash balance already
   includes it, it is not also a holding. Otherwise it stays a holding.
   Example: cash $5,200 includes a $1,000 fund, stock is $8,000, total
   $13,200 — the fund is cash. Cash $4,200, same fund and stock, same total
   — the fund stays a holding and its cost is in the account cost basis.

## Still open

5. **Confirm the live institution name on first connect.** If it is not a
   Fidelity* first word, the rows miss this partition. Do not add an alias
   until that string is seen. The example to watch for is `National Financial
   Services`.

6. **Fees stay labeled ADR Mgmt Fee.** A $1.25 Fidelity fee uses that seed
   action on purpose. Renaming it would split the dedup key and duplicate
   rows already stored for other brokers.
