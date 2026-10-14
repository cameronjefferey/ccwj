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
call/put). Long-form option text is promoted to OCC. `units` is the share
count. `fractional_units` is deprecated. A remainder strictly between 0 and 1
is added to a whole-share `units` (12 and 0.25 is 12.25). The same number in
both fields is stored once. A `fractional_units` of 1 or more is not added.

**Core position.** Money-market funds are included in cash and also returned
as positions with `cash_equivalent: true`. SnapTrade warns that adding those
positions on top of cash double-counts the account. SPAXX and FDRXX are the
usual Fidelity names, but the code trusts the flag, not the ticker. When the
flag is true the snapshot security type is `Cash and Money Market`.
`stg_current` drops that type, so it is not a holding. Share buys, sells, and
reinvestment companion buys of that symbol are not written on a sync that
still sees the flagged position. Dividends and interest on it are kept.
Buys already stored from an earlier sync are not deleted (the dedup key
does not remove rows). When the flag is false the symbol stays Equity.

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

**Activities the sync drops.** TRANSFER, share JOURNAL, DISTRIBUTION, SPLIT,
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

## Decisions that need Cameron

All dollars are examples, not a customer's book.

1. **Flag false on SPAXX / FDRXX.** Keep it as Equity. A $1,000 settlement
   fund with `cash_equivalent` false can show both as a holding and inside
   cash. When the broker sends an account total, that total is what the
   balance row uses.

2. **DISTRIBUTION stays dropped.** A $40 fund distribution (symbol set) and
   a $40 retirement cash-out (no symbol) look the same in the type string.
   Mapping either one without a live sample would book the other wrong.

3. **Share transfers stay dropped.** TRANSFER and EXTERNAL_ASSET_TRANSFER_IN
   / OUT are not cash and not a priced fill. Example: 10 shares arrive with
   no cash, then are sold inside the window. Opening-balance inference
   recovers the shares that are still held. A transfer that arrived and was
   fully sold before the first snapshot can be missing from realized P&L
   until a CSV fills the buy.

4. **`fractional_units` of 1 or more is not added to `units`.** SnapTrade
   deprecated the field. Example: units 40 and fractional_units 1.44 stores
   40. units 12 and fractional_units 0.25 stores 12.25. The same number in
   both fields stays that number.

5. **Confirm the live institution name on first connect.** If it is not a
   Fidelity* first word, the rows miss this partition. Do not add an alias
   until that string is seen.

6. **Fees stay labeled ADR Mgmt Fee.** A $1.25 Fidelity fee uses that seed
   action on purpose. Renaming it would split the dedup key and duplicate
   rows already stored for other brokers.
