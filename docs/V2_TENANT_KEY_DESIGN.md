# V2 Tenant Key Design — SnapTrade-only Architecture

**Status**: Active. Supersedes `docs/BROKER_ACCOUNT_ID_MIGRATION.md` and
`docs/USER_ID_TENANCY_EXPLAINER.md` (both archived).

**Why this exists.** Between 2026-05-19 and 2026-05-24 we shipped three
separate drift events on `broker_account_id` (Postgres SERIAL on
`broker_accounts.id`):

1. May 20 — `stg_canonical_account_owner` collapsed concurrent-active
   tenants under one canonical uid, hiding the signed-in user's data.
2. May 21 — Schwab refresh-token detector blind to `invalid_grant`,
   silent sync failures.
3. May 24 — Postgres SERIAL collision: `broker_accounts.id=14` and
   `=15` simultaneously stamped on (Alpaca Paper / deleted user) AND
   on (Cam's Schwab ••••5167 / Schwab ••••7686 / live uid=9) after a
   re-auth allocated those slot numbers fresh.

All three trace back to the same structural choice: **we minted our own
tenant identifier (`(user_id, account_name)` → later `broker_account_id`)
when the broker already ships a stable one**. SnapTrade per-account UUIDs
and Schwab per-account hashes never recycle. Postgres SERIALs do (on
deletes / migrations / DB resets). The v2 design uses the broker-issued
identifier directly and never mints one of our own.

This document is the contract between Postgres, dbt, BigQuery, and Flask
for the v2 architecture. Every change to one of those surfaces must
preserve the invariants below.

---

## Locked design decisions

1. **One broker integration: SnapTrade.** All broker connectivity flows
   through SnapTrade's Connection Portal. Direct broker integrations
   (e.g. native Schwab) are only added when SnapTrade demonstrably does
   not support the broker — codified in
   `.cursor/rules/snaptrade-only-broker-integrations.mdc`.
2. **One warehouse tenant key: `tenant_id`.** Format:
   `"<broker_slug>:<broker_uuid>"`. Examples:
   - `"snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"` (Fidelity)
   - `"snaptrade:7456275292a8a909ce7cd7423d9abc910a82d72b284d65138bd2d9b59397cde7"` (Schwab via SnapTrade)
3. **`tenant_id` is broker-stable, never recycled, never minted by us.**
4. **`tenant_id` is the join key in the warehouse.** `account_name`
   stays in the seeds as an informational display string. `user_id`
   stays in the seeds as informational metadata. Neither is the join
   key for tenant isolation.
5. **History loss accepted.** No backfill of pre-v2 data. Seeds wipe to
   header-only. Daily Review starts at zero on cutover.

---

## Tenant key format spec

```
tenant_id := "<broker_slug>:<broker_uuid>"

broker_slug  := lowercase letters, digits, underscores. Currently always
                "snaptrade". Future direct integrations (rare) get their
                own slug, e.g. "schwab_direct", "ibkr_direct".

broker_uuid  := the broker-issued stable identifier for one physical
                account, exactly as returned by the broker's API. For
                SnapTrade this is the "id" field on
                AccountSimple.id (lowercase UUID, e.g.
                "bed78305-a764-4c4d-b4c7-fe59e391f661"). NEVER munged,
                NEVER stripped, NEVER re-cased.

Total shape  := utf-8 string, max ~128 chars. Globally unique by
                construction (broker_slug namespace + broker UUID
                uniqueness within broker).
```

**Properties that fall out of this:**

- **Collision-proof across Postgres resets.** Drop the
  `broker_tenants` table tomorrow and recreate it: re-syncs from
  SnapTrade produce the EXACT SAME `tenant_id` strings. Seed rows on
  disk still link to live broker_tenants rows. No drift possible.
- **No fan-out per user.** One physical broker account → one
  `tenant_id`. If parent + child both connect to the same brokerage,
  they're two SnapTrade users, two SnapTrade connections, two
  `tenant_id`s — even if they have visibility into the same physical
  account. (SnapTrade enforces one connection per SnapTrade user per
  broker.)
- **No orphan tenancy on user_id changes.** Postgres `users.id`
  renumbers or merges don't break warehouse joins because the join key
  doesn't reference `users.id`.

---

## Postgres schema

### New table: `broker_tenants`

```sql
CREATE TABLE broker_tenants (
    tenant_id          TEXT PRIMARY KEY,
    user_id            INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    broker_slug        TEXT NOT NULL,           -- always 'snaptrade' for now
    broker_uuid        TEXT NOT NULL,           -- SnapTrade's account UUID
    account_name       TEXT NOT NULL,           -- display string e.g. "Fidelity ••••6342"
    account_mask       TEXT,                    -- last-4 if known
    broker_label       TEXT,                    -- which broker this is (Schwab, Fidelity, etc.)
    snaptrade_connection_id TEXT,               -- SnapTrade brokerageAuthorization UUID
    connection_status  TEXT DEFAULT 'active',   -- 'active' | 'disabled' | 'pending_reconnect'
    connection_broken_at TIMESTAMP,             -- set when SnapTrade webhook says disabled
    first_sync_completed BOOLEAN DEFAULT FALSE,
    display_nickname   TEXT,                    -- optional user-set name for multi-account UX
    created_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (broker_slug, broker_uuid),
    UNIQUE (user_id, snaptrade_connection_id, broker_uuid)
);

CREATE INDEX broker_tenants_user_id_idx ON broker_tenants(user_id);
CREATE INDEX broker_tenants_status_idx ON broker_tenants(connection_status) WHERE connection_status != 'active';
```

The `(broker_slug, broker_uuid)` UNIQUE constraint is the structural
guarantee that the same physical broker account always resolves to the
same `tenant_id`. The `(user_id, snaptrade_connection_id, broker_uuid)`
UNIQUE is the per-user-per-account uniqueness (one row per user per
physical account).

### Keep: `snaptrade_users` (renamed from `snaptrade_connections`)

```sql
CREATE TABLE snaptrade_users (
    user_id        INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    snaptrade_user_id TEXT NOT NULL,             -- SnapTrade's userId (we set to "happytrader-<user_id>")
    snaptrade_user_secret TEXT NOT NULL,         -- bearer credential, encrypted at rest
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
```

This holds the OAuth-side credentials for talking to SnapTrade.
One row per HappyTrader user. Separate from `broker_tenants` because
SnapTrade credentials are per-user, not per-account.

### Drop entirely

- `schwab_connections` — direct Schwab is gone.
- `broker_accounts` — replaced by `broker_tenants` with broker-stable key.
- `snaptrade_accounts` — collapsed into `broker_tenants`.
- `snaptrade_connections` — renamed to `snaptrade_users` (rename + drop
  unused columns).

### Keep unchanged

- `users` — login table, untouched.
- `journal_entries` / `journal_tags` — dead but not blocking the migration; sweep later.

---

## Postgres helpers (`app/models.py`)

New API (replaces every `schwab_*` and `snaptrade_*` connection helper):

```python
def get_or_create_broker_tenant(
    user_id: int,
    broker_slug: str,
    broker_uuid: str,
    account_name: str,
    account_mask: str | None = None,
    broker_label: str | None = None,
    snaptrade_connection_id: str | None = None,
) -> str:
    """Returns the tenant_id ('<broker_slug>:<broker_uuid>').
    Upsert on (broker_slug, broker_uuid). Idempotent."""

def get_tenant_ids_for_user(user_id: int) -> list[str]:
    """Active tenants the user can see. Excludes 'disabled' status.
    Returns [] for unknown user or user with no connections."""

def get_broker_tenant(tenant_id: str) -> dict | None:
    """Full row by tenant_id, or None."""

def get_broker_tenants_for_user(user_id: int) -> list[dict]:
    """Full rows (for the Settings → Account page list)."""

def mark_tenant_connection_broken(tenant_id: str) -> None:
    """Set connection_status='disabled' + connection_broken_at=NOW().
    Idempotent on connection_broken_at (preserves first-detection ts)."""

def clear_tenant_connection_broken(tenant_id: str) -> None:
    """Set connection_status='active' + connection_broken_at=NULL.
    Called from snaptrade_callback after successful re-auth."""

def get_snaptrade_user(user_id: int) -> dict | None:
    """SnapTrade userId/userSecret pair, or None if user hasn't
    signed up with SnapTrade yet."""

def upsert_snaptrade_user(user_id: int, snaptrade_user_id: str,
                          snaptrade_user_secret: str) -> None:
    """Upsert the SnapTrade credentials for a HappyTrader user."""
```

---

## Seed CSV schema

Three primary tenant-tied seeds. Each gets the new `tenant_id`
column. **`broker_account_id` is REMOVED entirely.** `account` and
`user_id` stay for display / legacy compat but are NOT the join key.

### `dbt/seeds/trade_history.csv`

```
account, user_id, tenant_id, Date, Action, Symbol, Description,
Quantity, Price, fees_and_comm, Amount
```

### `dbt/seeds/current_positions.csv`

```
account, user_id, tenant_id, Symbol, Description, Quantity, Price,
price_change_dollar, price_change_percent, market_value,
day_change_dollar, day_change_percent, cost_bases, gain_or_loss_dollat,
gain_or_loss_percent, rating, divident_reinvestment, is_capital_gain,
percent_of_account, expiration_date, cost_per_share,
last_earnings_date, dividend_yield, last_dividend, ex_dividend_date,
pe_ratio, annual_week_low, annual_week_high, volume,
intrinsic_value, in_the_money, security_type, margin_requirement
```

### `dbt/seeds/account_balances.csv`

```
account, user_id, tenant_id, row_type, market_value, cost_basis,
unrealized_pnl, unrealized_pnl_pct, percent_of_account
```

### Dropped seeds

- `dbt/seeds/schwab_account_balances.csv` — direct Schwab is gone;
  collapse into `account_balances.csv`.

### Demo seeds (untouched)

`demo_current.csv` / `demo_history.csv` keep their existing shape;
they're the shared-demo-user fixtures.

---

## dbt model changes

### New staging convention

Every user-tied staging model (`stg_history`, `stg_current`,
`stg_account_balances`) reads `tenant_id` from the seed and emits it
unchanged. Stamping at the seed level means downstream models don't
have to do any tenancy gymnastics — `tenant_id` is just another column.

### New dim model: `dim_broker_tenants`

```sql
-- dbt/models/staging/dim_broker_tenants.sql
-- Sourced from the raw seeds (NOT from staging, same rationale as
-- the old dim_broker_accounts: avoid canonical-uid rewrites in staging).
-- Materializes the (tenant_id → user_id, account_name, broker_label)
-- mapping that Flask reads to filter at the warehouse boundary.

with tenants as (
    select distinct
        tenant_id,
        safe_cast(user_id as int64) as user_id,
        trim(account) as account_name
    from {{ ref('trade_history') }}
    where tenant_id is not null and tenant_id != ''

    union distinct

    select distinct
        tenant_id,
        safe_cast(user_id as int64) as user_id,
        trim(account) as account_name
    from {{ ref('current_positions') }}
    where tenant_id is not null and tenant_id != ''

    union distinct

    select distinct
        tenant_id,
        safe_cast(user_id as int64) as user_id,
        trim(account) as account_name
    from {{ ref('account_balances') }}
    where tenant_id is not null and tenant_id != ''
)

select
    tenant_id,
    any_value(user_id) as user_id,
    any_value(account_name) as account_name,
    count(*) as source_row_count
from tenants
group by tenant_id
```

### Deleted models

- `dbt/models/staging/stg_canonical_account_owner.sql` — orphan
  tenancy is structurally impossible under v2.
- `dbt/models/staging/dim_broker_accounts.sql` — replaced by
  `dim_broker_tenants`.
- `dbt/models/staging/stg_snapshot_account_balances_daily.sql` —
  canonical-uid wrapper, not needed.
- `dbt/models/staging/stg_snapshot_options_market_values_daily.sql` —
  same.

### Deleted tests

- `dbt/tests/no_orphan_user_id_per_account.sql`
- `dbt/tests/no_stale_user_id_in_history.sql`
- `dbt/tests/every_seed_row_has_broker_account_id.sql`
- `dbt/tests/seed_broker_account_id_unique_per_account_user.sql`
- `dbt/tests/dim_broker_accounts_unique_per_id.sql`
- `dbt/tests/dim_broker_accounts_unique_per_user_account_pair.sql`
- `dbt/tests/every_broker_account_id_exists_in_dim_broker_accounts.sql`

### New tests

- `dbt/tests/every_seed_row_has_tenant_id.sql` — error severity day 1.
  Every row in `trade_history`, `current_positions`, `account_balances`
  has `tenant_id IS NOT NULL AND tenant_id LIKE '%:%'`.
- `dbt/tests/tenant_id_format_valid.sql` — error severity. Every
  `tenant_id` matches `^[a-z_]+:[a-zA-Z0-9-]+$`.
- `dbt/tests/dim_broker_tenants_unique.sql` — error severity. One row
  per `tenant_id` in the dim (structural; falls out of the GROUP BY but
  belt-and-suspenders).

### Models that need rewrites

Every model that currently does `... AND user_id = X AND account IN
(...)` becomes `... AND tenant_id IN (...)`. The full list (~30-40):

- All `stg_*` (3 main models): pass `tenant_id` through.
- All `int_*` models that union/join across staging: pass through.
- All `mart_*` that aggregate per-tenant: group by `tenant_id` instead
  of `(account, user_id)`.
- `int_strategy_classification`, `int_position_legs`,
  `int_equity_sessions`, `int_closed_equity_legs`,
  `int_option_contracts`, `int_option_contract_daily_pnl`,
  `int_option_rolls`, `int_dividend_events`, `int_tax_lots`,
  `int_enriched_current`, `int_drip_fills`, etc.
- `mart_daily_pnl`, `mart_account_equity_daily`,
  `mart_account_snapshots_enriched`, `mart_strategy_performance`,
  `mart_strategy_trend`, `mart_benchmark`, `mart_weekly_summary`,
  `mart_weekly_trades`, `mart_weekly_streaks`, `mart_weekly_behavior_enriched`,
  `mart_coaching_signals`, `mart_daily_trading_metrics`,
  `mart_wealth_daily`, `mart_option_trades_by_kind`,
  `positions_summary`.

### Untouched

- `stg_daily_prices` (symbol-only, no tenancy).
- `int_split_factors`, `stg_split_events`, `daily_split_events`
  (symbol-only).
- `stg_crypto_symbols` (symbol-only).
- `int_demo_equity_daily`, `stg_demo_history`, `stg_demo_current`
  (demo user, tenancy by `user_id='demo'` literal — keep as-is).
- `stg_history` macros, `attribute_dividends_to_strategy` macro
  (logic-only, tenancy passes through).

---

## Flask filter API

### New helpers (`app/routes.py`)

```python
def _resolve_filter_tenant_ids(user_id: int | None,
                               requested: list[str] | None) -> list[str] | None:
    """
    Returns the tenant_id list to scope a query by, or None for admin
    bypass. Semantics mirror the legacy _resolve_filter_user_id:
        - user_id=None → None (admin bypass, no tenancy filter)
        - user_id set, requested=None → ALL the user's tenants
        - user_id set, requested set → INTERSECTION of requested and the
          user's tenants (so a malicious URL ?tenant=other-users-id is
          dropped at this layer, not at SQL).
        - Empty list → empty list (fail-closed; downstream produces
          ``AND 1 = 0``).
    """

def _tenant_sql_filter(tenant_ids: list[str] | None,
                       col: str = "tenant_id") -> str:
    """
    Returns an ``AND <col> IN ('a', 'b', ...)`` SQL fragment, or empty
    string for admin bypass (tenant_ids=None), or ``AND 1 = 0`` for
    empty list. Identifiers are passed through ``re.sub(r'[^A-Za-z0-9_.]', '', col)``
    and values are passed through string-escape for safety; tenant_ids
    are well-formed UUIDs by construction so injection risk is minimal,
    but we still escape.
    """

def _filter_df_by_tenant_ids(df: pd.DataFrame,
                             tenant_ids: list[str] | None,
                             col: str = "tenant_id") -> pd.DataFrame:
    """
    Python-side belt-and-suspenders filter. tenant_ids=None →
    return df unchanged (admin). Empty list → return empty df. Drops
    rows where ``df[col]`` is NULL or not in the allowlist.
    """
```

### Deleted helpers

- `_resolve_filter_user_id` — `user_id` is no longer the tenancy key.
- `_account_sql_and`, `_account_sql_filter` — replaced by
  `_tenant_sql_filter`.
- `_filter_df_by_accounts` — replaced by `_filter_df_by_tenant_ids`.
- `_resolve_filter_broker_account_ids`, `_broker_account_sql_and`,
  `_broker_account_sql_filter`, `_filter_df_by_broker_account_ids`
  (Stage 3A helpers that never got wired in — short-lived dead code).

### Usage pattern (all routes)

```python
@app.route("/some-page")
@login_required
def some_page():
    requested = request.args.getlist("tenant")  # was: request.args.getlist("account")
    tenant_ids = _resolve_filter_tenant_ids(current_user.id, requested or None)

    df = _bq_query(QUERY, params={"tenant_ids": tenant_ids})
    df = _filter_df_by_tenant_ids(df, tenant_ids)  # defense in depth
    # ... render ...
```

The `?account=` URL parameter becomes `?tenant=`. Old `?account=`
URLs will silently land on "no filter" (all tenants) which is the
existing safe default for unknown filter values.

---

## Sync emit shape

### `app/snaptrade.py` writes seed rows

For each broker account SnapTrade returns:

```python
tenant_id = get_or_create_broker_tenant(
    user_id=current_user.id,
    broker_slug="snaptrade",
    broker_uuid=snaptrade_account.id,       # UUID string from SnapTrade
    account_name=snaptrade_account.name,    # "Fidelity ••••6342"
    account_mask=last4(snaptrade_account.number),
    broker_label=snaptrade_account.brokerage_authorization.brokerage.name,
    snaptrade_connection_id=snaptrade_account.brokerage_authorization.id,
)

# Then every row emitted to trade_history.csv / current_positions.csv /
# account_balances.csv carries tenant_id verbatim.
row = {
    "account": snaptrade_account.name,   # display only
    "user_id": current_user.id,           # informational
    "tenant_id": tenant_id,              # the join key
    # ... rest of row ...
}
```

### `app/upload.py` `merge_and_push_seeds`

Dedup key on writes:

- `trade_history.csv` → dedup on `(tenant_id, Date, Action, Symbol, Quantity, Price, Amount)`.
- `current_positions.csv` → dedup on `(tenant_id, Symbol, security_type)` (one row per
  open position per tenant).
- `account_balances.csv` → dedup on `(tenant_id, row_type)`.

Note: `(tenant_id, ...)` everywhere replaces the v1 `(account, user_id, ...)`
dedup keys. Same logical effect, structurally collision-proof.

---

## Migration sequencing recap

Cross-references the parent plan in `.cursor/plans/snaptrade-only_v2_reset_*.plan.md`:

1. Phase 0 — verify SnapTrade-Schwab works for Cam (operator action).
2. Phase 1 — this doc.
3. Phase 2 — Postgres groundwork (additive: new tables alongside old).
4. Phase 3 — dbt seed truncation + model rewrite (deploys empty marts).
5. Phase 4 — SnapTrade sync emits new schema.
6. Phase 5 — Flask routes cutover.
7. Phase 6 — production cutover (drop old Postgres tables, Cam + Jeff re-auth).
8. Phase 7 — delete direct Schwab.
9. Phase 8 — docs + policy lockdown.

---

## Invariants (write-protect after Phase 6)

After cutover, every new pull request that touches a broker surface
MUST preserve these:

1. **No row enters `trade_history.csv`, `current_positions.csv`, or
   `account_balances.csv` without a valid `tenant_id`.** Enforced by
   `every_seed_row_has_tenant_id` dbt test (error severity).
2. **`tenant_id` is never minted, transformed, hashed, or re-cased
   in transit.** What SnapTrade returns is what hits the seed.
3. **No `(user_id, account)` join key in dbt models.** Tenant joins
   are on `tenant_id` and only on `tenant_id`. The `(user_id,
   account)` pair stays in marts for display but is never the
   isolation boundary.
4. **No `broker_account_id` anywhere.** Column is gone from seeds,
   models, Postgres, Python helpers, and tests.
5. **One SnapTrade connection per HappyTrader user per broker.**
   SnapTrade enforces this; we don't try to work around it.
6. **`tenant_id` IS NULL means "demo data" or "ingestion gap"**, both
   of which fail-closed (admin-only visibility, never shown to a
   logged-in user). The fail-closed branch lives in
   `_filter_df_by_tenant_ids` Python-side.

Future bug entries in `~/.cursor/skills/broker-sync-safety/SKILL.md`
that touch tenancy must cross-reference which invariant fired.

---

## What this design explicitly does NOT solve

- **Schwab 7-day refresh-token policy.** SnapTrade still hits Schwab's
  hard 7-day OAuth wall when their integration runs against Schwab.
  Same UX as v1 in that respect — SnapTrade just owns the reconnect
  UI now. A separate "proactive reconnect banner T-minus-1-day"
  feature is out of scope for v2 and tracked separately.
- **Multi-tenant on one broker login.** SnapTrade enforces one
  SnapTrade-user per broker login. If two HappyTrader users share a
  brokerage account (parent + child) they'll have two distinct
  `tenant_id`s. That's correct by design — they should see different
  filtered slices.
- **Real-time pricing.** Whether SnapTrade is real-time or
  daily-cached is a billing-plan question, orthogonal to the tenant
  key design.
- **Historical pre-cutover data.** Deleted on purpose.
