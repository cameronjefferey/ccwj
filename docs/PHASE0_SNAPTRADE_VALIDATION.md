# Phase 0 — SnapTrade validation gate

**Status:** Operator checklist. Must pass before production v2 cutover (Phase 6).

Phase 0 proves SnapTrade can replace native Schwab for HappyTrader's primary user(s).
If any blocking item fails, stop cutover and file a SnapTrade support ticket or amend
`.cursor/rules/snaptrade-only-broker-integrations.mdc` with a documented exception.

**Active architecture:** [`docs/V2_TENANT_KEY_DESIGN.md`](V2_TENANT_KEY_DESIGN.md)

---

## 1. Schwab via SnapTrade (required)

| Check | Pass? | Notes |
|-------|-------|-------|
| Schwab appears in SnapTrade Connection Portal for production Client ID | ☐ | |
| OAuth completes and returns ≥1 account row in `/snaptrade/accounts` | ☐ | |
| `broker_tenants.tenant_id` shape is `snaptrade:<uuid>` (lowercase UUID) | ☐ | |
| First sync writes header + rows to all three seeds with `tenant_id` populated | ☐ | |
| `dbt build` green after push; `every_seed_row_has_tenant_id` passes | ☐ | |
| Position Detail renders for a Schwab-linked symbol with non-zero KPIs | ☐ | |
| Option contract on Schwab account shows MTM chart shape (not cash-only spikes) | ☐ | |
| Dividend ETF (e.g. JEPI) shows dividend dollars after sync + dbt | ☐ | |

## 2. Data shape checklist (all brokers)

Every synced row in `dbt/seeds/*.csv` must match:

| Column | Rule |
|--------|------|
| `tenant_id` | Required on every row; format `snaptrade:<uuid>` |
| `account` | Display string from SnapTrade; **not** the join key |
| `user_id` | Informational metadata only; **not** the join key |
| `broker_account_id` | Must **not** appear (v1 column dropped) |

Singular tests (error severity):

- `every_seed_row_has_tenant_id`
- `dim_broker_tenants_unique`
- `stg_history_no_duplicate_fills_per_tenant`

## 3. Non-Schwab smoke (recommended)

Repeat connect + sync for at least one non-Schwab broker (Fidelity, Vanguard, or Robinhood):

| Check | Pass? |
|-------|-------|
| Connect + first sync completes | ☐ |
| Positions list shows expected open symbols | ☐ |
| Daily Review attribution table non-empty | ☐ |

## 4. Known limitations (accept or document)

- **History depth varies by broker.** SnapTrade clamps to broker file depth; first sync may
  not reach 5 years even when UI asks for full history.
- **Schwab 7-day OAuth.** SnapTrade owns reconnect UX; stale connections surface via
  `connection_broken_at` banner (not native Schwab refresh-token cron).
- **Indexing lag.** First trade history may land hours after positions.

## 5. Sign-off

| Role | Name | Date | Result |
|------|------|------|--------|
| Operator | | | ☐ Go / ☐ No-go |

On **Go**, proceed to Phase 6 cutover using `scripts/admin/v2_cutover_reset.py`.
