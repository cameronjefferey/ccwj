# Local test environment

How to test changes locally against **real production data** before they
reach paying customers — including logging in as your own prod-scoped self.

The warehouse mirror lives in BigQuery (`analytics_dev`), not on your laptop.
Any machine with the local `.env` (laptop, cloud agent, a staging Render
service) reads the same clone. CI refreshes that clone after every prod
warehouse build, so "the local app doesn't have the right data" should no
longer mean a 10-minute `dbt build` on your machine.

## TL;DR — one command

```bash
./scripts/dev.sh            # start the app at http://localhost:5000 (hot-reload)
./scripts/dev.sh --sync     # clone latest prod marts into analytics_dev, then start
./scripts/dev.sh --link     # one-time: become your prod-scoped self, then start
./scripts/dev.sh --refresh  # rebuild analytics_dev from YOUR dbt models (slow)
```

`scripts/dev.sh` is the single entry point. The daily loop is just
`./scripts/dev.sh` — edits to templates/routes/Python hot-reload instantly.
Add `--sync` when the UI looks empty or stale (fast BigQuery COPY; CI
usually already did this). Add `--refresh` only when you changed **dbt
models** and need to see *your* SQL against real raw data. `--link` is a
one-time step to "become" your prod self.

First-time setup (once):

```bash
# In .env (see .env.example):
#   BQ_DATASET=analytics_dev
#   BQ_RAW_DATASET=analytics_raw_dev
#   SNAPTRADE_USER_NAMESPACE=local
#   DEV_PROD_USERNAME=<your-prod-username>
#   DEV_PROD_USER_ID=<n>          # from ./scripts/dev.sh --list-tenants --no-run
./scripts/dev.sh --sync --link    # clone marts + attach your tenants, then start
flask reset-password --username <your-prod-username>   # set a known local pw
```

Then every day after: just `./scripts/dev.sh`. If numbers look yesterday's,
`--sync`. If you edited dbt models, `--refresh`.

`dev.sh` refuses to start unless `BQ_DATASET=analytics_dev` **and**
`BQ_RAW_DATASET=analytics_raw_dev`, so you can never accidentally run the
local app against the prod warehouse or write local syncs into the prod
raw seed tables.

## Two speeds of "get the right data"

| Flag | What it does | When to use |
| --- | --- | --- |
| `--sync` | COPY prod `analytics` → `analytics_dev` (tables + rewritten views). Read-only on prod. Does **not** run dbt. Does **not** touch `analytics_raw_dev`. | UI / Flask / template work. "Local doesn't look like prod." |
| `--refresh` | Merge prod raw + local syncs into `analytics_raw_dev`, backfill snapshots, `dbt build` into `analytics_dev` from **this working tree** (`dbt/profiles.dev.yml`). | You changed a dbt model and need to verify it against real data *before* it ships. |

CI (`.github/workflows/dev_mirror.yml`) runs `--sync` after every prod
`bigquery_update` / evening prices job, so `analytics_dev` stays within one
build of what customers see. `--sync` locally is the on-demand backstop
(manual Actions dispatch also exists).

`--refresh` overwrites the clone with *your* computed marts. That's the
point of a model change; run `--sync` afterwards if you want to throw the
experiment away and go back to prod numbers.

## Under the hood

`dev.sh` orchestrates the pieces below; you can also run them directly.

## Why this is set up the way it is

Local dev is **environment-separated** from production so dev writes can never
corrupt prod (numeric `user_id`s collide across the two Postgres databases — an
admin purge by `user_id` once deleted the other environment's rows). Two knobs
in `.env` enforce the split:

| Knob | Local value | Effect |
| --- | --- | --- |
| `BQ_DATASET` | `analytics_dev` | Every app query's hardcoded `ccwj-dbt.analytics.` ref is rewritten to `analytics_dev` at the `get_bigquery_client()` chokepoint. Local reads never touch prod's warehouse. |
| `BQ_RAW_DATASET` | `analytics_raw_dev` | The app's seed writers (sync/upload/purge, `app/seed_store.py`) WRITE_TRUNCATE the raw seed tables in this dataset. Prod writes go to `analytics_raw`; dev writes never touch it, and non-prod writes never dispatch a CI rebuild. |

Production leaves **both unset**.

## Seeing a scoped single-user view: the tenant mapping

The warehouse holds everyone's data, but the Flask UI scopes a logged-in user
to the rows whose **`tenant_id`** is in *their* local Postgres `broker_tenants`
table (`get_tenant_ids_for_user`). Local Postgres is a different database from
prod, so prod users (and their tenant rows) don't exist locally. That's the one
gap, and you have two ways to close it:

### Option A — Admin all-data view (zero setup)

Log in locally as an admin (`ADMIN_USERS=cameron3,happycameron`). Admins
**bypass** tenant scoping (`_user_tenant_list` returns `None`), so you instantly
see *all* of `analytics_dev`, including your own data.

- **Good for:** "does my change render against real data."
- **Not good for:** testing per-user scoping, empty states, or the exact
  single-user experience — you see the union of everyone.

### Option B — Be your own prod-scoped self (recommended)

Mirror your prod `broker_tenants` rows into local Postgres under a local user,
preserving `tenant_id` verbatim (the env-stable warehouse join key).

**Preferred — no prod database password.** Usernames are not in the warehouse,
so you pick your prod numeric `user_id`:

```bash
./scripts/dev.sh --list-tenants --no-run     # prints user_id → accounts
# then in .env: DEV_PROD_USER_ID=<n>  DEV_PROD_USERNAME=<you>
./scripts/dev.sh --link
```

or directly:

```bash
python scripts/dev-link-prod-tenants.py --from-warehouse \
  --prod-user-id <n> --local-username <you> --create-local-user
```

The warehouse path reads `dim_broker_tenants` in prod `analytics` (read-only)
and upserts those `tenant_id`s into local Postgres. It never opens prod
Postgres.

**If you do have a read-only `PROD_DATABASE_URL`:**

```bash
PROD_DATABASE_URL=postgresql://USER:PASS@HOST:PORT/DB \
  python scripts/dev-link-prod-tenants.py \
    --prod-username <your-prod-username> \
    --create-local-user
```

Then `flask reset-password --username <you>` to set a known local password and
log in. You now see **exactly your prod-scoped slice**, behaving like prod.

The script:
- opens prod **read-only** (Postgres path) or BigQuery **read-only** (warehouse
  path) and never writes to either;
- copies only `tenant_id` + display columns, **never** the numeric `user_id`
  (remapped to your local user's id — ids aren't env-stable);
- keeps `user_accounts` labels in sync for legacy surfaces;
- supports `--dry-run`, `--include-inactive`, `--local-username`, and
  `--rows-file` (manual JSON export fallback).

After linking, the warehouse rows for those tenant_ids are typically already in
`analytics_dev` (CI clone, or `--sync`). Run `./scripts/dev.sh --sync` if the
UI is empty. Note: `scripts/dev_refresh_raw.py` keeps a mirrored prod tenant's
prod data on `--refresh` — it only replaces prod rows for local tenants that
have a fresher local copy in `analytics_raw_dev` — so refreshing won't blank
your mirrored view.

## Impersonation

Once a prod-mirrored user exists locally (Option B), admins can switch into it
via the `/admin/users` **Impersonate** button (or `/admin/impersonate/<username>`).
Impersonation operates on **local Postgres users only** — it can't reach a prod
user that doesn't exist in your local DB, which is exactly what Option B
creates.

## Gotcha: don't re-link via SnapTrade to get prod data

`SNAPTRADE_USER_NAMESPACE=local` namespaces local SnapTrade userIds, so
re-linking an account through the local Connect Portal mints **new**
`tenant_id`s (different broker UUID). Those won't match the prod seed rows —
you'd see freshly-synced local data, not your prod history. To see prod
history, **copy the existing tenant_ids** (Option B); don't re-sync.

## Validating dbt changes before pushing

```bash
cd dbt && ../.venv/bin/dbt parse                            # fast, offline
./scripts/dev.sh --refresh --no-run                         # builds analytics_dev only
# equivalent:
./scripts/dev-refresh.sh
```

`dbt parse` is fast/offline. Dev builds use `dbt/profiles.dev.yml`
(`dataset: analytics_dev`) via a temp `--profiles-dir` — the repo's
`dbt/profiles.yml` otherwise wins and silently targets prod `analytics`.
Plain `cd dbt && dbt build` is still the prod path; do not point it at
`analytics_dev` by editing that file.

## Optional: a Render staging service (from anywhere)

`analytics_dev` is already shared. A second Render web service that sets
`BQ_DATASET=analytics_dev`, `BQ_RAW_DATASET=analytics_raw_dev`,
`SNAPTRADE_USER_NAMESPACE=staging`, and its **own** Postgres (never prod
`DATABASE_URL`) is a hosted twin of local `./scripts/dev.sh`. Same safety
rails, reachable from a phone. Do **not** point staging at prod Postgres or
unset `BQ_DATASET` — that is production.
