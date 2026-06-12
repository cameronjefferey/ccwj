#!/usr/bin/env python
"""Merge production seeds with local dev-seeds for the dev warehouse build.

The dev environment (analytics_dev) is a FULL MIRROR of production data
plus the local environment's own syncs, so everything can be tested
locally with real data before code ships:

    merged seed = origin/master rows (prod tenants, local tenants excluded)
                + origin/dev-seeds rows (local tenants, freshest copy)

Local tenants are whatever the local Postgres ``broker_tenants`` table
says — never a hardcoded list, and never a numeric ``user_id`` (ids
collide across environments; tenant_id is the only cross-env-stable key).
Master may still carry stale copies of local tenants (synced before the
env split); those are dropped in favor of the dev-seeds rows.

Usage (called by scripts/dev-refresh.sh):

    merge_dev_seeds.py <repo_root> <worktree_seeds_dir>

``worktree_seeds_dir`` must already contain the dev-seeds checkout; the
tenant-keyed seed files are rewritten in place with the merged rows.
"""

import io
import subprocess
import sys

import pandas as pd

TENANT_SEEDS = ["trade_history.csv", "current_positions.csv", "account_balances.csv"]


def local_tenant_ids():
    from dotenv import load_dotenv

    load_dotenv()  # DATABASE_URL for app.db
    from app.db import fetch_all

    rows = fetch_all("SELECT tenant_id FROM broker_tenants")
    ids = {r["tenant_id"] for r in rows if r.get("tenant_id")}
    if not ids:
        # Fail loudly: an empty list would silently keep master's stale
        # copies of local tenants alongside fresh dev-seeds rows (dupes).
        raise SystemExit("merge_dev_seeds: local broker_tenants returned no tenant_ids")
    return ids


def master_seed(repo_root, name):
    out = subprocess.run(
        ["git", "-C", repo_root, "show", f"origin/master:dbt/seeds/{name}"],
        check=True,
        capture_output=True,
    )
    return pd.read_csv(io.BytesIO(out.stdout), dtype=str, keep_default_na=False)


def main():
    repo_root, seeds_dir = sys.argv[1], sys.argv[2]
    sys.path.insert(0, repo_root)
    local_ids = local_tenant_ids()
    print(f"local tenants: {len(local_ids)}")

    for name in TENANT_SEEDS:
        prod = master_seed(repo_root, name)
        dev = pd.read_csv(f"{seeds_dir}/{name}", dtype=str, keep_default_na=False)
        prod_rows = prod[~prod["tenant_id"].isin(local_ids)]
        # concat unions columns if the two branches' schemas ever drift;
        # column order follows master (schema changes land there first).
        merged = pd.concat([prod_rows, dev], ignore_index=True)
        merged = merged[
            list(prod.columns) + [c for c in dev.columns if c not in prod.columns]
        ]
        merged.to_csv(f"{seeds_dir}/{name}", index=False)
        print(
            f"{name}: {len(prod)} master rows -> kept {len(prod_rows)} prod"
            f" + {len(dev)} dev = {len(merged)}"
        )


if __name__ == "__main__":
    main()
