"""
Analyze the append-only ``snaptrade_sync_observations`` log to answer:

    "How many minutes after the 4pm ET close does each broker's
     holdings_last_successful_sync actually advance?"

This is the empirical input for retiming the SnapTrade sync cron (CLOSE-BASED
REPORTING plan, Phases 3 & 4). Each successful sync run appends a row with:
  cron_run_at                   = when our sync ran (UTC)
  holdings_last_successful_sync = SnapTrade's authoritative "broker data as of"

The lag we care about is ``cron_run_at - holdings_last_successful_sync`` at run
time (how stale the broker data was when we read it) and, day over day, the
clock time at which ``holdings_last_successful_sync`` first lands on/after the
session's 4pm ET close. The earlier the cron can run while still catching the
settled close, the fresher the page is each evening.

Run locally:   python scripts/analyze_snaptrade_sync_timing.py [--days 14]
Read-only — never writes.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Allow ``python scripts/analyze_snaptrade_sync_timing.py`` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import fetch_all  # noqa: E402

ET = ZoneInfo("America/New_York")


def _to_et(dt: datetime | None):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ET)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=14,
                    help="Look back this many days (default 14).")
    args = ap.parse_args()

    rows = fetch_all(
        """
        SELECT broker_slug, snaptrade_account_id, cron_run_at,
               holdings_last_successful_sync, ok
        FROM snaptrade_sync_observations
        WHERE cron_run_at >= NOW() - (%s || ' days')::interval
        ORDER BY broker_slug, cron_run_at
        """,
        (str(args.days),),
    )
    if not rows:
        print(f"No observations in the last {args.days} days. "
              "Has the cron run since this table was created?")
        return 0

    print(f"snaptrade_sync_observations — last {args.days} days "
          f"({len(rows)} rows)\n")

    # Per-run staleness: how old was the broker data when we read it.
    lags_by_broker: dict[str, list[float]] = defaultdict(list)
    # Per (broker, ET date): earliest cron_run_at whose holdings sync had
    # already advanced to that day's session — proxies "settled close landed".
    for r in rows:
        if not r["ok"]:
            continue
        run = r["cron_run_at"]
        holds = r["holdings_last_successful_sync"]
        if run is None or holds is None:
            continue
        lag_min = (run - holds).total_seconds() / 60.0
        lags_by_broker[r["broker_slug"] or "unknown"].append(lag_min)

    print("Broker data staleness at sync time "
          "(cron_run_at − holdings_last_successful_sync):")
    print(f"  {'broker':<14}{'n':>5}{'min':>9}{'median':>9}{'max':>9}  (minutes)")
    for broker in sorted(lags_by_broker):
        lags = sorted(lags_by_broker[broker])
        n = len(lags)
        med = lags[n // 2]
        print(f"  {broker:<14}{n:>5}{lags[0]:>9.1f}{med:>9.1f}{lags[-1]:>9.1f}")

    # When did holdings_last_successful_sync land each day (ET wall-clock)?
    print("\nholdings_last_successful_sync timestamps (ET), most recent per "
          "account/day — when the broker's settled data actually appeared:")
    seen: set[tuple] = set()
    for r in reversed(rows):
        holds_et = _to_et(r["holdings_last_successful_sync"])
        if holds_et is None:
            continue
        key = (r["snaptrade_account_id"], holds_et.date())
        if key in seen:
            continue
        seen.add(key)
        run_et = _to_et(r["cron_run_at"])
        print(f"  {r['broker_slug'] or 'unknown':<10} "
              f"acct=…{(r['snaptrade_account_id'] or '')[-6:]:<8} "
              f"holdings_as_of={holds_et:%Y-%m-%d %H:%M ET}  "
              f"(read by cron at {run_et:%H:%M ET})")

    print("\nGuidance: the cron should fire AFTER the daily 4:00 PM ET close "
          "plus the typical broker settlement lag above, so the evening build "
          "captures the settled close rather than a transient after-hours mark.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
