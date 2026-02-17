#!/usr/bin/env python3
"""
Weekly batch job to compute Mirror Scores for all users with trade data.

Run via cron, e.g. every Monday:
  0 8 * * 1 cd /path/to/ccwj && python scripts/compute_mirror_scores.py

Or manually:
  python scripts/compute_mirror_scores.py
  python scripts/compute_mirror_scores.py --week 2025-02-10
"""
import os
import sys
import argparse
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import _get_db, get_accounts_for_user, save_mirror_score
from app.mirror_score import compute_mirror_score, _week_start
from app.routes import get_bigquery_client


def main():
    parser = argparse.ArgumentParser(description="Compute weekly Mirror Scores")
    parser.add_argument("--week", type=str, help="Week start date (YYYY-MM-DD, Monday)")
    args = parser.parse_args()

    if args.week:
        try:
            week_start = date.fromisoformat(args.week)
            week_start = _week_start(week_start)
        except ValueError:
            print("Invalid --week format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        # Default: previous complete week
        today = date.today()
        week_start = _week_start(today) - timedelta(days=7)

    conn = _get_db()
    users = conn.execute("SELECT id FROM users").fetchall()
    conn.close()

    client = get_bigquery_client()
    count = 0
    for (user_id,) in users:
        accounts = get_accounts_for_user(user_id)
        if not accounts:
            continue
        try:
            result = compute_mirror_score(user_id, accounts, week_start, client)
            if result:
                save_mirror_score(
                    user_id, result["week_start_date"],
                    result["discipline_score"], result["intent_score"],
                    result["risk_alignment_score"], result["consistency_score"],
                    result["mirror_score"], result["confidence_level"],
                    result["diagnostic_sentence"],
                )
                count += 1
                print(f"User {user_id}: Mirror Score {result['mirror_score']} for week {result['week_start_date']}")
        except Exception as e:
            print(f"User {user_id}: {e}", file=sys.stderr)

    print(f"Computed {count} Mirror Scores for week {week_start}")


if __name__ == "__main__":
    main()
