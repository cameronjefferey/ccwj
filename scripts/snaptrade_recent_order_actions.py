"""
Diagnostic: what does SnapTrade's ``recent_orders`` endpoint actually return
for OPTION orders, per broker?

WHY THIS EXISTS. ``app/snaptrade_normalize.py:orders_to_history_df`` currently
SKIPS every option order in the real-time ``recent_orders`` path, on the premise
that the orders payload can't disambiguate Buy-to-Open vs Sell-to-Close. But the
SnapTrade API contract says the order-record ``action`` field CAN carry option
open/close ("BUY_OPEN"/"SELL_CLOSE"/… or "BUY_TO_OPEN"/… — the
``ActionStrictWithOptions`` enum) *depending on the brokerage*. This script prints
the RAW ``action`` value SnapTrade returns for each recent order so we can see
whether a given broker (Schwab in particular) populates the open/close variant or
just ships bare ``BUY``/``SELL``. If it ships the explicit variant we can include
options in the fast path with ZERO guessing (the safe fix); if it ships bare
BUY/SELL we keep skipping (defer to the slower ``activities`` feed).

recent_orders only covers the LAST 24 HOURS, so run it the same day you traded
options.

DB mode — resolves the account from Postgres (point DATABASE_URL at prod;
needs SNAPTRADE_CLIENT_ID / SNAPTRADE_CONSUMER_KEY):

    .venv/bin/python scripts/snaptrade_recent_order_actions.py --user-id 9
    .venv/bin/python scripts/snaptrade_recent_order_actions.py --user-id 9 --nickname "Cameron Investment"

DIRECT mode — inspect a PROD account from a LOCAL shell WITHOUT pointing
DATABASE_URL at prod and WITHOUT running migrations. Pass the SnapTrade userId
+ account UUID directly; the per-user secret comes from the environment so it
never lands in argv / the process list. Uses the shared SnapTrade app creds
that already live in the local env:

    SNAPTRADE_USER_SECRET='<secret>' .venv/bin/python \
        scripts/snaptrade_recent_order_actions.py \
        --snap-user-id happytrader-9 --account-id <uuid> --nickname "Cameron Investment"

(Grab the userId/secret with a read-only query:
 SELECT snaptrade_user_id, snaptrade_secret FROM snaptrade_connections WHERE user_id=9.)

Read-only — never writes to Postgres, the warehouse, or SnapTrade. Prints NO
secrets (never echoes the user secret).
"""

from __future__ import annotations

import argparse
import os
import sys

# Allow ``python scripts/snaptrade_recent_order_actions.py`` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("FLASK_APP", "app:app")


def _get(obj, key, default=None):
    """Read ``key`` from a dict-like OR an SDK model (attr access)."""
    if obj is None:
        return default
    if hasattr(obj, "get"):
        try:
            val = obj.get(key, default)
            if val is not default:
                return val
        except Exception:
            pass
    return getattr(obj, key, default)


def _option_label(order) -> str:
    """Best-effort readable option contract for display; '' if not an option."""
    osym = _get(order, "option_symbol")
    if not osym:
        return ""
    # option_symbol may nest ticker/underlying differently per broker.
    ticker = _get(osym, "ticker") or _get(osym, "raw_symbol") or ""
    if not ticker:
        under = _get(osym, "underlying_symbol")
        ticker = _get(under, "raw_symbol") or _get(under, "symbol") or ""
    strike = _get(osym, "strike_price")
    otype = _get(osym, "option_type")
    expiry = _get(osym, "expiration_date")
    bits = [str(ticker or "?")]
    if otype:
        bits.append(str(otype))
    if strike is not None:
        bits.append(f"${strike}")
    if expiry:
        bits.append(str(expiry).split("T", 1)[0])
    return " ".join(bits)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--user-id", type=int, default=None,
                    help="HappyTrader user_id whose accounts to inspect "
                         "(DB mode). Not needed in --snap-user-id direct mode.")
    ap.add_argument("--account-id", default=None,
                    help="Limit to a single snaptrade_account_id (UUID).")
    ap.add_argument("--nickname", default=None,
                    help="Limit to accounts whose display_nickname matches this.")
    ap.add_argument("--only-options", action="store_true",
                    help="Only print option orders.")
    ap.add_argument("--snap-user-id", default=None,
                    help="DIRECT mode: SnapTrade userId. Bypasses the DB "
                         "lookup entirely (secret comes from env "
                         "SNAPTRADE_USER_SECRET). Useful when local DATABASE_URL "
                         "points at dev but you want to inspect a prod account.")
    args = ap.parse_args()

    from app.snaptrade import _get_snaptrade_client, _fetch_recent_orders

    client = _get_snaptrade_client()
    if client is None:
        print("Could not build SnapTrade client — check SNAPTRADE_CLIENT_ID / "
              "SNAPTRADE_CONSUMER_KEY.", file=sys.stderr)
        return 1

    # DIRECT mode: caller supplies the SnapTrade userId (argv) + secret (env)
    # + account_id (argv) — no Postgres read, so it works regardless of which
    # database DATABASE_URL points at. Secret is read from the environment so
    # it never appears in argv / the process list.
    if args.snap_user_id:
        secret = os.environ.get("SNAPTRADE_USER_SECRET", "")
        if not secret:
            print("DIRECT mode needs the per-user secret in env "
                  "SNAPTRADE_USER_SECRET.", file=sys.stderr)
            return 1
        if not args.account_id:
            print("DIRECT mode needs --account-id.", file=sys.stderr)
            return 1
        snap = {"snaptrade_user_id": args.snap_user_id, "snaptrade_secret": secret}
        accounts = [{"snaptrade_account_id": args.account_id,
                     "display_nickname": args.nickname or args.account_id,
                     "broker_slug": "?"}]
    else:
        # DB mode (reads only — no init_db, so no migrations run against prod).
        from app.models import get_snaptrade_user, get_snaptrade_accounts
        snap = get_snaptrade_user(args.user_id)
        if not snap:
            print(f"No SnapTrade connection for user_id={args.user_id}. "
                  "Is DATABASE_URL pointed at the right (prod) database?",
                  file=sys.stderr)
            return 1
        accounts = get_snaptrade_accounts(args.user_id) or []
        if args.account_id:
            accounts = [a for a in accounts if a.get("snaptrade_account_id") == args.account_id]
        if args.nickname:
            want = args.nickname.strip().lower()
            accounts = [a for a in accounts
                        if (a.get("display_nickname") or "").strip().lower() == want]
        if not accounts:
            print("No matching accounts.", file=sys.stderr)
            return 1

    # Aggregate the distinct action strings we see, split by option vs equity.
    option_actions: dict[str, int] = {}
    equity_actions: dict[str, int] = {}

    for acc in accounts:
        acc_id = acc.get("snaptrade_account_id")
        label = acc.get("display_nickname") or acc.get("account_name") or acc_id
        broker = acc.get("broker_slug") or "?"
        try:
            orders = _fetch_recent_orders(
                client, snap["snaptrade_user_id"], snap["snaptrade_secret"], acc_id,
            )
        except Exception as exc:
            print(f"\n### {label} [{broker}] — recent_orders FAILED: {exc}",
                  file=sys.stderr)
            continue

        orders = list(orders or [])
        print(f"\n### {label} [{broker}] — {len(orders)} recent order(s) "
              f"(last 24h)")
        print(f"{'STATUS':<10} {'ACTION':<16} {'OPT?':<5} {'QTY':>8} "
              f"{'PRICE':>10}  SYMBOL / CONTRACT")
        print("-" * 88)

        for o in orders:
            status = str(_get(o, "status") or "").strip()
            action = str(_get(o, "action") or "").strip()
            opt_label = _option_label(o)
            is_opt = bool(opt_label) or bool(_get(o, "option_symbol"))
            if args.only_options and not is_opt:
                continue

            qty = _get(o, "filled_quantity") or _get(o, "total_quantity") or ""
            price = _get(o, "execution_price") or _get(o, "limit_price") or ""
            if is_opt:
                sym = opt_label
                option_actions[action] = option_actions.get(action, 0) + 1
            else:
                usym = _get(o, "universal_symbol") or {}
                sym = _get(usym, "raw_symbol") or _get(usym, "symbol") or _get(o, "symbol") or "?"
                equity_actions[action] = equity_actions.get(action, 0) + 1

            print(f"{status:<10} {action:<16} {('YES' if is_opt else 'no'):<5} "
                  f"{str(qty):>8} {str(price):>10}  {sym}")

    print("\n" + "=" * 60)
    print("SUMMARY — distinct `action` values SnapTrade returned")
    print("=" * 60)
    print("OPTION orders:")
    if option_actions:
        for a, n in sorted(option_actions.items(), key=lambda kv: -kv[1]):
            explicit = a.upper() in (
                "BUY_TO_OPEN", "BUY_TO_CLOSE", "SELL_TO_OPEN", "SELL_TO_CLOSE",
                "BUY_OPEN", "BUY_CLOSE", "SELL_OPEN", "SELL_CLOSE",
            )
            tag = "  <- explicit open/close (SAFE to fast-path)" if explicit else \
                  "  <- bare BUY/SELL (must keep deferring to activities)"
            print(f"   {a!r}: {n}{tag}")
    else:
        print("   (none seen in the last 24h)")
    print("EQUITY orders:")
    if equity_actions:
        for a, n in sorted(equity_actions.items(), key=lambda kv: -kv[1]):
            print(f"   {a!r}: {n}")
    else:
        print("   (none seen in the last 24h)")

    print("\nVERDICT: if OPTION actions above are the explicit open/close "
          "variants, the fast-path fix is zero-guessing and safe to ship. If "
          "they're bare 'BUY'/'SELL', options must keep deferring to the "
          "activities feed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
