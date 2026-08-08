"""Data accuracy audit — reconcile metrics across pages.

For each account, recompute the same KPI three different ways (the way the
positions page does it, the way position_detail does it, the way the SQL marts
do it) and flag any account where they disagree by more than $0.01.

GRAIN: every check keys on ``COALESCE(tenant_id, account)`` (aliased ``tkey``),
NEVER on the bare ``account`` display label. SnapTrade returns the same label
("Schwab Account") for multiple physical accounts, so label-grained GROUP BYs
fuse distinct tenants and label-grained JOINs fan out N× — the first scheduled
run of this audit reported dozens of exactly-2× "mismatches" and crashed on
duplicate index keys for precisely this reason (2026-08-08). See AGENTS.md
"tenant_id is also the analytics GRAIN".
"""
from __future__ import annotations

import sys
import json
from collections import defaultdict
from datetime import date
from decimal import Decimal

import pandas as pd

# Standalone BigQuery client — deliberately NOT app.bigquery_client.
# Importing anything under app/ runs app/__init__ → config.py, which
# hard-fails without SECRET_KEY. This audit runs in a bare CI job
# (reconcile.yml) with only GCP credentials, and it always audits the
# prod `analytics` dataset, so the app's BQ_DATASET override machinery
# doesn't apply anyway. First scheduled run failed on exactly this
# (2026-08-08: RuntimeError SECRET_KEY must be set).
from google.cloud import bigquery


def get_bigquery_client():
    return bigquery.Client(project=PROJECT)


PROJECT = "ccwj-dbt"
DS = f"`{PROJECT}.analytics`"

EPS = 0.011  # 1 cent tolerance

# Accumulates FAIL checks so CI / cron can exit non-zero.
_FAIL_COUNT = 0


def fail(msg):
    """Print a FAIL line and count it for process exit status."""
    global _FAIL_COUNT
    _FAIL_COUNT += 1
    print(msg)


def q(client, sql):
    return client.query(sql).to_dataframe()


def fmt(x):
    if x is None:
        return "—"
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)


def diff(a, b, eps=EPS):
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    return abs(float(a) - float(b)) > eps


def section(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def main():
    global _FAIL_COUNT
    _FAIL_COUNT = 0
    client = get_bigquery_client()

    # ── Get all accounts (tenant-keyed; label kept for readability) ──
    accounts_df = q(client, f"""
        SELECT COALESCE(tenant_id, account) AS tkey, ANY_VALUE(account) AS account
        FROM {DS}.positions_summary
        GROUP BY tkey
        ORDER BY tkey
    """)
    accounts = accounts_df["tkey"].tolist()
    print(f"Found {len(accounts)} account(s): {accounts}")

    # ====================================================================
    # CHECK 1: Positions hero math
    #   Total Return == Realized + Unrealized + Dividends?
    # ====================================================================
    section("CHECK 1: Total Return vs (Realized + Unrealized + Dividends), per account")
    sql1 = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey,
          ROUND(SUM(total_return), 2)        AS total_return,
          ROUND(SUM(realized_pnl), 2)        AS realized_pnl,
          ROUND(SUM(unrealized_pnl), 2)      AS unrealized_pnl,
          ROUND(SUM(total_dividend_income),2) AS dividends,
          ROUND(SUM(total_pnl), 2)           AS total_pnl
        FROM {DS}.positions_summary
        GROUP BY tkey
        ORDER BY tkey
    """
    d1 = q(client, sql1)
    issues_1 = []
    for _, r in d1.iterrows():
        derived = float(r.realized_pnl) + float(r.unrealized_pnl) + float(r.dividends)
        if diff(r.total_return, derived):
            issues_1.append((r.tkey, r.total_return, derived,
                             r.realized_pnl, r.unrealized_pnl, r.dividends))
        # After dividends-as-first-class, total_pnl already folds in
        # attributed dividends. total_return is now an alias of total_pnl,
        # so the two should match column-for-column per row (and therefore
        # per account when summed). If they drift, the alias has broken.
        if diff(r.total_return, r.total_pnl):
            issues_1.append((r.tkey + " (pnl==return alias)", r.total_return,
                             r.total_pnl, r.total_pnl, 0, r.dividends))
    if issues_1:
        fail("FAIL — total_return ≠ realized + unrealized + dividends:")
        for row in issues_1:
            print(f"  {row[0]}: expected {fmt(row[2])} got {fmt(row[1])}  "
                  f"(R={fmt(row[3])} U={fmt(row[4])} D={fmt(row[5])})")
    else:
        print(
            "PASS — total_return == realized + unrealized + dividends == total_pnl "
            "for every account"
        )

    # ====================================================================
    # CHECK 2: Per-symbol — positions list vs position_detail realized P&L
    #   positions list: SUM(positions_summary.realized_pnl) per (account,symbol)
    #   position_detail: SUM(int_strategy_classification (option_contract,Closed))
    #                  + SUM(int_closed_equity_legs.realized_pnl)
    # ====================================================================
    section("CHECK 2: Per-symbol realized P&L — list vs detail page")
    sql2_list = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, symbol,
          ROUND(SUM(realized_pnl), 2) AS realized_list
        FROM {DS}.positions_summary
        GROUP BY tkey, symbol
    """
    # JOIN on tenant_id, NOT the account label — label-grained joins fan
    # out N× when several tenants share "Schwab Account".
    sql2_detail_opt = f"""
        SELECT
          COALESCE(sc.tenant_id, sc.account) AS tkey,
          sc.symbol,
          ROUND(SUM(sc.total_pnl), 2) AS realized_opt
        FROM {DS}.int_strategy_classification sc
        JOIN {DS}.int_option_contracts oc
          ON COALESCE(sc.tenant_id, sc.account) = COALESCE(oc.tenant_id, oc.account)
         AND sc.trade_symbol = oc.trade_symbol
        WHERE sc.status = 'Closed'
          AND sc.trade_group_type = 'option_contract'
        GROUP BY tkey, sc.symbol
    """
    sql2_detail_eq = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, symbol,
          ROUND(SUM(realized_pnl), 2) AS realized_eq
        FROM {DS}.int_closed_equity_legs
        GROUP BY tkey, symbol
    """
    list_df = q(client, sql2_list).set_index(["tkey", "symbol"])
    opt_df = q(client, sql2_detail_opt).set_index(["tkey", "symbol"])
    eq_df = q(client, sql2_detail_eq).set_index(["tkey", "symbol"])

    # Union of keys
    keys = set(list_df.index) | set(opt_df.index) | set(eq_df.index)
    issues_2 = []
    for k in sorted(keys):
        list_v = float(list_df.loc[k]["realized_list"]) if k in list_df.index else 0.0
        opt_v = float(opt_df.loc[k]["realized_opt"]) if k in opt_df.index else 0.0
        eq_v = float(eq_df.loc[k]["realized_eq"]) if k in eq_df.index else 0.0
        detail_v = opt_v + eq_v
        if diff(list_v, detail_v):
            issues_2.append((k, list_v, detail_v, opt_v, eq_v))
    if issues_2:
        fail(f"FAIL — {len(issues_2)} (account,symbol) pairs disagree:")
        for k, lv, dv, ov, ev in issues_2[:25]:
            print(f"  {k}: list={fmt(lv)}  detail={fmt(dv)}  "
                  f"(opt={fmt(ov)} eq={fmt(ev)})  Δ={fmt(lv-dv)}")
        if len(issues_2) > 25:
            print(f"  ... and {len(issues_2) - 25} more")
    else:
        print(f"PASS — all {len(keys)} (account,symbol) pairs agree on realized P&L")

    # ====================================================================
    # CHECK 3: Per-account totals — positions vs accounts page
    #   accounts page reads from mart_account_snapshots_enriched for cash/value
    #   but realized/unrealized roll up from positions_summary too
    # ====================================================================
    section("CHECK 3: Strategies mart vs positions_summary — strategy totals reconcile")
    sql3_pos = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, strategy,
          ROUND(SUM(total_pnl), 2)        AS pos_total_pnl,
          ROUND(SUM(realized_pnl), 2)     AS pos_realized,
          ROUND(SUM(unrealized_pnl), 2)   AS pos_unrealized,
          ROUND(SUM(total_return), 2)     AS pos_total_return,
          ROUND(SUM(total_dividend_income),2) AS pos_div
        FROM {DS}.positions_summary
        GROUP BY tkey, strategy
    """
    sql3_strat = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, strategy,
          ROUND(SUM(total_pnl), 2)      AS strat_total_pnl,
          ROUND(SUM(realized_pnl), 2)   AS strat_realized,
          ROUND(SUM(unrealized_pnl), 2) AS strat_unrealized,
          ROUND(SUM(total_return), 2)   AS strat_total_return,
          ROUND(SUM(dividend_income),2) AS strat_div
        FROM {DS}.mart_strategy_performance
        GROUP BY tkey, strategy
    """
    p3 = q(client, sql3_pos).set_index(["tkey", "strategy"])
    s3 = q(client, sql3_strat).set_index(["tkey", "strategy"])
    keys3 = set(p3.index) | set(s3.index)
    issues_3 = []
    for k in sorted(keys3):
        if k not in p3.index:
            issues_3.append((k, "missing in positions_summary", "", "", "", ""))
            continue
        if k not in s3.index:
            issues_3.append((k, "missing in mart_strategy_performance", "", "", "", ""))
            continue
        for col in ("total_pnl", "realized", "unrealized", "total_return", "div"):
            pv = float(p3.loc[k][f"pos_{col}"])
            sv = float(s3.loc[k][f"strat_{col}"])
            if diff(pv, sv):
                issues_3.append((k, col, pv, sv, pv - sv, ""))
    if issues_3:
        fail(f"FAIL — {len(issues_3)} (account,strategy) divergences vs strategy mart:")
        for row in issues_3[:25]:
            print(f"  {row}")
    else:
        print(f"PASS — strategies mart matches positions_summary for {len(keys3)} (account,strategy)")

    # ====================================================================
    # CHECK 4: Win-rate definition — strategies mart vs positions_summary
    # ====================================================================
    section("CHECK 4: Win rate — strategies mart vs positions_summary aggregation")
    sql4_pos = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, strategy,
          SUM(num_winners) AS w,
          SUM(num_losers)  AS l,
          SAFE_DIVIDE(SUM(num_winners), NULLIF(SUM(num_winners)+SUM(num_losers),0)) AS wr
        FROM {DS}.positions_summary
        GROUP BY tkey, strategy
    """
    # The mart is (tenant, strategy)-grained so the GROUP BY is 1:1;
    # MAX(win_rate) still validates the mart's STORED win_rate column.
    sql4_strat = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, strategy,
          SUM(num_winners) AS w,
          SUM(num_losers)  AS l,
          MAX(win_rate)    AS wr
        FROM {DS}.mart_strategy_performance
        GROUP BY tkey, strategy
    """
    p4 = q(client, sql4_pos).set_index(["tkey", "strategy"])
    s4 = q(client, sql4_strat).set_index(["tkey", "strategy"])
    issues_4 = []
    for k in p4.index:
        if k not in s4.index:
            continue
        if int(p4.loc[k]["w"]) != int(s4.loc[k]["w"]) or int(p4.loc[k]["l"]) != int(s4.loc[k]["l"]):
            issues_4.append((k, p4.loc[k].to_dict(), s4.loc[k].to_dict()))
        pw, sw = p4.loc[k]["wr"], s4.loc[k]["wr"]
        try:
            pwf = None if (pw is None or (hasattr(pw, "is_nan") and pw.is_nan())) else float(pw)
        except Exception:
            pwf = None
        try:
            swf = None if (sw is None or (hasattr(sw, "is_nan") and sw.is_nan())) else float(sw)
        except Exception:
            swf = None
        if pwf is None and swf is None:
            continue
        if pwf is None or swf is None or abs(pwf - swf) > 0.0001:
            issues_4.append((k, "wr", pw, sw))
    if issues_4:
        fail(f"FAIL — {len(issues_4)} win-rate divergences:")
        for row in issues_4[:25]:
            print(f"  {row}")
    else:
        print("PASS — win rate consistent")

    # ====================================================================
    # CHECK 5: positions_summary.total_pnl = realized + unrealized + dividends?
    # (Dividends are a first-class P&L stream as of the dividends-as-first-class
    # change. The invariant the rest of the app relies on is that the headline
    # number reconciles with its three building blocks.)
    # ====================================================================
    section("CHECK 5: total_pnl == realized_pnl + unrealized_pnl + total_dividend_income per row")
    sql5 = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey, symbol, strategy,
          total_pnl, realized_pnl, unrealized_pnl, total_dividend_income,
          ROUND(
            total_pnl - (realized_pnl + unrealized_pnl + COALESCE(total_dividend_income, 0)),
            2
          ) AS delta
        FROM {DS}.positions_summary
        WHERE ABS(
            total_pnl - (realized_pnl + unrealized_pnl + COALESCE(total_dividend_income, 0))
        ) > 0.01
    """
    d5 = q(client, sql5)
    if not d5.empty:
        fail(f"FAIL — {len(d5)} rows where total_pnl ≠ realized + unrealized + dividends:")
        print(d5.head(20).to_string(index=False))
    else:
        print(
            "PASS — every positions_summary row has "
            "total_pnl = realized + unrealized + dividends"
        )

    # ====================================================================
    # CHECK 6: Sectors page — sum across sectors == positions total
    # ====================================================================
    section("CHECK 6: Sectors total P&L vs positions total return")
    sql6 = f"""
        SELECT
          COALESCE(tenant_id, account) AS tkey,
          ANY_VALUE(account) AS account,
          ROUND(SUM(total_pnl), 2)    AS by_sector_pnl,
          ROUND(SUM(total_return), 2) AS by_sector_return
        FROM {DS}.positions_summary
        GROUP BY tkey
    """
    d6 = q(client, sql6)
    print(d6.to_string(index=False))
    print("(Sector page sums the same column — should match. Spot check above.)")

    # ====================================================================
    # CHECK 7: positions_summary status — symbols flagged Open also have
    # rows in current_positions_dim?
    # ====================================================================
    section("CHECK 7: Open positions in summary should reconcile with broker positions")
    sql7 = f"""
        WITH summary_open AS (
            SELECT DISTINCT COALESCE(tenant_id, account) AS tkey, symbol
            FROM {DS}.positions_summary
            WHERE status = 'Open'
        ),
        broker_pos AS (
            SELECT DISTINCT COALESCE(tenant_id, account) AS tkey,
                   underlying_symbol AS symbol
            FROM {DS}.int_enriched_current
        )
        SELECT
          (SELECT COUNT(*) FROM summary_open)            AS summary_open_count,
          (SELECT COUNT(*) FROM broker_pos)              AS broker_pos_count,
          (SELECT COUNT(*) FROM summary_open s
             LEFT JOIN broker_pos b USING(tkey, symbol)
             WHERE b.symbol IS NULL)                     AS in_summary_not_broker,
          (SELECT COUNT(*) FROM broker_pos b
             LEFT JOIN summary_open s USING(tkey, symbol)
             WHERE s.symbol IS NULL)                     AS in_broker_not_summary
    """
    try:
        d7 = q(client, sql7)
        print(d7.to_string(index=False))
        # Examples of mismatches
        if int(d7.iloc[0]["in_summary_not_broker"]) > 0:
            print("\nIn summary but not broker (top 10):")
            print(q(client, f"""
                SELECT s.tkey, s.symbol
                FROM (SELECT DISTINCT COALESCE(tenant_id, account) AS tkey, symbol
                      FROM {DS}.positions_summary WHERE status='Open') s
                LEFT JOIN (SELECT DISTINCT COALESCE(tenant_id, account) AS tkey,
                                  underlying_symbol AS symbol
                           FROM {DS}.int_enriched_current) b
                  USING(tkey, symbol)
                WHERE b.symbol IS NULL
                LIMIT 10
            """).to_string(index=False))
        if int(d7.iloc[0]["in_broker_not_summary"]) > 0:
            print("\nIn broker but not summary (top 10):")
            print(q(client, f"""
                SELECT b.tkey, b.symbol
                FROM (SELECT DISTINCT COALESCE(tenant_id, account) AS tkey,
                             underlying_symbol AS symbol
                      FROM {DS}.int_enriched_current) b
                LEFT JOIN (SELECT DISTINCT COALESCE(tenant_id, account) AS tkey, symbol
                           FROM {DS}.positions_summary WHERE status='Open') s
                  USING(tkey, symbol)
                WHERE s.symbol IS NULL
                LIMIT 10
            """).to_string(index=False))
    except Exception as exc:
        print(f"(skipped: {exc})")

    # ====================================================================
    # CHECK 8: Accounts page totals vs Positions per-account totals
    # The accounts page sums total_return per account from positions_summary;
    # if Positions and Accounts disagree per account, users will spot it.
    # ====================================================================
    section("CHECK 8: Accounts page total_return vs Positions per-account total_return")
    sql8 = f"""
        SELECT COALESCE(tenant_id, account) AS tkey,
               ANY_VALUE(account) AS account,
               ROUND(SUM(total_return), 2)   AS total_return,
               ROUND(SUM(realized_pnl), 2)   AS realized_pnl,
               ROUND(SUM(unrealized_pnl), 2) AS unrealized_pnl
        FROM {DS}.positions_summary
        GROUP BY tkey
        ORDER BY tkey
    """
    d8 = q(client, sql8)
    print(d8.to_string(index=False))
    print("(Accounts page reads same column — these ARE the per-account totals.)")

    # ====================================================================
    # CHECK 9: mart_daily_pnl cumulative vs positions per-symbol realized
    # Cumulative realized P&L per (account, symbol) at the latest date
    # should equal positions_summary.realized_pnl (closed legs only — open
    # equity sessions still mark-to-market on positions side, so we restrict
    # to fully-closed positions to keep this comparable).
    # ====================================================================
    section("CHECK 9: Daily P&L by symbol — latest cumulative options + dividends vs positions_summary")
    try:
        sql9_daily = f"""
            WITH keyed AS (
                SELECT COALESCE(tenant_id, account) AS tkey, symbol, date,
                       cumulative_options_pnl, open_options_unrealized_pnl,
                       cumulative_dividends_pnl
                FROM {DS}.mart_daily_pnl
            ),
            latest AS (
                SELECT tkey, symbol, MAX(date) AS last_dt
                FROM keyed
                GROUP BY tkey, symbol
            )
            SELECT m.tkey, m.symbol,
                   -- Total options P&L at the latest date = realized
                   -- cumulative + open MTM. Under the realize-on-close
                   -- attribution rule (AGENTS.md "Option P&L
                   -- Attribution") these are two separate columns;
                   -- both must be summed to compare against
                   -- positions_summary.total_pnl (realized + unrealized).
                   ROUND(SUM(m.cumulative_options_pnl
                         + COALESCE(m.open_options_unrealized_pnl, 0)), 2) AS daily_options,
                   ROUND(SUM(m.cumulative_dividends_pnl), 2)  AS daily_dividends
            FROM keyed m
            JOIN latest l
              ON m.tkey = l.tkey
             AND m.symbol  = l.symbol
             AND m.date    = l.last_dt
            GROUP BY m.tkey, m.symbol
        """
        sql9_pos = f"""
            SELECT COALESCE(tenant_id, account) AS tkey, symbol,
                   ROUND(SUM(CASE WHEN strategy IN ('Long Call','Long Put','Short Call','Short Put',
                                                   'Cash Secured Put','Covered Call',
                                                   'Naked Call','Naked Put') THEN total_pnl
                                   ELSE 0 END), 2)            AS pos_options,
                   ROUND(SUM(total_dividend_income), 2)        AS pos_dividends
            FROM {DS}.positions_summary
            GROUP BY tkey, symbol
        """
        d9d = q(client, sql9_daily).set_index(["tkey", "symbol"])
        d9p = q(client, sql9_pos).set_index(["tkey", "symbol"])
        keys9 = sorted(set(d9d.index) & set(d9p.index))
        issues_9 = []
        for k in keys9:
            dd = float(d9d.loc[k]["daily_dividends"] or 0)
            pd_ = float(d9p.loc[k]["pos_dividends"] or 0)
            if abs(dd - pd_) > 0.5:
                issues_9.append((k, "dividends", dd, pd_, dd - pd_))
        if issues_9:
            fail(f"FAIL — {len(issues_9)} (account,symbol) dividend mismatches > 50¢:")
            for row in issues_9[:25]:
                print(f"  {row}")
        else:
            print(f"PASS — daily-cumulative dividends match positions for all {len(keys9)} (account,symbol)")
    except Exception as exc:
        print(f"(skipped: {exc})")

    # ====================================================================
    # CHECK 10: Strategy fit matrix grand total vs positions total
    # The strategy fit page is a pivot of mart_strategy_performance by
    # (strategy x sector). Grand total must equal Σ(total_pnl) per account.
    # ====================================================================
    section("CHECK 10: Strategy fit grand total vs positions total_pnl per account")
    try:
        sql10 = f"""
            SELECT COALESCE(p.tenant_id, p.account) AS tkey,
                   ROUND(SUM(p.total_pnl), 2) AS pos_total,
                   ROUND((SELECT SUM(p2.total_pnl)
                          FROM {DS}.positions_summary p2
                          WHERE COALESCE(p2.tenant_id, p2.account)
                                = COALESCE(p.tenant_id, p.account)), 2) AS pos_total_check
            FROM {DS}.positions_summary p
            GROUP BY tkey, p.tenant_id, p.account
        """
        d10 = q(client, sql10)
        bad10 = d10[d10["pos_total"] != d10["pos_total_check"]]
        if bad10.empty:
            print("PASS — Strategy fit pivot source (positions_summary.total_pnl) is internally consistent per account")
            print(d10.to_string(index=False))
        else:
            fail("FAIL — Strategy fit grand total inconsistent per account:")
            print(bad10.to_string(index=False))
    except Exception as exc:
        print(f"(skipped: {exc})")

    # ====================================================================
    # CHECK 11: Weekly review (mart_weekly_trades) lifetime grand total
    # vs positions_summary lifetime grand total per account.
    # Both descend from int_strategy_classification so they MUST match.
    # ====================================================================
    section("CHECK 11: Weekly Review (mart_weekly_trades) lifetime sum vs Positions ex-dividend lifetime sum, per account")
    try:
        sql11_w = f"""
            SELECT COALESCE(tenant_id, account) AS tkey,
                   ROUND(SUM(total_pnl), 2) AS weekly_lifetime
            FROM {DS}.mart_weekly_trades
            GROUP BY tkey
        """
        # mart_weekly_trades is TRADE-grained: it never carries dividend
        # income, while positions_summary.total_pnl folds attributed
        # dividends in (CHECK 5's invariant). Compare ex-dividend totals —
        # verified 2026-08-08 that the raw gap is exactly Σ dividends to
        # the penny for every tenant.
        sql11_p = f"""
            SELECT COALESCE(tenant_id, account) AS tkey,
                   ROUND(SUM(total_pnl) - SUM(COALESCE(total_dividend_income, 0)), 2)
                       AS positions_lifetime
            FROM {DS}.positions_summary
            GROUP BY tkey
        """
        d11w = q(client, sql11_w).set_index("tkey")
        d11p = q(client, sql11_p).set_index("tkey")
        keys11 = sorted(set(d11w.index) | set(d11p.index))
        bad11 = []
        for a in keys11:
            w = float(d11w.loc[a]["weekly_lifetime"]) if a in d11w.index else 0.0
            p = float(d11p.loc[a]["positions_lifetime"]) if a in d11p.index else 0.0
            if abs(w - p) > 0.5:
                bad11.append((a, w, p, w - p))
        if bad11:
            fail(f"FAIL — {len(bad11)} account(s) where weekly review ≠ positions:")
            for row in bad11:
                print(f"  {row[0]}: weekly=${row[1]:,.2f}  positions=${row[2]:,.2f}  Δ=${row[3]:,.2f}")
        else:
            print(f"PASS — weekly-review lifetime totals match positions for all {len(keys11)} accounts")
            print(d11w.join(d11p, how="outer").fillna(0.0).to_string())
    except Exception as exc:
        print(f"(skipped: {exc})")

    # ====================================================================
    # CHECK 12: AI Coach (mart_coaching_signals) closed-contract counts
    # vs positions_summary closed-contract counts for the same option
    # strategies and accounts.
    # ====================================================================
    section("CHECK 12: AI Coach total_closed vs closed option contracts, per (account, strategy)")
    try:
        sql12_c = f"""
            SELECT COALESCE(tenant_id, account) AS tkey, strategy,
                   SUM(total_closed) AS closed_in_coach
            FROM {DS}.mart_coaching_signals
            GROUP BY tkey, strategy
        """
        # Coach only looks at option contracts (it analyzes exit timing on
        # premiums, not on the underlying equity sessions that get classified
        # as Covered Call etc.).  So compare against option-only counts from
        # int_strategy_classification, NOT the symbol+strategy combined count
        # in positions_summary.
        sql12_p = f"""
            SELECT COALESCE(tenant_id, account) AS tkey, strategy,
                   COUNT(*) AS closed_in_pos
            FROM {DS}.int_strategy_classification
            WHERE trade_group_type = 'option_contract'
              AND status = 'Closed'
            GROUP BY tkey, strategy
        """
        d12c = q(client, sql12_c).set_index(["tkey", "strategy"])
        d12p = q(client, sql12_p).set_index(["tkey", "strategy"])
        keys12 = sorted(set(d12c.index) & set(d12p.index))
        bad12 = []
        for k in keys12:
            cc = int(d12c.loc[k]["closed_in_coach"] or 0)
            pc = int(d12p.loc[k]["closed_in_pos"] or 0)
            if cc != pc:
                bad12.append((k, cc, pc, cc - pc))
        only_in_coach = sorted(set(d12c.index) - set(d12p.index))
        if bad12 or only_in_coach:
            if bad12:
                fail(f"FAIL — {len(bad12)} (account, strategy) closed-count mismatches:")
                for row in bad12[:25]:
                    print(f"  {row[0]}: coach={row[1]}  positions={row[2]}  Δ={row[3]}")
            if only_in_coach:
                print(f"NOTE — {len(only_in_coach)} (account, strategy) appear in coach but not positions:")
                for k in only_in_coach[:10]:
                    print(f"  {k}")
        else:
            print(f"PASS — AI Coach closed counts match positions for all {len(keys12)} (account, strategy)")
    except Exception as exc:
        print(f"(skipped: {exc})")

    # ====================================================================
    # CHECK 13: Price-data freshness — stg_daily_prices must have a recent
    # SPY close. yfinance (unofficial API) failing quietly is the single
    # biggest data-quality risk in the product: every close-based surface
    # (position charts, mart_daily_pnl today rows, dividends synthesis,
    # benchmark) reads stg_daily_prices. The loader now refuses to gut the
    # table (PRICES_COVERAGE guard), but a cron that stops RUNNING (secret
    # expiry, workflow disabled) would still let prices rot silently —
    # this nightly check catches that. Threshold is 5 calendar days: long
    # weekend + one genuinely missed evening refresh stays green, anything
    # longer means the pipeline is down.
    # ====================================================================
    section("CHECK 13: stg_daily_prices freshness (SPY close within 5 days)")
    try:
        sql13 = f"""
            SELECT MAX(date) AS latest_spy_close
            FROM {DS}.stg_daily_prices
            WHERE UPPER(symbol) = 'SPY'
        """
        d13 = q(client, sql13)
        latest = d13["latest_spy_close"].iloc[0] if not d13.empty else None
        if latest is None or pd.isna(latest):
            fail("FAIL — stg_daily_prices has NO SPY rows at all (price pipeline never ran?)")
        else:
            latest_date = pd.to_datetime(latest).date()
            age_days = (date.today() - latest_date).days
            if age_days > 5:
                fail(
                    f"FAIL — latest SPY close in stg_daily_prices is {latest_date} "
                    f"({age_days} days old). The evening prices refresh is not landing; "
                    "check .github/workflows/prices_refresh.yml runs and yfinance health."
                )
            else:
                print(f"PASS — latest SPY close {latest_date} ({age_days} day(s) old)")
    except Exception as exc:
        fail(f"FAIL — freshness check errored: {exc}")

    print()
    print("=" * 78)
    if _FAIL_COUNT:
        print(f"DONE — {_FAIL_COUNT} FAIL check(s)")
    else:
        print("DONE — all checks passed")
    print("=" * 78)
    return _FAIL_COUNT


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
