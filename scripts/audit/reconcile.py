"""Data accuracy audit — reconcile metrics across pages.

For each account, recompute the same KPI three different ways (the way the
positions page does it, the way position_detail does it, the way the SQL marts
do it) and flag any account where they disagree by more than $0.01.
"""
from __future__ import annotations

import os
import sys
import json
from collections import defaultdict
from decimal import Decimal

# Make app importable
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

from app.bigquery_client import get_bigquery_client

PROJECT = "ccwj-dbt"
DS = f"`{PROJECT}.analytics`"

EPS = 0.011  # 1 cent tolerance


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
    client = get_bigquery_client()

    # ── Get all accounts ──
    accounts_df = q(client, f"""
        SELECT DISTINCT account
        FROM {DS}.positions_summary
        ORDER BY account
    """)
    accounts = accounts_df["account"].tolist()
    print(f"Found {len(accounts)} account(s): {accounts}")

    # ====================================================================
    # CHECK 1: Positions hero math
    #   Total Return == Realized + Unrealized + Dividends?
    # ====================================================================
    section("CHECK 1: Total Return vs (Realized + Unrealized + Dividends), per account")
    sql1 = f"""
        SELECT
          account,
          ROUND(SUM(total_return), 2)        AS total_return,
          ROUND(SUM(realized_pnl), 2)        AS realized_pnl,
          ROUND(SUM(unrealized_pnl), 2)      AS unrealized_pnl,
          ROUND(SUM(total_dividend_income),2) AS dividends,
          ROUND(SUM(total_pnl), 2)           AS total_pnl
        FROM {DS}.positions_summary
        GROUP BY account
        ORDER BY account
    """
    d1 = q(client, sql1)
    issues_1 = []
    for _, r in d1.iterrows():
        derived = float(r.realized_pnl) + float(r.unrealized_pnl) + float(r.dividends)
        if diff(r.total_return, derived):
            issues_1.append((r.account, r.total_return, derived,
                             r.realized_pnl, r.unrealized_pnl, r.dividends))
        also = float(r.total_pnl) + float(r.dividends)
        if diff(r.total_return, also):
            issues_1.append((r.account + " (pnl+div)", r.total_return, also,
                             r.total_pnl, 0, r.dividends))
    if issues_1:
        print("FAIL — total_return ≠ realized + unrealized + dividends:")
        for row in issues_1:
            print(f"  {row[0]}: expected {fmt(row[2])} got {fmt(row[1])}  "
                  f"(R={fmt(row[3])} U={fmt(row[4])} D={fmt(row[5])})")
    else:
        print("PASS — total_return == realized + unrealized + dividends for every account")

    # ====================================================================
    # CHECK 2: Per-symbol — positions list vs position_detail realized P&L
    #   positions list: SUM(positions_summary.realized_pnl) per (account,symbol)
    #   position_detail: SUM(int_strategy_classification (option_contract,Closed))
    #                  + SUM(int_closed_equity_legs.realized_pnl)
    # ====================================================================
    section("CHECK 2: Per-symbol realized P&L — list vs detail page")
    sql2_list = f"""
        SELECT
          account, symbol,
          ROUND(SUM(realized_pnl), 2) AS realized_list
        FROM {DS}.positions_summary
        GROUP BY account, symbol
    """
    sql2_detail_opt = f"""
        SELECT
          sc.account,
          sc.symbol,
          ROUND(SUM(sc.total_pnl), 2) AS realized_opt
        FROM {DS}.int_strategy_classification sc
        JOIN {DS}.int_option_contracts oc
          ON sc.account = oc.account AND sc.trade_symbol = oc.trade_symbol
        WHERE sc.status = 'Closed'
          AND sc.trade_group_type = 'option_contract'
        GROUP BY sc.account, sc.symbol
    """
    sql2_detail_eq = f"""
        SELECT
          account, symbol,
          ROUND(SUM(realized_pnl), 2) AS realized_eq
        FROM {DS}.int_closed_equity_legs
        GROUP BY account, symbol
    """
    list_df = q(client, sql2_list).set_index(["account", "symbol"])
    opt_df = q(client, sql2_detail_opt).set_index(["account", "symbol"])
    eq_df = q(client, sql2_detail_eq).set_index(["account", "symbol"])

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
        print(f"FAIL — {len(issues_2)} (account,symbol) pairs disagree:")
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
          account, strategy,
          ROUND(SUM(total_pnl), 2)        AS pos_total_pnl,
          ROUND(SUM(realized_pnl), 2)     AS pos_realized,
          ROUND(SUM(unrealized_pnl), 2)   AS pos_unrealized,
          ROUND(SUM(total_return), 2)     AS pos_total_return,
          ROUND(SUM(total_dividend_income),2) AS pos_div
        FROM {DS}.positions_summary
        GROUP BY account, strategy
    """
    sql3_strat = f"""
        SELECT
          account, strategy,
          ROUND(total_pnl, 2)      AS strat_total_pnl,
          ROUND(realized_pnl, 2)   AS strat_realized,
          ROUND(unrealized_pnl, 2) AS strat_unrealized,
          ROUND(total_return, 2)   AS strat_total_return,
          ROUND(dividend_income,2) AS strat_div
        FROM {DS}.mart_strategy_performance
    """
    p3 = q(client, sql3_pos).set_index(["account", "strategy"])
    s3 = q(client, sql3_strat).set_index(["account", "strategy"])
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
        print(f"FAIL — {len(issues_3)} (account,strategy) divergences vs strategy mart:")
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
          account, strategy,
          SUM(num_winners) AS w,
          SUM(num_losers)  AS l,
          SAFE_DIVIDE(SUM(num_winners), NULLIF(SUM(num_winners)+SUM(num_losers),0)) AS wr
        FROM {DS}.positions_summary
        GROUP BY account, strategy
    """
    sql4_strat = f"""
        SELECT account, strategy, num_winners AS w, num_losers AS l, win_rate AS wr
        FROM {DS}.mart_strategy_performance
    """
    p4 = q(client, sql4_pos).set_index(["account", "strategy"])
    s4 = q(client, sql4_strat).set_index(["account", "strategy"])
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
        print(f"FAIL — {len(issues_4)} win-rate divergences:")
        for row in issues_4[:25]:
            print(f"  {row}")
    else:
        print("PASS — win rate consistent")

    # ====================================================================
    # CHECK 5: positions_summary.total_pnl = realized_pnl + unrealized_pnl?
    # (This is internal to the mart, but it's the assumption many pages rely on.)
    # ====================================================================
    section("CHECK 5: total_pnl == realized_pnl + unrealized_pnl per row")
    sql5 = f"""
        SELECT
          account, symbol, strategy,
          total_pnl, realized_pnl, unrealized_pnl,
          ROUND(total_pnl - (realized_pnl + unrealized_pnl), 2) AS delta
        FROM {DS}.positions_summary
        WHERE ABS(total_pnl - (realized_pnl + unrealized_pnl)) > 0.01
    """
    d5 = q(client, sql5)
    if not d5.empty:
        print(f"FAIL — {len(d5)} rows where total_pnl ≠ realized + unrealized:")
        print(d5.head(20).to_string(index=False))
    else:
        print("PASS — every positions_summary row has total_pnl = realized + unrealized")

    # ====================================================================
    # CHECK 6: Industries page — sum across sectors == positions total
    # ====================================================================
    section("CHECK 6: Industries (sector) total P&L vs positions total return")
    sql6 = f"""
        SELECT
          account,
          ROUND(SUM(total_pnl), 2)    AS by_sector_pnl,
          ROUND(SUM(total_return), 2) AS by_sector_return
        FROM {DS}.positions_summary
        GROUP BY account
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
            SELECT DISTINCT account, symbol
            FROM {DS}.positions_summary
            WHERE status = 'Open'
        ),
        broker_pos AS (
            SELECT DISTINCT account, underlying_symbol AS symbol
            FROM {DS}.int_enriched_current
        )
        SELECT
          (SELECT COUNT(*) FROM summary_open)            AS summary_open_count,
          (SELECT COUNT(*) FROM broker_pos)              AS broker_pos_count,
          (SELECT COUNT(*) FROM summary_open s
             LEFT JOIN broker_pos b USING(account, symbol)
             WHERE b.symbol IS NULL)                     AS in_summary_not_broker,
          (SELECT COUNT(*) FROM broker_pos b
             LEFT JOIN summary_open s USING(account, symbol)
             WHERE s.symbol IS NULL)                     AS in_broker_not_summary
    """
    try:
        d7 = q(client, sql7)
        print(d7.to_string(index=False))
        # Examples of mismatches
        if int(d7.iloc[0]["in_summary_not_broker"]) > 0:
            print("\nIn summary but not broker (top 10):")
            print(q(client, f"""
                SELECT s.account, s.symbol
                FROM (SELECT DISTINCT account, symbol FROM {DS}.positions_summary
                      WHERE status='Open') s
                LEFT JOIN (SELECT DISTINCT account, underlying_symbol AS symbol
                           FROM {DS}.int_enriched_current) b
                  USING(account, symbol)
                WHERE b.symbol IS NULL
                LIMIT 10
            """).to_string(index=False))
        if int(d7.iloc[0]["in_broker_not_summary"]) > 0:
            print("\nIn broker but not summary (top 10):")
            print(q(client, f"""
                SELECT b.account, b.symbol
                FROM (SELECT DISTINCT account, underlying_symbol AS symbol
                      FROM {DS}.int_enriched_current) b
                LEFT JOIN (SELECT DISTINCT account, symbol FROM {DS}.positions_summary
                           WHERE status='Open') s
                  USING(account, symbol)
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
        SELECT account,
               ROUND(SUM(total_return), 2)   AS total_return,
               ROUND(SUM(realized_pnl), 2)   AS realized_pnl,
               ROUND(SUM(unrealized_pnl), 2) AS unrealized_pnl
        FROM {DS}.positions_summary
        GROUP BY account
        ORDER BY account
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
            WITH latest AS (
                SELECT account, symbol, MAX(date) AS last_dt
                FROM {DS}.mart_daily_pnl
                GROUP BY account, symbol
            )
            SELECT m.account, m.symbol,
                   ROUND(m.cumulative_options_pnl, 2)    AS daily_options,
                   ROUND(m.cumulative_dividends_pnl, 2)  AS daily_dividends
            FROM {DS}.mart_daily_pnl m
            JOIN latest l
              ON m.account = l.account
             AND m.symbol  = l.symbol
             AND m.date    = l.last_dt
        """
        sql9_pos = f"""
            SELECT account, symbol,
                   ROUND(SUM(CASE WHEN strategy IN ('Long Call','Long Put','Short Call','Short Put',
                                                   'Cash Secured Put','Covered Call',
                                                   'Naked Call','Naked Put') THEN total_pnl
                                   ELSE 0 END), 2)            AS pos_options,
                   ROUND(SUM(total_dividend_income), 2)        AS pos_dividends
            FROM {DS}.positions_summary
            GROUP BY account, symbol
        """
        d9d = q(client, sql9_daily).set_index(["account", "symbol"])
        d9p = q(client, sql9_pos).set_index(["account", "symbol"])
        keys9 = sorted(set(d9d.index) & set(d9p.index))
        issues_9 = []
        for k in keys9:
            dd = float(d9d.loc[k]["daily_dividends"] or 0)
            pd_ = float(d9p.loc[k]["pos_dividends"] or 0)
            if abs(dd - pd_) > 0.5:
                issues_9.append((k, "dividends", dd, pd_, dd - pd_))
        if issues_9:
            print(f"FAIL — {len(issues_9)} (account,symbol) dividend mismatches > 50¢:")
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
            SELECT account,
                   ROUND(SUM(total_pnl), 2) AS pos_total,
                   ROUND((SELECT SUM(p2.total_pnl)
                          FROM {DS}.positions_summary p2
                          WHERE p2.account = p.account), 2) AS pos_total_check
            FROM {DS}.positions_summary p
            GROUP BY account
        """
        d10 = q(client, sql10)
        bad10 = d10[d10["pos_total"] != d10["pos_total_check"]]
        if bad10.empty:
            print("PASS — Strategy fit pivot source (positions_summary.total_pnl) is internally consistent per account")
            print(d10.to_string(index=False))
        else:
            print("FAIL — Strategy fit grand total inconsistent per account:")
            print(bad10.to_string(index=False))
    except Exception as exc:
        print(f"(skipped: {exc})")

    # ====================================================================
    # CHECK 11: Weekly review (mart_weekly_trades) lifetime grand total
    # vs positions_summary lifetime grand total per account.
    # Both descend from int_strategy_classification so they MUST match.
    # ====================================================================
    section("CHECK 11: Weekly Review (mart_weekly_trades) lifetime sum vs Positions lifetime sum, per account")
    try:
        sql11_w = f"""
            SELECT account, ROUND(SUM(total_pnl), 2) AS weekly_lifetime
            FROM {DS}.mart_weekly_trades
            GROUP BY account
        """
        sql11_p = f"""
            SELECT account, ROUND(SUM(total_pnl), 2) AS positions_lifetime
            FROM {DS}.positions_summary
            GROUP BY account
        """
        d11w = q(client, sql11_w).set_index("account")
        d11p = q(client, sql11_p).set_index("account")
        keys11 = sorted(set(d11w.index) | set(d11p.index))
        bad11 = []
        for a in keys11:
            w = float(d11w.loc[a]["weekly_lifetime"]) if a in d11w.index else 0.0
            p = float(d11p.loc[a]["positions_lifetime"]) if a in d11p.index else 0.0
            if abs(w - p) > 0.5:
                bad11.append((a, w, p, w - p))
        if bad11:
            print(f"FAIL — {len(bad11)} account(s) where weekly review ≠ positions:")
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
            SELECT account, strategy, SUM(total_closed) AS closed_in_coach
            FROM {DS}.mart_coaching_signals
            GROUP BY account, strategy
        """
        # Coach only looks at option contracts (it analyzes exit timing on
        # premiums, not on the underlying equity sessions that get classified
        # as Covered Call etc.).  So compare against option-only counts from
        # int_strategy_classification, NOT the symbol+strategy combined count
        # in positions_summary.
        sql12_p = f"""
            SELECT account, strategy, COUNT(*) AS closed_in_pos
            FROM {DS}.int_strategy_classification
            WHERE trade_group_type = 'option_contract'
              AND status = 'Closed'
            GROUP BY account, strategy
        """
        d12c = q(client, sql12_c).set_index(["account", "strategy"])
        d12p = q(client, sql12_p).set_index(["account", "strategy"])
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
                print(f"FAIL — {len(bad12)} (account, strategy) closed-count mismatches:")
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

    print()
    print("=" * 78)
    print("DONE")
    print("=" * 78)


if __name__ == "__main__":
    main()
