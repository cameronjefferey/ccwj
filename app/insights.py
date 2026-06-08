import os
import re
import pandas as pd
import markupsafe
from flask import render_template, request, redirect, url_for, flash, jsonify, abort
from flask_login import login_required, current_user
from google.cloud import bigquery as bq

from app import app
from app.bigquery_client import get_bigquery_client
from app.extensions import limiter
from app.models import (
    get_accounts_for_user, is_admin,
    save_insight, get_insight_for_user,
    get_user_llm_model, set_user_llm_model,
)
# Tenant-scoped query helpers live in app.routes so the same user_id
# predicate and Stage 0/1 NULL-leniency apply everywhere. See
# docs/USER_ID_TENANCY.md.
from app.routes import (
    _tenants_for_scope,
    _tenant_sql_filter,
    _tenant_sql_and,
    _filter_df_by_tenant_ids,
    _user_account_list,
)
from app.utils import demo_block_writes
from app.llm import (
    call_llm, llm_available, selectable_models,
    resolve_model_key, selectable_model_keys,
)


# ------------------------------------------------------------------
# Queries — coaching signals from unique data
# ------------------------------------------------------------------

COACHING_SIGNALS_QUERY = """
SELECT
    account, strategy,
    total_closed, reliable_contracts, pct_contracts_reliable,
    avg_giveback_pct, avg_pnl_given_back, avg_days_held_past_peak,
    optimal_exit_rate, avg_pct_premium_captured, avg_actual_pnl,
    total_pnl_given_back,
    best_dte_bucket, best_dte_win_rate, best_dte_trades,
    worst_dte_bucket, worst_dte_win_rate, worst_dte_trades
FROM `ccwj-dbt.analytics.mart_coaching_signals`
{where}
ORDER BY total_pnl_given_back DESC
"""

RECENT_EXITS_QUERY = """
SELECT
    trade_symbol, underlying_symbol, strategy, direction,
    open_date, close_date, close_type, days_in_trade,
    actual_pnl, peak_unrealized_pnl, peak_date,
    days_held_past_peak, pnl_given_back, giveback_pct,
    pct_of_premium_captured, optimal_exit,
    snapshot_count, snapshot_density, data_reliable
FROM `ccwj-dbt.analytics.int_option_exit_analysis`
WHERE close_date >= @since_date
  AND data_reliable = true
  {tenant_filter}
ORDER BY pnl_given_back DESC
LIMIT 20
"""

INSIGHTS_DATA_QUERY = """
SELECT
    account, symbol, strategy, status,
    total_pnl, realized_pnl, unrealized_pnl,
    total_premium_received, total_premium_paid,
    num_trade_groups, num_individual_trades,
    num_winners, num_losers, win_rate,
    avg_pnl_per_trade, avg_days_in_trade,
    first_trade_date, last_trade_date,
    total_dividend_income, total_return
FROM `ccwj-dbt.analytics.positions_summary`
{where}
ORDER BY account, symbol, strategy
"""

BEHAVIOR_OBSERVATIONS_QUERY = """
SELECT
    account,
    trade_symbol,
    underlying_symbol,
    strategy,
    open_date,
    close_date,
    size_vs_30d_baseline,
    size_vs_90d_baseline,
    strategy_win_rate_180d,
    strategy_prior_trades_180d,
    consecutive_losses_before,
    observation_text,
    anomaly_score,
    is_anomaly
FROM `ccwj-dbt.ml_models.account_trade_insights`
WHERE observation_text IS NOT NULL
  AND open_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {tenant_filter}
ORDER BY anomaly_score DESC, open_date DESC
LIMIT 5
"""


WEEKLY_QA_QUERY = """
SELECT
  week_start,
  SUM(trades_closed) AS trades_closed,
  SUM(trades_opened) AS trades_opened,
  SUM(total_pnl)     AS total_pnl,
  SUM(dividends_amount) AS dividends_amount,
  SUM(total_return)  AS total_return,
  SUM(num_winners)   AS num_winners,
  SUM(num_losers)    AS num_losers,
  SUM(premium_received) AS premium_received,
  SUM(premium_paid)     AS premium_paid,
  ANY_VALUE(best_symbol)      AS best_symbol,
  ANY_VALUE(best_strategy)    AS best_strategy,
  ANY_VALUE(best_pnl)         AS best_pnl,
  ANY_VALUE(worst_symbol)     AS worst_symbol,
  ANY_VALUE(worst_strategy)   AS worst_strategy,
  ANY_VALUE(worst_pnl)        AS worst_pnl
FROM `ccwj-dbt.analytics.mart_weekly_summary`
{where}
GROUP BY week_start
ORDER BY week_start DESC
LIMIT 1
"""

# Portfolio-level “discovery” metrics (calendar, concentration, DTE, post-loss
# sequence). One row; scoped with {tenant_clause} = _tenant_sql_and(...).
# Mirrors logic we would put in dbt except the tenancy filter is request-time.
_DISCOVERY_SQL = """
WITH rel AS (
  SELECT
    e.close_date,
    e.giveback_pct,
    e.pnl_given_back,
    e.underlying_symbol,
    e.dte_at_open,
    e.direction,
    e.account,
    e.user_id
  FROM `ccwj-dbt.analytics.int_option_exit_analysis` e
  WHERE e.data_reliable
  {tenant_clause}
),

dow_agg AS (
  SELECT
    extract(dayofweek from close_date) AS dow_num,
    count(*) AS n_trades,
    avg(giveback_pct) AS avg_gb,
    sum(pnl_given_back) AS sum_gb_dollars
  FROM rel
  GROUP BY 1
  HAVING count(*) >= 4
),

dow_wide AS (
  SELECT
    (select count(*) from dow_agg) AS dow_bucket_count,

    (select dow_num from dow_agg order by avg_gb desc, n_trades desc limit 1) AS worst_dow_num,
    (select avg_gb from dow_agg order by avg_gb desc, n_trades desc limit 1) AS worst_dow_avg_gb,
    (select n_trades from dow_agg order by avg_gb desc, n_trades desc limit 1) AS worst_dow_n,

    (select dow_num from dow_agg order by avg_gb asc, n_trades desc limit 1) AS best_dow_num,
    (select avg_gb from dow_agg order by avg_gb asc, n_trades desc limit 1) AS best_dow_avg_gb,
    (select n_trades from dow_agg order by avg_gb asc, n_trades desc limit 1) AS best_dow_n
),

tot_gb AS (
  SELECT COALESCE(SUM(pnl_given_back), 0) AS total_givenback_dollars FROM rel
),

top_sym_ranked AS (
  SELECT underlying_symbol AS sym, SUM(pnl_given_back) AS gb
  FROM rel
  GROUP BY 1
  ORDER BY gb DESC
  LIMIT 1
),

sym_stats AS (
  SELECT
    tg.total_givenback_dollars AS total_givenback_dollars,
    t.sym AS top_symbol,
    t.gb AS top_symbol_givenback_sum,
    CASE
      WHEN tg.total_givenback_dollars > 300 AND t.gb IS NOT NULL THEN
        ROUND(100.0 * t.gb / tg.total_givenback_dollars, 1)
    END AS symbol_giveback_concentration_pct
  FROM tot_gb tg
  LEFT JOIN top_sym_ranked t ON TRUE
),

sold_dte AS (
  SELECT
    countif(direction = 'Sold' and dte_at_open is not null and dte_at_open <= 14) AS n_short_open,
    countif(direction = 'Sold' and dte_at_open is not null and dte_at_open >= 45) AS n_long_open,
    avg(case when direction = 'Sold' and dte_at_open is not null and dte_at_open <= 14
        then giveback_pct end) AS short_avg_gb,
    avg(case when direction = 'Sold' and dte_at_open is not null and dte_at_open >= 45
        then giveback_pct end) AS long_avg_gb
  FROM rel
),

seq_agg AS (
  SELECT
    count(*) AS n_closed_seq,
    countif(prev_trade_outcome = 'Loser') AS n_after_loss,
    countif(prev_trade_outcome = 'Loser' and outcome = 'Winner') AS wins_after_loss,
    countif(outcome = 'Winner') AS wins_total
  FROM `ccwj-dbt.analytics.int_trade_sequence` s
  WHERE s.trade_group_type = 'option_contract'
  {sequence_clause}
)

SELECT
  (select count(*) from rel) AS reliable_contracts,

  dw.dow_bucket_count,

  dw.worst_dow_num,
  dw.worst_dow_avg_gb,
  dw.worst_dow_n,
  dw.best_dow_num,
  dw.best_dow_avg_gb,
  dw.best_dow_n,

  case
    when dw.dow_bucket_count >= 2
         and dw.worst_dow_num is not null
         and dw.best_dow_num is not null
         and dw.worst_dow_num != dw.best_dow_num
      then round(dw.worst_dow_avg_gb - dw.best_dow_avg_gb, 1)
    else null
  end AS weekday_gb_spread_pp,

  ss.total_givenback_dollars,
  ss.top_symbol,
  ss.top_symbol_givenback_sum,
  ss.symbol_giveback_concentration_pct,

  sd.n_short_open,
  sd.n_long_open,
  round(sd.short_avg_gb, 1) AS short_dte_avg_giveback_pp,
  round(sd.long_avg_gb, 1) AS long_dte_avg_giveback_pp,
  case
    when sd.n_short_open >= 8 and sd.n_long_open >= 8
      then round(sd.short_avg_gb - sd.long_avg_gb, 1)
    else null
  end AS sold_short_vs_long_gb_gap_pp,

  sa.n_closed_seq,
  sa.n_after_loss,
  sa.wins_after_loss,
  round(safe_divide(sa.wins_total, nullif(sa.n_closed_seq, 0)), 4) AS overall_trade_wr,
  round(safe_divide(sa.wins_after_loss, nullif(sa.n_after_loss, 0)), 4)
    AS win_rate_after_prior_loss,
  /* Positive => next trade wins more often after a prior loss vs your overall WR. */
  round(
      safe_divide(sa.wins_after_loss, nullif(sa.n_after_loss, 0))
      - safe_divide(sa.wins_total, nullif(sa.n_closed_seq, 0)),
      4
  ) AS rebound_vs_overall_gap
FROM dow_wide dw
CROSS JOIN sym_stats ss
CROSS JOIN sold_dte sd
CROSS JOIN seq_agg sa
"""

# BigQuery EXTRACT(dayofweek): Sunday = 1 ... Saturday = 7
_DOW_EN = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday",
}


def _safe_float(x, default=None):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _discovery_cards_from_series(r: pd.Series):
    """Build ranked discovery cards + a DISCOVERY LAB block for Gemini.

    Thresholds emphasize surprising *contrasts* backed by sufficient n.
    """
    cards = []
    n_rel = int(_safe_float(r.get("reliable_contracts"), 0) or 0)
    if n_rel < 12:
        return [], ""

    dow_spread = _safe_float(r.get("weekday_gb_spread_pp"))
    dow_cnt = int(_safe_float(r.get("dow_bucket_count"), 0) or 0)
    wn = r.get("worst_dow_num")
    bn = r.get("best_dow_num")
    if dow_spread is not None and dow_cnt >= 2 and dow_spread >= 10:
        ww = _DOW_EN.get(int(wn), "?") if pd.notna(wn) else "?"
        bw = _DOW_EN.get(int(bn), "?") if pd.notna(bn) else "?"
        wf = _safe_float(r.get("worst_dow_avg_gb"))
        bf = _safe_float(r.get("best_dow_avg_gb"))
        cards.append({
            "tag": "Calendar",
            "title": "Your exits do not behave the same every day of the week",
            "stat": f"+{dow_spread:.0f} pp",
            "body": (
                f"When you close on **{ww}**, you surrender about **{wf:.0f}%** of peak unrealized profit on "
                f"average (from daily marks). Your best-reviewed weekday cluster is **{bw}** (~{bf:.0f}% avg giveback)."
            ),
            "score": dow_spread * 3.5,
            "muted": "",
        })

    conc = _safe_float(r.get("symbol_giveback_concentration_pct"))
    tot_gb = _safe_float(r.get("total_givenback_dollars"))
    tsym = r.get("top_symbol")
    if conc is not None and tot_gb and tot_gb > 200 and conc >= 34 and pd.notna(tsym):
        cards.append({
            "tag": "Concentration",
            "title": "One ticker owns an outsized share of “money left after the peak”",
            "stat": f"{conc:.0f}% of ${tot_gb:,.0f}",
            "body": (
                f"Around **{conc:.0f}%** of the dollars you theoretically left on the table vs peak "
                f"clusters on **{tsym}** — worth asking whether sizing or exits differ there versus the rest "
                f"of your book."
            ),
            "score": conc * 2.8,
            "muted": "",
        })

    dte_gap = _safe_float(r.get("sold_short_vs_long_gb_gap_pp"))
    ns = int(_safe_float(r.get("n_short_open"), 0) or 0)
    nl = int(_safe_float(r.get("n_long_open"), 0) or 0)
    if dte_gap is not None and ns >= 8 and nl >= 8 and abs(dte_gap) >= 11:
        sh = _safe_float(r.get("short_dte_avg_giveback_pp"))
        lg = _safe_float(r.get("long_dte_avg_giveback_pp"))
        if dte_gap > 0:
            cards.append({
                "tag": "Timing",
                "title": "Short-DTE shorts show more peak giveback than your long-leg opens",
                "stat": f"+{dte_gap:.0f} pp avg giveback",
                "body": (
                    f"Selling premium **inside ~14 DTE** shows **{sh:.0f}%** avg giveback vs peak snapshots; "
                    f"Opens **beyond ~45 DTE** average **{lg:.0f}%**. That differential is measurable only "
                    f"because we mark every day — not broker cash alone."
                ),
                "score": abs(dte_gap) * 2.9,
                "muted": "Sold short legs only.",
            })
        else:
            cards.append({
                "tag": "Timing",
                "title": "Your long-dated short premium behaves differently than short-dated",
                "stat": f"{dte_gap:+.0f} pp avg giveback (long worse)",
                "body": (
                    f"Holds on **extended-dated shorts** correlate with higher giveback vs peak (**{lg:.0f}%** avg) "
                    f"than very short ladders (**{sh:.0f}%**) — unusual and worth inspecting by symbol."
                ),
                "score": abs(dte_gap) * 2.9,
                "muted": "Sold short legs only.",
            })

    rgap = _safe_float(r.get("rebound_vs_overall_gap"))
    n_al = int(_safe_float(r.get("n_after_loss"), 0) or 0)
    n_seq = int(_safe_float(r.get("n_closed_seq"), 0) or 0)
    ov = _safe_float(r.get("overall_trade_wr"))
    al = _safe_float(r.get("win_rate_after_prior_loss"))
    if rgap is not None and abs(rgap) >= 0.07 and n_al >= 14 and ov is not None and al is not None:
        pct_overall = ov * 100
        pct_al = al * 100
        if rgap >= 0.07:
            cards.append({
                "tag": "Sequence",
                "title": "You bounce harder after losses than almost anyone tracks",
                "stat": f"+{rgap * 100:.1f} pts vs baseline WR",
                "body": (
                    f"When the **prior** closed trade was a loser, your next listed option-trade win rate runs "
                    f"**~{pct_al:.0f}%** vs **~{pct_overall:.0f}%** overall (n≥{n_al} sequencing windows). Retail "
                    f"risk tools never quantify that."
                ),
                "score": abs(rgap) * 500,
                "muted": f"Across {n_seq:,} qualifying closed trades in sequence.",
            })
        elif rgap <= -0.07:
            cards.append({
                "tag": "Sequence",
                "title": "Win rate dips right after losses — sequencing you can now see",
                "stat": f"{rgap * 100:.1f} pts vs baseline WR",
                "body": (
                    f"The trade **after** a losing close wins **~{pct_al:.0f}%** vs **~{pct_overall:.0f}%** overall; "
                    f"that's a disciplined thing to stare at rather than intuit."
                ),
                "score": abs(rgap) * 520,
                "muted": f"Across {n_seq:,} qualifying closed trades in sequence.",
            })

    if not cards:
        return [], ""

    cards.sort(key=lambda c: float(c["score"]), reverse=True)
    for i, c in enumerate(cards):
        c["rank"] = i + 1
    blob_lines = [
        "DISCOVERY LAB (deterministic contrasts; not advice):",
        f"- Snapshot-quality contracts summarized: ~{n_rel} reliable closes (daily MTM-backed).",
    ]
    for c in cards[:5]:
        blob_lines.append(f"- [{c['tag']}] {c['title']}: {_strip_md_for_brief(c['body'])}")
    return cards[:5], "\n".join(blob_lines)


def _strip_md_for_brief(s: str) -> str:
    return s.replace("**", "").replace("*", "")


# ------------------------------------------------------------------
# Coaching brief builder — the core differentiator
# ------------------------------------------------------------------

def _build_coaching_brief(client, tenant_ids):
    """Build a structured coaching brief from pre-computed dbt signals.

    Returns (brief_text, coaching_data_dict) where coaching_data_dict
    contains the raw data for deterministic rendering in the template.
    """
    where = _tenant_sql_filter(tenant_ids)
    tenant_and = _tenant_sql_and(tenant_ids)
    sections = []
    coaching_data = {
        "signals": [],
        "recent_exits": [],
        "behavior_observations": [],
        "discoveries": [],
        "discovery_headline": None,
        "has_data": False,
        "total_closed": 0,
        "reliable_contracts": 0,
        "pct_reliable": 0,
    }

    disco_cards = []
    disco_txt = ""

    # 1. Coaching signals per strategy
    try:
        signals_df = client.query(
            COACHING_SIGNALS_QUERY.format(where=where)
        ).to_dataframe()
        if not signals_df.empty:
            coaching_data["has_data"] = True
            for col in ["avg_giveback_pct", "avg_pnl_given_back", "avg_days_held_past_peak",
                         "optimal_exit_rate", "avg_pct_premium_captured", "total_pnl_given_back",
                         "total_closed", "reliable_contracts", "pct_contracts_reliable",
                         "best_dte_win_rate", "worst_dte_win_rate"]:
                if col in signals_df.columns:
                    signals_df[col] = pd.to_numeric(signals_df[col], errors="coerce").fillna(0)

            total_given_back = float(signals_df["total_pnl_given_back"].sum())
            total_closed = int(signals_df["total_closed"].sum())
            reliable_contracts = int(signals_df["reliable_contracts"].sum())
            pct_reliable = round(reliable_contracts / total_closed * 100, 0) if total_closed > 0 else 0

            coaching_data["total_closed"] = total_closed
            coaching_data["reliable_contracts"] = reliable_contracts
            coaching_data["pct_reliable"] = pct_reliable

            # Exit timing section
            exit_lines = []
            if reliable_contracts > 0:
                avg_gb = float(signals_df["avg_giveback_pct"].mean())
                avg_days = float(signals_df["avg_days_held_past_peak"].mean())
                exit_lines.append(
                    f"- Based on {reliable_contracts} closed options with sufficient daily snapshot data "
                    f"({pct_reliable:.0f}% of {total_closed} total closed), "
                    f"you give back an average of {avg_gb:.0f}% of peak profit."
                )
                exit_lines.append(
                    f"- Total profit left on the table: ${total_given_back:,.0f}.")
                exit_lines.append(
                    f"- Average days held past peak: {avg_days:.1f}.")

                strat_rows = []
                for _, r in signals_df.iterrows():
                    if int(r.get("reliable_contracts", 0)) >= 3:
                        strat_rows.append({
                            "strategy": r["strategy"],
                            "giveback_pct": float(r["avg_giveback_pct"]),
                            "days_past_peak": float(r["avg_days_held_past_peak"]),
                            "pnl_given_back": float(r["total_pnl_given_back"]),
                            "trades": int(r["reliable_contracts"]),
                            "total_closed": int(r["total_closed"]),
                            "pct_reliable": float(r["pct_contracts_reliable"]),
                            "pct_premium_captured": float(r.get("avg_pct_premium_captured") or 0),
                        })
                        coaching_data["signals"].append(strat_rows[-1])

                strat_rows.sort(key=lambda x: x["giveback_pct"], reverse=True)
                for s in strat_rows[:3]:
                    exit_lines.append(
                        f"  - {s['strategy']}: {s['giveback_pct']:.0f}% giveback, "
                        f"{s['days_past_peak']:.0f} days past peak, "
                        f"${s['pnl_given_back']:,.0f} left on table "
                        f"({s['trades']} reliable trades)."
                    )

            if exit_lines:
                sections.append("EXIT TIMING PROFILE\n" + "\n".join(exit_lines))

            # DTE sweet spots
            dte_lines = []
            for _, r in signals_df.iterrows():
                best_b = r.get("best_dte_bucket")
                worst_b = r.get("worst_dte_bucket")
                strat = r.get("strategy", "")
                if best_b and worst_b and best_b != worst_b:
                    bwr = float(r.get("best_dte_win_rate", 0))
                    wwr = float(r.get("worst_dte_win_rate", 0))
                    if bwr - wwr >= 15:
                        dte_lines.append(
                            f"- {strat}: best at {best_b} ({bwr:.0f}% WR), "
                            f"worst at {worst_b} ({wwr:.0f}% WR)."
                        )
            if dte_lines:
                sections.append("DTE SWEET SPOTS\n" + "\n".join(dte_lines[:5]))

    except Exception:
        pass

    # 2. Recent exits (last 90 days, for weekly context)
    try:
        from datetime import date, timedelta
        since = date.today() - timedelta(days=90)
        cfg = bq.QueryJobConfig(query_parameters=[
            bq.ScalarQueryParameter("since_date", "DATE", since),
        ])
        exits_df = client.query(
            RECENT_EXITS_QUERY.format(tenant_filter=tenant_and),
            job_config=cfg,
        ).to_dataframe()
        if not exits_df.empty:
            recent_lines = []
            for _, r in exits_df.head(10).iterrows():
                gb = float(r.get("pnl_given_back", 0) or 0)
                sym = r.get("underlying_symbol", "")
                strat = r.get("strategy", "")
                pnl = float(r.get("actual_pnl", 0) or 0)
                peak = float(r.get("peak_unrealized_pnl", 0) or 0)
                days_past = int(r.get("days_held_past_peak", 0) or 0)

                coaching_data["recent_exits"].append({
                    "symbol": sym,
                    "strategy": strat,
                    "actual_pnl": pnl,
                    "peak_pnl": peak,
                    "given_back": gb,
                    "days_past_peak": days_past,
                    "close_date": str(r.get("close_date", ""))[:10],
                    "pct_premium_captured": float(r.get("pct_of_premium_captured") or 0),
                })

                if gb > 10:
                    recent_lines.append(
                        f"  - {sym} ({strat}): peaked at +${peak:,.0f}, "
                        f"closed at +${pnl:,.0f}, gave back ${gb:,.0f} "
                        f"({days_past}d past peak)"
                    )
            if recent_lines:
                sections.append("RECENT EXIT EXAMPLES (last 90 days)\n" + "\n".join(recent_lines))
    except Exception:
        pass

    try:
        e_and = _tenant_sql_and(tenant_ids, col="e.tenant_id")
        s_and = _tenant_sql_and(tenant_ids, col="s.tenant_id")
        dq = client.query(
            _DISCOVERY_SQL.format(
                tenant_clause=e_and if e_and else "",
                sequence_clause=s_and if s_and else "",
            )
        ).to_dataframe()
        if not dq.empty:
            disco_cards, disco_txt = _discovery_cards_from_series(dq.iloc[0])
            coaching_data["discoveries"] = disco_cards
            if disco_cards:
                coaching_data["has_data"] = True
                coaching_data["discovery_headline"] = disco_cards[0].get("title")
            if disco_txt:
                sections.append(disco_txt)
    except Exception:
        coaching_data["discoveries"] = coaching_data.get("discoveries") or []

    # 3. Behavior observations (BQML-ranked, neutral evidence).
    #    Reads ml_models.account_trade_insights which already filters by
    #    observation_text IS NOT NULL.  The text is pre-rendered in dbt
    #    so Flask does no phrasing — we just quote it verbatim.
    if app.config.get("BEHAVIOR_INSIGHTS_ENABLED", True):
        try:
            obs_df = client.query(
                BEHAVIOR_OBSERVATIONS_QUERY.format(tenant_filter=tenant_and)
            ).to_dataframe()
            # Belt-and-suspenders tenant scoping: also filter client-side.
            obs_df = _filter_df_by_tenant_ids(obs_df, tenant_ids)
            if not obs_df.empty:
                obs_lines = []
                for _, r in obs_df.iterrows():
                    text = str(r.get("observation_text") or "").strip()
                    if not text:
                        continue
                    date_str = str(r.get("open_date", ""))[:10]
                    sym = str(r.get("underlying_symbol", "") or "")
                    line = f"  - ({date_str}) {sym}: {text}"
                    obs_lines.append(line)
                    coaching_data["behavior_observations"].append({
                        "symbol": sym,
                        "strategy": str(r.get("strategy", "") or ""),
                        "open_date": date_str,
                        "size_vs_30d_baseline": float(r.get("size_vs_30d_baseline") or 0),
                        "strategy_win_rate_180d": float(r.get("strategy_win_rate_180d") or 0),
                        "strategy_prior_trades_180d": int(r.get("strategy_prior_trades_180d") or 0),
                        "anomaly_score": float(r.get("anomaly_score") or 0),
                        "observation_text": text,
                    })
                if obs_lines:
                    sections.append("BEHAVIOR OBSERVATIONS (last 30 days)\n" + "\n".join(obs_lines))
        except Exception:
            # Missing ml_models dataset or untrained model should not break
            # the coach — the deterministic signals above still render.
            pass

    brief_text = "\n\n".join(sections) if sections else None
    return brief_text, coaching_data


def _build_prompt_data(df):
    """Fallback: flat portfolio summary when coaching signals aren't available."""
    if df.empty:
        return None

    num_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "total_return",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    total_return = float(df["total_return"].sum())
    realized = float(df["realized_pnl"].sum())
    unrealized = float(df["unrealized_pnl"].sum())
    dividend_income = (
        float(df["total_dividend_income"].sum())
        if "total_dividend_income" in df.columns else 0.0
    )
    premium_received = float(df["total_premium_received"].sum())
    premium_paid = float(df["total_premium_paid"].sum())
    total_trades = int(df["num_individual_trades"].sum())
    total_winners = int(df["num_winners"].sum())
    total_losers = int(df["num_losers"].sum())
    total_closed = total_winners + total_losers
    overall_win_rate = total_winners / total_closed if total_closed else 0
    num_symbols = df["symbol"].nunique()

    first_date = str(df["first_trade_date"].min())
    last_date = str(df["last_trade_date"].max())

    strat_agg = df.groupby("strategy").agg(
        total_return=("total_return", "sum"),
        dividend_income=("total_dividend_income", "sum"),
        num_trades=("num_individual_trades", "sum"),
        num_winners=("num_winners", "sum"),
        num_losers=("num_losers", "sum"),
        avg_days=("avg_days_in_trade", "mean"),
        premium_received=("total_premium_received", "sum"),
    ).reset_index()

    strategy_lines = []
    for _, r in strat_agg.iterrows():
        closed = int(r["num_winners"] + r["num_losers"])
        wr = r["num_winners"] / closed if closed else 0
        div_part = (
            f", divs=${r['dividend_income']:,.2f}"
            if r.get("dividend_income", 0) and float(r["dividend_income"]) != 0
            else ""
        )
        strategy_lines.append(
            f"  - {r['strategy']}: return=${r['total_return']:,.2f}{div_part}, "
            f"WR={wr:.1%}, trades={int(r['num_trades'])}, avg_days={r['avg_days']:.1f}"
        )

    div_line = (
        f", dividends ${dividend_income:,.2f}"
        if dividend_income else ""
    )

    return f"""PORTFOLIO OVERVIEW
- Symbols: {num_symbols}, Trades: {total_trades}, Range: {first_date} to {last_date}
- Return: ${total_return:,.2f} (realized ${realized:,.2f}, unrealized ${unrealized:,.2f}{div_line})
- Win rate: {overall_win_rate:.1%} ({total_winners}W / {total_losers}L)
- Net premium: ${premium_received - premium_paid:,.2f}

STRATEGY BREAKDOWN
{chr(10).join(strategy_lines)}"""


# ------------------------------------------------------------------
# AI prompts — the AI narrates pre-computed signals
# ------------------------------------------------------------------

SYSTEM_PROMPT = """You are narrating a trader's behavioral insights report. The data below
contains PRE-COMPUTED signals about their option trading behavior — exit timing
and DTE performance. These signals come from daily option
mark-to-market data that no other retail tool tracks.

You surface OBSERVATIONS, not financial advice. Never recommend trades,
strikes, expirations, position sizes, or strategies; describe the patterns
the data shows.

IMPORTANT — DATA COVERAGE: The signals are computed only from contracts with
sufficient daily snapshot data (at least 40% of hold days covered, minimum 3
snapshots). The data will tell you how many contracts qualified. If coverage
is low (e.g., "15 of 40 contracts"), acknowledge that the patterns are based
on a subset and may become clearer as more daily data accumulates. Do NOT
present partial-coverage findings as definitive.

DISCOVERY LAB (when present): These are deterministic contrasts surfaced only
because we reconstruct daily unrealized curves — e.g., weekday clustering of
peak givebacks, ticker concentration vs total dollars surrendered to the peak,
DTE tenor differences on sold short premium, sequencing after prior losses versus
overall win frequency. Quote at least ONE discovery fact by number as a headline
finding if DISCOVERY LAB appears below. Do not inflate or invent discoveries that
were not listed.

Your job:
1. Lead with the MOST ACTIONABLE finding — the behavior change that would
   save the most money if corrected.
2. Use specific numbers from the signals. Never generalize when you have data.
3. Frame everything as process, not outcome. Say "You held 8 days past peak"
   not "you lost money." Say "Your strongest DTE bucket is 45-60d at 65% win rate"
   not "you should only trade that tenor."
4. Write 3-4 concise paragraphs. No section headings. No bullet lists.
   Write like an analyst summarizing a game film — direct, specific,
   observational, never prescriptive.
5. End with ONE concrete thing to watch next week.

Rules:
- Do NOT give financial advice or recommend specific trades.
- Do NOT recommend securities, strikes, expirations, or position sizes.
- Do NOT make price predictions.
- Focus only on behavioral patterns visible in the data.
- Write in second person ("You...").

If a BEHAVIOR OBSERVATIONS section is present in the data:
- You may quote one observation_text verbatim when it's the most
  informative signal this week.
- Do NOT add severity labels ("HIGH", "MEDIUM", "ALERT", "WARNING").
- Do NOT dramatize. Present the observation as evidence, not accusation.
- Do NOT speculate about the trader's emotional state or motives
  (no "revenge trading", "tilt", "FOMO", etc.).
- Do NOT recommend changing position sizes or strategies.

IMPORTANT: Start with a 2-sentence summary under "## Summary" that captures
the single most important behavioral insight. Then write the full analysis."""


QA_SYSTEM_PROMPT = """You are a trading-data analyst with access to detailed behavioral
data about this trader's option trading — including daily mark-to-market curves,
exit timing analysis, and DTE performance breakdowns. You answer
questions with OBSERVATIONS grounded in the data; you do NOT give financial
advice or recommend trades.

You will receive:
- BEHAVIORAL SIGNALS: Pre-computed metrics (exit timing, giveback patterns, DTE sweet spots)
- DISCOVERY LAB (optional): Deterministic calendar / concentration / tenor / sequencing contrasts
- PORTFOLIO OVERVIEW: Lifetime strategy performance
- Optionally: RECENT EXITS showing specific trades where profit was left on the table
- Optionally: LAST WEEK performance summary

The behavioral signals only include contracts with reliable daily data (40%+
snapshot density). If the data mentions "X of Y contracts," the remaining
contracts lacked sufficient daily data. Do not extrapolate beyond what the
data covers.

Answer the user's question in 3-6 short paragraphs. Be specific — use exact
numbers, trade symbols, and dates from the data. If the question asks about
exit timing or holding behavior, lean heavily on the behavioral signals.

Rules:
- Do NOT give financial advice or trade recommendations.
- If data isn't available to answer, say so honestly.
- Focus on behavioral patterns, not market predictions.
- Write in second person ("You...").

If a BEHAVIOR OBSERVATIONS section is present in the data:
- Quote observation_text verbatim when relevant to the question.
- Do NOT add severity labels ("HIGH", "MEDIUM", "ALERT").
- Do NOT speculate about psychological state or motive.
- Do NOT recommend changing size or strategy."""


def _call_coach(data_text, model_key=None):
    """Narrate the coaching brief and return ((summary, full_analysis), None).

    Vendor-agnostic: app.llm.call_llm dispatches to the chosen model
    (model_key) or the default. The summary section is parsed out of the
    markdown the model returns.
    """
    full_text, error = call_llm(
        SYSTEM_PROMPT,
        "Here is the trader's behavioral data:\n\n" + data_text,
        kind="coach.generate",
        max_tokens=2000,
        temperature=0.7,
        model_key=model_key,
    )
    if error:
        return None, error

    summary = ""
    if "## Summary" in full_text:
        after_summary = full_text.split("## Summary", 1)[1]
        next_heading = after_summary.find("\n## ")
        if next_heading != -1:
            summary = after_summary[:next_heading].strip()
        else:
            summary = after_summary.strip()
    else:
        summary = full_text[:200].strip()

    return (summary, full_text), None


def _call_coach_question(coaching_text, portfolio_text, weekly_text, question, model_key=None):
    """Narrate a Q&A answer grounded in coaching + portfolio + weekly data."""
    parts = []
    if coaching_text:
        parts.append("BEHAVIORAL SIGNALS:\n" + coaching_text)
    if weekly_text:
        parts.append("LAST WEEK DATA:\n" + weekly_text)
    if portfolio_text:
        parts.append("PORTFOLIO OVERVIEW:\n" + portfolio_text)
    parts.append(
        "\nAnswer the user's question below. Be concise and specific, "
        "grounded strictly in the data above.\n"
        f"User question: {question}\n"
    )
    user_prompt = "\n\n".join(parts)

    return call_llm(
        QA_SYSTEM_PROMPT,
        user_prompt,
        kind="coach.ask",
        max_tokens=800,
        temperature=0.6,
        model_key=model_key,
    )


def _md_to_html(md_text):
    """Simple markdown-to-HTML for Gemini output."""
    lines = md_text.split("\n")
    html_lines = []
    in_list = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{markupsafe.escape(stripped[3:])}</h2>")
            continue
        if stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            item = stripped[2:]
            item = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', item)
            html_lines.append(f"<li>{item}</li>")
            continue
        if in_list:
            html_lines.append("</ul>")
            in_list = False
        if not stripped:
            continue
        text = markupsafe.escape(stripped)
        text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', str(text))
        html_lines.append(f"<p>{text}</p>")

    if in_list:
        html_lines.append("</ul>")
    return markupsafe.Markup("\n".join(html_lines))


def _get_user_accounts(selected_account=""):
    """Display account names for the account picker."""
    return _user_account_list()


def _get_tenant_scope(selected_account=""):
    return _tenants_for_scope(selected_account)


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

_INSIGHTS_ENDPOINTS = frozenset({"insights", "generate_insights", "insights_ask"})


@app.before_request
def _require_insights_feature():
    if app.config.get("INSIGHTS_ENABLED", True):
        return None
    if request.endpoint in _INSIGHTS_ENDPOINTS:
        abort(404)
    return None


@app.route("/insights")
@login_required
def insights():
    """Show coaching data + cached AI analysis."""
    from app.routes import _redirect_if_no_accounts
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    selected_account = request.args.get("account", "")
    user_accounts = _get_user_accounts(selected_account)
    tenant_ids = _get_tenant_scope(selected_account)

    if is_admin(current_user.username):
        accounts = []
    else:
        accounts = user_accounts or []

    cached = get_insight_for_user(current_user.id)
    gemini_available = llm_available()
    llm_models = selectable_models()
    selected_model = resolve_model_key(get_user_llm_model(current_user.id))

    if cached:
        cached["full_analysis_html"] = _md_to_html(cached["full_analysis"])

    # Load deterministic coaching data for the template
    coaching_data = {
        "has_data": False,
        "signals": [],
        "recent_exits": [],
        "behavior_observations": [],
        "discoveries": [],
        "discovery_headline": None,
        "total_closed": 0,
        "reliable_contracts": 0,
        "pct_reliable": 0,
    }
    try:
        client = get_bigquery_client()
        _, coaching_data = _build_coaching_brief(client, tenant_ids)
    except Exception:
        pass

    return render_template(
        "insights.html",
        title="AI Insights",
        insight=cached,
        gemini_available=gemini_available,
        llm_models=llm_models,
        selected_model=selected_model,
        accounts=accounts,
        selected_account=selected_account,
        coaching=coaching_data,
    )


@app.route("/insights/generate", methods=["POST"])
@login_required
@limiter.limit("3 per minute; 10 per hour; 30 per day")
def generate_insights():
    """Build coaching brief, call Gemini, cache the result.

    Rate-limited per signed-in user (extensions._rate_limit_key returns
    user:<id>): 3/min/10/hour/30/day. The cached insight rarely needs
    refresh, so even an over-eager tester hits a generous ceiling without
    burning Gemini quota for the rest of the beta.
    """
    blocked = demo_block_writes("regenerating AI Insights")
    if blocked:
        return blocked
    selected_account = request.args.get("account", "")
    user_accounts = _get_user_accounts(selected_account)
    tenant_ids = _get_tenant_scope(selected_account)
    redir = url_for("insights", account=selected_account) if selected_account else url_for("insights")

    # If the generate form carried a model choice, persist it (validated)
    # so this and future generations / Q&A use it.
    posted_model = (request.form.get("model") or "").strip()
    if posted_model and posted_model in selectable_model_keys():
        set_user_llm_model(current_user.id, posted_model)
    model_key = resolve_model_key(get_user_llm_model(current_user.id))

    try:
        client = get_bigquery_client()

        # Try coaching brief first (the unique data)
        coaching_text, _ = _build_coaching_brief(client, tenant_ids)

        # Fallback to portfolio summary if no coaching data
        if not coaching_text:
            where = _tenant_sql_filter(tenant_ids)
            df = client.query(INSIGHTS_DATA_QUERY.format(where=where)).to_dataframe()
            if df.empty:
                flash("No portfolio data found. Upload your trading data first.", "warning")
                return redirect(redir)
            coaching_text = _build_prompt_data(df)

        if not coaching_text:
            flash("Not enough data to generate insights.", "warning")
            return redirect(redir)

        result, error = _call_coach(coaching_text, model_key=model_key)
        if error:
            flash(error, "danger")
            return redirect(redir)

        summary, full_analysis = result
        save_insight(current_user.id, summary, full_analysis)
        flash("AI Insights analysis generated.", "success")

    except Exception as exc:
        app.logger.exception("AI Insights generation failed: %s", exc)
        flash("Couldn't generate insights right now. Try again in a moment.", "danger")

    return redirect(redir)


@app.route("/insights/model", methods=["POST"])
@login_required
@limiter.limit("30 per minute; 200 per hour")
def set_insights_model():
    """Persist the user's chosen AI model (dropdown auto-save).

    Validates against the live allowlist so a disabled/paid model can't be
    forced in by a hand-crafted POST. Returns JSON for the inline picker."""
    blocked = demo_block_writes("changing the AI model")
    if blocked:
        return blocked
    model_key = (request.form.get("model") or "").strip()
    if model_key not in selectable_model_keys():
        return jsonify({"ok": False, "error": "That model isn't available."}), 400
    set_user_llm_model(current_user.id, model_key)
    return jsonify({"ok": True, "model": model_key})


@app.route("/insights/ask", methods=["POST"])
@login_required
@limiter.limit("10 per minute; 60 per hour; 200 per day")
def insights_ask():
    """Q&A endpoint grounded in coaching signals + portfolio data.

    Each call invokes Gemini with several thousand tokens of context, so
    we cap conversational rate. The 200/day ceiling is roughly a
    multi-hour deep-dive; anything past that is plausibly automated.
    """
    # The demo's pre-seeded insight is its showcase; live Q&A would burn
    # Gemini quota for every stranger that pokes at the chat box. Block
    # at the JSON layer with a 403 so the chat UI can render a banner.
    blocked = demo_block_writes("asking AI Insights questions")
    if blocked:
        return blocked
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question", "")).strip()
    if not question:
        return jsonify({"error": "Question is required."}), 400
    if len(question) > 800:
        question = question[:800]

    selected_account = request.args.get("account", "")
    user_accounts = _get_user_accounts(selected_account)
    tenant_ids = _get_tenant_scope(selected_account)

    try:
        client = get_bigquery_client()

        # Coaching signals (the unique data)
        coaching_text, _ = _build_coaching_brief(client, tenant_ids)

        # Portfolio fallback
        where = _tenant_sql_filter(tenant_ids)
        df = client.query(INSIGHTS_DATA_QUERY.format(where=where)).to_dataframe()
        portfolio_text = _build_prompt_data(df) if not df.empty else None

        # Weekly context
        weekly_text = None
        try:
            wdf = client.query(WEEKLY_QA_QUERY.format(where=where)).to_dataframe()
            if not wdf.empty:
                row = wdf.iloc[0]
                tc = int(row.get("trades_closed", 0) or 0)
                to = int(row.get("trades_opened", 0) or 0)
                tp = float(row.get("total_pnl", 0) or 0)
                divs = float(row.get("dividends_amount", 0) or 0)
                tr = float(row.get("total_return", tp + divs) or 0)
                nw = int(row.get("num_winners", 0) or 0)
                nl = int(row.get("num_losers", 0) or 0)
                total_c = nw + nl
                wr = nw / total_c if total_c else 0
                ws = str(row.get("week_start", ""))
                divs_part = (
                    f", divs ${divs:,.2f}, total return ${tr:,.2f}"
                    if divs else ""
                )
                weekly_text = (
                    f"WEEK {ws}: {tc} closed ({nw}W/{nl}L, {wr:.0%}), "
                    f"{to} opened, trade P&L ${tp:,.2f}{divs_part}"
                )
        except Exception:
            pass

        if not coaching_text and not portfolio_text:
            return jsonify({"error": "No data available to answer questions."}), 400

        answer_md, error = _call_coach_question(
            coaching_text, portfolio_text, weekly_text, question,
            model_key=resolve_model_key(get_user_llm_model(current_user.id)),
        )
        if error:
            return jsonify({"error": error}), 500

        return jsonify({"answer_html": str(_md_to_html(answer_md)), "error": None})

    except Exception as exc:
        return jsonify({"error": f"Could not process question: {exc}"}), 500
