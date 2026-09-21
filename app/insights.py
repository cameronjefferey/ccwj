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
from app.query_cache import cached_query_df
from app.models import (
    get_accounts_for_user, is_admin,
    save_insight, get_insight_for_user,
    get_user_llm_model, set_user_llm_model,
    get_insight_messages, append_insight_message, clear_insight_messages,
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
    _scope_keep_kwargs,
)
from app.utils import demo_block_writes
from app.llm import (
    call_llm, llm_available, selectable_models,
    selectable_model_keys,
    resolved_user_model_key, model_is_paid,
)
from app.llm_access import user_can_use_paid_llm


# ------------------------------------------------------------------
# Queries — coaching signals from unique data
# ------------------------------------------------------------------

COACHING_SIGNALS_QUERY = """
SELECT
    account, tenant_id, strategy,
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

# Coverage denominator is contracts held in the option-marks window
# (close_date >= first captured mark), not lifetime closed. Reads
# int_option_exit_analysis directly so this card is correct even before
# mart_coaching_signals.eligible_closed has been rebuilt. Projects
# tenant_id for the fail-closed DataFrame filter.
EXIT_COVERAGE_QUERY = """
WITH marks AS (
  SELECT MIN(date) AS marks_start
  FROM `ccwj-dbt.analytics.int_option_marks_daily`
)
SELECT
    e.tenant_id,
    e.strategy,
    COUNT(*) AS total_closed,
    COUNTIF(e.close_date >= m.marks_start) AS eligible_closed,
    COUNTIF(e.data_reliable) AS reliable_contracts
FROM `ccwj-dbt.analytics.int_option_exit_analysis` e
CROSS JOIN marks m
{where}
GROUP BY 1, 2
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
    tenant_id,
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


def strategy_display_label(name, account, tenant_id, name_counts, tenant_labels=None):
    """Attach an account nickname when the same strategy name is repeated.

    Exit Timing "By Strategy" does not use this. Those cards collapse
    through ``_rollup_exit_signals`` — two Covered Call books are one
    row, not "Covered Call" twice and not "Covered Call · Schwab Account"
    twice when both accounts share that display name.
    """
    label_name = (name or "").strip()
    if not label_name:
        return ""
    if (name_counts or {}).get(label_name, 0) <= 1:
        return label_name
    account_label = ""
    if tenant_labels and tenant_id:
        account_label = (
            tenant_labels.get(tenant_id)
            or tenant_labels.get(str(tenant_id))
            or ""
        )
    account_label = (account_label or account or "").strip()
    if account_label:
        return f"{label_name} · {account_label}"
    return label_name


# A strategy card needs this many reliable contracts after accounts are
# combined. Below that, the average is too noisy to label.
_MIN_RELIABLE_FOR_STRATEGY_ROW = 3

# "4 of 391 closed contracts" in a saved narration. Small "N of M" phrases
# (a single example trade) are not coverage claims.
_COVERAGE_CITE_RE = re.compile(
    r"(\d[\d,]*)\s+of\s+(\d[\d,]*)\s+(?:closed\s+)?contracts",
    re.IGNORECASE,
)


def _strategy_label(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "Other Option"
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return "Other Option"
    return text


def _rollup_exit_signals(signals_df):
    """One exit-timing row per strategy name.

    ``mart_coaching_signals`` is grained by (account, strategy). The
    Insights "By Strategy" list used to print that grain directly, so two
    accounts that both trade Covered Calls showed up as two rows with the
    same label and different stats. Dollars sum. Giveback and days past
    peak are weighted by reliable contracts so a 9-contract account does
    not count the same as a 4-contract account.
    """
    if signals_df is None or getattr(signals_df, "empty", True):
        return []

    grouped = {}
    for _, r in signals_df.iterrows():
        name = _strategy_label(r.get("strategy"))
        g = grouped.setdefault(name, {
            "strategy": name,
            "giveback_weighted": 0.0,
            "days_weighted": 0.0,
            "weight": 0,
            "pnl_given_back": 0.0,
            "trades": 0,
            "total_closed": 0,
            "premium_weighted": 0.0,
            "premium_weight": 0,
            "accounts": set(),
            "tenants": set(),
            "best_dte_bucket": None,
            "best_dte_win_rate": 0.0,
            "best_dte_trades": 0,
            "worst_dte_bucket": None,
            "worst_dte_win_rate": 0.0,
            "worst_dte_trades": 0,
        })
        n = int(r.get("reliable_contracts") or 0)
        closed = int(r.get("total_closed") or 0)
        g["trades"] += n
        g["total_closed"] += closed
        g["pnl_given_back"] += float(r.get("total_pnl_given_back") or 0)
        g["giveback_weighted"] += float(r.get("avg_giveback_pct") or 0) * n
        g["days_weighted"] += float(r.get("avg_days_held_past_peak") or 0) * n
        g["weight"] += n
        account = r.get("account")
        if account is not None and not (isinstance(account, float) and pd.isna(account)):
            label = str(account).strip()
            if label and label.lower() != "nan":
                g["accounts"].add(label)
        tenant = r.get("tenant_id")
        if tenant is not None and not (isinstance(tenant, float) and pd.isna(tenant)):
            tid = str(tenant).strip()
            if tid and tid.lower() != "nan":
                g["tenants"].add(tid)
        prem = float(r.get("avg_pct_premium_captured") or 0)
        if prem:
            g["premium_weighted"] += prem * max(n, 1)
            g["premium_weight"] += max(n, 1)
        bt = int(float(r.get("best_dte_trades") or 0))
        best_bucket = r.get("best_dte_bucket")
        if best_bucket and not (isinstance(best_bucket, float) and pd.isna(best_bucket)):
            if bt >= g["best_dte_trades"]:
                g["best_dte_bucket"] = str(best_bucket)
                g["best_dte_win_rate"] = float(r.get("best_dte_win_rate") or 0)
                g["best_dte_trades"] = bt
        wt = int(float(r.get("worst_dte_trades") or 0))
        worst_bucket = r.get("worst_dte_bucket")
        if worst_bucket and not (isinstance(worst_bucket, float) and pd.isna(worst_bucket)):
            if wt >= g["worst_dte_trades"]:
                g["worst_dte_bucket"] = str(worst_bucket)
                g["worst_dte_win_rate"] = float(r.get("worst_dte_win_rate") or 0)
                g["worst_dte_trades"] = wt

    rows = []
    for g in grouped.values():
        if g["trades"] < _MIN_RELIABLE_FOR_STRATEGY_ROW:
            continue
        weight = g["weight"] or 1
        rows.append({
            "strategy": g["strategy"],
            "giveback_pct": g["giveback_weighted"] / weight,
            "days_past_peak": g["days_weighted"] / weight,
            "pnl_given_back": g["pnl_given_back"],
            "trades": g["trades"],
            "total_closed": g["total_closed"],
            "pct_reliable": (
                100.0 * g["trades"] / g["total_closed"] if g["total_closed"] else 0
            ),
            "pct_premium_captured": (
                g["premium_weighted"] / g["premium_weight"] if g["premium_weight"] else 0
            ),
            # Prefer tenant ids. Two Schwab accounts often share the
            # display label "Schwab Account", which would count as one.
            "account_count": len(g["tenants"]) if g["tenants"] else len(g["accounts"]),
            "strategy_label": g["strategy"],
            "best_dte_bucket": g["best_dte_bucket"],
            "best_dte_win_rate": g["best_dte_win_rate"],
            "best_dte_trades": g["best_dte_trades"],
            "worst_dte_bucket": g["worst_dte_bucket"],
            "worst_dte_win_rate": g["worst_dte_win_rate"],
            "worst_dte_trades": g["worst_dte_trades"],
        })
    rows.sort(key=lambda row: row["pnl_given_back"], reverse=True)
    return rows


def _exit_timing_headlines(rows, reliable_contracts, total_closed):
    """The four numbers on the Exit Timing cards.

    Giveback and days-past-peak are the unweighted mean of the strategy
    rows the page lists (same formula the template used to recompute).
    Coverage stays book-wide: contracts below the per-strategy minimum
    still count in "15 of 410".
    """
    if not rows:
        return None
    n = len(rows)
    total_closed = int(total_closed or 0)
    reliable_contracts = int(reliable_contracts or 0)
    return {
        "total_given_back": float(sum(r["pnl_given_back"] for r in rows)),
        "avg_giveback": float(sum(r["giveback_pct"] for r in rows) / n),
        "avg_days": float(sum(r["days_past_peak"] for r in rows) / n),
        "reliable_contracts": reliable_contracts,
        "total_closed": total_closed,
        "pct_reliable": (
            round(reliable_contracts / total_closed * 100, 0) if total_closed else 0
        ),
    }


def _format_exit_timing_brief(headlines, rows):
    """Plain-text copy of the live cards. Ask AI must quote this block."""
    lines = [
        "CANONICAL EXIT TIMING (these are the live Insights cards. "
        "Quote these figures exactly. Do not recompute a different total, "
        "giveback percent, coverage count, or strategy name.):",
        f"- Total left on table: ${headlines['total_given_back']:,.0f}",
        f"- Avg giveback: {headlines['avg_giveback']:.0f}% of peak profit",
        f"- Avg days past peak: {headlines['avg_days']:.0f}",
        (
            f"- Data coverage: {headlines['reliable_contracts']} of "
            f"{headlines['total_closed']} closed contracts "
            f"({headlines['pct_reliable']:.0f}%)"
        ),
        (
            "BY STRATEGY (exit-timing order, profit left on the table. "
            "This is not the Strategies page, which ranks by lifetime P&L. "
            "Use these names as written — do not rename a row.):"
        ),
    ]
    for s in rows:
        accounts = ""
        if int(s.get("account_count") or 0) > 1:
            accounts = f", {int(s['account_count'])} accounts combined"
        lines.append(
            f"- {s.get('strategy_label') or s['strategy']}: ${s['pnl_given_back']:,.0f} left on table, "
            f"{s['giveback_pct']:.0f}% giveback, "
            f"{s['days_past_peak']:.0f} days past peak, "
            f"{s['trades']} of {s['total_closed']} trades with daily marks"
            f"{accounts}."
        )
    return "\n".join(lines)


def analysis_coverage_conflict(text, reliable, total):
    """Return the first coverage cite that disagrees with the live cards.

    Saved narrations say "4 of 391 closed contracts" long after the cards
    have moved to 15 of 410. Small example counts are ignored.
    """
    if not text or not total:
        return None
    live_rel = int(reliable or 0)
    live_tot = int(total or 0)
    for match in _COVERAGE_CITE_RE.finditer(str(text)):
        cited_rel = int(match.group(1).replace(",", ""))
        cited_tot = int(match.group(2).replace(",", ""))
        if cited_tot < 20:
            continue
        if cited_rel != live_rel or cited_tot != live_tot:
            return {
                "cited_reliable": cited_rel,
                "cited_total": cited_tot,
                "live_reliable": live_rel,
                "live_total": live_tot,
            }
    return None


def _insights_scope_narrowed():
    """True when the request is a group, account, or tenant slice.

    The saved AI Analysis is one row per user (the whole book). A narrowed
    page must not show that text next to scoped cards.
    """
    try:
        return bool(_scope_keep_kwargs())
    except Exception:
        return False


def _exit_coverage(signals_df):
    """Coverage vs the option-marks window, not lifetime closed.

    Daily marks only exist from 2026-08-04. Dividing reliable contracts
    by lifetime closed (12 / 407) made a working pipeline look 3% healthy.
    ``eligible_closed`` is the in-window count; fall back to total_closed
    only if the mart has not been rebuilt with the new column yet.
    """
    if signals_df is None or signals_df.empty:
        return 0, 0, 0
    reliable = int(signals_df["reliable_contracts"].sum())
    if "eligible_closed" in signals_df.columns:
        eligible = int(signals_df["eligible_closed"].sum())
    else:
        eligible = int(signals_df["total_closed"].sum())
    pct = round(reliable / eligible * 100, 0) if eligible > 0 else 0
    return reliable, eligible, int(pct)


def _weighted_avg(frame, value_col, weight_col):
    w = frame[weight_col].sum()
    if w <= 0:
        return 0.0
    return float((frame[value_col] * frame[weight_col]).sum() / w)


def _strategy_exit_rows(signals_df, coverage_df=None):
    """Collapse (account × tenant × strategy) to one row per strategy.

    The mart is tenant-grained, so two Schwab accounts both labeled
    Covered Call used to render as duplicate rows (6 of 74 and 4 of 101).

    ``coverage_df`` supplies the marks-window denominator (eligible_closed)
    so "10 of 18" is in-window contracts, not lifetime.
    """
    if signals_df is None or signals_df.empty:
        return []
    cov_by_strat = None
    if coverage_df is not None and not coverage_df.empty:
        cov_by_strat = coverage_df.groupby("strategy", sort=False).agg(
            eligible_closed=("eligible_closed", "sum"),
            reliable_contracts=("reliable_contracts", "sum"),
        )
    rows = []
    for strategy, g in signals_df.groupby("strategy", sort=False):
        if cov_by_strat is not None and strategy in cov_by_strat.index:
            reliable = int(cov_by_strat.loc[strategy, "reliable_contracts"])
            eligible = int(cov_by_strat.loc[strategy, "eligible_closed"])
        else:
            reliable = int(g["reliable_contracts"].sum())
            denom_col = (
                "eligible_closed" if "eligible_closed" in g.columns
                else "total_closed"
            )
            eligible = int(g[denom_col].sum())
        if reliable < _MIN_RELIABLE_FOR_STRATEGY_ROW:
            continue
        rows.append({
            "strategy": strategy,
            "giveback_pct": _weighted_avg(g, "avg_giveback_pct", "reliable_contracts"),
            "days_past_peak": _weighted_avg(
                g, "avg_days_held_past_peak", "reliable_contracts"),
            "pnl_given_back": float(g["total_pnl_given_back"].sum()),
            "trades": reliable,
            "total_closed": eligible,
            "pct_reliable": round(reliable / eligible * 100, 0) if eligible > 0 else 0,
            "pct_premium_captured": _weighted_avg(
                g, "avg_pct_premium_captured", "reliable_contracts"),
        })
    rows.sort(key=lambda x: x["giveback_pct"], reverse=True)
    return rows


# ------------------------------------------------------------------
# Coaching brief builder — the core differentiator
# ------------------------------------------------------------------

def _build_coaching_brief(client, tenant_ids):
    """Build a structured coaching brief from pre-computed dbt signals.

    Returns (brief_text, coaching_data_dict) where coaching_data_dict
    contains the raw data for deterministic rendering in the template.
    """
    where = _tenant_sql_filter(tenant_ids)
    coverage_where = _tenant_sql_filter(tenant_ids, col="e.tenant_id")
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
        "eligible_closed": 0,
        "reliable_contracts": 0,
        "pct_reliable": 0,
        "exit_timing": None,
    }

    disco_cards = []
    disco_txt = ""

    # All four brief queries are independent — one parallel cached wave
    # instead of four serial round trips (/insights clocked 15-17s in
    # production; most of it was this serial fan-out plus the LLM call).
    from datetime import date, timedelta
    from app.routes import _bq_parallel

    since = date.today() - timedelta(days=90)
    exits_cfg = bq.QueryJobConfig(query_parameters=[
        bq.ScalarQueryParameter("since_date", "DATE", since),
    ])
    e_and = _tenant_sql_and(tenant_ids, col="e.tenant_id")
    s_and = _tenant_sql_and(tenant_ids, col="s.tenant_id")
    batch_specs = {
        "coach_signals": COACHING_SIGNALS_QUERY.format(where=where),
        "coach_coverage": EXIT_COVERAGE_QUERY.format(where=coverage_where),
        "coach_exits": (RECENT_EXITS_QUERY.format(tenant_filter=tenant_and), exits_cfg),
        "coach_discovery": _DISCOVERY_SQL.format(
            tenant_clause=e_and if e_and else "",
            sequence_clause=s_and if s_and else "",
        ),
    }
    if app.config.get("BEHAVIOR_INSIGHTS_ENABLED", True):
        batch_specs["coach_behavior"] = BEHAVIOR_OBSERVATIONS_QUERY.format(
            tenant_filter=tenant_and
        )
    batch = _bq_parallel(client, batch_specs)

    # 1. Coaching signals per strategy
    try:
        signals_df = _filter_df_by_tenant_ids(
            batch.get("coach_signals", pd.DataFrame()), tenant_ids)
        if not signals_df.empty:
            coaching_data["has_data"] = True
            for col in ["avg_giveback_pct", "avg_pnl_given_back", "avg_days_held_past_peak",
                         "optimal_exit_rate", "avg_pct_premium_captured", "total_pnl_given_back",
                         "total_closed", "reliable_contracts", "pct_contracts_reliable",
                         "best_dte_win_rate", "worst_dte_win_rate"]:
                if col in signals_df.columns:
                    signals_df[col] = pd.to_numeric(signals_df[col], errors="coerce").fillna(0)

            coverage_df = batch.get("coach_coverage", pd.DataFrame())
            coverage_df = _filter_df_by_tenant_ids(coverage_df, tenant_ids)
            for col in ["total_closed", "eligible_closed", "reliable_contracts"]:
                if col in coverage_df.columns:
                    coverage_df[col] = pd.to_numeric(
                        coverage_df[col], errors="coerce").fillna(0)
            eligible_by_key = {}
            if not coverage_df.empty and "eligible_closed" in coverage_df.columns:
                for _, c in coverage_df.iterrows():
                    eligible_by_key[(
                        str(c.get("tenant_id") or ""),
                        str(c.get("strategy") or ""),
                    )] = int(c.get("eligible_closed") or 0)

            total_given_back = float(signals_df["total_pnl_given_back"].sum())
            coverage_src = coverage_df if not coverage_df.empty else signals_df
            reliable_contracts, eligible_closed, pct_reliable = _exit_coverage(
                coverage_src)

            coaching_data["total_closed"] = int(signals_df["total_closed"].sum())
            coaching_data["eligible_closed"] = eligible_closed
            coaching_data["reliable_contracts"] = reliable_contracts
            coaching_data["pct_reliable"] = pct_reliable

            # One card per strategy name. The mart is (account, strategy);
            # the template, Regenerate, and Ask AI all read this list.
            strat_rows = _rollup_exit_signals(signals_df)
            if eligible_by_key:
                window_by_strategy = {}
                for (_tid, strategy), n in eligible_by_key.items():
                    window_by_strategy[strategy] = window_by_strategy.get(strategy, 0) + n
                for s in strat_rows:
                    window = window_by_strategy.get(str(s.get("strategy") or ""))
                    if window is None:
                        continue
                    s["total_closed"] = window
                    trades = int(s.get("trades") or 0)
                    s["pct_reliable"] = (
                        round(trades / window * 100, 0) if window else 0
                    )
            coaching_data["signals"] = strat_rows
            headlines = _exit_timing_headlines(
                strat_rows,
                reliable_contracts,
                eligible_closed or coaching_data["total_closed"],
            )
            coaching_data["exit_timing"] = headlines
            if headlines:
                sections.append(_format_exit_timing_brief(headlines, strat_rows))

            dte_lines = []
            for s in strat_rows:
                best_b = s.get("best_dte_bucket")
                worst_b = s.get("worst_dte_bucket")
                if best_b and worst_b and best_b != worst_b:
                    bwr = float(s.get("best_dte_win_rate") or 0)
                    wwr = float(s.get("worst_dte_win_rate") or 0)
                    if bwr - wwr >= 15:
                        dte_lines.append(
                            f"- {s.get('strategy_label') or s['strategy']}: best at {best_b} ({bwr:.0f}% WR, "
                            f"{int(s.get('best_dte_trades') or 0)} trades), "
                            f"worst at {worst_b} ({wwr:.0f}% WR). "
                            f"Win rate by tenor, separate from the giveback dollars above."
                        )
            if dte_lines:
                sections.append(
                    "DTE WIN RATES (not giveback, not the Strategies page ranking)\n"
                    + "\n".join(dte_lines[:5])
                )

    except Exception as exc:
        app.logger.warning("insights: DTE sweet-spots section failed (prompt degrades): %s", exc)

    # 2. Recent exits (last 90 days, for weekly context)
    try:
        exits_df = batch.get("coach_exits", pd.DataFrame())
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
    except Exception as exc:
        app.logger.warning("insights: recent-exits section failed (prompt degrades): %s", exc)

    try:
        dq = batch.get("coach_discovery", pd.DataFrame())
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
            obs_df = batch.get("coach_behavior", pd.DataFrame())
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

    return f"""PORTFOLIO OVERVIEW (lifetime P&L by strategy — a different ranking from CANONICAL EXIT TIMING; do not use these dollars or this order when answering about giveback, coverage, or profit left on the table)
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
sufficient daily snapshot data (at least 40% of hold days covered, minimum 2
snapshots) that closed since daily option marks began (August 2026). The
denominator is that marks window, not lifetime closed — a trader with years
of history will have many contracts that can never be scored. The data will
tell you how many contracts qualified. If coverage is low (e.g., "15 of 40
contracts since marks began"), acknowledge that the patterns are based on a
subset and may become clearer as more daily data accumulates. Do NOT present
partial-coverage findings as definitive. Do NOT treat a small numerator over
a large lifetime closed count as a data-quality failure.

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

CANONICAL EXIT TIMING (when present) is the live Insights cards. Those
dollars, percents, coverage counts, and strategy names are the only
exit-timing figures you may cite. Do not average the rows into a new
percent, do not add a second "left on the table" total, and do not rename
a strategy (a row labeled Covered Call stays Covered Call). BY STRATEGY
ranks profit left on the table. The Strategies page ranks lifetime P&L.
Do not mix those rankings. DTE WIN RATES are win rates by tenor, not
giveback.

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
- Optionally: PRIOR ANALYSIS — the cached insights report already generated for this trader
- Optionally: EXECUTION REVIEW — early-exit grades vs holding to expiry
- Prior turns of this conversation, when present. Follow up in context.

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
- Do NOT recommend changing size or strategy.

CANONICAL EXIT TIMING (when present) is the live Insights cards. Quote
those dollars, percents, coverage counts, and strategy names exactly.
Do not recompute a different total, giveback percent, or coverage.
Do not rename a strategy row. BY STRATEGY ranks profit left on the table;
PORTFOLIO OVERVIEW ranks lifetime P&L. Do not answer an exit-timing
question with the portfolio ranking, and do not answer a P&L question
with the giveback ranking, without saying which one you are using.

PRIOR ANALYSIS is an older narration. If any figure in it disagrees with
CANONICAL EXIT TIMING, ignore the prior figure."""


def _call_coach(data_text, model_key=None, allow_paid=False):
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
        allow_paid=allow_paid,
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


def _call_coach_question(brief_text, question, model_key=None,
                         allow_paid=False, history=None):
    """Narrate a Q&A answer. Brief stays in the system block so follow-ups
    do not re-paste it as a fake user turn."""
    system = QA_SYSTEM_PROMPT
    if brief_text:
        system = (
            QA_SYSTEM_PROMPT
            + "\n\nGround every answer in this trader's data:\n\n"
            + brief_text
        )
    return call_llm(
        system,
        question,
        kind="coach.ask",
        max_tokens=1500,
        temperature=0.6,
        model_key=model_key,
        allow_paid=allow_paid,
        history=history,
    )


def _execution_brief_text(client, tenant_ids):
    """Plain-text rollup of the execution-review card, or None."""
    from app.execution_quality import EXECUTION_REVIEW_QUERY, summarize_execution

    try:
        sql = EXECUTION_REVIEW_QUERY.format(
            tenant_filter=_tenant_sql_and(tenant_ids),
        )
        df = cached_query_df(client, sql, label="insights_execution")
        df = _filter_df_by_tenant_ids(df, tenant_ids)
        summary = summarize_execution(df)
    except Exception as exc:
        app.logger.warning("insights: execution brief failed: %s", exc)
        return None
    if not summary:
        return None
    lines = ["EXECUTION REVIEW"]
    headline = summary.get("headline") or {}
    if headline.get("value"):
        lines.append(
            f"- {headline.get('value')} {headline.get('text') or ''}. "
            f"{headline.get('sub') or ''}".strip()
        )
    for row in summary.get("findings") or []:
        lines.append(
            f"- {row.get('label')}: {row.get('value')} — {row.get('detail')}"
        )
    return "\n".join(lines) if len(lines) > 1 else None


def _prior_analysis_brief(user_id):
    cached = get_insight_for_user(user_id)
    if not cached:
        return None
    text = (cached.get("full_analysis") or cached.get("summary") or "").strip()
    if not text:
        return None
    if len(text) > 3000:
        text = text[:3000].rstrip() + "\n[truncated]"
    return (
        "PRIOR ANALYSIS (older narration — coverage and dollars here can "
        "disagree with the live cards. If any figure disagrees with "
        "CANONICAL EXIT TIMING, ignore the prior figure):\n" + text
    )


def _ask_brief(coaching_text, portfolio_text, weekly_text,
               execution_text=None, prior_text=None):
    parts = []
    if coaching_text:
        parts.append("BEHAVIORAL SIGNALS:\n" + coaching_text)
    if weekly_text:
        parts.append("LAST WEEK DATA:\n" + weekly_text)
    if portfolio_text:
        parts.append("PORTFOLIO OVERVIEW:\n" + portfolio_text)
    if execution_text:
        parts.append(execution_text)
    if prior_text:
        parts.append(prior_text)
    return "\n\n".join(parts) if parts else None


_MD_HEADING_RE = re.compile(r"^(#{1,3})\s+(.*?)\s*$")


def _md_inline(text):
    """Escape, then restore **bold**. Asterisks are not HTML-special."""
    escaped = str(markupsafe.escape(text))
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)


def _md_to_html(md_text, compact_headings=False):
    """Simple markdown-to-HTML for model output.

    ``compact_headings`` is the Ask AI thread: ``#`` / ``##`` become a
    small label, not a page-sized ``h2``. The saved analysis keeps ``h2``
    so "## Summary" still reads as a section.
    """
    lines = (md_text or "").split("\n")
    html_lines = []
    in_list = False

    def _close_list():
        nonlocal in_list
        if in_list:
            html_lines.append("</ul>")
            in_list = False

    for line in lines:
        stripped = line.strip()
        heading = _MD_HEADING_RE.match(stripped)
        if heading:
            _close_list()
            title = _md_inline(heading.group(2))
            if compact_headings:
                html_lines.append(f'<div class="ask-md-heading">{title}</div>')
            else:
                level = len(heading.group(1))
                tag = "h2" if level <= 2 else "h3"
                html_lines.append(f"<{tag}>{title}</{tag}>")
            continue
        if stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{_md_inline(stripped[2:])}</li>")
            continue
        _close_list()
        if not stripped:
            continue
        html_lines.append(f"<p>{_md_inline(stripped)}</p>")

    _close_list()
    return markupsafe.Markup("\n".join(html_lines))


def _get_user_accounts(selected_account=""):
    """Display account names for the account picker."""
    return _user_account_list()


def _get_tenant_scope(selected_account=""):
    return _tenants_for_scope(selected_account)


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

_INSIGHTS_ENDPOINTS = frozenset({
    "insights", "generate_insights", "insights_ask",
    "insights_ask_clear", "set_insights_model",
})

_ASK_THREAD_LIMIT = 12


def _ask_not_using_paid():
    """Skip the tighter paid-model rate limit when this ask will use Haiku/Flash."""
    try:
        if not current_user.is_authenticated:
            return True
        key = resolved_user_model_key(
            current_user.id, get_user_llm_model(current_user.id),
        )
        return not model_is_paid(key)
    except Exception:
        return True


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
    can_use_paid = user_can_use_paid_llm(current_user.id)
    llm_models = selectable_models()
    selected_model = resolved_user_model_key(
        current_user.id, get_user_llm_model(current_user.id),
    )
    ask_thread = []
    for msg in get_insight_messages(current_user.id, limit=_ASK_THREAD_LIMIT):
        content = msg.get("content") or ""
        ask_thread.append({
            "role": msg.get("role"),
            "content": content,
            "html": (
                _md_to_html(content, compact_headings=True)
                if msg.get("role") == "assistant" else None
            ),
        })

    scope_narrowed = _insights_scope_narrowed()

    # Load deterministic coaching data for the template
    coaching_data = {
        "has_data": False,
        "signals": [],
        "recent_exits": [],
        "behavior_observations": [],
        "discoveries": [],
        "discovery_headline": None,
        "total_closed": 0,
        "eligible_closed": 0,
        "reliable_contracts": 0,
        "pct_reliable": 0,
        "exit_timing": None,
    }
    try:
        client = get_bigquery_client()
        _, coaching_data = _build_coaching_brief(client, tenant_ids)
    except Exception as exc:
        app.logger.warning("insights: coaching brief failed (page renders without coach data): %s", exc)

    insight_conflict = None
    if cached and not scope_narrowed:
        cached["full_analysis_html"] = _md_to_html(cached.get("full_analysis") or "")
        insight_conflict = analysis_coverage_conflict(
            cached.get("full_analysis") or cached.get("summary") or "",
            coaching_data.get("reliable_contracts"),
            coaching_data.get("eligible_closed") or coaching_data.get("total_closed"),
        )

    return render_template(
        "insights.html",
        title="AI Insights",
        insight=cached,
        gemini_available=gemini_available,
        llm_models=llm_models,
        selected_model=selected_model,
        can_use_paid_llm=can_use_paid,
        ai_addon_enabled=_ai_addon_enabled(),
        ai_price_display=_ai_price_display(),
        ask_thread=ask_thread,
        accounts=accounts,
        selected_account=selected_account,
        coaching=coaching_data,
        scope_narrowed=scope_narrowed,
        insight_conflict=insight_conflict,
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
    redir = url_for("insights", **_scope_keep_kwargs())
    # The saved analysis is one whole-book row. Regenerating under a group
    # or account filter would replace it with a slice. The filtered page
    # hides that write-up instead.
    if _insights_scope_narrowed():
        flash(
            "AI Analysis is saved for your whole book. Clear the filter to regenerate it.",
            "info",
        )
        return redirect(redir)

    # If the generate form carried a model choice, persist it (validated)
    # so this and future generations / Q&A use it.
    posted_model = (request.form.get("model") or "").strip()
    if posted_model and posted_model in selectable_model_keys():
        if not model_is_paid(posted_model) or user_can_use_paid_llm(current_user.id):
            set_user_llm_model(current_user.id, posted_model)
    allow_paid = user_can_use_paid_llm(current_user.id)
    model_key = resolved_user_model_key(
        current_user.id, get_user_llm_model(current_user.id),
    )

    try:
        client = get_bigquery_client()

        # Try coaching brief first (the unique data)
        coaching_text, _ = _build_coaching_brief(client, tenant_ids)

        # Fallback to portfolio summary if no coaching data
        if not coaching_text:
            where = _tenant_sql_filter(tenant_ids)
            df = cached_query_df(
                client, INSIGHTS_DATA_QUERY.format(where=where),
                label="insights_data",
            )
            if df.empty:
                flash("No portfolio data found. Upload your trading data first.", "warning")
                return redirect(redir)
            coaching_text = _build_prompt_data(df)

        if not coaching_text:
            flash("Not enough data to generate insights.", "warning")
            return redirect(redir)

        result, error = _call_coach(
            coaching_text, model_key=model_key, allow_paid=allow_paid,
        )
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
    if model_is_paid(model_key) and not user_can_use_paid_llm(current_user.id):
        return jsonify({
            "ok": False,
            "error": "That model is part of the AI add-on.",
            "upgrade": True,
        }), 403
    set_user_llm_model(current_user.id, model_key)
    return jsonify({"ok": True, "model": model_key})


@app.route("/insights/ask", methods=["POST"])
@login_required
@limiter.limit("10 per minute; 60 per hour; 200 per day")
@limiter.limit("20 per hour", exempt_when=_ask_not_using_paid)
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
        df = cached_query_df(
            client, INSIGHTS_DATA_QUERY.format(where=where),
            label="insights_data",
        )
        portfolio_text = _build_prompt_data(df) if not df.empty else None

        # Weekly context
        weekly_text = None
        try:
            wdf = cached_query_df(
                client, WEEKLY_QA_QUERY.format(where=where),
                label="insights_weekly_qa",
            )
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
        except Exception as exc:
            app.logger.warning("insights: weekly-context section failed (coach answers without it): %s", exc)

        execution_text = _execution_brief_text(client, tenant_ids)
        # A narrowed filter must not answer from the whole-book narration.
        prior_text = None if _insights_scope_narrowed() else _prior_analysis_brief(current_user.id)
        brief_text = _ask_brief(
            coaching_text, portfolio_text, weekly_text,
            execution_text=execution_text, prior_text=prior_text,
        )
        if not brief_text:
            if _insights_scope_narrowed():
                return jsonify({
                    "error": "Nothing in this filter to answer from.",
                }), 400
            return jsonify({"error": "No data available to answer questions."}), 400

        allow_paid = user_can_use_paid_llm(current_user.id)
        model_key = resolved_user_model_key(
            current_user.id, get_user_llm_model(current_user.id),
        )
        # A filtered page answers from the scoped brief only. The saved
        # thread is whole-book and would reintroduce the numbers the
        # filter just hid.
        history = []
        if not _insights_scope_narrowed():
            history = [
                {"role": m.get("role"), "content": m.get("content")}
                for m in get_insight_messages(
                    current_user.id, limit=_ASK_THREAD_LIMIT)
            ]
        answer_md, error = _call_coach_question(
            brief_text, question,
            model_key=model_key,
            allow_paid=allow_paid,
            history=history,
        )
        if error:
            return jsonify({"error": error}), 500

        append_insight_message(current_user.id, "user", question, model_key)
        append_insight_message(current_user.id, "assistant", answer_md, model_key)
        return jsonify({
            "answer_html": str(_md_to_html(answer_md, compact_headings=True)),
            "error": None,
        })

    except Exception as exc:
        return jsonify({"error": f"Could not process question: {exc}"}), 500


@app.route("/insights/ask/clear", methods=["POST"])
@login_required
@limiter.limit("10 per hour")
def insights_ask_clear():
    """Wipe this user's Ask AI thread. Demo stays write-blocked."""
    blocked = demo_block_writes("clearing the Ask AI thread")
    if blocked:
        return blocked
    clear_insight_messages(current_user.id)
    if request.accept_mimetypes.best == "application/json" or (
        request.headers.get("X-Requested-With", "") == "XMLHttpRequest"
    ):
        return jsonify({"ok": True})
    return redirect(url_for("insights", **_scope_keep_kwargs()))


def _ai_addon_enabled():
    try:
        from app.billing import ai_addon_enabled
        return ai_addon_enabled()
    except Exception:
        return False


def _ai_price_display():
    try:
        from app.billing import PRICE_AI_MONTHLY_DISPLAY
        return PRICE_AI_MONTHLY_DISPLAY
    except Exception:
        return "9.99"
