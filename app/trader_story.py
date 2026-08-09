"""Your Story (/story) — the trader novel.

Runs the position-story engine (app/position_story.py) across EVERY symbol
the user has ever traded, collects each position's behavioral fingerprint,
and composes the cross-position narrative the mirror concept has been
building toward: "here you are as a trader" — identity, eras, standout
stories, and how each style of trading has actually paid.

Every sentence is evidence the position pages can prove: the fingerprints
are recorded by the exact code branches that write the per-position
chapters (see `_new_stats` in app/position_story.py), and every standout
card links to the position page whose story backs the claim. No
psychological labeling — behaviors, counts, and dollars only (AGENTS.md
pattern-detection rules).
"""

from datetime import date

import pandas as pd
from flask import render_template, request
from flask_login import login_required, current_user  # noqa: F401

from app import app
from app.bigquery_client import get_bigquery_client
from app.skeleton import skeleton_page
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.position_story import (
    _behavior_candidates,
    _money,
    _span_text,
    build_position_story,
)
from app.routes import (
    _bq_parallel,
    _redirect_if_no_accounts,
    _tenants_for_scope,
    _user_account_list,
)

# ── Queries ──────────────────────────────────────────────────────────────
# All-symbol versions of the Position Detail story inputs. Same DRIP
# detection join as POSITION_TRADES_QUERY so reinvestments narrate (and
# count) identically to the per-position page. Tenant-scoped in SQL via
# {tenant_filter} AND DataFrame-filtered after fetch — both queries project
# tenant_id (pinned by tests/test_tenant_filtered_queries_carry_tenant_id.py).

STORY_TRADES_QUERY = """
    SELECT
        h.account,
        h.tenant_id,
        h.underlying_symbol AS symbol,
        h.trade_date,
        CASE WHEN d.matched_ex_div_date IS NOT NULL
             THEN 'dividend_reinvest'
             ELSE h.action
        END AS action,
        h.trade_symbol,
        h.instrument_type,
        h.quantity,
        h.price,
        h.amount
    FROM `ccwj-dbt.analytics.stg_history` h
    LEFT JOIN `ccwj-dbt.analytics.int_drip_fills` d
        ON  (d.tenant_id IS NOT DISTINCT FROM h.tenant_id)
        AND d.account            = h.account
        AND (d.user_id IS NOT DISTINCT FROM h.user_id)
        AND d.trade_date         = h.trade_date
        AND d.underlying_symbol  = h.underlying_symbol
    WHERE h.trade_date IS NOT NULL
      AND h.underlying_symbol IS NOT NULL
    {tenant_filter}
    ORDER BY h.trade_date
"""

STORY_DIVIDENDS_QUERY = """
    SELECT account, tenant_id, symbol, trade_date, amount
    FROM `ccwj-dbt.analytics.int_dividend_events`
    WHERE symbol IS NOT NULL
    {tenant_filter}
"""

# Per-symbol lifetime rollup for P&L ranks / open-story counts. Reuses the
# positions_summary grain the tab strip uses so the novel's ranks agree
# with the per-position mirror's "#4 of 94 symbols".
STORY_SUMMARY_QUERY = """
    SELECT
        tenant_id,
        symbol,
        SUM(COALESCE(total_return, 0)) AS total_return,
        MAX(IF(LOWER(TRIM(COALESCE(status, ''))) = 'open', 1, 0)) AS has_open_leg
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE symbol IS NOT NULL
      {tenant_filter}
    GROUP BY tenant_id, symbol
"""

# Symbol-grain public market data (same class as POSITION_SPLITS_QUERY):
# no tenant column exists and none is needed. Do NOT run through
# _filter_df_by_tenant_ids (it would fail-closed to empty for non-admins).
STORY_SPLITS_QUERY = """
    SELECT symbol, split_date, split_ratio
    FROM `ccwj-dbt.analytics.stg_split_events`
    ORDER BY split_date
"""


# ── The book: one fingerprint per symbol ─────────────────────────────────

def build_book(trades_df, div_df, splits_df, summary_df):
    """Run the story engine per symbol → {SYMBOL: entry} where entry has
    the behavioral fingerprint plus the lifetime P&L / open flag from
    positions_summary.

    Interlude stats (quiet_gain/quiet_loss) stay zero here: they need the
    per-day chart series, which is a per-position build too heavy to run
    94 times per page load. The novel narrates from fills + dividends;
    the quiet-stretch voice stays a Position Detail feature.
    """
    book = {}

    def _sym_frames(df, col="symbol"):
        out = {}
        if df is None or df.empty or col not in df.columns:
            return out
        for sym, g in df.groupby(df[col].astype(str).str.strip().str.upper()):
            if sym:
                out[sym] = g
        return out

    trades_by_sym = _sym_frames(trades_df)
    divs_by_sym = _sym_frames(div_df)
    splits_by_sym = _sym_frames(splits_df)

    pnl_by_sym, open_by_sym = {}, {}
    if summary_df is not None and not summary_df.empty:
        for _, r in summary_df.iterrows():
            sym = str(r.get("symbol") or "").strip().upper()
            if not sym:
                continue
            pnl = pd.to_numeric(pd.Series([r.get("total_return")]), errors="coerce").iloc[0]
            pnl_by_sym[sym] = pnl_by_sym.get(sym, 0.0) + (0.0 if pd.isna(pnl) else float(pnl))
            if int(r.get("has_open_leg") or 0):
                open_by_sym[sym] = True

    for sym, sym_trades in trades_by_sym.items():
        try:
            _, _, stats = build_position_story(
                sym_trades,
                divs_by_sym.get(sym),
                None,
                splits_df=splits_by_sym.get(sym),
            )
        except Exception as exc:  # one bad symbol must not sink the book
            app.logger.warning("trader story: engine failed for %s: %s", sym, exc)
            continue
        if not stats["chapters"]:
            continue
        dates = pd.to_datetime(sym_trades["trade_date"], errors="coerce").dropna()
        book[sym] = {
            "stats": stats,
            "pnl": pnl_by_sym.get(sym, 0.0),
            "open": open_by_sym.get(sym, False),
            "first": dates.min().date() if not dates.empty else None,
            "last": dates.max().date() if not dates.empty else None,
        }
    return book


def classify_style(stats):
    """income / directional / stock, by where the dollars went."""
    premium = stats["premium_collected"]
    risk = stats["long_risk"]
    if premium <= 1 and risk <= 1:
        return "stock"
    return "income" if premium >= risk else "directional"


# ── The novel ────────────────────────────────────────────────────────────

_STYLE_LABELS = {
    "income": "Income trades",
    "directional": "Directional trades",
    "stock": "Stock positions",
}

_STYLE_DESC = {
    "income": "positions where you mostly sold premium",
    "directional": "positions where you mostly bought options",
    "stock": "pure stock positions — no options",
}


def _sum_stats(book):
    totals = None
    for entry in book.values():
        s = entry["stats"]
        if totals is None:
            totals = {k: v for k, v in s.items()}
        else:
            for k, v in s.items():
                totals[k] += v
    return totals or {}


def _identity_sentences(book, totals, first_day, last_day):
    """The profile summary: what the fills show."""
    n = len(book)
    chapters = totals.get("chapters", 0)
    sentences = []

    span = ""
    if first_day and last_day and (last_day - first_day).days >= 14:
        span = f" over {_span_text((last_day - first_day).days)}"
    started = f" since {first_day.strftime('%B %Y')}" if first_day else ""
    sentences.append(
        f"You've traded {n} symbols across {chapters:,} trade days"
        f"{span}{started}."
    )

    premium = totals.get("premium_collected", 0.0)
    risk = totals.get("long_risk", 0.0)
    n_long = totals.get("long_opens", 0)
    long_desc = f"{n_long} long-option purchase{'s' if n_long != 1 else ''}"
    short_bits = []
    if totals.get("covered_calls"):
        short_bits.append(f"{totals['covered_calls']} covered call"
                          f"{'s' if totals['covered_calls'] != 1 else ''}")
    if totals.get("puts_sold"):
        short_bits.append(f"{totals['puts_sold']} short put"
                          f"{'s' if totals['puts_sold'] != 1 else ''}")
    if totals.get("rolls"):
        short_bits.append(f"{totals['rolls']} roll"
                          f"{'s' if totals['rolls'] != 1 else ''}")
    short_desc = f" across {', '.join(short_bits)}" if short_bits else ""

    if premium > 1 and risk > 1 and max(premium, risk) < 2 * min(premium, risk):
        sentences.append(
            f"You trade two distinct styles in similar size: income "
            f"({_money(premium)} of premium collected{short_desc}) and "
            f"directional ({_money(risk)} at risk across {long_desc})."
        )
    elif premium > 1 and premium >= risk:
        sentences.append(
            f"Your primary style is income: {_money(premium)} of option "
            f"premium collected{short_desc}."
        )
        if risk > 1:
            sentences.append(
                f"Directional trades are the smaller book — {_money(risk)} "
                f"at risk across {long_desc}."
            )
    elif risk > 1:
        sentences.append(
            f"Your primary style is directional: {_money(risk)} at risk "
            f"across {long_desc}."
        )
        if premium > 1:
            sentences.append(
                f"Income trades are the smaller book — {_money(premium)} of "
                f"premium collected{short_desc}."
            )
    else:
        adds = totals.get("adds", 0) + totals.get("stock_opens", 0)
        sentences.append(
            f"You build stock positions incrementally — {adds} separate "
            f"buys, no options."
        )

    # Signature adjustment: the maneuver that shows up most in the record.
    moves = [
        (totals.get("rolls", 0),
         lambda: (f"Your most frequent adjustment is the roll — "
                  f"{totals['rolls']} times you repositioned a strike "
                  f"rather than closing it.")),
        (totals.get("expired_kept", 0),
         lambda: (f"{totals['expired_kept']} short contracts were held to "
                  f"worthless expiry, keeping the full "
                  f"{_money(totals.get('expired_premium', 0.0))} of premium.")),
        (totals.get("wheels_completed", 0),
         lambda: (f"You've completed {totals['wheels_completed']} full wheel "
                  f"cycle{'s' if totals['wheels_completed'] != 1 else ''} — "
                  f"put premium, assignment, call premium, called away.")),
    ]
    moves.sort(key=lambda m: m[0], reverse=True)
    if moves[0][0] >= 3:
        sentences.append(moves[0][1]())

    w = totals.get("contract_wins", 0)
    losses = totals.get("contract_losses", 0)
    if w + losses >= 5:
        sentences.append(
            f"Across all closed contracts, your record is {w}W / {losses}L."
        )
    if totals.get("dividend_total", 0.0) > 100:
        sentences.append(
            f"Dividends added {_money(totals['dividend_total'])} on top of "
            f"trading P&L."
        )
    return sentences


def _number_chips(book, totals, busiest):
    chips = [
        {"label": "Symbols traded", "value": f"{len(book)}"},
        {"label": "Trade days", "value": f"{totals.get('chapters', 0):,}"},
    ]
    if totals.get("premium_collected", 0.0) > 1:
        chips.append({"label": "Premium collected",
                      "value": _money(totals["premium_collected"])})
    w, losses = totals.get("contract_wins", 0), totals.get("contract_losses", 0)
    if w + losses:
        chips.append({"label": "Contract record", "value": f"{w}W / {losses}L"})
    if totals.get("rolls"):
        chips.append({"label": "Rolls", "value": f"{totals['rolls']}"})
    if totals.get("wheels_completed"):
        chips.append({"label": "Wheels completed",
                      "value": f"{totals['wheels_completed']}"})
    if totals.get("dividend_total", 0.0) > 1:
        chips.append({"label": "Dividends",
                      "value": _money(totals["dividend_total"])})
    if busiest:
        chips.append({"label": "Busiest day", "value": busiest})
    return chips[:6]


def _build_eras(trades_df):
    """One narrative row per calendar year, computed straight from fills."""
    if trades_df is None or trades_df.empty:
        return []
    df = trades_df.copy()
    df["_d"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["_d"])
    if df.empty:
        return []
    df["_year"] = df["_d"].dt.year
    df["_sym"] = df["symbol"].astype(str).str.strip().str.upper()
    df["_amt"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    first_year_by_sym = df.groupby("_sym")["_year"].min()
    years = sorted(df["_year"].unique())
    per_year = {}
    for y in years:
        g = df[df["_year"] == y]
        premium = float(g.loc[
            (g["action"] == "option_sell_to_open") & (g["_amt"] > 0), "_amt"
        ].sum())
        risk = float(-g.loc[
            (g["action"] == "option_buy_to_open") & (g["_amt"] < 0), "_amt"
        ].sum())
        top = g["_sym"].value_counts()
        per_year[y] = {
            "fills": len(g),
            "trade_days": int(g["_d"].dt.date.nunique()),
            "new_symbols": int((first_year_by_sym == y).sum()),
            "premium": premium,
            "risk": risk,
            "top_symbol": top.index[0] if len(top) else "",
        }

    max_premium_year = max(years, key=lambda y: per_year[y]["premium"])
    max_new_year = max(years, key=lambda y: per_year[y]["new_symbols"])
    max_fills_year = max(years, key=lambda y: per_year[y]["fills"])
    this_year = date.today().year

    eras = []
    for y in years:
        p = per_year[y]
        if y == years[0]:
            title = "First year of activity"
        elif y == this_year:
            title = "Year to date"
        elif y == max_premium_year and p["premium"] > 1:
            title = "Highest premium collected"
        elif y == max_new_year and p["new_symbols"] > 2:
            title = "Most new symbols"
        elif y == max_fills_year:
            title = "Most active year"
        else:
            title = "Steady activity"
        bits = [
            f"{p['fills']:,} fills over {p['trade_days']} trading days",
            f"{p['new_symbols']} new symbol{'s' if p['new_symbols'] != 1 else ''}",
        ]
        if p["premium"] > 1:
            bits.append(f"{_money(p['premium'])} premium collected")
        if p["risk"] > 1:
            bits.append(f"{_money(p['risk'])} placed on long options")
        if p["top_symbol"]:
            bits.append(f"most-traded: {p['top_symbol']}")
        eras.append({"year": y, "title": title, "line": " · ".join(bits)})
    return eras


def _hook_for(sym, entry):
    """One-line evidence hook for a standout card: the position's single
    most load-bearing behavior, falling back to its shape."""
    cands = _behavior_candidates(entry["stats"], sym)
    if cands:
        return cands[0][1]
    s = entry["stats"]
    shape = f"{s['chapters']} trade day{'s' if s['chapters'] != 1 else ''}"
    if s["span_days"] >= 14:
        shape += f" across {_span_text(s['span_days'])}"
    return shape + "."


def _build_standouts(book):
    """The stories worth pulling off the shelf, each linking to the
    position page whose chapters prove the claim."""
    if not book:
        return []
    used = set()
    cards = []

    def _pick(label, key, predicate=lambda e: True, note=None):
        best_sym, best_entry = None, None
        for sym, entry in book.items():
            if sym in used or not predicate(entry):
                continue
            if best_entry is None or key(entry) > key(best_entry):
                best_sym, best_entry = sym, entry
        if best_sym is None:
            return
        used.add(best_sym)
        pnl = best_entry["pnl"]
        cards.append({
            "label": label,
            "symbol": best_sym,
            "pnl": pnl,
            "pnl_text": ("+" if pnl >= 0 else "\u2212") + _money(pnl),
            "open": best_entry["open"],
            "hook": note(best_entry) if note else _hook_for(best_sym, best_entry),
        })

    _pick("Top performer", key=lambda e: e["pnl"],
          predicate=lambda e: e["pnl"] > 0)
    _pick("Largest loss", key=lambda e: -e["pnl"],
          predicate=lambda e: e["pnl"] < 0)
    _pick("Most active", key=lambda e: e["stats"]["chapters"],
          predicate=lambda e: e["stats"]["chapters"] >= 5)
    _pick("Longest held", key=lambda e: e["stats"]["span_days"],
          predicate=lambda e: e["stats"]["span_days"] >= 90,
          note=lambda e: (f"{e['stats']['chapters']} trade days across "
                          f"{_span_text(e['stats']['span_days'])} — "
                          f"your longest-running position."))
    _pick("Most re-entered",
          key=lambda e: e["stats"]["away_breaks"],
          predicate=lambda e: e["stats"]["away_breaks"] >= 2,
          note=lambda e: (f"Fully closed and re-entered "
                          f"{e['stats']['away_breaks']} times."))
    return cards


def _build_scoreboard(book):
    """How each style of trading has actually paid, by position outcome."""
    rows = {}
    for sym, entry in book.items():
        style = classify_style(entry["stats"])
        row = rows.setdefault(style, {
            "style": style,
            "label": _STYLE_LABELS[style],
            "desc": _STYLE_DESC[style],
            "symbols": 0, "green": 0, "pnl": 0.0,
            "best_symbol": None, "best_pnl": None,
        })
        row["symbols"] += 1
        if entry["pnl"] > 0:
            row["green"] += 1
        row["pnl"] += entry["pnl"]
        if row["best_pnl"] is None or entry["pnl"] > row["best_pnl"]:
            row["best_symbol"], row["best_pnl"] = sym, entry["pnl"]
    out = sorted(rows.values(), key=lambda r: r["pnl"], reverse=True)
    for r in out:
        r["pnl_text"] = ("+" if r["pnl"] >= 0 else "\u2212") + _money(r["pnl"])
        if r["best_pnl"] is not None:
            r["best_text"] = ("+" if r["best_pnl"] >= 0 else "\u2212") + _money(r["best_pnl"])
    return out


def _busiest_day(trades_df):
    if trades_df is None or trades_df.empty:
        return None, None
    df = trades_df.copy()
    df["_d"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df = df.dropna(subset=["_d"])
    if df.empty:
        return None, None
    counts = df.groupby("_d").agg(
        fills=("_d", "size"),
        syms=("symbol", lambda s: s.astype(str).str.upper().nunique()),
    )
    day = counts["fills"].idxmax()
    row = counts.loc[day]
    if int(row["fills"]) < 5:
        return None, None
    chip = day.strftime("%b %-d, %Y")
    line = (f"Your busiest day was {day.strftime('%B %-d, %Y')} — "
            f"{int(row['fills'])} fills across {int(row['syms'])} "
            f"symbol{'s' if int(row['syms']) != 1 else ''}.")
    return chip, line


def compose_novel(book, trades_df):
    """Assemble the full template context from the per-symbol book."""
    if not book:
        return None
    totals = _sum_stats(book)
    firsts = [e["first"] for e in book.values() if e["first"]]
    lasts = [e["last"] for e in book.values() if e["last"]]
    first_day = min(firsts) if firsts else None
    last_day = max(lasts) if lasts else None
    open_stories = sum(1 for e in book.values() if e["open"])
    busiest_chip, busiest_line = _busiest_day(trades_df)

    identity = _identity_sentences(book, totals, first_day, last_day)
    if busiest_line:
        identity.append(busiest_line)

    return {
        "hero_counts": {
            "stories": len(book),
            "chapters": totals.get("chapters", 0),
            "open_stories": open_stories,
            "since": first_day.strftime("%B %Y") if first_day else "",
        },
        "identity": identity,
        "chips": _number_chips(book, totals, busiest_chip),
        "eras": _build_eras(trades_df),
        "standouts": _build_standouts(book),
        "scoreboard": _build_scoreboard(book),
        "open_stories": open_stories,
    }


# ── Route ────────────────────────────────────────────────────────────────

def story_query_batch(tenant_ids):
    """The /story page's query set, keyed for `_bq_parallel`. Shared with
    the cache warmer (app/cache_ops.py) so warmed keys are EXACTLY the keys
    a request looks up — same discipline as build_daily_review_batch."""
    return {
        "story_trades": STORY_TRADES_QUERY.format(
            tenant_filter=_tenant_sql_and(tenant_ids, col="h.tenant_id")),
        "story_divs": STORY_DIVIDENDS_QUERY.format(
            tenant_filter=_tenant_sql_and(tenant_ids)),
        "story_summary": STORY_SUMMARY_QUERY.format(
            tenant_filter=_tenant_sql_and(tenant_ids)),
        # Public symbol-grain market data — NOT tenant filtered (see
        # STORY_SPLITS_QUERY comment).
        "story_splits": STORY_SPLITS_QUERY,
    }


@app.route("/story")
@login_required
@skeleton_page
def trader_story():
    """The trader novel: every position's story, folded into one book."""
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce

    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "").strip()
    tenant_scope = _tenants_for_scope(selected_account)

    context = {
        "title": "Trader Profile",
        "novel": None,
        "accounts": sorted(user_accounts) if user_accounts else [],
        "selected_account": selected_account,
        "error": None,
    }

    try:
        client = get_bigquery_client()
        dfs = _bq_parallel(client, story_query_batch(tenant_scope))
        trades_df = _filter_df_by_tenant_ids(
            dfs.get("story_trades", pd.DataFrame()), tenant_scope)
        div_df = _filter_df_by_tenant_ids(
            dfs.get("story_divs", pd.DataFrame()), tenant_scope)
        summary_df = _filter_df_by_tenant_ids(
            dfs.get("story_summary", pd.DataFrame()), tenant_scope)
        splits_df = dfs.get("story_splits", pd.DataFrame())

        book = build_book(trades_df, div_df, splits_df, summary_df)
        context["novel"] = compose_novel(book, trades_df)
    except Exception as e:
        if app.debug:
            raise
        app.logger.warning("trader story page failed: %s", e)
        context["error"] = "Couldn't load your trader profile right now."

    return render_template("trader_story.html", **context)
