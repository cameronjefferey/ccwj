import re
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from google.cloud import bigquery


# ---------------------------------------------------------------------------
# Yahoo symbol mapping
# ---------------------------------------------------------------------------
# Brokers and Yahoo use different conventions for some equity classes.
# Most notably PREFERRED SHARES: Schwab's API returns a preferred series
# as ``<root>-<class>`` (e.g. GLOP/PRC → ``GLOP-C`` in the API), but
# Yahoo's symbol for the same security is ``<root>-P<class>``
# (``GLOP-PC``). Dual-class COMMON shares like Berkshire Class B and
# Brown-Forman Class B use the broker form on Yahoo too (``BRK-B``,
# ``BF-B`` both resolve), so we cannot blindly inject the ``P``.
#
# The collision is real. ``XYZ-C`` could be either:
#   * a third class of common stock (rare; Yahoo: ``XYZ-C``)
#   * a preferred series C  (common; Yahoo: ``XYZ-PC``)
# We cannot tell from the broker symbol alone — Schwab strips the ``PR``
# from ``XYZ/PRC`` and gives us only ``XYZ-C``. So we try the broker
# form first; if that returns no data we fall back to the preferred-
# style ``<root>-P<class>``. Worst case for a delisted symbol: one
# extra HTTP roundtrip per cron run.
#
# Why this matters: when the primary symbol returns no data, NOTHING
# downstream of ``daily_position_performance`` (price, dividends,
# splits) reaches the user. The synthetic dividend path in
# ``int_dividend_events.sql`` multiplies ``shares_held × div_per_share``
# from ``stg_daily_prices``, so an empty Yahoo fetch silently zeros out
# every dividend on the position. (May 2026 GLOP-C: position showed
# $0.00 in dividends despite the user receiving $2,809.76 in qualified
# dividends per the Schwab transaction history.)

_PREFERRED_CANDIDATE_RE = re.compile(r"^([A-Z][A-Z0-9.]{0,5})-([A-Z])$")


def _yahoo_symbol_candidates(broker_sym):
    """Return ordered list of Yahoo-symbol candidates to try for one broker symbol.

    The first non-empty history wins; the loader writes rows back under
    the original ``broker_sym`` so downstream joins on
    ``underlying_symbol`` keep working unchanged.
    """
    if not isinstance(broker_sym, str):
        return []
    sym = broker_sym.strip()
    if not sym:
        return []
    candidates = [sym]
    m = _PREFERRED_CANDIDATE_RE.match(sym)
    if m and not sym.startswith(m.group(1) + "-P"):
        candidates.append(f"{m.group(1)}-P{m.group(2)}")
    return candidates


def _fetch_history_for_symbol(broker_sym, start_iso, end_iso):
    """Fetch ``(history_df, ticker_used, yahoo_symbol_used)`` for one symbol.

    Returns ``(None, None, None)`` when no candidate yields any rows.
    Caller is responsible for the split-adjust pass and the rename/
    write-back into ``daily_position_performance`` under ``broker_sym``.
    """
    last_err = None
    for cand in _yahoo_symbol_candidates(broker_sym):
        try:
            t = yf.Ticker(cand)
            h = t.history(start=start_iso, end=end_iso)
        except Exception as e:
            last_err = e
            continue
        if h is not None and not h.empty:
            if cand != broker_sym:
                print(
                    f"Yahoo symbol fallback for {broker_sym!r}: using {cand!r} "
                    f"({len(h)} rows)."
                )
            return h, t, cand
    if last_err is not None:
        print(f"Error fetching data for {broker_sym}: {last_err}")
    return None, None, None


# ---------------------------------------------------------------------------
# Main script body
# ---------------------------------------------------------------------------
# Guarded by ``if __name__ == "__main__"`` so unit tests can import the
# helper functions above (``_yahoo_symbol_candidates``,
# ``_fetch_history_for_symbol``) without triggering BigQuery client
# construction. Pre-guard the import did:
#   - top-level ``client = bigquery.Client()``  → ADC required
#   - top-level ``client.query(...).result()``  → live BQ query
# Both made the test module uncollectable when running outside CI.
def _main():
    # Step 1: Initialize BigQuery client
    client = bigquery.Client()

    # Step 2: Query BigQuery to get all traded underlyings (equity AND option positions)
    # so we also have stock prices for symbols held only as options.
    query = """
        SELECT account, user_id, underlying_symbol AS symbol, MIN(trade_date) AS position_open_date
        FROM `ccwj-dbt.analytics.stg_history`
        WHERE underlying_symbol IS NOT NULL
        GROUP BY 1, 2, 3
    """
    positions = client.query(query).result()
    positions_df = pd.DataFrame([dict(row.items()) for row in positions])

    # Step 2b: Add benchmark tickers (SPY, QQQ) so weekly review can skip live yfinance.
    # We mint one benchmark row per (account, user_id) so prices propagate to every
    # tenant — see docs/USER_ID_TENANCY.md.
    BENCHMARK_TICKERS = ["SPY", "QQQ"]
    benchmark_start = date(date.today().year, 1, 1)
    benchmark_rows = []
    if not positions_df.empty:
        tenant_pairs = positions_df[["account", "user_id"]].drop_duplicates().to_records(index=False)
    else:
        tenant_pairs = [("_benchmark", None)]
    for account, user_id in tenant_pairs:
        for sym in BENCHMARK_TICKERS:
            already_present = not positions_df.empty and (
                (positions_df["symbol"] == sym)
                & (positions_df["account"] == account)
                & (positions_df["user_id"].astype("object") == user_id)
            ).any()
            if not already_present:
                benchmark_rows.append({
                    "account": account,
                    "user_id": user_id,
                    "symbol": sym,
                    "position_open_date": benchmark_start,
                })
    if benchmark_rows:
        positions_df = pd.concat(
            [positions_df, pd.DataFrame(benchmark_rows)], ignore_index=True
        )

    # Step 3: Collect daily price & dividend data
    today = date.today()
    # yfinance's end is exclusive, so use tomorrow to include today's close price
    end_date = (today + timedelta(days=1)).isoformat()
    all_data = []

    # Stock-split ledger. Per-symbol (de-duplicated below) so a single split
    # event is not written N times when the same symbol appears across many
    # (account, user_id) pairs. Schema: (symbol, split_date, split_ratio).
    #
    # yfinance's `ticker.splits` ratio convention: 2.0 for a 2:1 forward
    # split (1 share becomes 2), 0.0333 for a 1:30 reverse (30 shares
    # become 1). Apply forward to all trades BEFORE split_date to express
    # pre-split fills in the same share-unit as the post-split snapshot.
    # `int_split_factors` does the cumulative product downstream.
    #
    # Why a separate table (vs. piggybacking daily_position_performance):
    # splits are SYMBOL-grain, not (account, user_id, symbol, date)-grain.
    # Splitting them onto the per-tenant per-day table wastes ~10x rows
    # and forces split-adjustment models into ``stg_daily_prices+`` (the
    # CI two-pass build's pass-2 bucket). Keeping them in their own source
    # means `int_split_factors` builds in pass 1 alongside `int_equity_sessions`,
    # which is where split-adjusted running quantities matter most.
    splits_seen = {}  # (symbol, split_date) -> split_ratio (last write wins)

    for _, row in positions_df.iterrows():
        account = row["account"]
        user_id = row["user_id"] if "user_id" in row else None
        symbol = row["symbol"]
        start_date = row["position_open_date"].isoformat()

        try:
            # Some preferred-share symbols (e.g. Schwab "GLOP-C") return no
            # data when queried under the broker form because Yahoo uses
            # "<root>-P<class>" for preferreds (e.g. "GLOP-PC"). The helper
            # tries the broker form first, then falls back to the preferred
            # form — see ``_yahoo_symbol_candidates``. Rows are still
            # written back keyed on the broker symbol so downstream joins
            # (``stg_history.underlying_symbol`` → ``stg_daily_prices.symbol``)
            # don't need to know about the Yahoo translation.
            hist, ticker, yahoo_sym = _fetch_history_for_symbol(
                symbol, start_date, end_date
            )
            if hist is None or hist.empty:
                print(
                    f"No yfinance data for {symbol} (start={start_date}, "
                    f"end={end_date}); skipping."
                )
                continue

            # Undo yfinance's split back-adjustment.
            #
            # yfinance always returns historical close prices retroactively
            # adjusted for splits — pre-split prices are scaled so that they
            # are directly comparable, on a per-current-share basis, to
            # post-split prices. For a 1-for-30 reverse split this means
            # pre-split closes are *multiplied* by 30 (because 1 new share
            # represents 30 old shares); two consecutive 1-for-30 reverse
            # splits inflate pre-split closes by 900×.
            #
            # That convention is the wrong default for us because our share
            # ledger is built from raw broker tickets (``stg_history``), not
            # from current-shares. Marking 8000 raw RVSN shares against a
            # back-adjusted $2,214 close produced a $17.7M phantom equity
            # peak on the /accounts chart (real peak ~$10K). The user never
            # held shares at $2,214 — that's just yfinance reverse-engineering
            # 2026 splits onto 2024 prices.
            #
            # Fix: multiply each historical close by the product of split
            # ratios for splits that occur AFTER its date. For RVSN on
            # 2024-12-30 the future ratios are 0.033333 × 0.033333 ≈
            # 0.001111 → raw close $2.46. For dates after all known splits
            # (or for symbols with no splits) the factor is 1 and we keep
            # whatever yfinance returned. Dividends from yfinance are not
            # consumed downstream (mart_daily_pnl reads them from
            # stg_history), so we leave the Dividends column alone.
            try:
                splits = ticker.splits
            except Exception:
                splits = None
            if splits is not None and len(splits) and not hist.empty:
                split_dates = list(splits.index)
                split_ratios = [float(r) for r in splits.values]
                close = hist["Close"].astype(float).copy()
                hist_dates = hist.index
                for sd, ratio in zip(split_dates, split_ratios):
                    if not (ratio > 0):
                        continue
                    # Compare CALENDAR DATES, not timestamps. yfinance ships
                    # split timestamps at 09:30 ET (the moment of the split,
                    # at market open) while history rows are indexed at
                    # midnight ET. A naive ``hist_dates < sd`` comparison
                    # treats the split-day close (4:00 PM ET) as pre-split
                    # and incorrectly multiplies it by the ratio — producing
                    # a one-day chart spike (May 2026 XLU: 2025-12-05 close
                    # stored as $85.36 instead of the actual $42.68 post-
                    # split close, drawing a $137K MTM cliff on the chart
                    # for a single day). yfinance already returns the close
                    # in POST-split units on the split day itself; only
                    # PRE-split-DATE closes need un-adjustment.
                    try:
                        sd_date = sd.date() if hasattr(sd, "date") else sd
                    except Exception:
                        sd_date = sd
                    mask = pd.Series(hist_dates).dt.date < sd_date
                    mask.index = hist_dates
                    if mask.any():
                        close.loc[mask] = close.loc[mask] * ratio
                    # Record this split for the daily_split_events table.
                    # Same symbol may iterate many times (one per tenant) —
                    # dict de-dup ensures one row per (symbol, split_date).
                    splits_seen[(symbol, sd_date)] = ratio
                hist["Close"] = close

            hist = hist[["Close", "Dividends"]].reset_index()
            hist["account"] = account
            hist["user_id"] = user_id
            hist["symbol"] = symbol
            hist.rename(columns={"Date": "date", "Close": "close_price", "Dividends": "dividend"}, inplace=True)

            # Ensure we're only keeping data from on/after position_open_date
            hist = hist[hist["date"].dt.date >= row["position_open_date"]]

            all_data.append(hist)
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    # Step 4: Upload results to BigQuery
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # user_id round-trips through pandas as object/Int64; coerce to nullable Int64
        # so BigQuery loads it cleanly as INT64 NULLABLE alongside legacy NULL rows.
        final_df["user_id"] = pd.to_numeric(final_df["user_id"], errors="coerce").astype("Int64")
        final_df = final_df[["account", "user_id", "symbol", "date", "close_price", "dividend"]]

        table_id = "ccwj-dbt.analytics.daily_position_performance"
        final_df = final_df.drop_duplicates()
        job = client.load_table_from_dataframe(
            final_df,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        )
        job.result()  # Waits for the job to complete
        print("Upload successful.")
    else:
        print("No data to upload.")

    # Step 5: Upload stock-split events to BigQuery (separate table).
    #
    # WRITE_TRUNCATE on every run so a yfinance correction (e.g. retroactively
    # de-listed split, mis-reported ratio) self-heals without manual cleanup.
    # Schema is symbol-grain (no account/user_id) — splits are corporate
    # actions and apply identically to every tenant who held the symbol.
    splits_table_id = "ccwj-dbt.analytics.daily_split_events"
    if splits_seen:
        splits_df = pd.DataFrame(
            [
                {"symbol": sym, "split_date": sd, "split_ratio": float(ratio)}
                for (sym, sd), ratio in splits_seen.items()
                if ratio is not None and float(ratio) > 0
            ]
        )
    else:
        # Always emit an EMPTY table so the dbt source registration has a
        # relation to bind to on first deploy. Without this, the very first
        # CI build of a fresh deploy would fail when stg_split_events tries
        # to read a non-existent table.
        splits_df = pd.DataFrame(
            columns=["symbol", "split_date", "split_ratio"]
        )

    if not splits_df.empty:
        splits_df["split_date"] = pd.to_datetime(splits_df["split_date"]).dt.date

    splits_job = client.load_table_from_dataframe(
        splits_df,
        splits_table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("symbol", "STRING"),
                bigquery.SchemaField("split_date", "DATE"),
                bigquery.SchemaField("split_ratio", "FLOAT64"),
            ],
        ),
    )
    splits_job.result()
    print(f"Uploaded {len(splits_df)} split event(s) to {splits_table_id}.")


if __name__ == "__main__":
    _main()
