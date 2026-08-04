"""Unit tests for the Position Leg Tags feature.

Covers three surfaces without a live database:
  1. Postgres CRUD helpers (app.models) — isolation-by-user_id + tag
     normalization, verified against an in-memory stand-in for the
     ``position_leg_tags`` table (the real SQL is monkeypatched at the
     app.db chokepoint, same pattern as the other model tests).
  2. The date-containment tag→leg matcher (`_tags_for_leg_range`) and the
     leg-grained `_build_tag_breakdown` rollup (pure functions).
  3. Daily Review "Trades this week" tag attachment.
"""
import os

# app import triggers init_db unless this is set (mirrors other model tests).
os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

from datetime import date  # noqa: E402

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from app import models  # noqa: E402
from app import routes  # noqa: E402
from app.routes import (  # noqa: E402
    _tags_for_leg_range,
    _build_tag_breakdown,
    _tag_scoped_positions_df,
)
from app.weekly_review import _build_trades_this_week  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# In-memory fake for the position_leg_tags table
# ──────────────────────────────────────────────────────────────────────────
class FakePositionLegTagsDB:
    """Honors the (user_id, tenant_id, symbol, leg_open_date, tag) unique key
    and the user_id isolation contract. Interprets the small set of SQL shapes
    the CRUD helpers emit — enough to prove isolation, not a general engine."""

    def __init__(self):
        self.rows = []

    def _key(self, r):
        return (r["user_id"], r["tenant_id"], r["symbol"], r["leg_open_date"], r["tag"])

    def execute(self, sql, params=None):
        params = tuple(params or ())
        s = " ".join(sql.split())
        if s.startswith("INSERT INTO position_leg_tags"):
            uid, tid, sym, dt, tag = params
            row = {"user_id": uid, "tenant_id": tid, "symbol": sym,
                   "leg_open_date": dt, "tag": tag}
            if not any(self._key(r) == self._key(row) for r in self.rows):
                self.rows.append(row)
        elif s.startswith("DELETE FROM position_leg_tags"):
            uid, tid, sym, dt, tag = params
            self.rows = [
                r for r in self.rows
                if not (r["user_id"] == uid and r["tenant_id"] == tid
                        and r["symbol"] == sym and r["leg_open_date"] == dt
                        and r["tag"] == tag)
            ]
        else:  # pragma: no cover - defensive
            raise AssertionError(f"unexpected execute: {s}")

    def fetch_all(self, sql, params=None):
        params = list(params or ())
        s = " ".join(sql.split())
        # EVERY read must scope on user_id as the first bound param — this is
        # the isolation contract we are asserting.
        assert "WHERE user_id = %s" in s, f"read not scoped by user_id: {s}"
        uid = params[0]
        rows = [r for r in self.rows if r["user_id"] == uid]
        idx = 1
        if "AND symbol = %s" in s:
            sym = params[idx]
            idx += 1
            rows = [r for r in rows if r["symbol"] == sym]
        if "tenant_id = ANY(%s)" in s:
            ids = set(params[-1])
            rows = [r for r in rows if r["tenant_id"] in ids]
        if "SELECT DISTINCT tag" in s:
            return [{"tag": t} for t in sorted({r["tag"] for r in rows})]
        return [dict(r) for r in rows]


@pytest.fixture
def fake_db(monkeypatch):
    db = FakePositionLegTagsDB()
    monkeypatch.setattr(models, "execute", db.execute)
    monkeypatch.setattr(models, "fetch_all", db.fetch_all)
    return db


# ──────────────────────────────────────────────────────────────────────────
# 1. CRUD + normalization + isolation
# ──────────────────────────────────────────────────────────────────────────
class TestTagNormalization:
    def test_trims_lowercases_collapses(self):
        assert models._normalize_leg_tag("  EarningsFollower  ") == "earningsfollower"
        assert models._normalize_leg_tag("EF   Trade") == "ef trade"

    def test_length_cap(self):
        long = "x" * 100
        assert len(models._normalize_leg_tag(long)) == models._MAX_LEG_TAG_LEN

    def test_empty_and_none(self):
        assert models._normalize_leg_tag("") == ""
        assert models._normalize_leg_tag("   ") == ""
        assert models._normalize_leg_tag(None) == ""


class TestTagCrudIsolation:
    T1 = "snaptrade:aaaa"
    T2 = "snaptrade:bbbb"

    def test_add_normalizes_and_returns_tag(self, fake_db):
        out = models.add_position_leg_tag(1, self.T1, "aapl", "2026-01-05", "  EarningsFollower ")
        assert out == "earningsfollower"
        assert fake_db.rows[0]["tag"] == "earningsfollower"
        assert fake_db.rows[0]["symbol"] == "AAPL"  # symbol uppercased

    def test_add_is_idempotent(self, fake_db):
        models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        assert len(fake_db.rows) == 1

    def test_add_rejects_missing_fields(self, fake_db):
        assert models.add_position_leg_tag(1, "", "AAPL", "2026-01-05", "ef") is None
        assert models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "  ") is None
        assert fake_db.rows == []

    def test_user_cannot_read_another_users_tags(self, fake_db):
        models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        models.add_position_leg_tag(2, self.T1, "AAPL", "2026-01-05", "secret")
        # User 1 sees only their own tag for this symbol.
        rows1 = models.get_leg_tags_for_symbol(1, "AAPL")
        assert [r["tag"] for r in rows1] == ["ef"]
        rows2 = models.get_leg_tags_for_symbol(2, "AAPL")
        assert [r["tag"] for r in rows2] == ["secret"]

    def test_remove_is_scoped_by_user(self, fake_db):
        models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        models.add_position_leg_tag(2, self.T1, "AAPL", "2026-01-05", "ef")
        # User 1 removing does not touch user 2's identical row.
        models.remove_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        assert models.get_leg_tags_for_symbol(1, "AAPL") == []
        assert [r["tag"] for r in models.get_leg_tags_for_symbol(2, "AAPL")] == ["ef"]

    def test_get_for_symbol_restricts_to_scoped_tenants(self, fake_db):
        models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        models.add_position_leg_tag(1, self.T2, "AAPL", "2026-02-05", "other")
        rows = models.get_leg_tags_for_symbol(1, "AAPL", tenant_ids=[self.T1])
        assert [r["tag"] for r in rows] == ["ef"]

    def test_get_all_and_distinct(self, fake_db):
        models.add_position_leg_tag(1, self.T1, "AAPL", "2026-01-05", "ef")
        models.add_position_leg_tag(1, self.T2, "TSLA", "2026-03-05", "swing")
        models.add_position_leg_tag(1, self.T2, "TSLA", "2026-03-05", "ef")
        assert models.get_distinct_tags_for_user(1) == ["ef", "swing"]
        all_rows = models.get_all_leg_tags_for_user(1)
        assert len(all_rows) == 3
        scoped = models.get_all_leg_tags_for_user(1, tenant_ids=[self.T1])
        assert {r["symbol"] for r in scoped} == {"AAPL"}


# ──────────────────────────────────────────────────────────────────────────
# 2. Date-containment matcher + rollup
# ──────────────────────────────────────────────────────────────────────────
def _tag(tenant_id, symbol, leg_open_date, tag):
    return {"tenant_id": tenant_id, "symbol": symbol,
            "leg_open_date": leg_open_date, "tag": tag}


class TestTagsForLegRange:
    T = "snaptrade:aaaa"

    def test_matches_within_range_same_tenant(self):
        rows = [_tag(self.T, "AAPL", date(2026, 3, 1), "ef")]
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", "2026-06-01") == ["ef"]

    def test_no_match_wrong_tenant(self):
        rows = [_tag("snaptrade:other", "AAPL", date(2026, 3, 1), "ef")]
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", "2026-06-01") == []

    def test_no_match_outside_range(self):
        rows = [_tag(self.T, "AAPL", date(2025, 1, 1), "ef")]
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", "2026-06-01") == []

    def test_survives_leg_merge_open_date_shift(self):
        # A tag anchored on the leg's ORIGINAL open date (2026-03-15). After a
        # dbt re-chapter, the merged leg's open_date shifted EARLIER to
        # 2026-01-01 and its last_activity extended — the anchor is still
        # contained, so the tag must still match.
        rows = [_tag(self.T, "AAPL", date(2026, 3, 15), "ef")]
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", "2026-09-01") == ["ef"]

    def test_open_leg_uses_open_as_hi_when_no_last(self):
        rows = [_tag(self.T, "AAPL", date(2026, 1, 1), "ef")]
        # last_date None → hi falls back to open_date; the anchor == open matches.
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", None) == ["ef"]

    def test_sorted_distinct(self):
        rows = [
            _tag(self.T, "AAPL", date(2026, 2, 1), "zeta"),
            _tag(self.T, "AAPL", date(2026, 2, 1), "alpha"),
            _tag(self.T, "AAPL", date(2026, 2, 1), "alpha"),
        ]
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", "2026-06-01") == ["alpha", "zeta"]

    def test_symbol_filter_excludes_cross_symbol_anchor(self):
        # The reported bug: a tag is stored (tenant, symbol, leg_open_date). A
        # DIFFERENT symbol's leg whose window happens to CONTAIN the anchor date
        # must NOT inherit the tag. Real case: an open BP Covered Call spanning
        # 2026-08-03 cross-matched an ASTS tag anchored on 2026-08-03.
        rows = [_tag(self.T, "ASTS", date(2026, 8, 3), "earningsfollower")]
        # BP leg window contains the anchor date, but symbol differs.
        assert _tags_for_leg_range(
            rows, self.T, "2026-07-01", "2026-09-01", symbol="BP"
        ) == []
        # Same window, correct symbol → matches.
        assert _tags_for_leg_range(
            rows, self.T, "2026-07-01", "2026-09-01", symbol="ASTS"
        ) == ["earningsfollower"]

    def test_symbol_none_preserves_legacy_tenant_only_match(self):
        # When symbol is omitted (Position Detail passes already-symbol-scoped
        # rows) the matcher stays tenant + date only.
        rows = [_tag(self.T, "ASTS", date(2026, 3, 1), "ef")]
        assert _tags_for_leg_range(rows, self.T, "2026-01-01", "2026-06-01") == ["ef"]


class TestBuildTagBreakdown:
    T = "snaptrade:aaaa"

    def _leg(self, symbol, open_date, last, equity=0.0, opt_closed=0.0, opt_open=0.0):
        return {
            "tenant_id": self.T, "symbol": symbol,
            "open_date": open_date, "last_activity_date": last,
            "equity_pnl": equity, "closed_options_pnl": opt_closed,
            "open_options_pnl": opt_open,
            "combined_pnl": equity + opt_closed + opt_open,
            "status": "Closed",
        }

    def test_only_tagged_legs_counted(self):
        legs = pd.DataFrame([
            self._leg("AAPL", date(2026, 1, 1), date(2026, 2, 1), equity=100.0),
            self._leg("AAPL", date(2026, 3, 1), date(2026, 4, 1), equity=-50.0),
        ])
        tags = [_tag(self.T, "AAPL", date(2026, 1, 1), "ef")]  # only first leg
        out = _build_tag_breakdown(legs, tags)
        assert len(out) == 1
        row = out[0]
        assert row["tag"] == "ef"
        assert row["num_legs"] == 1
        assert row["num_symbols"] == 1
        assert row["net_pnl"] == 100.0  # untagged -50 leg excluded
        assert row["wins"] == 1 and row["losses"] == 0
        assert row["win_rate"] == 100.0

    def test_multi_tag_leg_contributes_to_each_bucket(self):
        legs = pd.DataFrame([
            self._leg("AAPL", date(2026, 1, 1), date(2026, 2, 1), equity=100.0),
        ])
        tags = [
            _tag(self.T, "AAPL", date(2026, 1, 1), "ef"),
            _tag(self.T, "AAPL", date(2026, 1, 1), "swing"),
        ]
        out = {r["tag"]: r for r in _build_tag_breakdown(legs, tags)}
        assert set(out) == {"ef", "swing"}
        assert out["ef"]["net_pnl"] == 100.0
        assert out["swing"]["net_pnl"] == 100.0

    def test_option_and_equity_split(self):
        legs = pd.DataFrame([
            self._leg("AAPL", date(2026, 1, 1), date(2026, 2, 1),
                      equity=30.0, opt_closed=20.0, opt_open=10.0),
        ])
        tags = [_tag(self.T, "AAPL", date(2026, 1, 1), "ef")]
        row = _build_tag_breakdown(legs, tags)[0]
        assert row["equity_pnl"] == 30.0
        assert row["option_pnl"] == 30.0  # 20 closed + 10 open
        assert row["net_pnl"] == 60.0

    def test_empty_inputs(self):
        assert _build_tag_breakdown(pd.DataFrame(), []) == []
        legs = pd.DataFrame([self._leg("AAPL", date(2026, 1, 1), date(2026, 2, 1))])
        assert _build_tag_breakdown(legs, []) == []

    def test_cross_symbol_leg_not_credited(self):
        # A tag on AAPL must not pull in a TSLA leg whose window merely contains
        # the AAPL tag's anchor date (same tenant). Pre-fix the tenant+date-only
        # matcher credited BOTH legs to the tag.
        legs = pd.DataFrame([
            self._leg("AAPL", date(2026, 1, 1), date(2026, 2, 1), equity=100.0),
            self._leg("TSLA", date(2025, 12, 1), date(2026, 3, 1), equity=999.0),
        ])
        tags = [_tag(self.T, "AAPL", date(2026, 1, 1), "ef")]
        out = _build_tag_breakdown(legs, tags)
        assert len(out) == 1
        assert out[0]["num_legs"] == 1
        assert out[0]["num_symbols"] == 1
        assert out[0]["net_pnl"] == 100.0  # TSLA's 999 excluded


# ──────────────────────────────────────────────────────────────────────────
# 2b. /positions tag filter — leg-scoped P&L rebuild
# ──────────────────────────────────────────────────────────────────────────
class TestTagScopedPositionsDf:
    """The reported bug: filtering /positions by a tag showed the WHOLE
    symbol's P&L (all 8 ASTS legs, $5,624 realized) when only one leg (the
    +$905 open Leg 8) carried the tag. The rebuild must report ONLY the tagged
    leg's trade groups, and never leak a different symbol's leg (BP) whose
    window merely contains the tag's anchor date.
    """
    T = "snaptrade:aaaa"

    def _base_df(self):
        # positions_summary-shaped frame (only the columns the rebuild reads for
        # sector/subsector metadata + the empty-frame column contract).
        return pd.DataFrame([
            {"tenant_id": self.T, "account": "Schwab Account", "user_id": 9,
             "symbol": "ASTS", "strategy": "Covered Call", "status": "Closed",
             "total_pnl": 5624.5, "realized_pnl": 5624.5, "unrealized_pnl": 0.0,
             "total_return": 6530.15, "total_dividend_income": 0.0,
             "total_premium_received": 0.0, "num_individual_trades": 40,
             "num_winners": 12, "num_losers": 5,
             "sector": "Technology", "subsector": "Communication Equipment"},
            {"tenant_id": self.T, "account": "Schwab Account", "user_id": 9,
             "symbol": "BP", "strategy": "Covered Call", "status": "Closed",
             "total_pnl": -856.45, "realized_pnl": -856.45, "unrealized_pnl": 0.0,
             "total_return": -856.45, "total_dividend_income": 0.0,
             "total_premium_received": 0.0, "num_individual_trades": 4,
             "num_winners": 1, "num_losers": 1,
             "sector": "Energy", "subsector": "Oil & Gas Integrated"},
        ])

    def _legs_df(self):
        return pd.DataFrame([
            # ASTS tagged open leg (Leg 8) — anchor 2026-08-03 falls here.
            {"tenant_id": self.T, "symbol": "ASTS",
             "open_date": date(2026, 8, 3), "last_activity_date": date(2026, 8, 4),
             "equity_pnl": 1318.0, "closed_options_pnl": 0.0,
             "open_options_pnl": -412.35, "combined_pnl": 905.65, "status": "Open"},
            # ASTS older closed leg — NOT tagged, must be excluded.
            {"tenant_id": self.T, "symbol": "ASTS",
             "open_date": date(2024, 8, 5), "last_activity_date": date(2024, 9, 19),
             "equity_pnl": 0.0, "closed_options_pnl": 5288.08,
             "open_options_pnl": 0.0, "combined_pnl": 5288.08, "status": "Closed"},
            # BP leg whose window CONTAINS 2026-08-03 — cross-symbol trap.
            {"tenant_id": self.T, "symbol": "BP",
             "open_date": date(2026, 7, 1), "last_activity_date": date(2026, 9, 1),
             "equity_pnl": -856.45, "closed_options_pnl": 0.0,
             "open_options_pnl": 0.0, "combined_pnl": -856.45, "status": "Closed"},
        ])

    def _strat_df(self):
        def g(symbol, strat, status, od, total, real, unreal, win, ntr=1, cd=None):
            return {"tenant_id": self.T, "account": "Schwab Account", "user_id": 9,
                    "symbol": symbol, "strategy": strat, "status": status,
                    "open_date": od, "close_date": cd, "days_in_trade": 1,
                    "total_pnl": total, "realized_pnl": real, "unrealized_pnl": unreal,
                    "num_trades": ntr, "premium_received": 0.0, "premium_paid": 0.0,
                    "is_winner": win}
        return pd.DataFrame([
            # ASTS in Leg 8 window (Open) — the two groups that make up +905.65.
            g("ASTS", "Covered Call", "Open", date(2026, 8, 3), -412.35, 0.0, -412.35, False),
            g("ASTS", "Covered Call", "Open", date(2026, 8, 3), 1318.0, 0.0, 1318.0, True),
            # ASTS closed group OUTSIDE the tagged window — excluded.
            g("ASTS", "Long Call", "Closed", date(2024, 8, 5), 5288.08, 5288.08, 0.0, True, cd=date(2024, 9, 19)),
            # BP group inside the same calendar window — excluded (wrong symbol).
            g("BP", "Covered Call", "Closed", date(2026, 8, 3), -856.45, -856.45, 0.0, False, cd=date(2026, 8, 10)),
        ])

    def _patched(self, monkeypatch):
        legs, strat = self._legs_df(), self._strat_df()

        def fake_cached_query_df(client, sql, **kw):
            if "int_position_legs" in sql:
                return legs.copy()
            if "int_strategy_classification" in sql:
                return strat.copy()
            raise AssertionError(f"unexpected query: {sql[:80]}")

        monkeypatch.setattr(routes, "cached_query_df", fake_cached_query_df)

    def test_only_tagged_leg_pnl(self, monkeypatch):
        self._patched(monkeypatch)
        tags = [_tag(self.T, "ASTS", date(2026, 8, 3), "earningsfollower")]
        out = _tag_scoped_positions_df(
            None, [self.T], "", tags, "earningsfollower", self._base_df()
        )
        assert list(out["symbol"]) == ["ASTS"], "cross-symbol BP leaked into the tag"
        assert round(out["total_return"].sum(), 2) == 905.65
        assert round(out["realized_pnl"].sum(), 2) == 0.0
        assert round(out["unrealized_pnl"].sum(), 2) == 905.65
        # Open leg → no closed winners/losers, dividends not leg-scoped.
        assert int(out["num_winners"].sum()) == 0
        assert int(out["num_losers"].sum()) == 0
        assert float(out["total_dividend_income"].sum()) == 0.0
        # Metadata carried from the base mart frame.
        assert out.iloc[0]["subsector"] == "Communication Equipment"

    def test_unknown_tag_returns_empty(self, monkeypatch):
        self._patched(monkeypatch)
        tags = [_tag(self.T, "ASTS", date(2026, 8, 3), "earningsfollower")]
        out = _tag_scoped_positions_df(
            None, [self.T], "", tags, "does-not-exist", self._base_df()
        )
        assert out.empty
        # Empty frame keeps the base columns so downstream KPI sums don't KeyError.
        assert "num_winners" in out.columns and "total_return" in out.columns


# ──────────────────────────────────────────────────────────────────────────
# 3. Daily Review "Trades this week" tag attachment
# ──────────────────────────────────────────────────────────────────────────
class TestTradesThisWeekTags:
    WEEK_START = date(2026, 6, 8)
    WEEK_END = date(2026, 6, 14)
    T = "snaptrade:abc"

    def _row(self, **kw):
        base = {
            "tenant_id": self.T, "account": "Schwab Account",
            "symbol": "ASTS", "trade_symbol": "ASTS  260605C00102000",
            "strategy": "Covered Call", "status": "Closed",
            "open_date": date(2026, 6, 9), "close_date": date(2026, 6, 12),
            "total_pnl": 226.0, "trade_cost": 226.0, "num_trades": 2,
            "current_unrealized_pnl": 0.0, "current_market_value": 0.0,
        }
        base.update(kw)
        return base

    def test_tag_attached_when_anchor_in_week_range(self):
        df = pd.DataFrame([self._row()])
        tags = [_tag(self.T, "ASTS", date(2026, 6, 9), "earningsfollower")]
        out = _build_trades_this_week(
            df, self.WEEK_START, self.WEEK_END, tag_rows=tags
        )
        assert out["trades"][0]["tags"] == ["earningsfollower"]

    def test_stale_tag_on_reused_symbol_not_attached(self):
        # A different (old) leg of ASTS was tagged last year; this week's row
        # is a fresh leg opened 2026-06-09. The old anchor is before this
        # week's leg range, so it must NOT show up on this week's row.
        df = pd.DataFrame([self._row()])
        tags = [_tag(self.T, "ASTS", date(2025, 1, 1), "old")]
        out = _build_trades_this_week(
            df, self.WEEK_START, self.WEEK_END, tag_rows=tags
        )
        assert out["trades"][0]["tags"] == []

    def test_wrong_tenant_not_attached(self):
        df = pd.DataFrame([self._row()])
        tags = [_tag("snaptrade:other", "ASTS", date(2026, 6, 9), "ef")]
        out = _build_trades_this_week(
            df, self.WEEK_START, self.WEEK_END, tag_rows=tags
        )
        assert out["trades"][0]["tags"] == []

    def test_no_tags_key_defaults_empty(self):
        df = pd.DataFrame([self._row()])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["trades"][0]["tags"] == []
