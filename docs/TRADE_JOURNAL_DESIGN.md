# Trade Journal & Behavioral Analytics Engine

## Overview

A trade-level journal with behavioral metadata that correlates to actual outcomes. Not just notes—an analytics engine that surfaces "how I actually trade" over time.

## Data Model

### Journal Entry (SQLite)
```
journal_entries
├── id
├── user_id
├── account          -- links to user's accounts
├── symbol
├── strategy         -- Covered Call, CSP, Wheel, etc.
├── trade_open_date  -- when the trade was opened (links to BigQuery)
├── trade_close_date -- optional, for closed trades
├── trade_symbol     -- optional, e.g. "AAPL 12/20/2025 180 C" for option-level granularity
├── thesis           -- "Why I'm taking this trade"
├── notes            -- freeform
├── reflection       -- post-trade: "What did I learn?"
├── confidence       -- 1-10
├── mood             -- calm, anxious, euphoric, frustrated, neutral, etc.
├── sleep_quality    -- 1-10 or null
├── entry_time       -- time of day (for time-of-day correlation)
├── created_at
├── updated_at
```

### Journal Tags (many-to-many)
```
journal_tags
├── journal_entry_id
├── tag              -- FOMO, earnings_play, boredom_trade, revenge_trade, high_conviction, etc.
```

### Standard Tags (predefined)
- `fomo` — Chased a move
- `earnings_play` — Trading around earnings
- `boredom_trade` — Trading to do something
- `revenge_trade` — Trying to recover a loss
- `high_conviction` — Strong thesis, sized up
- `scaling_in` — Adding to position
- `scaling_out` — Taking profits gradually
- `hedge` — Risk management
- `thesis_break` — Thesis invalidated, exiting
- `roll` — Rolling a position
- `assignment_plan` — Expecting/handling assignment

## Linking to Trades

**Option A: Match by (account, symbol, strategy, open_date)**
- Join journal to `int_strategy_classification` or `positions_summary`
- Good for aggregate correlation; may match multiple trade groups if dates overlap

**Option B: Match by (account, trade_symbol)** for options
- Most precise for option contracts
- `trade_symbol` = e.g. "AAPL 12/20/2025 180 C"

**Hybrid**: Store `trade_symbol` when available (user selected from their trades). Fall back to (account, symbol, strategy, trade_open_date) for manual entries.

## Correlation Engine

### 1. Tag → Win Rate
```sql
-- For each tag, compute win rate among journaled trades with outcomes
SELECT tag, 
       COUNT(*) as trades,
       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winners,
       AVG(pnl) as avg_pnl
FROM journal_entries e
JOIN journal_tags t ON e.id = t.journal_entry_id
JOIN outcomes o ON e.id = o.journal_entry_id  -- matched trade outcome
GROUP BY tag
```

### 2. Confidence → Performance
- Bin confidence (1-3 low, 4-7 mid, 8-10 high)
- Correlate with realized P&L, win rate

### 3. Mood → P/L
- Aggregate P&L by mood state
- Surface: "You lose X% more when trading anxious"

### 4. Time of Day → Performance
- Morning vs afternoon vs after hours
- Store `entry_time` on journal entry

### 5. Day of Week → Performance
- Derived from trade_open_date

### 6. Strategy → Sharpe (or risk-adjusted return)
- Use existing strategy breakdown + journal linkage
- Add volatility / drawdown if we have daily data

## Data Portability

**Export** (user never loses their data):
- **Full export**: JSON with all journal entries, tags, metadata
- **CSV export**: Flat table for spreadsheets
- **Scheduled backup**: Optional email/S3 backup

**Import**:
- Import from previous export (restore)
- Import from CSV (migration from other tools)

**Format** (export JSON):
```json
{
  "version": 1,
  "exported_at": "2025-02-14T...",
  "entries": [
    {
      "account": "Demo Account",
      "symbol": "AAPL",
      "strategy": "Covered Call",
      "trade_open_date": "2025-01-15",
      "thesis": "...",
      "tags": ["high_conviction"],
      "confidence": 8,
      "mood": "calm",
      ...
    }
  ]
}
```

## UI Flow

1. **Add entry** — From position detail page ("Journal this trade") or standalone Journal page
2. **List entries** — Filterable by symbol, strategy, tag, date range
3. **Edit/reflect** — Add post-trade reflection when trade closes
4. **Analytics dashboard** — Tag→win rate, confidence→P&L, mood→P&L, time patterns
5. **Export** — One-click "Export my journal" (Settings or Journal page)

## Phased Rollout

| Phase | Scope |
|-------|-------|
| 1 | Schema, CRUD, basic form, link from position detail |
| 2 | Correlation engine (needs BQ join or cached outcomes) |
| 3 | Analytics dashboard (charts, insights) |
| 4 | Export/import, backup |
