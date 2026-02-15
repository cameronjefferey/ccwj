# "What If" Trade Simulator — Pre-Trade Planning Tool

## Vision

Before placing a trade, show the user what similar setups have done historically: win rate, worst-case drawdown, 30-day forward scenarios, and P&L probability distribution. Turns gut feel into evidence-based planning. **Sticky** because it’s used right before every trade.

---

## Core Features

| Feature | Description | Why it matters |
|---------|-------------|----------------|
| **1. Similar setup performance** | Historical win rate, avg P&L, avg duration for trades that match this setup | "Covered calls on AAPL: 78% win rate, $120 avg" |
| **2. Worst-case drawdown** | Max peak-to-trough loss during the life of similar trades | "Worst drawdown in similar trades: -$450" |
| **3. 30-day forward scenario** | Simulated or historical outcomes over the next ~30 days | "In 100 similar paths: 60 profitable, 25 breakeven, 15 losers" |
| **4. Probability distribution** | P&L histogram or percentiles (5th, 25th, 50th, 75th, 95th) | "80% chance of $0–$200, 5% chance of worse than -$300" |

---

## What Is a "Similar Setup"?

Similarity is the core of the product. Define it in layers.

### Tier 1: Exact match (your own history)

- Same **strategy** (Covered Call, CSP, Wheel, etc.)
- Same **symbol** (AAPL, NVDA, …)
- Optional: same **moneyness** (ATM, OTM by delta band)

**Data source:** `int_strategy_classification`, `int_option_contracts`, `int_equity_sessions`  
**Limit:** Requires enough user history (e.g. 10+ similar trades).

### Tier 2: Strategy + symbol class

- Same **strategy**
- Similar **symbol type** (e.g. mega-cap tech, ETF, high IV) instead of exact ticker

**Data source:** Same BQ models + symbol metadata (sector, market cap, IV rank if available).

### Tier 3: Strategy only (fallback)

- Same **strategy** across all symbols

**Data source:** All closed trades for that strategy in `int_strategy_classification`.

### Optional filters

- **DTE band** (e.g. 30–45 days)
- **Delta band** (e.g. 0.15–0.25 for short options)
- **Size** (if position size is available)

---

## Data Requirements

### Existing (today)

| Source | Content | Use |
|--------|---------|-----|
| `int_strategy_classification` | Per-trade: symbol, strategy, open/close dates, total_pnl, status | Similar-setup queries, avg P&L, win rate |
| `int_option_contracts` | Strike, expiry, premium, direction | DTE, delta, moneyness filters |
| `positions_summary` | Aggregated P&L by strategy/symbol | High-level validation |

### Gaps

| Gap | Options | Effort |
|-----|---------|--------|
| **Intra-trade drawdown** | Need daily P&L or marks for each trade | Option A: Schwab daily sync (option prices) → store daily marks. Option B: Simulate using underlying returns + option model. Option C: Use max adverse excursion from tick data (if ever available). |
| **30-day scenarios** | Need underlying price paths | Historical returns bootstrap, or Monte Carlo (lognormal/other), or implied vol surface if available |
| **Delta / moneyness** | Not in current schema | Add to `int_option_contracts` (Black–Scholes or provided by data source) |
| **Symbol metadata** | Sector, market cap, IV rank | External data (e.g. yfinance, Polygon) or manual mapping |

---

## Architecture

### Flow

```
User enters: symbol, strategy, optional params (strike, expiry, size)
       │
       ▼
Similarity engine
  - Query BQ for matching historical trades
  - Fallback: Strategy+symbol → Strategy only
       │
       ▼
Analytics engine
  - Compute: win rate, avg P&L, avg duration
  - Drawdown: from daily marks (if available) or simulation
  - 30-day: bootstrap or Monte Carlo
  - Distribution: empirical from history + simulated tails
       │
       ▼
UI: cards + charts
  - Similar setup performance
  - Worst-case drawdown
  - 30-day scenario summary
  - P&L probability distribution (histogram / percentiles)
```

### Endpoints

| Route | Method | Purpose |
|-------|--------|---------|
| `/simulator` | GET | Main simulator page |
| `/api/simulator/analyze` | POST | Input: symbol, strategy, params → JSON with all four outputs |

### Caching

- Cache similarity results (e.g. symbol+strategy) for ~1 hour.
- Cache 30-day simulations (same inputs) for the session.

---

## Implementation Phases

### Phase 1: Similar setup performance (MVP)

**Scope**

- User inputs: symbol, strategy.
- Query `int_strategy_classification` for closed trades: same strategy, same symbol (Tier 1).
- If &lt; 5 trades: fallback to same strategy only (Tier 3).
- Output: win rate, avg P&L, avg duration, count of similar trades.
- UI: simple cards on a simulator page.

**Data:** Existing BQ models.  
**Effort:** ~1–2 days.

---

### Phase 2: Probability distribution (empirical)

**Scope**

- Use the same similar-set query.
- Build empirical P&L distribution.
- Show: histogram, percentiles (5th, 25th, 50th, 75th, 95th).
- UI: histogram + percentile table.

**Data:** Same as Phase 1.  
**Effort:** ~0.5–1 day.

---

### Phase 3: Worst-case drawdown (approximation)

**Scope**

- **If daily marks exist:** Compute max peak-to-trough for each similar trade, then show worst and median.
- **If not:** Use a simple proxy, e.g. "Worst single-trade loss in similar setups: -$X" (from `total_pnl`), until daily data exists.
- UI: card with worst drawdown and sample size.

**Data:** Daily option/position prices (Schwab sync, or future) or simulated marks.  
**Effort:** ~1–2 days with proxy; more if building daily mark storage.

---

### Phase 4: 30-day forward scenario

**Scope**

- Bootstrap: Resample historical returns of the underlying (or similar underlyings).
- For each path, value the option/position at T+30 (e.g. Black–Scholes for options).
- Aggregate: % profitable, % breakeven, % loss, avg P&L.
- UI: scenario summary (e.g. pie or bar) + short narrative.

**Data:** Historical daily returns (e.g. yfinance, or stored from Schwab), option structure.  
**Effort:** ~2–3 days.

---

### Phase 5: Full probability (history + simulation)

**Scope**

- Combine empirical (Phase 2) with simulated tails (Phase 4).
- Blend or show both: "Based on N similar trades + 1000 simulated paths."
- UI: unified distribution view.

**Effort:** ~1 day.

---

## UI Wireframe (Conceptual)

```
┌─────────────────────────────────────────────────────────────────┐
│  What If Simulator                                    [Save]    │
├─────────────────────────────────────────────────────────────────┤
│  Plan your trade before you place it.                            │
├─────────────────────────────────────────────────────────────────┤
│  Symbol: [AAPL ▼]   Strategy: [Covered Call ▼]                   │
│  (optional) Strike: [180]  Expiry: [12/20/2025]  Size: [1]      │
│                                          [Analyze]               │
├─────────────────────────────────────────────────────────────────┤
│  SIMILAR SETUPS (23 past trades)                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                         │
│  │ Win Rate │ │ Avg P&L  │ │ Avg Days │                         │
│  │   78%    │ │  $127    │ │   24     │                         │
│  └──────────┘ └──────────┘ └──────────┘                         │
├─────────────────────────────────────────────────────────────────┤
│  WORST-CASE DRAWDOWN                                             │
│  Worst in similar trades: -$412 (1 of 23)                        │
├─────────────────────────────────────────────────────────────────┤
│  30-DAY FORWARD SCENARIO (1000 paths)                            │
│  [=========>        ] 62% profit  [====>    ] 28% flat  [=>] 10% loss│
├─────────────────────────────────────────────────────────────────┤
│  P&L PROBABILITY DISTRIBUTION                                    │
│  [ histogram of P&L ]                                            │
│  5th %ile: -$180    50th: $95    95th: $280                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Entry Points

- **Nav:** "Simulator" link in main nav.
- **Pre-trade:** Optional "Simulate first" link from Positions or before creating a journal entry.
- **Journal:** "Simulate similar" when creating a new journal entry.

---

## Success Metrics

- **Usage:** % of users who open Simulator at least once per week.
- **Conversion:** % of Simulator sessions followed by a journal entry or trade.
- **Retention:** Correlation between Simulator usage and 30-day retention.

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Not enough similar trades | Tier 2/3 fallbacks; clearly state sample size; suggest more journaling |
| Past performance ≠ future | Disclaimers; show confidence intervals; avoid overprecision |
| Daily data for drawdown | Phase 3 starts with proxy; add real drawdown when Schwab sync provides daily marks |
| Option pricing complexity | Start with simple strategies (CC, CSP); add spreads later |

---

## Summary

| Phase | Feature | Data | Effort |
|-------|---------|------|--------|
| 1 | Similar setup performance | BQ (existing) | 1–2 days |
| 2 | Empirical P&L distribution | BQ (existing) | 0.5–1 day |
| 3 | Worst-case drawdown | Proxy now; daily marks later | 1–2 days |
| 4 | 30-day forward scenario | Historical returns + option model | 2–3 days |
| 5 | Combined distribution | Phases 2 + 4 | ~1 day |

**Total:** ~5–9 days for the full feature.

Phase 1 alone delivers the main value: "How have similar trades performed?" and can ship quickly.
