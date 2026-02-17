# "If You Did Nothing" Benchmark — Design (Strategy-Driven)

**Core idea:** *"What if you didn't trade options?"* — For each **strategy** (Covered Call, Wheel, CSP, etc.), compare your actual outcome to simply buying and holding the underlying. Strategy-first, not portfolio-first.

---

## 1. Value Proposition

**The core question:** *"For the stocks I chose to run covered calls / wheels / CSPs on — would I have been better off just buying and holding?"*

The feature is **strategy-driven**:

- **Covered Call:** You did covered calls on AAPL, NVDA, META. What if you'd just bought and held those names?
- **Wheel:** You ran the wheel on TSLA. What if you'd just bought TSLA and held?
- **CSP:** You sold cash-secured puts on GOOGL. What if you'd just bought GOOGL and held?

This answers: *Did the options strategy add value over plain buy-and-hold for the same underlyings and same effective capital?*

It is **not** about going passive in general. It's about: *Given that you picked these stocks, did options help or hurt vs. simply holding them?*

---

## 2. Strategy as the Primary Unit

### 2.1 What We Compare (Per Strategy)

For each **strategy** (Covered Call, Wheel, Cash-Secured Put, etc.):

| Metric | "You" (actual) | "If you did nothing" (benchmark) |
|--------|-----------------|-----------------------------------|
| **Capital** | Same capital deployed in that strategy | Same capital in buy-and-hold of the underlyings |
| **Entry** | First equity buy or assignment date, per position | Same date (cost basis = capital deployed) |
| **Exit** | Actual exit (sell, assignment, expiry) or today | Same date (mark at exit/today) |
| **Return** | Realized + unrealized P&L (options + equity) | Price return of underlying(s) over same period |

So for **Covered Call** we get:

- **Your P&L:** Sum of (realized + unrealized) across all positions classified as Covered Call.
- **Buy-and-hold P&L:** For each Covered Call position, compute "if I'd put the same capital in the stock at first entry and held to exit/today"; sum across positions.
- **Difference:** Your P&L − Buy-and-hold P&L. Positive = options added value; negative = you’d have been better off just holding.

Repeat for **Wheel**, **CSP**, and any other strategy we classify.

### 2.2 Why Strategy-First

- Matches how people think: "I do covered calls" / "I run the wheel."
- Fair comparison: same stocks, same timing, same capital — only the *instrument* (options + equity vs equity only) changes.
- Surfaces which strategies actually beat buy-and-hold and which don’t.
- Lets us say: "Your covered calls underperformed hold by $X. Your wheels beat hold by $Y."

---

## 3. Benchmarks (Strategy-Driven)

### 3.1 Per-Strategy: "What If You Just Bought and Held?" (Primary)

**Concept:** For each strategy, compare your P&L to the P&L of buying and holding the same underlyings over the same periods.

**Computation (per strategy, e.g. Covered Call):**

1. **Positions:** All positions with `strategy = 'Covered Call'` (from positions_summary / classification).
2. **Your P&L:** Sum of `total_return` (or realized + unrealized) for those positions.
3. **Buy-and-hold P&L (counterfactual):**
   - For each position:  
     - Entry: first equity buy or assignment date; cost = cost basis at that point (or capital deployed).  
     - Exit: last sell / assignment / expiry or today.  
     - Hold return = (exit price − entry price) × shares, or equivalent using total cost basis and market value at exit.
   - If we have daily prices (daily_position_performance), we can use first-entry cost basis and exit/today value.
   - Sum across all positions in that strategy.
4. **Difference:** Your P&L − Hold P&L (and optionally % of capital).

**Edge cases:**

- **CSP with no assignment:** No equity ever held; "hold" could be "if you’d bought at the same time you sold the put, at strike." Or we only include CSP positions that eventually had equity (assigned).
- **Wheel:** Same as above; entry = assignment or first buy, exit = final exit or today.
- **Multi-leg / complex:** Use strategy classification as-is; hold benchmark = same underlying, same entry/exit dates.

**UI:**

- **Primary view:** One row per strategy.
  - Strategy | Your P&L | Buy-and-hold P&L | Difference | "You beat hold" / "Hold beat you"
- **Drill-down:** Click strategy → list of positions (symbol, your P&L, hold P&L, difference).
- **Chart (optional):** Cumulative P&L (you vs hold) over time for that strategy.

---

### 3.2 Portfolio-Level: vs SPY (Context)

**Concept:** Keep SPY as a single portfolio-level benchmark: "Your total return vs SPY over the same period."

**Role:** Puts strategy-level "you vs hold" in context. You might beat hold on covered calls but still lag SPY because of stock selection or other strategies.

**Computation:** Unchanged from before: total portfolio return (or invested capital return) vs SPY total return over first trade → last trade (or today).

**UI:** One card or section: "Overall: You vs SPY" — e.g. "You: +8% | SPY: +12%."

---

### 3.3 Worst X (Strategy-Aware)

**Concept:** "How much did your worst X positions cost you?" — still useful, but we can make it **strategy-aware**.

**Options:**

- **A) Worst X positions overall** — Same as before; exclude worst 1/3/5/10 positions by P&L (any strategy).
- **B) Worst X per strategy** — "Your worst 3 covered call positions cost you $X."
- **C) Both** — Overall "worst 5" card + per-strategy "worst 2" for each strategy.

**Computation:** Same as before: rank positions by P&L, drop worst X, recompute total. Positions are already tagged with strategy, so (B) and (C) are filters.

**UI:** Slider "Exclude worst [1] [3] [5] [10] positions" + optional "By strategy" toggle. List of excluded positions (with strategy label).

---

### 3.4 Excluding Earnings Trades (Strategy Filter)

**Concept:** Same as before: identify positions that overlapped with earnings; show P&L with and without them. Can be shown **per strategy** or overall.

**Computation:** Unchanged: earnings dates (API or manual tag), flag positions, sum P&L excluding those. We can break down by strategy: "Covered call earnings P&L vs non-earnings."

**UI:** Toggle "Include earnings trades" + optional breakdown by strategy.

---

## 4. Data Requirements (Strategy-Centric)

| Need | Source | Notes |
|------|--------|-------|
| Strategy per position | positions_summary (strategy) | Already from int_strategy_classification |
| Your P&L per position | positions_summary (total_return, realized, unrealized) | Existing |
| First entry date, cost basis | stg_history (first equity buy or assignment) | Per position / symbol |
| Exit date, value | stg_history (last sell/assignment) + daily_position_performance or current | For hold benchmark |
| Underlying prices | daily_position_performance, yfinance | Same as current app |
| SPY | yfinance (or BigQuery if we backfill) | Portfolio-level only |

**Important:** The hold benchmark needs, per position:

- **Entry:** Date and cost (or shares × price at first buy/assignment).
- **Exit:** Date and value (or shares × price at last sell / today).

We already have trade history and can derive first/last dates and cost basis; we may need a small dbt model or app-level aggregation that outputs, per position (account, symbol, strategy): `first_entry_date`, `first_cost_basis`, `last_exit_date`, `last_value` (or current value). Then "hold P&L" = last_value − first_cost_basis (and we can add dividends if desired).

---

## 5. UI / UX (Strategy-First)

### 5.1 Primary Question

- **Headline:** "What if you didn't trade options?"
- **Sub:** "For each strategy, we compare your result to buying and holding the same stocks."

### 5.2 Dedicated Page: `/benchmark` (or `/if-you-did-nothing`)

```
┌─────────────────────────────────────────────────────────────────┐
│  What if you didn't trade options?                               │
│  Compare each strategy to simply buying and holding.              │
├─────────────────────────────────────────────────────────────────┤
│  [Time range: YTD | 12mo | All]  [Account: All | ...]            │
├─────────────────────────────────────────────────────────────────┤
│  By strategy                                                      │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Strategy        │ Your P&L │ Hold P&L │ Difference          │ │
│  │ Covered Call    │ +$1,200  │ +$1,500  │ Hold beat you by $300│ │
│  │ Wheel           │ +$800    │ +$400    │ You beat hold by $400│ │
│  │ Cash-Secured Put│ +$300    │ +$350    │ Hold beat you by $50 │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  [Drill: click row → positions for that strategy]                │
├─────────────────────────────────────────────────────────────────┤
│  Overall vs SPY   You: +8%  |  SPY: +12%  (SPY beat you by 4%)  │
├─────────────────────────────────────────────────────────────────┤
│  Worst positions  [Exclude worst 1|3|5|10]  "Cost you $X"         │
├─────────────────────────────────────────────────────────────────┤
│  (Phase 2) Excluding earnings  [Toggle]  Earnings contributed $X │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 Dashboard Card

- One compact card: "vs Buy-and-hold: Covered Call −$300, Wheel +$400, CSP −$50" (or top-level total: "Options added $50 vs hold across strategies").
- Link to full benchmark page.

### 5.4 Position Detail (Optional)

- On `/position/<symbol>`: "This position (Covered Call): You +$X vs Hold +$Y" for that single position.

---

## 6. Technical Approach (Strategy-Centric)

### 6.1 Backend

- **Route:** `/benchmark` (e.g. in `app/benchmark.py`).
- **Inputs:** Time range, account filter (same as rest of app).
- **Core logic:**
  1. Query positions_summary (and stg_history if needed) filtered by time range and account.
  2. Group by **strategy**.
  3. For each strategy:
     - Your P&L: sum of total_return (or realized + unrealized) for positions in that strategy.
     - Hold P&L: for each position, compute hold return (first cost → last value); sum.
  4. SPY: total portfolio return vs SPY over same period.
  5. Worst X: rank positions by P&L, exclude worst X, recompute (optionally per strategy).

### 6.2 Hold P&L Computation (Critical)

For each position we need:

- **First entry:** Date and cost (from first equity_buy or assignment that established the position).  
- **Last exit / current:** Date and value (from last equity_sell or assignment that closed, or today’s mark).

Implementation options:

- **A) In-app:** From stg_history, get first/last trade dates and amounts; get price at those dates from daily_position_performance or yfinance. Compute cost and value.
- **B) dbt:** New model, e.g. `position_hold_benchmark`, that outputs per position: `first_entry_date`, `first_cost_basis`, `last_exit_date`, `last_value`. App just sums by strategy.

We already have logic that walks trade history for cost basis and position state (e.g. in chart building). We can reuse that to derive "effective entry" and "effective exit" and then attach prices.

### 6.3 Caching

- Cache benchmark results per (user, time range, account) for the session or 24h.
- SPY series: cache by date range.

---

## 7. Phasing (Strategy-First)

### Phase 1 — Strategy vs hold (core)

- [ ] Per-strategy "your P&L" from positions_summary (group by strategy).
- [ ] Per-position hold P&L: first entry cost → last exit/today value (data + computation).
- [ ] Sum hold P&L by strategy; difference vs your P&L.
- [ ] Benchmark page: table by strategy (Your P&L | Hold P&L | Difference).
- [ ] Drill-down: list of positions for a strategy with same comparison.
- [ ] Dashboard card summarizing "vs hold" by strategy or total.

### Phase 2 — SPY + worst X

- [ ] Portfolio-level "you vs SPY" (same period).
- [ ] Worst X positions (overall and optionally per strategy); "cost you $X" messaging.
- [ ] Add to benchmark page and dashboard.

### Phase 3 — Earnings + polish

- [ ] Earnings flag (API or journal tag); exclude earnings positions toggle.
- [ ] Per-strategy earnings vs non-earnings breakdown.
- [ ] Export, weekly review hook ("Your covered calls underperformed hold by $X this week").

---

## 8. Psychological Framing (Strategy-Focused)

**Tone:** Informative. "Here’s what would have happened if you’d just held."

**Copy examples:**

- "Your covered calls returned $1,200. Buying and holding the same stocks would have returned $1,500."
- "Your wheel on TSLA beat buy-and-hold by $400."
- "Across all strategies, options added $50 vs. simply holding. Covered calls underperformed hold; wheels outperformed."
- "Your worst 5 positions (across strategies) cost you $800."

**Optional:** Short tip per strategy when hold wins: "Consider whether the premium was worth the upside given up on these names."

---

## 9. Open Questions

1. **CSP with no assignment:** Include in "hold" benchmark as "if you’d bought at strike on put sale date" or exclude?
2. **Multi-strategy same symbol:** Same symbol in Covered Call and Wheel in different periods — treat as separate positions (we already do by strategy)?
3. **Dividends:** Include in hold return (recommended)?
4. **SPY:** Compare to total portfolio return or only "invested" capital?
5. **Earnings:** Prefer API or journal tag for v1?
