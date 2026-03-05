# Mirror Score — Design & Specification

**Product:** Behavioral diagnostic analytics. Not gamification.

**Philosophy:** Measures how closely a trader's behavior aligns with their own historical process. No P/L, win rate, or returns. All metrics relative to user's rolling baseline.

---

## Scoring Formula

```
Mirror Score = 0.25 × Discipline + 0.25 × Intent + 0.25 × Risk Alignment + 0.25 × Consistency
```

Each component: 0–100. 100 = perfect alignment; 50 = significant deviation; <30 = strong drift.

---

## Component Definitions (Data-Driven)

### 1. Discipline Score (25%)

| Sub-metric | Inputs | Calculation |
|------------|--------|-------------|
| Position Size Deviation | 30d median size, trade size | Penalize if >150% of median; smooth decay |
| Large Outlier Frequency | 90th %ile size, weekly trades | % trades above 90th %ile; higher → lower score |
| Strategy Drift | Top 2 strategies (baseline), weekly strategies | % trades outside top 2; high % → lower score |

### 2. Intent Score (25%)

| Sub-metric | Inputs | Calculation |
|------------|--------|-------------|
| Trade Clustering | Time between trades, 30d baseline | Spike in frequency vs baseline → lower score |
| Post-Loss Escalation | Loss sequencing, position size | Size increase >X% after loss vs baseline → penalize |
| Holding Time Deviation | Weekly avg holding days, baseline | Deviation from baseline → lower score |

### 3. Risk Alignment Score (25%)

| Sub-metric | Inputs | Calculation |
|------------|--------|-------------|
| Exposure Drift | Weekly avg |amount|, 30d avg | Deviation from baseline |
| Concentration Increase | % to top symbol/strategy | Increase beyond baseline → lower score |
| Risk Expansion Days | Daily exposure vs 120% of avg | % days exceeding → lower score |

### 4. Consistency Score (25%)

| Sub-metric | Inputs | Calculation |
|------------|--------|-------------|
| Position Size Variance | Weekly std vs baseline std | Higher variance vs baseline → lower score |
| Daily Trade Count Variance | Trades/day std vs baseline | Higher → lower score |
| Strategy Switching Rate | Strategy changes per trade | Higher vs baseline → lower score |

---

## Database Schema

### weekly_mirror_scores

| Column | Type | Description |
|--------|------|-------------|
| user_id | INTEGER | FK to users |
| week_start_date | TEXT | ISO date (Monday) |
| discipline_score | REAL | 0–100 |
| intent_score | REAL | 0–100 |
| risk_alignment_score | REAL | 0–100 |
| consistency_score | REAL | 0–100 |
| mirror_score | REAL | 0–100 |
| confidence_level | TEXT | Low / Medium / High |
| diagnostic_sentence | TEXT | Largest deviation insight |
| generated_at | TEXT | ISO datetime |

Migration: `migrations/create_weekly_mirror_scores.sql`

---

## Confidence Level

| Level | Baseline trades |
|-------|-----------------|
| Low | < 30 |
| Medium | 30–100 |
| High | ≥ 100 |

Displayed but does not affect score.

---

## Baseline Computation

**Window:** Rolling 30 days ending the day before the target week's Monday.

**Data sources:**
- `stg_history`: trades (account, trade_date, action, amount, quantity, symbol, instrument_type)
- `int_strategy_classification`: strategy, open_date, close_date, days_in_trade per session

**Baseline metrics:**
- Position size: `abs(amount)` per trade
- Strategy: matched from classification by (account, symbol, trade_date ∈ [open_date, close_date])
- Daily exposure: sum of `abs(amount)` per trade_date
- Trades per day: count per trade_date
- Top 2 strategies: value_counts of strategy
- 90th percentile position size, median position size

---

## Weekly Scoring Query

**Target week:** Monday through Sunday.

**Process:**
1. Fetch baseline trades (30 days before week)
2. Fetch week trades
3. Join strategy + days_in_trade from int_strategy_classification
4. Compute 4 component scores from sub-metrics
5. Mirror Score = 0.25 × (D + I + R + C)
6. Select diagnostic from largest deviation
7. Determine confidence from baseline trade count

---

## Example Output JSON

```json
{
  "week_start_date": "2025-02-10",
  "mirror_score": 82,
  "label": "Aligned",
  "discipline_score": 88,
  "intent_score": 74,
  "risk_alignment_score": 80,
  "consistency_score": 86,
  "confidence_level": "High",
  "diagnostic_sentence": "Position sizes this week exceeded your 30-day median by 40%.",
  "generated_at": "2025-02-17T08:00:00Z"
}
```

---

## UI Component Spec

### Layout (neutral tones, no gamification)

```
┌─────────────────────────────────────────────────────────────┐
│  Mirror Score: 82                                            │
│  Aligned                                                     │
├─────────────────────────────────────────────────────────────┤
│  Discipline          88                                      │
│  Intent              74                                      │
│  Risk Alignment      80                                      │
│  Consistency         86                                      │
├─────────────────────────────────────────────────────────────┤
│  Position sizes this week exceeded your 30-day median by 40%.│
│  [Confidence: High · Based on 142 trades in baseline]        │
└─────────────────────────────────────────────────────────────┘
```

### Labels (score → text)

| Range | Label |
|-------|-------|
| 80–100 | Aligned |
| 60–79 | Moderate deviation |
| 40–59 | Significant drift |
| 20–39 | Strong drift |
| 0–19 | Major deviation |

### Styling

- No confetti, no red/green flashing
- Neutral gray/blue palette
- Analytical, reflective tone

---

## Redesign: Your Trading Fingerprint (product differentiator)

- **Framing:** "Your Trading Fingerprint" — you can't get this anywhere else. Your behavior vs your own baseline; no P/L, no leaderboards.
- **Diagnostic selection:** The single sentence is now taken from the *lowest-scoring* component (biggest deviation), not the first sub-80. More actionable.
- **Baseline summary:** When we compute the score, we also produce a one-line "What good looks like for you" from baseline metrics (e.g. "When you're aligned, you trade ~X times per day; you stick to [top 2 strategies]; position sizes around $Y median."). Displayed under the diagnostic when available.
- **UI:** Hero clarifies unique value; diagnostic is more prominent; baseline summary gives context.
