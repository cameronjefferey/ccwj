# Visual brand

HappyTrader should look like a trading record, not an AI SaaS template.
`app/templates/base.html` owns the tokens. Pages should use the CSS
variables instead of new hexes.

## Type

| Role | Face | Weights | Stack |
| --- | --- | --- | --- |
| UI, body, headings | Instrument Sans | 400–700 | `"Instrument Sans", ui-sans-serif, system-ui, sans-serif` |
| Tickers, prices, P&L, table dates | JetBrains Mono | 400, 500, 600, 700 | `"JetBrains Mono", ui-monospace, monospace` |

Loaded from Google Fonts in `base.html`. The skeleton shell loads Public Sans only.

- Hero H1s are font-weight **700** with letter-spacing no tighter than `-0.01em`.
- Plex is not loaded above 600. Do not set numeric text to 700 or 800.
- Utilities: `.ht-num` and `.font-mono-nums`. The design-refresh layer already applies Plex to `.kpi-value`, `.ht-statbar .ht-stat-v`, `.kpi-mini .value`, `.income-value`, `.pricing-card .price`, `.dd-money`, and `.dd-delta`.
- Email HTML stays on a system font stack. Email clients strip web fonts. The copper accent still applies.

Do not introduce Inter, Geist, Plus Jakarta Sans, Manrope, Space Grotesk, or Outfit.

## Color

The product is a dark desk. There is no light theme.

| Token | Hex | Use |
| --- | --- | --- |
| Page (public) | `#05070c` | Marketing field, with mint and blue glows |
| `--ht-page` | `#0a0e17` | Logged-in background |
| `--ht-surface` | `#121826` | Cards |
| `--ht-surface-2` | `#1a2233` | Nested stats, hover |
| `--ht-line` | `#243049` | Card borders |
| `--ht-ink` | `#e8edf7` | Body |
| `--ht-muted` | `#8a97b1` | Labels (public muted is `#7d8aa3`) |
| `--ht-mint` | `#5b8cff` | Wordmark tail and primary button. Purply-blue, not the P&L green. Text on it is `#0a0e17` |
| `--color-mirror` / `--ht-blue` | `#5b8cff` | Selected nav, links, focus |
| `--color-positive` | `#28c08a` | Gains only |
| `--color-negative` | `#f0556d` | Losses only |

Wordmark is lowercase `happy` in `#ffffff` plus `trader` in mint, weight 650, tracking `-0.03em`.

Buttons and other white-on-copper fills stay `#b87333` in both themes. White text on the dark-mode `#e0b56a` fails contrast, so do not bind a white label to `var(--color-mirror)`.

Primary Bootstrap chrome (`--bs-primary`, links, focus rings) uses the same copper. The nav progress bar runs `#1a1a2e` → `#b87333`.

App cards use a hairline border and no drop shadow. Marketing `.feature-card` may keep a mild hover-lift.

## Do not use as the brand

- Inter, or the other display faces listed above.
- Bootstrap purple `#6f42c1` or violet `#7c3aed` as an "AI" or "mirror" accent.
- Indigo SaaS chrome (`#6366f1`, `#4f46e5`, `#4338ca`, `#6d5dfc`) on AI cards, focus rings, or mirror callouts. Use copper.
- A 3-stop night-sky gradient (`#1a1a2e` → `#16213e` → `#0f3460`) or a blue night-sky (`#0f172a` → `#1e3a8a`, `#1e1b4b` → `#4c1d95`).
- Cool slate page fill `#eef1f6` or cool dark canvas `#151e30` / `#0b1220`.
- A cream or warm-off-white page fill (`#f7f5f2`, `#fffcf8`, `#f3efe9`). It was tried and rejected. The page is neutral gray; cards are white. Copper washes on a single callout (`#f8f1e7`) are fine. The canvas is not.

## Leave alone

Strategy classification swatches (Covered Call blue, Cash-Secured Put and Poor Man's Covered Call `#6f42c1`, Wheel orange, and the rest of those maps) are data colors. Do not repaint them to match the brand.

Equity-sleeve chart series that use navy `#0f3460` are data colors, not the hero gradient.

Buy / sell / lifecycle / income chips stay a distinct set so a lifecycle event is not the brand accent.

User profile accent presets (violet, teal, amber, rose, slate) are a settings choice, not the product brand.
