# Visual brand

HappyTrader should look like a trading record, not an AI SaaS template.
`app/templates/base.html` owns the tokens. Pages should use the CSS
variables instead of new hexes.

## Type

| Role | Face | Weights | Stack |
| --- | --- | --- | --- |
| UI, body, headings | Public Sans | 400, 500, 600, 700 | `"Public Sans", system-ui, sans-serif` |
| KPI values, stat bars, numeric utilities | IBM Plex Mono | 400, 500, 600 | `"IBM Plex Mono", ui-monospace, monospace` |

Loaded from Google Fonts in `base.html`. The skeleton shell loads Public Sans only.

- Hero H1s are font-weight **700** with letter-spacing no tighter than `-0.01em`.
- Plex is not loaded above 600. Do not set numeric text to 700 or 800.
- Utilities: `.ht-num` and `.font-mono-nums`. The design-refresh layer already applies Plex to `.kpi-value`, `.ht-statbar .ht-stat-v`, `.kpi-mini .value`, `.income-value`, `.pricing-card .price`, `.dd-money`, and `.dd-delta`.
- Email HTML stays on a system font stack. Email clients strip web fonts. The copper accent still applies.

Do not introduce Inter, Geist, Plus Jakarta Sans, Manrope, Space Grotesk, or Outfit.

## Color

| Token | Light | Dark | Use |
| --- | --- | --- | --- |
| `--color-mirror` | `#b87333` | `#e0b56a` | Brand copper. Fills, icons, focus. |
| `--color-mirror-ink` | `#7a4a1e` | `#e0b56a` | Accent text on a surface. The light fill fails AA as small text on white. |
| `--nav-bg` / heroes | `#1a1a2e` | `#1a1a2e` | Flat charcoal navy. Not a gradient. |
| Page background | `#f4f5f7` | `#12141a` | Neutral gray page. White cards sit on it. |
| `--ht-surface` | `#ffffff` | `#1c1e26` | Cards, stat bars, tables. |
| `--ht-surface-2` | `#f3f4f6` | `#16181f` | Inset strips. |
| `--ht-ink` | `#1c1917` | `#f4f0ea` | Primary text. |
| `--ht-label` | `#44403c` | `#d6d1c9` | Section labels. |
| `--ht-muted` | `#57534e` | `#a8a29e` | Secondary text. |
| `--ht-faint` | `#78716c` | `#78716c` | Tertiary text. |
| `--ht-line` | `#e5e7eb` | `#2e313a` | Hairline borders. |
| `--ht-hover` | `#f3f4f6` | `#262932` | Row hover. |
| `--color-positive` | `#3f7d5c` | same | P&L green. Desaturated tape, still clearly green. |
| `--color-negative` | `#b55249` | same | P&L red. Desaturated tape, still clearly red. |

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
