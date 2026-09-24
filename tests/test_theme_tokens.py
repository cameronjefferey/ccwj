"""Daily Review contrast is a theme issue, not a single hex.

Light-mode ink on a dark card and dark-mode muted on a white card are
the same bug: a color that is correct in only one theme. Tokens on
<html data-bs-theme> flip both.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_base_defines_paired_theme_tokens():
    src = (ROOT / "app/templates/base.html").read_text()
    assert "--ht-ink:" in src
    assert "--ht-muted:" in src
    assert "--ht-surface:" in src
    # Dark values live on the attribute selector so they inherit.
    dark_idx = src.index('[data-bs-theme="dark"]')
    dark_block = src[dark_idx:dark_idx + 800]
    assert "--ht-ink: #f4f0ea" in dark_block
    assert "--ht-surface: #1c1e26" in dark_block


def test_daily_review_identity_text_uses_tokens():
    src = (ROOT / "app/templates/_review_styles.html").read_text()
    for needle in (
        ".snapshot-table .acct-name { font-weight: 700; color: var(--ht-ink);",
        ".snapshot-table .acct-meta { font-size: .68rem; color: var(--ht-muted);",
        ".tt-muted { color: var(--ht-muted);",
        ".watch-sym { font-weight: 700; font-size: 1rem; color: var(--ht-ink);",
        ".section-label {",
    ):
        assert needle in src
    assert "color: var(--ht-label);" in src
    # The two one-theme hexes must not be reintroduced as body text.
    assert ".acct-name { font-weight: 700; color: #0f172a" not in src
    assert ".tt-muted { color: #94a3b8" not in src
    assert ".tt-muted { color: #475569" not in src


def test_snapshot_table_numeric_columns_line_up():
    """Overview multi-account snapshot: row rules + dollars share an edge."""
    src = (ROOT / "app/templates/_review_styles.html").read_text()
    table_idx = src.index(".snapshot-table {")
    table_block = src[table_idx:table_idx + 900]
    assert "align-items: stretch;" in table_block
    assert "align-items: baseline;" not in table_block
    assert "text-align: right;" in src
    assert ".snapshot-table-row > div:not(:first-child)" in src
    assert "font-variant-numeric: tabular-nums;" in src


def test_day_detail_muted_uses_token():
    src = (ROOT / "app/templates/day_detail.html").read_text()
    assert ".dd-muted { color: var(--ht-muted);" in src


def test_brand_fonts_and_copper_accent_not_inter_or_bootstrap_purple():
    """Sep 2026 de-AI pass: Public Sans + Plex, copper accent, flat navy."""
    base = (ROOT / "app/templates/base.html").read_text()
    skeleton = (ROOT / "app/templates/_skeleton.html").read_text()
    landing = (ROOT / "app/templates/landing.html").read_text()

    assert "family=Inter" not in base
    assert "font-family: \"Inter\"" not in base
    assert "font-family: Inter" not in skeleton
    assert "family=Public+Sans" in base
    assert "family=IBM+Plex+Mono" in base
    assert "Public Sans" in base
    assert "IBM Plex Mono" in base
    assert "Public Sans" in skeleton

    assert "--color-mirror: #6f42c1" not in base
    assert "--color-mirror: #b87333" in base
    assert "--color-mirror: #e0b56a" in base
    assert "#7c3aed" not in base
    assert "linear-gradient(135deg, #1a1a2e 0%, #16213e" not in base
    assert "--nav-bg: #1a1a2e" in base
    assert "background: #f4f5f7" in base
    assert "background: #f7f5f2" not in base
    assert "--ht-surface: #ffffff" in base
    assert "--ht-surface: #fffcf8" not in base

    assert "linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%)" not in landing
    assert "background: #1a1a2e" in landing
    assert "font-weight: 800" not in landing.split(".landing-hero h1")[1].split("}")[0]

    # Strategy swatches are data colors. CSP / PMCC still share #6f42c1;
    # that is not the brand accent and must not be "fixed" to copper.
    positions = (ROOT / "app/templates/positions.html").read_text()
    assert "Cash-Secured Put" in positions and "#6f42c1" in positions
