"""Daily Review contrast is a theme issue, not a single hex.

Light-mode ink (#0f172a) on a dark card and dark-mode muted (#94a3b8)
on a white card are the same bug: a color that is correct in only one
theme. Tokens on <html data-bs-theme> flip both.
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
    assert "--ht-ink: #e2e8f0" in dark_block
    assert "--ht-surface: #151e30" in dark_block


def test_daily_review_identity_text_uses_tokens():
    src = (ROOT / "app/templates/weekly_review.html").read_text()
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


def test_day_detail_muted_uses_token():
    src = (ROOT / "app/templates/day_detail.html").read_text()
    assert ".dd-muted { color: var(--ht-muted);" in src
