"""Site-wide gain/loss tokens live in one place.

Earnings and Admin stay on their own styles. Strategy swatches keep their
data colors (#198754 / #dc3545). Every other template and static stylesheet
must use var(--gain) / var(--loss) instead of a second green or red.
"""

from pathlib import Path

ROOT = Path("app")
EXEMPT = {
    "earnings_watch.html",
    "ef_bridge.html",
    "admin_overview.html",
    "admin_users.html",
    "admin_audit.html",
    "admin_feedback.html",
}

# Canonical pair. The hexes live only in this partial.
TOKEN_PARTIAL = ROOT / "templates" / "_gain_loss_tokens.html"
TOKEN_HEXES = ("#28c08a", "#f0556d")

# Other greens and reds that used to mean P&L, status, or a second gain/loss.
BANNED = (
    "#059669",
    "#16a34a",
    "#22c55e",
    "#4ade80",
    "#6fd3a0",
    "#86efac",
    "#10b981",
    "#6ee7b7",
    "#17804d",
    "#1f8a5b",
    "#2f8f62",
    "#bbf7d0",
    "#166534",
    "#dcfce7",
    "#14532d",
    "#f87171",
    "#fca5a5",
    "#ef4444",
    "#b83a2e",
    "#c4513f",
    "#d1614f",
    "#991b1b",
    "#dc2626",
    "#e7b2ad",
    "rgba(40,192,138",
    "rgba(40, 192, 138",
    "rgba(240,85,109",
    "rgba(240, 85, 109",
    "rgba(25,135,84",
    "rgba(25, 135, 84",
    "rgba(220,53,69",
    "rgba(220, 53, 69",
)


def _sources():
    for path in (ROOT / "templates").rglob("*.html"):
        if path.name in EXEMPT:
            continue
        yield path
    static = ROOT / "static"
    if static.exists():
        for path in static.rglob("*"):
            if path.suffix in {".css", ".js", ".html"}:
                yield path


def test_gain_and_loss_tokens_are_defined_once():
    partial = TOKEN_PARTIAL.read_text()
    assert partial.count("--gain: #28c08a") == 1
    assert partial.count("--loss: #f0556d") == 1
    base = (ROOT / "templates" / "base.html").read_text()
    auth = (ROOT / "templates" / "_auth_styles.html").read_text()
    assert '{% include "_gain_loss_tokens.html" %}' in base
    assert '{% include "_gain_loss_tokens.html" %}' in auth
    assert "--gain: #28c08a" not in base
    assert "--loss: #f0556d" not in base
    assert "--gain: #28c08a" not in auth
    for name in (
        "login.html",
        "signup.html",
        "forgot_password.html",
        "reset_password.html",
        "unsubscribed.html",
    ):
        page = (ROOT / "templates" / name).read_text()
        assert '{% include "_auth_styles.html" %}' in page
    assert "--color-positive: var(--gain)" in base
    assert "--color-negative: var(--loss)" in base
    assert "--pd-gain: var(--gain)" in base
    assert "--pd-loss: var(--loss)" in base
    assert '{% include "_design_system.html" %}' in base
    assert "ht-exempt" in base
    system = (ROOT / "templates" / "_design_system.html").read_text()
    for needle in (".badge", ".section-header", ".disclosure", ".tile", "var(--gain)", "var(--loss)"):
        assert needle in system


def test_no_hardcoded_gain_loss_hexes_outside_the_token():
    offenders = []
    for path in _sources():
        text = path.read_text(errors="replace").lower()
        for banned in BANNED:
            if banned.lower() in text:
                offenders.append(f"{path}: {banned}")
        if path == TOKEN_PARTIAL:
            continue
        for hex_code in TOKEN_HEXES:
            if hex_code in text:
                offenders.append(f"{path}: {hex_code}")
    assert offenders == []
