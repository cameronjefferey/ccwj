"""Document titles for the pages the reliability audit found untitled.

base.html renders "<title>{{ title }} - HappyTrader</title>" only when the
view passes ``title``. These routes used to omit it, so the tab read
"HappyTrader" on /positions, /accounts, and /position/<symbol>.
"""

from unittest.mock import MagicMock, patch

import pytest


def _stub_user(user_id=42, username="acme"):
    u = MagicMock()
    u.is_authenticated = True
    u.is_active = True
    u.is_anonymous = False
    u.id = user_id
    u.username = username
    u.get_id = lambda: str(user_id)
    return u


def _client():
    from app import app
    return app.test_client()


def test_accounts_performance_title():
    import app.accounts_page as accounts

    user = _stub_user()
    with patch.object(accounts, "current_user", user), \
         patch("flask_login.utils._get_user", lambda: user), \
         patch.object(accounts, "_user_account_list", lambda: ["Cameron Investment"]), \
         patch.object(accounts, "_tenants_for_scope", lambda *_a, **_k: ["snaptrade:cameron"]), \
         patch("app.routes._user_tenant_list", lambda: ["snaptrade:cameron"]), \
         patch.object(accounts, "get_bigquery_client", lambda: object()), \
         patch.object(accounts, "_bq_parallel", side_effect=RuntimeError("bq down")):
        r = _client().get("/accounts")
    assert r.status_code == 200
    html = r.data.decode()
    assert "<title>Accounts — Performance - HappyTrader</title>" in html
    assert ">Account performance<" in html


def test_position_detail_title_uses_symbol():
    import app.position_detail as detail

    user = _stub_user()
    with patch.object(detail, "current_user", user), \
         patch("flask_login.utils._get_user", lambda: user), \
         patch.object(detail, "_redirect_if_no_accounts", lambda: None), \
         patch.object(detail, "_user_account_list", lambda: ["Cameron Investment"]), \
         patch.object(detail, "_user_tenant_list", lambda: ["snaptrade:cameron"]), \
         patch.object(detail, "_tenants_for_scope", lambda *_a, **_k: ["snaptrade:cameron"]), \
         patch.object(detail, "get_bigquery_client", lambda: object()), \
         patch.object(detail, "_bq_parallel", side_effect=RuntimeError("bq down")):
        r = _client().get("/position/COST")
    assert r.status_code == 200, r.data[:500]
    html = r.data.decode()
    assert "<title>COST - HappyTrader</title>" in html
    assert "<h1>COST</h1>" in html


def test_table_search_zeros_pager_and_shows_empty_state(tmp_path):
    """Impossible in-page search must not leave Strategy Detail on
    "Showing 1-25 of 26" with a blank table."""
    import shutil
    import subprocess
    from pathlib import Path

    chrome = shutil.which("google-chrome") or shutil.which("google-chrome-stable")
    if not chrome:
        pytest.skip("google-chrome is not installed")

    root = Path(__file__).resolve().parents[1]
    tables = root / "app" / "static" / "js" / "tables.js"
    html_path = tmp_path / "table_search.html"
    html_path.write_text(
        """<!doctype html>
<html><head><meta charset="utf-8">
<style>.d-none { display: none !important; }</style>
</head><body>
<input id="q" data-ht-search="#positionsTable" data-ht-empty="#strategySearchEmpty"
       data-ht-pager="#strategyPager">
<p id="strategySearchEmpty" class="d-none">No results match this search.</p>
<table id="positionsTable"><tbody>
<tr><td>Alpha Covered Call</td></tr>
<tr><td>Beta Buy and Hold</td></tr>
</tbody></table>
<nav id="strategyPager">
  <span data-page-label data-unfiltered="Showing 1-2 of 26">Showing 1-2 of 26</span>
  <ul class="pagination"><li>Next</li></ul>
</nav>
<script src="FILE"></script>
<script>
document.addEventListener("DOMContentLoaded", function () {
  var input = document.getElementById("q");
  function snap(id) {
    var label = document.querySelector("[data-page-label]").textContent;
    var emptyHidden = document.getElementById("strategySearchEmpty").classList.contains("d-none");
    var pagerHidden = document.querySelector(".pagination").classList.contains("d-none");
    var visible = Array.prototype.filter.call(
      document.querySelectorAll("#positionsTable tbody tr"),
      function (row) { return row.style.display !== "none"; }
    ).length;
    var node = document.createElement("p");
    node.id = id;
    node.textContent = [label, emptyHidden ? "empty-hidden" : "empty-shown",
                        pagerHidden ? "pager-hidden" : "pager-shown",
                        "visible-" + visible].join("|");
    document.body.appendChild(node);
  }
  input.value = "ZZZZZ";
  input.dispatchEvent(new Event("input"));
  snap("empty-case");
  input.value = "Alpha";
  input.dispatchEvent(new Event("input"));
  snap("partial-case");
  input.value = "";
  input.dispatchEvent(new Event("input"));
  snap("cleared-case");
});
</script>
</body></html>
""".replace("FILE", tables.as_uri()),
        encoding="utf-8",
    )
    try:
        # Headless Chrome prints the DOM and then lingers on DBus, so bound
        # the process. stdout is complete before the timeout.
        proc = subprocess.run(
            ["timeout", "20", chrome, "--headless=new", "--disable-gpu",
             "--no-sandbox", "--disable-dev-shm-usage",
             f"--user-data-dir={tmp_path / 'chrome'}",
             "--virtual-time-budget=3000", "--dump-dom", html_path.as_uri()],
            capture_output=True, text=True, timeout=30,
        )
    finally:
        html_path.unlink(missing_ok=True)
    dom = proc.stdout
    assert proc.returncode in (0, 124), proc.stderr[-500:]
    assert 'id="empty-case">Showing 0|empty-shown|pager-hidden|visible-0' in dom
    assert 'id="partial-case">Showing 1 of 2 on this page|empty-hidden|pager-shown|visible-1' in dom
    assert 'id="cleared-case">Showing 1-2 of 26|empty-hidden|pager-shown|visible-2' in dom
