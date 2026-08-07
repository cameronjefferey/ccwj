/*
 * Shared table behavior: column sorting + row-click navigation.
 *
 * Loaded once from base.html. Uses a single document-level click listener
 * (event delegation) so tables injected AFTER load — e.g. the /accounts
 * breakdown fragments swapped in over AJAX — work with no re-binding.
 *
 * Markup contract (opt-in per table, no config needed):
 *   Sorting:
 *     <th class="sortable" data-sort="num">   // "num" = numeric, else text
 *     <td data-val="123.45">$123.45</td>       // optional explicit sort key
 *     <tr data-no-sort> ... </tr>               // pinned to the bottom (totals)
 *   Row navigation:
 *     <tr data-href="/position/AAPL"> ... </tr>
 *     <td data-no-row-nav> ... </td>            // clicks here never navigate
 *   Clicks on <a>/<button>/<input>/<select>/<textarea>/<label> and
 *   modifier/middle clicks are always left alone (so links open new tabs,
 *   inline editors and action buttons keep working).
 */
(function () {
  "use strict";

  function sortTable(th) {
    var table = th.closest("table");
    if (!table) return;
    var tbody = table.querySelector("tbody");
    if (!tbody) return;

    var idx = th.cellIndex;
    var isNum = th.dataset.sort === "num";
    var asc = !th.classList.contains("asc");

    // Reset sort indicators across this table's headers only.
    table.querySelectorAll("th.sortable").forEach(function (h) {
      h.classList.remove("asc", "desc");
    });
    th.classList.add(asc ? "asc" : "desc");

    var all = Array.from(tbody.querySelectorAll(":scope > tr"));
    // Totals / footer rows stay pinned at the bottom, unsorted.
    var pinned = all.filter(function (r) { return r.hasAttribute("data-no-sort"); });
    var rows = all.filter(function (r) { return !r.hasAttribute("data-no-sort"); });

    function keyFor(row) {
      var cell = row.cells[idx];
      if (!cell) return isNum ? 0 : "";
      var raw = cell.dataset.val != null ? cell.dataset.val : cell.textContent.trim();
      return isNum ? (parseFloat(raw) || 0) : raw;
    }

    rows.sort(function (a, b) {
      var va = keyFor(a), vb = keyFor(b);
      if (va < vb) return asc ? -1 : 1;
      if (va > vb) return asc ? 1 : -1;
      return 0;
    });

    rows.forEach(function (r) { tbody.appendChild(r); });
    pinned.forEach(function (r) { tbody.appendChild(r); });
  }

  document.addEventListener("click", function (e) {
    // ---- Column sort ---------------------------------------------------
    var th = e.target.closest("th.sortable");
    if (th && th.closest("table")) {
      sortTable(th);
      return;
    }

    // ---- Row navigation ------------------------------------------------
    var row = e.target.closest("tr[data-href]");
    if (!row) return;
    // Let links, buttons, form controls and opt-out cells behave normally.
    if (e.target.closest("a, button, input, select, textarea, label, [data-no-row-nav]")) {
      return;
    }
    // Preserve new-tab / new-window intent.
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.button === 1) return;
    var href = row.dataset.href;
    if (href) window.location.href = href;
  });
})();
