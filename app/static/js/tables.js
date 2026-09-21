/*
 * Shared table behavior: column sorting + row-click navigation + mobile
 * column priority.
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
 *   Mobile column priority:
 *     <th data-m="hide">                        // hide this column on phones
 *   Phones can't fit 8-12 column financial tables; without priorities the
 *   columns that matter (P&L) end up clipped off the right edge and the
 *   page reads as broken. Mark the low-priority columns and they collapse
 *   under 768px, leaving identity + headline numbers visible. Rows whose
 *   cell count differs from the header (colspan totals/footers) are left
 *   untouched.
 *   Clicks on <a>/<button>/<input>/<select>/<textarea>/<label> and
 *   modifier/middle clicks are always left alone (so links open new tabs,
 *   inline editors and action buttons keep working).
 */
(function () {
  "use strict";

  /* ---- Mobile column priority ---------------------------------------- */
  var mq = window.matchMedia("(max-width: 767px)");

  function applyMobileColumns() {
    var hide = mq.matches;
    document.querySelectorAll("table").forEach(function (table) {
      // table.tHead / table.rows scope to THIS table only — a nested table
      // (e.g. raw trades inside an expanded leg row) is handled by its own
      // pass and must not pollute the outer table's column indexes.
      var thead = table.tHead;
      if (!thead || !thead.rows.length) return;
      var ths = thead.rows[0].cells;
      var idxs = [];
      for (var i = 0; i < ths.length; i++) {
        if (ths[i].dataset.m === "hide") idxs.push(i);
      }
      if (!idxs.length) return;
      var n = ths.length;
      for (var r = 0; r < table.rows.length; r++) {
        var row = table.rows[r];
        if (row.cells.length !== n) continue; // colspan rows: leave alone
        for (var k = 0; k < idxs.length; k++) {
          row.cells[idxs[k]].style.display = hide ? "none" : "";
        }
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", applyMobileColumns);
  } else {
    applyMobileColumns();
  }
  if (mq.addEventListener) mq.addEventListener("change", applyMobileColumns);

  // Tables injected after load (accounts breakdown fragments) get the same
  // treatment. Debounced so a burst of DOM writes costs one pass.
  var moTimer = null;
  new MutationObserver(function (muts) {
    for (var i = 0; i < muts.length; i++) {
      if (muts[i].addedNodes.length) {
        clearTimeout(moTimer);
        moTimer = setTimeout(applyMobileColumns, 50);
        return;
      }
    }
  }).observe(document.documentElement, { childList: true, subtree: true });

  function sortTable(th) {
    var table = th.closest("table");
    if (!table) return;
    // Paginated tables sort on the server (full set, then the page).
    // A header link navigates; sorting the rows already on screen would
    // hide the real top and bottom of the book.
    if (table.dataset.serverSort) return;
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

  /* In-page table search. Opt in with:
   *   <input data-ht-search="#tableId" data-ht-empty="#emptyId" data-ht-pager="#pagerId">
   * An impossible query hides every row. The empty-state node is shown,
   * and a pager's "Showing 1-25 of 26" label is zeroed so it doesn't
   * claim rows that the filter removed. */
  function bindTableSearch(input) {
    var tableSel = input.getAttribute("data-ht-search");
    var table = tableSel ? document.querySelector(tableSel) : null;
    if (!table || !table.tBodies.length || input.dataset.htSearchBound) return;
    input.dataset.htSearchBound = "1";
    var emptySel = input.getAttribute("data-ht-empty");
    var empty = emptySel ? document.querySelector(emptySel) : null;
    var pagerSel = input.getAttribute("data-ht-pager");
    var pager = pagerSel ? document.querySelector(pagerSel) : null;
    var label = pager ? pager.querySelector("[data-page-label]") : null;
    var pages = pager ? pager.querySelector(".pagination") : null;
    var unfiltered = label ? (label.getAttribute("data-unfiltered") || label.textContent) : "";

    input.addEventListener("input", function () {
      var q = this.value.toLowerCase().trim();
      var rows = Array.prototype.filter.call(table.tBodies[0].rows, function (row) {
        return !row.classList.contains("ht-search-empty");
      });
      var shown = 0;
      rows.forEach(function (row) {
        var match = !q || row.textContent.toLowerCase().indexOf(q) !== -1;
        row.style.display = match ? "" : "none";
        if (match) shown += 1;
      });
      if (empty) empty.classList.toggle("d-none", !(q && shown === 0));
      if (!label) return;
      if (!q || shown === rows.length) {
        label.textContent = unfiltered;
        if (pages) pages.classList.remove("d-none");
        return;
      }
      if (pages) pages.classList.toggle("d-none", shown === 0);
      label.textContent = shown === 0
        ? "Showing 0"
        : ("Showing " + shown + " of " + rows.length + " on this page");
    });
  }

  function bindTableSearches() {
    document.querySelectorAll("input[data-ht-search]").forEach(bindTableSearch);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindTableSearches);
  } else {
    bindTableSearches();
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
