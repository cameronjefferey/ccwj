/*
 * Global navigation niceties, loaded once from base.html for signed-in users:
 *
 * 1. QUICK SWITCHER (Cmd+K / Ctrl+K / "/"): jump to any of your positions
 *    or any page without going back through /positions every time. Symbol
 *    list comes from /api/nav/symbols (tenant-scoped, server-cached) and is
 *    kept in sessionStorage for 10 minutes so reopening the palette is
 *    instant.
 *
 * 2. NAVIGATION PROGRESS BAR: a thin animated bar at the very top of the
 *    viewport that starts when you click an internal link or submit a form.
 *    The heavy pages are BigQuery-backed (1-3s uncached) and used to render
 *    a frozen screen until the response landed; the bar is the "working on
 *    it" signal. Pure perception — no behavior change.
 */
(function () {
  "use strict";

  /* ── 2. Progress bar ─────────────────────────────────────────── */
  var bar = document.createElement("div");
  bar.id = "ht-progress";
  bar.setAttribute("aria-hidden", "true");
  document.body.appendChild(bar);
  var progressTimer = null;

  function startProgress() {
    bar.style.transition = "none";
    bar.style.width = "0%";
    bar.style.opacity = "1";
    // Force reflow so the width reset applies before animating.
    void bar.offsetWidth;
    bar.style.transition = "width 6s cubic-bezier(.08,.75,.29,.99), opacity .3s";
    bar.style.width = "88%";
    if (progressTimer) clearTimeout(progressTimer);
    // Safety: if navigation somehow never happens (JS-cancelled click),
    // fade the bar out instead of leaving it stuck.
    progressTimer = setTimeout(resetProgress, 15000);
  }

  function resetProgress() {
    bar.style.opacity = "0";
    bar.style.width = "0%";
  }

  document.addEventListener("click", function (e) {
    if (e.defaultPrevented || e.button !== 0) return;
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    var a = e.target.closest && e.target.closest("a[href]");
    if (!a) return;
    if (a.target && a.target !== "_self") return;
    if (a.hasAttribute("download")) return;
    var href = a.getAttribute("href") || "";
    if (!href || href.startsWith("#") || href.startsWith("javascript:")) return;
    if (a.origin && a.origin !== window.location.origin) return;
    if (a.dataset.bsToggle) return; // bootstrap dropdowns/collapse
    startProgress();
  }, true);

  document.addEventListener("submit", function (e) {
    if (!e.defaultPrevented) startProgress();
  }, true);

  // Back/forward cache restores the page with the bar mid-flight — clear it.
  window.addEventListener("pageshow", resetProgress);

  /* ── 1. Quick switcher ───────────────────────────────────────── */
  var PAGES = [
    { s: "Daily Review", href: "/daily-review", kind: "page" },
    { s: "Trader Profile", href: "/story", kind: "page" },
    { s: "Positions", href: "/positions", kind: "page" },
    { s: "Accounts", href: "/accounts", kind: "page" },
    { s: "Account Value", href: "/accounts?view=value", kind: "page" },
    { s: "Strategies", href: "/strategies", kind: "page" },
    { s: "Strategy Fit", href: "/strategies?view=fit", kind: "page" },
    { s: "Sectors", href: "/sectors", kind: "page" },
    { s: "Earnings", href: "/earnings", kind: "page" },
    { s: "AI Insights", href: "/insights", kind: "page" },
    { s: "Profile", href: "/profile", kind: "page" }
  ];

  var overlay = null, input = null, list = null;
  var symbols = null; // [{s, open}]
  var results = [];
  var selected = 0;

  function buildOverlay() {
    overlay = document.createElement("div");
    overlay.id = "ht-palette";
    overlay.innerHTML =
      '<div class="ht-palette-box" role="dialog" aria-label="Quick switcher">' +
      '  <input type="text" class="ht-palette-input" placeholder="Jump to a symbol or page…" ' +
      '         aria-label="Search" autocomplete="off" spellcheck="false">' +
      '  <div class="ht-palette-list" role="listbox"></div>' +
      '  <div class="ht-palette-hint">↑↓ navigate · Enter open · Esc close</div>' +
      "</div>";
    document.body.appendChild(overlay);
    input = overlay.querySelector(".ht-palette-input");
    list = overlay.querySelector(".ht-palette-list");

    overlay.addEventListener("mousedown", function (e) {
      if (e.target === overlay) closePalette();
    });
    input.addEventListener("input", function () { render(input.value); });
    input.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); }
      else if (e.key === "ArrowUp") { e.preventDefault(); move(-1); }
      else if (e.key === "Enter") { e.preventDefault(); go(); }
      else if (e.key === "Escape") { closePalette(); }
    });
  }

  function fetchSymbols() {
    if (symbols !== null) return Promise.resolve(symbols);
    try {
      var cached = sessionStorage.getItem("ht-nav-symbols");
      if (cached) {
        var parsed = JSON.parse(cached);
        if (parsed && parsed.ts && Date.now() - parsed.ts < 10 * 60 * 1000) {
          symbols = parsed.symbols || [];
          return Promise.resolve(symbols);
        }
      }
    } catch (err) { /* sessionStorage unavailable — fall through */ }
    return fetch("/api/nav/symbols", { credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        symbols = (j && j.symbols) || [];
        try {
          sessionStorage.setItem(
            "ht-nav-symbols",
            JSON.stringify({ ts: Date.now(), symbols: symbols })
          );
        } catch (err) { /* quota — palette still works this page-load */ }
        return symbols;
      })
      .catch(function () { symbols = []; return symbols; });
  }

  function score(name, q) {
    // Prefix > word-boundary > substring. Case-insensitive.
    var n = name.toLowerCase(), s = q.toLowerCase();
    if (n === s) return 0;
    if (n.startsWith(s)) return 1;
    if (n.indexOf(" " + s) >= 0) return 2;
    if (n.indexOf(s) >= 0) return 3;
    return -1;
  }

  function render(q) {
    q = (q || "").trim();
    var syms = symbols || [];
    var items = [];
    if (!q) {
      // Empty query: open positions first, then pages.
      syms.forEach(function (x) {
        if (x.open) items.push({ s: x.s, href: "/position/" + encodeURIComponent(x.s), kind: "open" });
      });
      items = items.slice(0, 8).concat(PAGES.slice(0, 6));
    } else {
      var scored = [];
      syms.forEach(function (x) {
        var sc = score(x.s, q);
        if (sc >= 0) scored.push({ sc: sc - (x.open ? 0.5 : 0), item: { s: x.s, href: "/position/" + encodeURIComponent(x.s), kind: x.open ? "open" : "closed" } });
      });
      PAGES.forEach(function (p) {
        var sc = score(p.s, q);
        if (sc >= 0) scored.push({ sc: sc + 0.25, item: p });
      });
      scored.sort(function (a, b) { return a.sc - b.sc || a.item.s.localeCompare(b.item.s); });
      items = scored.slice(0, 12).map(function (x) { return x.item; });
    }
    results = items;
    selected = 0;
    list.innerHTML = "";
    if (!items.length) {
      list.innerHTML = '<div class="ht-palette-empty">No matches</div>';
      return;
    }
    items.forEach(function (it, i) {
      var row = document.createElement("div");
      row.className = "ht-palette-item" + (i === 0 ? " active" : "");
      row.setAttribute("role", "option");
      row.innerHTML =
        '<span class="ht-palette-name"></span><span class="ht-palette-kind"></span>';
      row.querySelector(".ht-palette-name").textContent = it.s;
      row.querySelector(".ht-palette-kind").textContent =
        it.kind === "open" ? "open position" : it.kind === "closed" ? "position" : "page";
      row.addEventListener("mouseenter", function () { setSelected(i); });
      row.addEventListener("mousedown", function (e) { e.preventDefault(); setSelected(i); go(); });
      list.appendChild(row);
    });
  }

  function setSelected(i) {
    selected = i;
    Array.prototype.forEach.call(list.children, function (el, j) {
      el.classList.toggle("active", j === i);
    });
  }

  function move(delta) {
    if (!results.length) return;
    var i = (selected + delta + results.length) % results.length;
    setSelected(i);
    var el = list.children[i];
    if (el && el.scrollIntoView) el.scrollIntoView({ block: "nearest" });
  }

  function go() {
    var it = results[selected];
    if (!it) return;
    closePalette();
    startProgress();
    window.location.href = it.href;
  }

  function openPalette() {
    if (!overlay) buildOverlay();
    overlay.classList.add("show");
    input.value = "";
    fetchSymbols().then(function () { render(""); });
    render(""); // immediate render with whatever we have
    setTimeout(function () { input.focus(); }, 10);
  }

  function closePalette() {
    if (overlay) overlay.classList.remove("show");
  }

  document.addEventListener("keydown", function (e) {
    var tag = (e.target.tagName || "").toLowerCase();
    var typing = tag === "input" || tag === "textarea" || tag === "select" || e.target.isContentEditable;
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      if (overlay && overlay.classList.contains("show")) closePalette();
      else openPalette();
    } else if (e.key === "/" && !typing && !e.metaKey && !e.ctrlKey && !e.altKey) {
      e.preventDefault();
      openPalette();
    } else if (e.key === "Escape" && overlay && overlay.classList.contains("show")) {
      closePalette();
    }
  });

  /* ── 3b. Dark mode toggle ────────────────────────────────────── */
  // The <head> script already applied the stored theme pre-paint; this
  // just wires the button. Reload after toggling so inline Chart.js
  // charts (built with light/dark colors at render time) recolor.
  var themeBtn = document.getElementById("ht-theme-toggle");
  if (themeBtn) {
    var syncThemeIcon = function () {
      var dark = document.documentElement.getAttribute("data-bs-theme") === "dark";
      var moon = themeBtn.querySelector(".ht-icon-moon");
      var sun = themeBtn.querySelector(".ht-icon-sun");
      if (moon) moon.style.display = dark ? "none" : "";
      if (sun) sun.style.display = dark ? "" : "none";
    };
    syncThemeIcon();
    themeBtn.addEventListener("click", function () {
      var dark = document.documentElement.getAttribute("data-bs-theme") === "dark";
      var next = dark ? "light" : "dark";
      try { localStorage.setItem("ht-theme", next); } catch (err) { /* ignore */ }
      document.documentElement.setAttribute("data-bs-theme", next);
      window.location.reload();
    });
  }

  /* ── 3. PWA install affordance ───────────────────────────────── */
  // Chrome (Android/desktop) fires beforeinstallprompt when the app is
  // installable; stash it and reveal "Install app" in the Account menu.
  // iOS Safari never fires this — installs happen via Share → Add to
  // Home Screen, which needs no affordance here.
  var deferredInstall = null;
  window.addEventListener("beforeinstallprompt", function (e) {
    e.preventDefault();
    deferredInstall = e;
    var item = document.getElementById("ht-install-item");
    if (item) item.classList.remove("d-none");
  });
  var installLink = document.getElementById("ht-install-link");
  if (installLink) {
    installLink.addEventListener("click", function (e) {
      e.preventDefault();
      if (!deferredInstall) return;
      deferredInstall.prompt();
      deferredInstall = null;
      var item = document.getElementById("ht-install-item");
      if (item) item.classList.add("d-none");
    });
  }

  // Navbar search buttons (desktop pill inside the collapse + phone icon
  // next to the hamburger — no keyboard shortcuts on mobile).
  document.querySelectorAll(".ht-palette-open").forEach(function (trigger) {
    trigger.addEventListener("click", function (e) {
      e.preventDefault();
      openPalette();
    });
  });
})();
