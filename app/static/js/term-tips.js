/*
 * Glossary tooltips (app/glossary.py, class ht-term).
 *
 * The header word is the control (dotted underline). A separate "i"
 * is only .ht-term-mark, used where the visible label cannot be the
 * control.
 *
 * Fine pointer: hover or keyboard focus opens the definition. A click
 * on a sort header or a sort link is left alone so the column still
 * sorts or the link still navigates.
 * Coarse pointer: the first tap opens the definition and does not
 * sort or follow the link. A second tap on that same word does.
 * Escape, scroll, resize, or a tap outside closes it.
 * The popover is position:fixed so a table's overflow does not clip it.
 */
(function (root, factory) {
  var api = factory();
  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
  if (root && root.document) {
    api.install(root.document, root);
  }
})(typeof window !== "undefined" ? window : globalThis, function () {
  function place(pop, btn) {
    var rect = btn.getBoundingClientRect();
    var width = Math.min(260, window.innerWidth - 16);
    pop.style.position = "fixed";
    pop.style.width = width + "px";
    var left = rect.left;
    if (left + width > window.innerWidth - 8) {
      left = Math.max(8, window.innerWidth - width - 8);
    }
    pop.style.left = left + "px";
    pop.hidden = false;
    var popRect = pop.getBoundingClientRect();
    var top = rect.bottom + 6;
    if (top + popRect.height > window.innerHeight - 8) {
      top = Math.max(8, rect.top - popRect.height - 6);
    }
    pop.style.top = top + "px";
  }

  function closeTip(tip) {
    if (!tip) return;
    tip.classList.remove("is-open");
    var btn = tip.querySelector(".ht-term-btn");
    var pop = tip.querySelector(".ht-term-pop");
    if (btn) btn.setAttribute("aria-expanded", "false");
    if (pop) pop.hidden = true;
  }

  function openTip(tip) {
    if (!tip) return;
    var btn = tip.querySelector(".ht-term-btn");
    var pop = tip.querySelector(".ht-term-pop");
    if (!btn || !pop) return;
    tip.classList.add("is-open");
    btn.setAttribute("aria-expanded", "true");
    place(pop, btn);
  }

  function closeAll(doc, except) {
    doc.querySelectorAll(".ht-term.is-open").forEach(function (tip) {
      if (tip !== except) closeTip(tip);
    });
  }

  // Sort headers and sort links keep their click. The compact "i"
  // never does — it only exists to open the definition.
  function keepsClick(btn) {
    if (!btn || !btn.tagName) return false;
    if (btn.classList && btn.classList.contains("ht-term-mark")) return false;
    if (String(btn.tagName).toUpperCase() === "A") return true;
    return !!(btn.closest && btn.closest("th.sortable"));
  }

  // prevent/stop: swallow the click. open/close: tooltip state.
  function clickAction(btn, opts) {
    var coarse = !!(opts && opts.coarse);
    var open = !!(opts && opts.open);
    if (!keepsClick(btn)) {
      return { prevent: true, stop: true, open: !open, close: !!open };
    }
    if (coarse && !open) {
      return { prevent: true, stop: true, open: true, close: false };
    }
    return { prevent: false, stop: false, open: false, close: true };
  }

  function install(doc, win) {
    if (!doc || !doc.documentElement) return;
    if (doc.documentElement.dataset.htTermTips) return;
    doc.documentElement.dataset.htTermTips = "1";
    var coarse = false;
    try {
      coarse = win.matchMedia("(pointer: coarse)").matches;
    } catch (err) {
      coarse = false;
    }

    doc.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest(".ht-term-btn");
      if (btn) {
        var tip = btn.closest(".ht-term");
        var open = tip && tip.classList.contains("is-open");
        var action = clickAction(btn, { coarse: coarse, open: !!open });
        if (action.prevent) e.preventDefault();
        if (action.stop) e.stopPropagation();
        if (action.open) {
          closeAll(doc, tip);
          openTip(tip);
        } else if (action.close) {
          closeTip(tip);
        }
        return;
      }
      if (!e.target.closest || !e.target.closest(".ht-term")) {
        closeAll(doc, null);
      }
    }, true);

    doc.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeAll(doc, null);
    });

    if (!coarse) {
      doc.addEventListener("mouseover", function (e) {
        var tip = e.target && e.target.closest && e.target.closest(".ht-term");
        if (!tip) return;
        closeAll(doc, tip);
        openTip(tip);
      });
      doc.addEventListener("mouseout", function (e) {
        var tip = e.target && e.target.closest && e.target.closest(".ht-term");
        if (!tip) return;
        var next = e.relatedTarget;
        if (next && tip.contains(next)) return;
        closeTip(tip);
      });
      doc.addEventListener("focusin", function (e) {
        var tip = e.target && e.target.closest && e.target.closest(".ht-term");
        if (!tip) return;
        closeAll(doc, tip);
        openTip(tip);
      });
      doc.addEventListener("focusout", function (e) {
        var tip = e.target && e.target.closest && e.target.closest(".ht-term");
        if (!tip) return;
        var next = e.relatedTarget;
        if (next && tip.contains(next)) return;
        closeTip(tip);
      });
    }

    win.addEventListener("scroll", function () {
      closeAll(doc, null);
    }, true);
    win.addEventListener("resize", function () {
      closeAll(doc, null);
    });
  }

  return {
    install: install,
    place: place,
    keepsClick: keepsClick,
    clickAction: clickAction,
  };
});
