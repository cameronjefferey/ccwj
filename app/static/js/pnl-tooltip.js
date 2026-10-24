/**
 * Compact card for the cumulative P&L chart.
 *
 * Date / amount / running-total rules live in app/chart_tooltip.py.
 * One row is the action and the cash. Rolls and spreads are already
 * collapsed before this clip. VISIBLE matches TIP_VISIBLE_MAX (5).
 * "+N more" expands the rest on hover or click — it is not a dead label.
 */
(function (global) {
  function token(name) {
    try {
      return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    } catch (e) {
      return "";
    }
  }
  function markerColors() {
    return {
      buy: "#1d4ed8",
      sell: token("--loss"),
      lifecycle: "#6d28d9",
      income: token("--gain")
    };
  }
  var MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];
  var VISIBLE = 5;
  var MINUS = "\u2212";

  function wholeDollars(n) {
    var v = Number(n);
    if (!isFinite(v)) return null;
    var sign = v < 0 ? -1 : 1;
    var abs = Math.abs(v);
    var whole = Math.floor(abs);
    var frac = abs - whole;
    if (frac > 0.5 || Math.abs(frac - 0.5) < 1e-9) whole += 1;
    return sign * whole;
  }

  function formatDate(iso, today) {
    var parts = String(iso || "").slice(0, 10).split("-");
    if (parts.length < 3) return "";
    var y = +parts[0], m = +parts[1], d = +parts[2];
    if (!y || !m || !d) return "";
    var label = MONTHS[m - 1] + " " + d;
    var year = today || new Date().getFullYear();
    if (y !== year) label += ", " + y;
    return label;
  }

  function formatRunningNumber(n) {
    var w = wholeDollars(n);
    if (w === null) return null;
    var body = "$" + Math.abs(w).toLocaleString("en-US");
    if (w < 0) body = MINUS + body;
    return body;
  }

  function clip(tips, showAll) {
    var list = (tips || []).filter(Boolean);
    if (showAll || list.length <= VISIBLE) return { shown: list, more: 0 };
    return { shown: list.slice(0, VISIBLE), more: list.length - VISIBLE };
  }

  function markExpanded(el, on) {
    if (!el) return;
    if (el.classList) {
      el.classList.toggle("is-expanded", !!on);
      return;
    }
    var cls = String(el.className || "");
    var has = /(^|\s)is-expanded(\s|$)/.test(cls);
    if (on && !has) el.className = (cls + " is-expanded").trim();
    if (!on && has) el.className = cls.replace(/\bis-expanded\b/g, "").replace(/\s+/g, " ").trim();
  }

  function appendNumbers(parent, text) {
    var re = /(\$[\d,]+(?:\.\d+)?|\d[\d,]*(?:\.\d+)?)/g;
    var last = 0;
    var match;
    var raw = String(text || "");
    while ((match = re.exec(raw))) {
      if (match.index > last) {
        parent.appendChild(document.createTextNode(raw.slice(last, match.index)));
      }
      var num = document.createElement("span");
      num.className = "ht-pnl-tip-num";
      num.textContent = match[0];
      parent.appendChild(num);
      last = match.index + match[0].length;
    }
    if (last < raw.length) parent.appendChild(document.createTextNode(raw.slice(last)));
  }

  function fill(el, model) {
    el.replaceChildren();
    markExpanded(el, !!(model && model.expanded));
    var colors = markerColors();
    var date = document.createElement("div");
    date.className = "ht-pnl-tip-date";
    date.textContent = model.when || "";
    el.appendChild(date);

    var clipped = clip(model.tips, model.expanded);
    clipped.shown.forEach(function (tip) {
      var row = document.createElement("div");
      row.className = "ht-pnl-tip-row";
      var dot = document.createElement("span");
      dot.className = "ht-pnl-tip-dot";
      dot.style.background = colors[tip.kind] || colors[model.kind] || "#64748b";
      var label = document.createElement("span");
      label.className = "ht-pnl-tip-label";
      var text = tip.label || "";
      if (tip.acct) text += " · " + tip.acct;
      appendNumbers(label, text);
      row.appendChild(dot);
      row.appendChild(label);
      if (tip.amt) {
        var tail = document.createElement("span");
        tail.className = "ht-pnl-tip-tail";
        var sep = document.createElement("span");
        sep.className = "ht-pnl-tip-sep";
        sep.textContent = "\u00b7";
        var amt = document.createElement("span");
        amt.className = "ht-pnl-tip-amt " + (tip.sign < 0 ? "neg" : "pos");
        amt.textContent = tip.amt;
        tail.appendChild(sep);
        tail.appendChild(amt);
        row.appendChild(tail);
      }
      el.appendChild(row);
    });

    if (clipped.more) {
      var extra = document.createElement("button");
      extra.type = "button";
      extra.className = "ht-pnl-tip-more";
      extra.textContent = "+" + clipped.more + " more";
      extra.setAttribute("aria-expanded", "false");
      function expand(ev) {
        if (ev && ev.preventDefault) ev.preventDefault();
        if (ev && ev.stopPropagation) ev.stopPropagation();
        el._htExpanded = true;
        el._htOver = true;
        var next = {};
        for (var k in model) next[k] = model[k];
        next.expanded = true;
        fill(el, next);
        if (el._htBox) {
          var pos = place(el, el._htBox.w, el._htBox.h, el._htBox.x, el._htBox.y);
          el.style.left = pos.left + "px";
          el.style.top = pos.top + "px";
        }
      }
      extra.addEventListener("click", expand);
      extra.addEventListener("mouseenter", expand);
      el.appendChild(extra);
    }

    if (model.total != null && isFinite(Number(model.total))) {
      var total = document.createElement("div");
      total.className = "ht-pnl-tip-total";
      var words = document.createElement("span");
      words.textContent = "Running total ";
      var num = document.createElement("span");
      num.className = "ht-pnl-tip-num";
      num.textContent = formatRunningNumber(model.total) || "";
      total.appendChild(words);
      total.appendChild(num);
      el.appendChild(total);
    }
  }

  function place(el, width, height, caretX, caretY) {
    var pad = 6;
    var gap = 12;
    var tipW = el.offsetWidth;
    var tipH = el.offsetHeight;
    var left = caretX + gap;
    if (left + tipW > width - pad) left = caretX - tipW - gap;
    if (left < pad) left = pad;
    if (left + tipW > width - pad) left = Math.max(pad, width - tipW - pad);
    var top = caretY - tipH / 2;
    if (top + tipH > height - pad) top = height - tipH - pad;
    if (top < pad) top = pad;
    return { left: Math.round(left), top: Math.round(top) };
  }

  function hostFor(chart) {
    var canvas = chart.canvas;
    var host = canvas.parentNode;
    if (host && getComputedStyle(host).position === "static") {
      host.style.position = "relative";
    }
    return host;
  }

  function cardFor(host) {
    var el = host.querySelector(":scope > .ht-pnl-tip");
    if (!el) {
      el = document.createElement("div");
      el.className = "ht-pnl-tip";
      el.setAttribute("role", "tooltip");
      el.style.opacity = "0";
      host.appendChild(el);
    }
    return el;
  }

  function hide(el) {
    if (!el) return;
    el._htExpanded = false;
    el._htOver = false;
    markExpanded(el, false);
    el.style.opacity = "0";
    el.setAttribute("hidden", "");
  }

  function scheduleHide(el) {
    clearTimeout(el._htHide);
    el._htHide = setTimeout(function () {
      if (el._htOver) return;
      hide(el);
    }, 220);
  }

  function bindCard(el) {
    if (el._htBound) return;
    el._htBound = true;
    el.addEventListener("mouseover", function () {
      el._htOver = true;
      clearTimeout(el._htHide);
    });
    el.addEventListener("mouseout", function (ev) {
      var next = ev && ev.relatedTarget;
      if (next && el.contains && el.contains(next)) return;
      el._htOver = false;
      scheduleHide(el);
    });
  }

  function external(context, modelFor) {
    var chart = context.chart;
    var tooltip = context.tooltip;
    var host = hostFor(chart);
    if (!host) return;
    var el = cardFor(host);
    bindCard(el);
    if (!tooltip || tooltip.opacity === 0) {
      scheduleHide(el);
      return;
    }
    clearTimeout(el._htHide);
    var model = modelFor(tooltip, chart);
    if (!model) {
      hide(el);
      return;
    }
    if (el._htWhen !== model.when) {
      el._htExpanded = false;
      el._htWhen = model.when;
    }
    el._htBox = {
      w: host.clientWidth,
      h: host.clientHeight,
      x: tooltip.caretX,
      y: tooltip.caretY
    };
    var shown = {};
    for (var k in model) shown[k] = model[k];
    shown.expanded = !!el._htExpanded;
    fill(el, shown);
    el.removeAttribute("hidden");
    el.style.opacity = "0";
    var pos = place(el, el._htBox.w, el._htBox.h, el._htBox.x, el._htBox.y);
    el.style.left = pos.left + "px";
    el.style.top = pos.top + "px";
    el.style.opacity = "1";
  }

  global.htPnlTooltip = {
    get colors() { return markerColors(); },
    formatDate: formatDate,
    formatRunningNumber: formatRunningNumber,
    wholeDollars: wholeDollars,
    clip: clip,
    fill: fill,
    place: place,
    external: external
  };
})(window);
