/**
 * Compact card for the cumulative P&L chart.
 *
 * Date / amount / running-total rules live in app/chart_tooltip.py.
 * This file is the same display: Instrument Sans for words, JetBrains Mono
 * for numbers, a 240px dark card that flips so it stays inside the chart.
 */
(function (global) {
  var COLORS = {
    buy: "#1d4ed8",
    sell: "#b91c1c",
    lifecycle: "#6d28d9",
    income: "#047857"
  };
  var MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];
  var VISIBLE = 3;
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

  function clip(tips) {
    var list = (tips || []).filter(Boolean);
    if (list.length <= VISIBLE) return { shown: list, more: 0 };
    return { shown: list.slice(0, VISIBLE), more: list.length - VISIBLE };
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
    var date = document.createElement("div");
    date.className = "ht-pnl-tip-date";
    date.textContent = model.when || "";
    el.appendChild(date);

    var clipped = clip(model.tips);
    clipped.shown.forEach(function (tip) {
      var row = document.createElement("div");
      row.className = "ht-pnl-tip-row";
      var dot = document.createElement("span");
      dot.className = "ht-pnl-tip-dot";
      dot.style.background = COLORS[tip.kind] || COLORS[model.kind] || "#64748b";
      var label = document.createElement("span");
      label.className = "ht-pnl-tip-label";
      var text = tip.label || "";
      if (tip.acct) text += " · " + tip.acct;
      appendNumbers(label, text);
      row.appendChild(dot);
      row.appendChild(label);
      el.appendChild(row);
      if (tip.amt) {
        var amt = document.createElement("div");
        amt.className = "ht-pnl-tip-amt " + (tip.sign < 0 ? "neg" : "pos");
        amt.textContent = tip.amt;
        el.appendChild(amt);
      }
    });

    if (clipped.more) {
      var extra = document.createElement("div");
      extra.className = "ht-pnl-tip-more";
      extra.textContent = "+" + clipped.more + " more";
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
    el.style.opacity = "0";
    el.setAttribute("hidden", "");
  }

  function external(context, modelFor) {
    var chart = context.chart;
    var tooltip = context.tooltip;
    var host = hostFor(chart);
    if (!host) return;
    var el = cardFor(host);
    if (!tooltip || tooltip.opacity === 0) {
      hide(el);
      return;
    }
    var model = modelFor(tooltip, chart);
    if (!model) {
      hide(el);
      return;
    }
    fill(el, model);
    el.removeAttribute("hidden");
    el.style.opacity = "0";
    var pos = place(el, host.clientWidth, host.clientHeight, tooltip.caretX, tooltip.caretY);
    el.style.left = pos.left + "px";
    el.style.top = pos.top + "px";
    el.style.opacity = "1";
  }

  global.htPnlTooltip = {
    colors: COLORS,
    formatDate: formatDate,
    formatRunningNumber: formatRunningNumber,
    wholeDollars: wholeDollars,
    clip: clip,
    fill: fill,
    place: place,
    external: external
  };
})(window);
