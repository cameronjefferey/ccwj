(function () {
    var root = document.getElementById("pp-open-orders");
    if (!root) return;
    var pollUrl = root.getAttribute("data-poll-url");
    var cancelUrl = root.getAttribute("data-cancel-url");
    var openList = document.getElementById("pp-open-list");
    var recentList = document.getElementById("pp-recent-list");
    var recentWrap = document.getElementById("pp-recent-orders");
    var errorEl = document.getElementById("pp-status-error");
    var csrfEl = document.getElementById("pp-csrf");
    if (!pollUrl || !openList) return;

    var started = Date.now();
    var timer = null;
    var stopped = false;
    var announced = {};
    var STATUSES = {
        open: 1,
        filled: 1,
        cancelled: 1,
        cancel_requested: 1,
        rejected: 1,
        expired: 1,
        unknown: 1
    };

    function esc(value) {
        return String(value || "").replace(/[&<>"']/g, function (ch) {
            return {
                "&": "&amp;",
                "<": "&lt;",
                ">": "&gt;",
                "\"": "&quot;",
                "'": "&#39;"
            }[ch];
        });
    }

    function hasOpen() {
        return !!openList.querySelector(
            '.pp-order[data-order-status="open"], .pp-order[data-order-status="cancel_requested"]'
        );
    }

    function tabVisible() {
        return document.visibilityState !== "hidden";
    }

    function nextDelay(elapsed) {
        if (elapsed > 8 * 60 * 1000) return 0;
        if (elapsed > 5 * 60 * 1000) return 30000;
        return 15000;
    }

    function showError(message) {
        if (!errorEl) return;
        if (!message) {
            errorEl.textContent = "";
            errorEl.classList.add("d-none");
            return;
        }
        errorEl.textContent = message;
        errorEl.classList.remove("d-none");
    }

    function toast(order) {
        var id = order.brokerage_order_id || order.link_symbol || order.symbol || "filled";
        if (announced[id]) return;
        announced[id] = true;
        var stack = document.querySelector(".ht-toast-stack");
        if (!stack) return;
        var el = document.createElement("div");
        el.className = "toast align-items-center border-0";
        el.setAttribute("role", "status");
        var href = order.position_url || "";
        var link = "";
        if (href.indexOf("/position/") === 0) {
            var symbol = esc(order.link_symbol || order.symbol || "the position");
            link = ' <a href="' + esc(href) + '">See ' + symbol + '</a>';
        }
        el.innerHTML = '<div class="d-flex"><div class="toast-body">Filled.' + link + '</div>' +
            '<button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Dismiss"></button></div>';
        stack.appendChild(el);
        if (window.bootstrap && bootstrap.Toast) {
            bootstrap.Toast.getOrCreateInstance(el, { delay: 8000 }).show();
        }
    }

    function rowHtml(order) {
        var status = STATUSES[order.status] ? order.status : "unknown";
        var id = esc(order.brokerage_order_id);
        var label = esc(order.status_label || "Unknown");
        var text = esc(order.sentence || order.symbol || "Paper order");
        var detail = order.detail
            ? '<div class="pp-order-meta">' + esc(order.detail) + '</div>'
            : "";
        var href = order.position_url || "";
        var link = "";
        if (href.indexOf("/position/") === 0) {
            var symbol = esc(order.link_symbol || order.symbol);
            link = '<a class="btn btn-outline-secondary btn-sm" href="' + esc(href) + '">See ' + symbol + '</a>';
        }
        var action = "";
        var csrf = csrfEl ? csrfEl.value : "";
        if (order.cancelable && order.brokerage_order_id && cancelUrl && csrf) {
            action = '<form method="POST" action="' + esc(cancelUrl) + '">' +
                '<input type="hidden" name="csrf_token" value="' + esc(csrf) + '">' +
                '<input type="hidden" name="brokerage_order_id" value="' + id + '">' +
                '<button class="btn btn-outline-secondary btn-sm" type="submit">Cancel</button></form>';
        }
        var pos = href.indexOf("/position/") === 0 ? ' data-position-url="' + esc(href) + '"' : "";
        return '<div class="pp-order" data-order-id="' + id + '" data-order-status="' + status + '"' + pos + '>' +
            '<div><div class="pp-status is-' + status + ' pp-live-status">' + label + '</div>' +
            '<div class="pp-order-text">' + text + '</div>' + detail + '</div>' +
            '<div class="d-flex flex-wrap gap-2">' + link + action + '</div></div>';
    }

    function updateReceipt(order) {
        if (!order.brokerage_order_id) return;
        var node = document.querySelector('[data-order-id="' + cssId(order.brokerage_order_id) + '"] .pp-live-status');
        var receipt = document.querySelector('p[data-order-id="' + cssId(order.brokerage_order_id) + '"]');
        if (receipt) {
            var live = receipt.querySelector(".pp-live-status");
            if (live) live.textContent = order.status_label || "";
            if (order.status === "filled" && order.position_url) {
                receipt.setAttribute("data-position-url", order.position_url);
            }
        } else if (node) {
            node.textContent = order.status_label || "";
        }
    }

    function cssId(value) {
        if (window.CSS && CSS.escape) return CSS.escape(String(value));
        return String(value).replace(/"/g, "");
    }

    function apply(data) {
        var orders = (data && data.orders) || [];
        var open = [];
        var recent = [];
        orders.forEach(function (order) {
            if (order.status === "open" || order.status === "cancel_requested") open.push(order);
            else recent.push(order);
            updateReceipt(order);
            if (order.status === "filled") toast(order);
        });
        openList.innerHTML = open.length
            ? open.map(rowHtml).join("")
            : '<p class="pp-note mb-0" data-empty>No open orders.</p>';
        if (recentList) {
            recentList.innerHTML = recent.map(rowHtml).join("");
            if (recentWrap) recentWrap.classList.toggle("d-none", recent.length === 0);
        }
        showError(data && data.error);
    }

    function schedule(ms) {
        if (stopped || !ms || !tabVisible() || !hasOpen()) return;
        if (timer) window.clearTimeout(timer);
        timer = window.setTimeout(tick, ms);
    }

    function tick() {
        if (!tabVisible() || !hasOpen()) return;
        var elapsed = Date.now() - started;
        var delay = nextDelay(elapsed);
        if (!delay) return;
        fetch(pollUrl, {
            headers: {
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest"
            },
            credentials: "same-origin"
        }).then(function (response) {
            if (response.status === 429) {
                showError("The paper account is busy. This status is the last one we received.");
                schedule(15000);
                return null;
            }
            if (!response.ok) throw new Error("status");
            return response.json();
        }).then(function (data) {
            if (!data) return;
            apply(data);
            var wait = nextDelay(Date.now() - started);
            if (!wait || data.poll_after_ms === 0) return;
            if (data.poll_after_ms) wait = Math.max(wait, data.poll_after_ms);
            schedule(wait);
        }).catch(function () {
            showError("We couldn't refresh order status. The last status on this page still stands.");
            var wait = nextDelay(Date.now() - started);
            schedule(wait ? Math.max(wait, 15000) : 0);
        });
    }

    document.querySelectorAll("[data-announce='filled']").forEach(function (node) {
        toast({
            brokerage_order_id: node.getAttribute("data-order-id") || "",
            position_url: node.getAttribute("data-position-url") || "",
            link_symbol: (node.getAttribute("data-position-url") || "").split("/").pop() || "",
            status: "filled"
        });
    });

    window.addEventListener("pagehide", function () {
        stopped = true;
        if (timer) window.clearTimeout(timer);
    });

    document.addEventListener("visibilitychange", function () {
        if (!tabVisible()) {
            if (timer) window.clearTimeout(timer);
            timer = null;
            return;
        }
        if (hasOpen()) tick();
    });

    if (hasOpen()) schedule(15000);
    window.htApplyPaperOrders = apply;
})();

(function () {
    var pending = document.getElementById("pp-broker-pending");
    var url = pending && pending.getAttribute("data-broker-url");
    if (!url) return;
    fetch(url, {
        headers: { "Accept": "application/json", "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin"
    }).then(function (response) {
        if (!response.ok) throw new Error("broker");
        return response.json();
    }).then(function (data) {
        pending.classList.add("d-none");
        var connect = document.getElementById("pp-connect");
        var ticket = document.getElementById("pp-ticket");
        if (data && data.connected) {
            if (connect) connect.classList.add("d-none");
            if (ticket) ticket.classList.remove("d-none");
            var power = document.getElementById("pp-buying-power");
            if (power && data.buying_power_label) {
                power.textContent = "Buying power on this paper account: " + data.buying_power_label + ". Buying power is what a new trade can use. Account value is the balance on Overview.";
                power.classList.remove("d-none");
            }
            if (window.htApplyPaperOrders) window.htApplyPaperOrders(data);
            return;
        }
        if (ticket) ticket.classList.add("d-none");
        if (connect) connect.classList.remove("d-none");
    }).catch(function () {
        pending.textContent = "We couldn't reach the paper account. Refresh to try again.";
    });
})();
