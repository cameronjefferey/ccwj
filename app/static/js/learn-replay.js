/* One replay: draw the path slowly, stop at each decision, then the recap.
   Money text is already formatted on the page. This file only reveals it. */
(function () {
    var page = document.querySelector(".learn-replay-page");
    var dataNode = document.getElementById("replay-data");
    var canvas = document.getElementById("replay-chart");
    if (!page || !dataNode) return;

    var data;
    try {
        data = JSON.parse(dataNode.textContent);
    } catch (err) {
        return;
    }
    if (!data || !data.days || !data.days.length) return;

    var STEP_MS = 560;
    var cursor = 0;
    var decisionCursor = 0;
    var timer = null;
    var chart = null;
    var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function styles() {
        var root = getComputedStyle(document.documentElement);
        return {
            accent: (root.getPropertyValue("--ht-blue") || "#5b8cff").trim(),
            muted: (root.getPropertyValue("--ht-muted") || "#8a97b1").trim(),
            line: (root.getPropertyValue("--ht-line") || "#243049").trim()
        };
    }

    function setText(id, text) {
        var el = document.getElementById(id);
        if (el) el.textContent = text;
    }

    function setMoney(id, text, value) {
        var el = document.getElementById(id);
        if (!el) return;
        el.textContent = text;
        el.classList.remove("is-gain", "is-loss");
        if (value > 0) el.classList.add("is-gain");
        else if (value < 0) el.classList.add("is-loss");
    }

    function drawThrough(index) {
        if (index < 0) index = 0;
        if (index > data.days.length - 1) index = data.days.length - 1;
        cursor = index;
        var labels = [];
        var pnl = [];
        var stock = [];
        var i;
        for (i = 0; i <= index; i++) {
            labels.push(String(data.days[i].day));
            pnl.push(data.days[i].pnl);
            stock.push(data.days[i].underlying);
        }
        if (chart) {
            chart.data.labels = labels;
            chart.data.datasets[0].data = pnl;
            chart.data.datasets[1].data = stock;
            chart.update("none");
        }
        var day = data.days[index];
        setText("replay-day", day.day_label);
        setText("replay-stock", day.underlying_text);
        setText("replay-option", day.option_text);
        setMoney("replay-pnl", day.pnl_text, day.pnl);
        setMoney("replay-shares", day.share_text, day.share_pnl);
        setText("replay-left", day.left_text);
    }

    function hideSteps() {
        page.querySelectorAll(".replay-decision, .replay-outcomes").forEach(function (el) {
            el.hidden = true;
        });
    }

    function showDecision(index) {
        hideSteps();
        var block = page.querySelector('.replay-decision[data-decision="' + index + '"]');
        if (block) block.hidden = false;
        var recap = document.getElementById("replay-recap");
        var checkpoint = document.getElementById("replay-checkpoint");
        if (recap) recap.hidden = true;
        if (checkpoint) checkpoint.hidden = true;
    }

    function showRecap() {
        hideSteps();
        var recap = document.getElementById("replay-recap");
        var checkpoint = document.getElementById("replay-checkpoint");
        if (recap) recap.hidden = false;
        if (checkpoint) checkpoint.hidden = false;
    }

    function atDecision() {
        var decision = data.decisions && data.decisions[decisionCursor];
        return !!(decision && cursor === decision.after_index);
    }

    function tick() {
        if (cursor >= data.days.length - 1) {
            showRecap();
            return;
        }
        drawThrough(cursor + 1);
        if (atDecision()) {
            showDecision(decisionCursor);
            return;
        }
        timer = setTimeout(tick, STEP_MS);
    }

    function pick(button) {
        var decision = button.closest(".replay-decision");
        if (!decision) return;
        var index = decision.getAttribute("data-decision");
        var outcomes = page.querySelector('.replay-outcomes[data-decision="' + index + '"]');
        if (!outcomes) return;
        var id = button.getAttribute("data-choice");
        decision.hidden = true;
        outcomes.hidden = false;
        outcomes.querySelectorAll(".replay-outcome-row[data-choice]").forEach(function (row) {
            row.classList.toggle("is-picked", row.getAttribute("data-choice") === id);
        });
        outcomes.querySelectorAll(".replay-picked").forEach(function (block) {
            block.hidden = block.getAttribute("data-choice") !== id;
        });
    }

    function resume(button) {
        var index = parseInt(button.getAttribute("data-decision"), 10);
        var outcomes = button.closest(".replay-outcomes");
        if (outcomes) outcomes.hidden = true;
        if (!isFinite(index)) index = decisionCursor;
        decisionCursor = index + 1;
        var more = data.decisions && data.decisions[decisionCursor];
        if (!more && cursor >= data.days.length - 1) {
            showRecap();
            return;
        }
        if (reduce) {
            if (more) {
                drawThrough(more.after_index);
                showDecision(decisionCursor);
            } else {
                drawThrough(data.days.length - 1);
                showRecap();
            }
            return;
        }
        timer = setTimeout(tick, STEP_MS);
    }

    if (window.Chart && canvas) {
        var color = styles();
        chart = new Chart(canvas, {
            type: "line",
            data: {
                labels: [],
                datasets: [
                    {
                        label: "Option result",
                        data: [],
                        borderColor: color.accent,
                        backgroundColor: "transparent",
                        borderWidth: 2,
                        pointRadius: 3,
                        pointHoverRadius: 3,
                        pointBackgroundColor: color.accent,
                        tension: 0.15,
                        yAxisID: "pnl"
                    },
                    {
                        label: "Stock",
                        data: [],
                        borderColor: color.muted,
                        backgroundColor: "transparent",
                        borderDash: [4, 3],
                        borderWidth: 1.5,
                        pointRadius: 0,
                        pointHoverRadius: 0,
                        tension: 0.15,
                        yAxisID: "stock"
                    }
                ]
            },
            options: {
                animation: false,
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false }, tooltip: { enabled: false } },
                scales: {
                    x: { display: false },
                    pnl: {
                        position: "left",
                        ticks: { display: false },
                        grid: { color: color.line },
                        border: { display: false }
                    },
                    stock: {
                        position: "right",
                        ticks: { display: false },
                        grid: { display: false },
                        border: { display: false }
                    }
                }
            }
        });
    }

    page.addEventListener("click", function (event) {
        var choice = event.target.closest(".replay-choice");
        if (choice) {
            pick(choice);
            return;
        }
        var cont = event.target.closest(".replay-continue");
        if (cont) resume(cont);
    });

    var yes = document.getElementById("replay-yes");
    if (yes) {
        yes.addEventListener("click", function () {
            if (window.__htLearnReplayDone) window.__htLearnReplayDone(data.slug);
            var saved = document.getElementById("replay-saved");
            if (saved) saved.hidden = false;
            yes.setAttribute("aria-pressed", "true");
        });
    }

    drawThrough(0);
    var start = page.getAttribute("data-start") || "";
    if (start === "recap") {
        drawThrough(data.days.length - 1);
        showRecap();
        return;
    }
    if ((start === "decision" || reduce) && data.decisions && data.decisions.length) {
        decisionCursor = 0;
        drawThrough(data.decisions[0].after_index);
        showDecision(0);
        return;
    }
    if (!atDecision()) timer = setTimeout(tick, STEP_MS);
    else showDecision(decisionCursor);
})();
