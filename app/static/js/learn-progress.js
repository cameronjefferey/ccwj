/* Options 101: remember the episode, how far into its video, and finished replays.
   localStorage always. Signed-in (non-demo) browsers also POST /learn/progress. */
(function () {
    var KEY = "ht-learn-progress";
    var SENT = "ht-learn-synced";
    var ORIGIN = "https://www.youtube-nocookie.com";
    var script = document.currentScript;
    var sync = !!(script && script.getAttribute("data-learn-sync") === "1");
    var page = document.querySelector(".learn-page");
    var syncTimer = null;

    function empty() {
        return { updated: 0, last: null, done: [], replays: [] };
    }

    function replaySlug(value) {
        return typeof value === "string" && /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(value);
    }

    function normalize(payload) {
        var out = empty();
        if (!payload || typeof payload !== "object") return out;
        var seen = {};
        var done = Array.isArray(payload.done) ? payload.done : [];
        for (var i = 0; i < done.length && out.done.length < 50; i++) {
            var slug = done[i];
            if (typeof slug === "string" && slug && !seen[slug]) {
                seen[slug] = true;
                out.done.push(slug);
            }
        }
        var last = payload.last;
        if (last && typeof last === "object" && typeof last.slug === "string" && last.slug) {
            var t = parseInt(last.t, 10);
            if (!isFinite(t) || t < 0) t = 0;
            if (t > 28800) t = 28800;
            var number = parseInt(last.number, 10);
            out.last = {
                slug: last.slug,
                t: t,
                title: typeof last.title === "string" ? last.title.slice(0, 160) : "",
                number: isFinite(number) ? number : 0
            };
        }
        var replays = Array.isArray(payload.replays) ? payload.replays : [];
        var seenReplay = {};
        for (var r = 0; r < replays.length && out.replays.length < 50; r++) {
            var replaySlugValue = replays[r];
            if (replaySlug(replaySlugValue) && !seenReplay[replaySlugValue]) {
                seenReplay[replaySlugValue] = true;
                out.replays.push(replaySlugValue);
            }
        }
        var updated = parseInt(payload.updated, 10);
        out.updated = isFinite(updated) && updated > 0 ? updated : 0;
        return out;
    }

    function read() {
        try {
            var raw = localStorage.getItem(KEY);
            if (!raw) return empty();
            return normalize(JSON.parse(raw));
        } catch (err) {
            return empty();
        }
    }

    function write(progress) {
        try {
            localStorage.setItem(KEY, JSON.stringify(progress));
        } catch (err) {}
    }

    function merge(left, right) {
        var seen = {};
        var done = [];
        [left.done, right.done].forEach(function (list) {
            (list || []).forEach(function (slug) {
                if (!seen[slug]) {
                    seen[slug] = true;
                    done.push(slug);
                }
            });
        });
        var replaySeen = {};
        var replays = [];
        [left.replays, right.replays].forEach(function (list) {
            (list || []).forEach(function (slug) {
                if (!replaySeen[slug]) {
                    replaySeen[slug] = true;
                    replays.push(slug);
                }
            });
        });
        var last = (left.updated || 0) >= (right.updated || 0) ? left.last : right.last;
        if (!last) last = left.last || right.last;
        return {
            updated: Math.max(left.updated || 0, right.updated || 0),
            last: last,
            done: done,
            replays: replays
        };
    }

    function clock(seconds) {
        var whole = Math.max(0, Math.floor(seconds));
        var minutes = Math.floor(whole / 60);
        var rest = whole % 60;
        return minutes + ":" + (rest < 10 ? "0" : "") + rest;
    }

    function publishedCards() {
        var rows = [];
        document.querySelectorAll(".learn-card[data-slug]").forEach(function (card) {
            if (card.getAttribute("data-published") !== "1") return;
            rows.push({
                slug: card.getAttribute("data-slug"),
                title: card.getAttribute("data-title") || "",
                number: parseInt(card.getAttribute("data-number"), 10) || 0
            });
        });
        return rows;
    }

    function paint() {
        if (!page) return;
        var progress = read();
        var finished = {};
        (progress.done || []).forEach(function (slug) { finished[slug] = true; });

        document.querySelectorAll(".learn-card[data-slug]").forEach(function (card) {
            card.classList.toggle("is-watched", !!finished[card.getAttribute("data-slug")]);
        });

        var finishedReplay = {};
        (progress.replays || []).forEach(function (slug) { finishedReplay[slug] = true; });
        document.querySelectorAll(".learn-replay[data-slug]").forEach(function (row) {
            var done = !!finishedReplay[row.getAttribute("data-slug")];
            row.classList.toggle("is-done", done);
            if (done) {
                var title = row.querySelector(".learn-replay-title");
                row.setAttribute("aria-label", (title ? title.textContent : "Replay") + ", done");
            } else {
                row.removeAttribute("aria-label");
            }
        });
        var replayPage = document.querySelector(".learn-replay-page");
        if (replayPage) {
            replayPage.classList.toggle(
                "is-done",
                !!finishedReplay[replayPage.getAttribute("data-slug")]
            );
        }

        var published = publishedCards();
        var watched = published.filter(function (row) { return finished[row.slug]; }).length;
        var line = document.getElementById("learn-progress");
        if (line) {
            if (watched > 0 && published.length) {
                line.textContent = watched + " of " + published.length + " watched";
                line.hidden = false;
            } else {
                line.hidden = true;
            }
        }

        var button = document.getElementById("learn-start");
        if (button && published.length) {
            var last = progress.last;
            var target = null;
            if (last && !finished[last.slug]) {
                for (var i = 0; i < published.length; i++) {
                    if (published[i].slug === last.slug) {
                        target = published[i];
                        break;
                    }
                }
            }
            if (!target && watched > 0) {
                target = published.filter(function (row) { return !finished[row.slug]; })[0] || null;
            }
            if (target) {
                button.textContent = "Continue · " + target.title;
                button.setAttribute("href", "/learn/" + encodeURIComponent(target.slug));
            } else if (button.getAttribute("data-start-label")) {
                button.textContent = button.getAttribute("data-start-label");
                button.setAttribute("href", button.getAttribute("data-start-href"));
            }
        }

        if (!page.classList.contains("learn-episode")) return;
        var slug = page.getAttribute("data-slug");
        page.classList.toggle("is-done", !!finished[slug]);
        var marked = document.getElementById("learn-marked");
        if (marked) marked.hidden = !finished[slug];
        var player = document.getElementById("episode-player");
        var resume = document.getElementById("learn-resume");
        var spot = progress.last && progress.last.slug === slug && progress.last.t >= 15 && !finished[slug];
        if (player) {
            if (spot) player.setAttribute("data-resume", String(progress.last.t));
            else player.removeAttribute("data-resume");
        }
        if (resume) {
            if (spot) {
                resume.textContent = "Picking up at " + clock(progress.last.t) + ".";
                resume.hidden = false;
            } else {
                resume.hidden = true;
            }
        }
    }

    function csrf() {
        var meta = document.querySelector('meta[name="csrf-token"]');
        return meta ? meta.getAttribute("content") : "";
    }

    function upload(keepalive) {
        if (!sync) return;
        var progress = read();
        if (!progress.last && !(progress.done || []).length && !(progress.replays || []).length) return;
        try {
            if (!page && sessionStorage.getItem(SENT) === String(progress.updated)) return;
        } catch (err) {}
        var token = csrf();
        fetch("/learn/progress", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-CSRFToken": token,
                "X-CSRF-Token": token
            },
            credentials: "same-origin",
            keepalive: !!keepalive,
            body: JSON.stringify(progress)
        }).then(function (response) {
            if (response.status === 204) return null;
            if (response.status === 429 && !keepalive) {
                setTimeout(function () { upload(false); }, 2000);
                return null;
            }
            if (!response.ok) return null;
            return response.json();
        }).then(function (server) {
            if (!server) return;
            try { sessionStorage.setItem(SENT, String(read().updated)); } catch (err) {}
            var merged = merge(read(), normalize(server));
            var current = read();
            if (JSON.stringify(merged) !== JSON.stringify(current)) {
                write(merged);
                paint();
            }
        }).catch(function () {});
    }

    function scheduleSync(immediate) {
        if (!sync) return;
        if (immediate) {
            if (syncTimer) clearTimeout(syncTimer);
            syncTimer = null;
            upload(false);
            return;
        }
        if (syncTimer) clearTimeout(syncTimer);
        syncTimer = setTimeout(function () { upload(false); }, 15000);
    }

    function touch(slug, title, number, seconds, markDone) {
        var progress = read();
        var changed = false;
        if (markDone && progress.done.indexOf(slug) === -1) {
            progress.done.push(slug);
            changed = true;
        }
        var nextT = Math.max(0, Math.floor(seconds || 0));
        var same = progress.last && progress.last.slug === slug && progress.last.t === nextT;
        if (!same) {
            progress.last = { slug: slug, t: nextT, title: title, number: number };
            changed = true;
        }
        if (!changed) return;
        progress.updated = Date.now();
        write(progress);
        paint();
        scheduleSync(!!markDone);
    }

    function pull() {
        if (!sync || !page) return;
        fetch("/learn/progress", { credentials: "same-origin" })
            .then(function (response) { return response.ok ? response.json() : null; })
            .then(function (server) {
                if (!server) return;
                var local = read();
                var merged = merge(local, normalize(server));
                write(merged);
                paint();
                if ((local.updated || 0) > (server.updated || 0)) scheduleSync(true);
                else {
                    try { sessionStorage.setItem(SENT, String(merged.updated)); } catch (err) {}
                }
            })
            .catch(function () {});
    }

    if (page && page.classList.contains("learn-episode")) {
        var existing = read();
        var slug = page.getAttribute("data-slug");
        var keep = existing.last && existing.last.slug === slug ? existing.last.t : 0;
        touch(
            slug,
            page.getAttribute("data-title") || "",
            parseInt(page.getAttribute("data-number"), 10) || 0,
            keep,
            false
        );
    }
    paint();
    pull();

    window.__htLearnReplayDone = function (slug) {
        if (!replaySlug(slug)) return;
        var progress = read();
        if (progress.replays.indexOf(slug) !== -1) return;
        progress.replays.push(slug);
        progress.updated = Date.now();
        write(progress);
        paint();
        scheduleSync(true);
    };

    window.__htLearnMarkDone = function () {
        if (!page || !page.classList.contains("learn-episode")) return;
        var slug = page.getAttribute("data-slug");
        if (!slug) return;
        var progress = read();
        var t = progress.last && progress.last.slug === slug ? progress.last.t : 0;
        touch(
            slug,
            page.getAttribute("data-title") || "",
            parseInt(page.getAttribute("data-number"), 10) || 0,
            t,
            true
        );
    };

    window.__htLearnNote = function (seconds, duration, ended) {
        if (!page || !page.classList.contains("learn-episode")) return;
        var slug = page.getAttribute("data-slug");
        if (!slug) return;
        var t = Math.floor(seconds || 0);
        var finished = !!ended;
        if (!finished && duration >= 30 && t / duration >= 0.9) finished = true;
        touch(
            slug,
            page.getAttribute("data-title") || "",
            parseInt(page.getAttribute("data-number"), 10) || 0,
            t,
            finished
        );
    };

    window.__htLearnPlayer = function (iframe) {
        var host = document.getElementById("episode-player");
        if (!host || !iframe || !host.contains(iframe) || iframe.dataset.htWatch === "1") return;
        iframe.dataset.htWatch = "1";
        var time = 0;
        var duration = 0;
        var resumeTries = 0;

        function send(payload) {
            try {
                iframe.contentWindow.postMessage(JSON.stringify(payload), ORIGIN);
            } catch (err) {}
        }

        function resumeToSaved() {
            if (resumeTries >= 2) return;
            var start = parseInt(host.getAttribute("data-resume") || "0", 10);
            if (!isFinite(start) || start < 15) return;
            if (time >= start - 1) {
                resumeTries = 2;
                return;
            }
            resumeTries += 1;
            send({
                event: "command",
                func: "seekTo",
                args: [start, true],
                id: "ht",
                channel: "widget"
            });
        }

        function listen() {
            send({ event: "listening", id: "ht", channel: "widget" });
            send({
                event: "command",
                func: "addEventListener",
                args: ["onStateChange"],
                id: "ht",
                channel: "widget"
            });
        }

        iframe.addEventListener("load", listen);
        window.addEventListener("message", function (event) {
            if (event.origin !== ORIGIN || event.source !== iframe.contentWindow) return;
            var data = event.data;
            if (typeof data === "string") {
                try { data = JSON.parse(data); } catch (err) { return; }
            }
            if (!data || typeof data !== "object") return;
            if (data.event === "onReady") {
                listen();
                resumeToSaved();
                setTimeout(resumeToSaved, 600);
            }
            if (data.event === "onStateChange" && Number(data.info) === 0) {
                window.__htLearnNote(time, duration, true);
                return;
            }
            if (data.event !== "infoDelivery") return;
            if (data.info && typeof data.info === "object") {
                if (typeof data.info.currentTime === "number") time = data.info.currentTime;
                if (typeof data.info.duration === "number") duration = data.info.duration;
                if (Number(data.info.playerState) === 0) {
                    window.__htLearnNote(time, duration, true);
                    return;
                }
                window.__htLearnNote(time, duration, false);
                return;
            }
            if (typeof data.info === "number") {
                if (data.id === "ht-d") duration = data.info;
                else time = data.info;
                window.__htLearnNote(time, duration, false);
            }
        });

        var timer = setInterval(function () {
            send({ event: "command", func: "getCurrentTime", args: [], id: "ht-t", channel: "widget" });
            send({ event: "command", func: "getDuration", args: [], id: "ht-d", channel: "widget" });
        }, 5000);
        window.addEventListener("pagehide", function () { clearInterval(timer); });
    };

    var markDone = document.getElementById("learn-mark-done");
    if (markDone) {
        markDone.addEventListener("click", function () {
            window.__htLearnMarkDone();
        });
    }
    document.querySelectorAll(".learn-check-choice").forEach(function (button) {
        button.addEventListener("click", function () {
            var question = button.getAttribute("data-question");
            document.querySelectorAll('.learn-check-choice[data-question="' + question + '"]').forEach(function (other) {
                other.classList.toggle("is-on", other === button);
            });
            document.querySelectorAll('.learn-check-explain[data-question="' + question + '"]').forEach(function (line) {
                line.hidden = line.getAttribute("data-choice") !== button.getAttribute("data-choice");
            });
            var box = document.querySelector(".learn-check");
            var needed = box ? parseInt(box.getAttribute("data-questions"), 10) : 0;
            var answered = {};
            document.querySelectorAll(".learn-check-choice.is-on").forEach(function (picked) {
                answered[picked.getAttribute("data-question")] = true;
            });
            if (needed > 0 && Object.keys(answered).length >= needed) {
                window.__htLearnMarkDone();
            }
        });
    });

    if (!page && sync) upload(false);
    window.addEventListener("pagehide", function () {
        if (syncTimer) {
            clearTimeout(syncTimer);
            syncTimer = null;
        }
        upload(true);
    });
})();
