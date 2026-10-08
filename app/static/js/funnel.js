/* First-party funnel beacon. Sends an event name, a path, and a short
   detail (CTA name, scroll mark, time bucket, or video id). No email
   and no click ids from the page — the server reads those from the cookie. */
(function () {
    var path = location.pathname || "/";
    var sent = {};

    // The inline head snippet posts client_seen before this file downloads.
    // Pages without that snippet still post once. A second post would only
    // repeat an UPDATE and spend the beacon rate limit.
    if (!window.__htClientSeen) {
        window.__htClientSeen = 1;
        post("client_seen", "1");
    }

    if (path.indexOf("/learn/") === 0 && path !== "/learn/progress") {
        post("lesson_started", "");
    }
    document.addEventListener("ht-funnel", function (ev) {
        var name = ev && ev.detail && ev.detail.event;
        if (name) post(name, "");
    });

    document.addEventListener("click", function (ev) {
        var node = ev.target && ev.target.closest ? ev.target.closest("[data-ht-cta], [data-youtube-id], .yt-lite") : null;
        if (!node) return;
        var cta = node.getAttribute("data-ht-cta");
        if (cta) post("cta_click", cta);
        var video = youtubeId(node);
        if (video) post("video_play", video);
    });

    var marks = { "25": false, "50": false, "75": false, "100": false };
    function onScroll() {
        var doc = document.documentElement;
        var scrolled = window.scrollY || doc.scrollTop || 0;
        var height = Math.max(doc.scrollHeight || 0, document.body ? document.body.scrollHeight : 0) - window.innerHeight;
        var pct = height <= 8 ? 100 : Math.min(100, Math.round((100 * scrolled) / height));
        ["25", "50", "75", "100"].forEach(function (mark) {
            if (!marks[mark] && pct >= Number(mark)) {
                marks[mark] = true;
                post("scroll_depth", mark);
            }
        });
    }
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("load", onScroll);

    // Visible time only. Each bucket is sent once (post() dedupes) as the
    // visitor crosses it, and again on hide/unload in case the timer was late.
    var TIME_MARKS = [5, 15, 30, 60, 120, 300];
    var dwellMs = 0;
    var dwellVisibleAt = document.visibilityState === "hidden" ? 0 : Date.now();
    var dwellTimer = 0;

    function flushDwell() {
        if (dwellVisibleAt) {
            dwellMs += Date.now() - dwellVisibleAt;
            dwellVisibleAt = document.visibilityState === "hidden" ? 0 : Date.now();
        }
        var seconds = Math.floor(dwellMs / 1000);
        for (var i = 0; i < TIME_MARKS.length; i++) {
            if (seconds >= TIME_MARKS[i]) post("time_on_page", String(TIME_MARKS[i]));
        }
        if (seconds >= 300 && dwellTimer) {
            clearInterval(dwellTimer);
            dwellTimer = 0;
        }
    }

    document.addEventListener("visibilitychange", function () {
        if (document.visibilityState === "hidden") flushDwell();
        else if (!dwellVisibleAt) dwellVisibleAt = Date.now();
    });
    window.addEventListener("pagehide", flushDwell);
    window.addEventListener("beforeunload", flushDwell);
    dwellTimer = setInterval(flushDwell, 5000);

    function youtubeId(node) {
        var raw = node.getAttribute("data-youtube-id") || "";
        if (/^[A-Za-z0-9_-]{6,64}$/.test(raw)) return raw;
        var src = node.getAttribute("data-src") || "";
        var match = src.match(/\/embed\/([A-Za-z0-9_-]{6,64})/);
        return match ? match[1] : "";
    }

    function post(eventName, detail) {
        detail = detail || "";
        var key = eventName + ":" + detail;
        if (sent[key]) return;
        sent[key] = true;
        var token = "";
        var meta = document.querySelector('meta[name="csrf-token"]');
        if (meta) token = meta.getAttribute("content") || "";
        var body = JSON.stringify({ event: eventName, path: path, detail: detail });
        try {
            if (navigator.sendBeacon && !token) {
                navigator.sendBeacon("/funnel/beacon", new Blob([body], { type: "application/json" }));
                return;
            }
        } catch (e) { /* fetch below */ }
        fetch("/funnel/beacon", {
            method: "POST",
            credentials: "same-origin",
            keepalive: true,
            headers: {
                "Content-Type": "application/json",
                "X-CSRFToken": token
            },
            body: body
        }).catch(function () {});
    }
})();
