/* First-party funnel beacon. Sends an event name and a path. No email,
   no click ids from the page — the server reads those from the cookie. */
(function () {
    var path = location.pathname || "/";
    if (path.indexOf("/learn/") === 0 && path !== "/learn/progress") {
        post("lesson_started");
    }
    document.addEventListener("ht-funnel", function (ev) {
        var name = ev && ev.detail && ev.detail.event;
        if (name) post(name);
    });

    function post(eventName) {
        var token = "";
        var meta = document.querySelector('meta[name="csrf-token"]');
        if (meta) token = meta.getAttribute("content") || "";
        var body = JSON.stringify({ event: eventName, path: path });
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
