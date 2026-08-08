/*
 * HappyTrader service worker — deliberately conservative.
 *
 * What it does:
 *   - Precaches the /offline fallback page at install.
 *   - Stale-while-revalidate for /static/ assets and the CDN CSS/JS the
 *     app shell needs (bootstrap, chart.js): instant repeat paints,
 *     refreshed in the background.
 *   - NAVIGATIONS AND DATA ARE NEVER CACHED. Every page is authenticated,
 *     tenant-scoped financial data — serving a stale (or worse, another
 *     session's) HTML response from a cache is a correctness and privacy
 *     hazard. Navigations pass straight through to the network; the only
 *     intervention is the /offline fallback when the network is down.
 *
 * Bump VERSION to invalidate everything after a static-asset change.
 */

const VERSION = "ht-v1";
const OFFLINE_URL = "/offline";

const STATIC_HOSTS = ["cdn.jsdelivr.net"];

self.addEventListener("install", (event) => {
    event.waitUntil(
        caches.open(VERSION).then((cache) => cache.add(OFFLINE_URL))
    );
    self.skipWaiting();
});

self.addEventListener("activate", (event) => {
    event.waitUntil(
        caches.keys().then((keys) =>
            Promise.all(keys.filter((k) => k !== VERSION).map((k) => caches.delete(k)))
        ).then(() => self.clients.claim())
    );
});

function isStaticAsset(url) {
    if (url.origin === self.location.origin) {
        return url.pathname.startsWith("/static/");
    }
    return STATIC_HOSTS.includes(url.hostname);
}

self.addEventListener("fetch", (event) => {
    const req = event.request;
    if (req.method !== "GET") return;

    const url = new URL(req.url);

    if (isStaticAsset(url)) {
        // Stale-while-revalidate.
        event.respondWith(
            caches.open(VERSION).then(async (cache) => {
                const cached = await cache.match(req);
                const refresh = fetch(req)
                    .then((resp) => {
                        if (resp && resp.status === 200) cache.put(req, resp.clone());
                        return resp;
                    })
                    .catch(() => cached);
                return cached || refresh;
            })
        );
        return;
    }

    if (req.mode === "navigate") {
        // Network always; offline page only when the network itself fails.
        event.respondWith(
            fetch(req).catch(() =>
                caches.open(VERSION).then((cache) => cache.match(OFFLINE_URL))
            )
        );
    }
    // Everything else (XHR, POST results…): untouched.
});
