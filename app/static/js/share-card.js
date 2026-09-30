/* Share-card modal. The PNG is built on the server from the viewer's
   warehouse row for this identifier. The URL does not carry P&L.
   Nothing is uploaded. Web Share hands the file to the phone;
   otherwise the links download it. */
(function () {
  var modal = document.getElementById("htShareModal");
  if (!modal) return;
  var square = document.getElementById("htShareSquare");
  var story = document.getElementById("htShareStory");
  var dlSquare = document.getElementById("htShareDownloadSquare");
  var dlStory = document.getElementById("htShareDownloadStory");
  var nativeBtn = document.getElementById("htShareNative");
  var closeBtn = document.getElementById("htShareClose");
  var current = { square: "", story: "", symbol: "trade" };

  function cardUrl(btn, layout) {
    var params = new URLSearchParams();
    params.set("layout", layout);
    params.set("ref", btn.getAttribute("data-ref") || "");
    params.set("symbol", btn.getAttribute("data-symbol") || "");
    var tenants = btn.getAttribute("data-tenants");
    if (tenants) params.set("tenants", tenants);
    var tenant = btn.getAttribute("data-tenant");
    if (tenant) params.set("tenant", tenant);
    var tradeSymbol = btn.getAttribute("data-trade-symbol");
    if (tradeSymbol) params.set("trade_symbol", tradeSymbol);
    var session = btn.getAttribute("data-session");
    if (session) params.set("session", session);
    var closeDate = btn.getAttribute("data-close");
    if (closeDate) params.set("close", closeDate);
    return "/share/card.png?" + params.toString();
  }

  function openFrom(btn) {
    current.symbol = (btn.getAttribute("data-symbol") || "trade").replace(/[^\w.-]+/g, "");
    current.square = cardUrl(btn, "square");
    current.story = cardUrl(btn, "story");
    square.src = current.square;
    story.src = current.story;
    dlSquare.href = current.square;
    dlStory.href = current.story;
    dlSquare.setAttribute("download", current.symbol + "-square.png");
    dlStory.setAttribute("download", current.symbol + "-story.png");
    var canShare = false;
    try {
      canShare = !!(navigator.share && navigator.canShare && navigator.canShare({
        files: [new File([""], "x.png", { type: "image/png" })]
      }));
    } catch (err) {
      canShare = !!navigator.share;
    }
    nativeBtn.hidden = !canShare;
    modal.hidden = false;
  }

  document.addEventListener("click", function (event) {
    var btn = event.target.closest && event.target.closest(".ht-share-btn");
    if (!btn) return;
    event.preventDefault();
    event.stopPropagation();
    openFrom(btn);
  }, true);

  function close() { modal.hidden = true; }
  closeBtn.addEventListener("click", close);
  modal.addEventListener("click", function (event) {
    if (event.target === modal) close();
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !modal.hidden) close();
  });

  nativeBtn.addEventListener("click", function () {
    var url = current.square;
    fetch(url, { credentials: "same-origin" })
      .then(function (res) { return res.blob(); })
      .then(function (blob) {
        var file = new File([blob], current.symbol + "-square.png", { type: "image/png" });
        if (navigator.canShare && !navigator.canShare({ files: [file] })) {
          return navigator.share({ title: current.symbol, url: url });
        }
        return navigator.share({ files: [file], title: current.symbol });
      })
      .catch(function () { /* user cancelled, or share is unavailable */ });
  });
})();
