/*
 * Groups / Account multi-select (templates/_account_scope_filters.html).
 *
 * Checkboxes have no name. Symbol, strategy, and the other GET controls
 * keep the last *applied* hidden groups/tenants fields. Apply copies the
 * checked boxes into those fields and navigates.
 *
 * The click listener is capture-phase on document. Bootstrap closes a
 * dropdown on the bubble, and the menu's hide handler resets the boxes
 * to the last applied value. A bubble-phase Apply can therefore read
 * every box unchecked, submit a GET with no groups/tenants, and land
 * back on All groups / All accounts. Capture reads the checks first.
 * Navigation is an explicit query string: HTMLFormElement.submit() does
 * not fire a submit event, so the progress bar and this handler would
 * otherwise disagree about whether the click did anything.
 */
(function (root, factory) {
  var api = factory();
  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
  if (root) {
    root.htAccountScope = api;
    if (root.document) api.install(root.document);
  }
})(typeof window !== "undefined" ? window : globalThis, function () {
  function scopeCsv(param, checked, boxCount, reset) {
    if (reset) return "";
    var values = (checked || []).filter(function (v) {
      return v != null && String(v) !== "";
    });
    if (param === "tenants") {
      // Every account checked is the same as no filter.
      if (!values.length || values.length >= boxCount) return "";
      return values.join(",");
    }
    return values.join(",");
  }

  function pruneTenants(tenantsCsv, memberLists) {
    // No group checked → leave the account filter alone.
    if (!memberLists || !memberLists.length) return tenantsCsv || "";
    if (!tenantsCsv) return "";
    var allowed = {};
    memberLists.forEach(function (members) {
      (members || []).forEach(function (t) {
        if (t) allowed[t] = true;
      });
    });
    return String(tenantsCsv)
      .split(",")
      .filter(function (t) { return t && allowed[t]; })
      .join(",");
  }

  function searchFromFields(fields) {
    var params = new URLSearchParams();
    (fields || []).forEach(function (pair) {
      var name = pair[0];
      var value = pair[1];
      if (!name || value == null || value === "") return;
      params.append(name, String(value));
    });
    return params.toString();
  }

  var TENANT_COOKIE = "ht_tenants";
  var TENANT_COOKIE_AGE = 60 * 60 * 24 * 365;

  function tenantCookieAssignment(csv, secure) {
    var tail = "; Path=/; SameSite=Lax" + (secure ? "; Secure" : "");
    if (!csv) return TENANT_COOKIE + "=; Max-Age=0" + tail;
    return (
      TENANT_COOKIE + "=" + encodeURIComponent(csv) +
      "; Max-Age=" + TENANT_COOKIE_AGE + tail
    );
  }

  function writeTenantCookie(doc, csv) {
    if (!doc) return;
    var secure = false;
    try {
      secure = doc.location && doc.location.protocol === "https:";
    } catch (err) {
      secure = false;
    }
    doc.cookie = tenantCookieAssignment(csv, secure);
  }

  function mergePreservedQuery(currentSearch, fields) {
    // Page-header picker: keep the rest of the page query (strategy, range,
    // leg, …) and replace only the account scope.
    var params = new URLSearchParams(
      (currentSearch || "").replace(/^\?/, "")
    );
    ["account", "tenant", "tenants", "scope"].forEach(function (key) {
      params.delete(key);
    });
    var replaced = {};
    (fields || []).forEach(function (pair) {
      var name = pair[0];
      var value = pair[1];
      if (!name) return;
      if (!replaced[name]) {
        params.delete(name);
        replaced[name] = true;
      }
      if (value == null || value === "") return;
      params.append(name, String(value));
    });
    return params.toString();
  }

  function install(doc) {
    if (!doc || !doc.documentElement) return;
    if (doc.documentElement.dataset.htScopeFilters) return;
    doc.documentElement.dataset.htScopeFilters = "1";

    function appliedValue(form, param) {
      var el = form && form.querySelector(
        'input[type="hidden"][name="' + param + '"]');
      return el ? el.value : "";
    }

    function setApplied(form, param, csv) {
      var el = form.querySelector('input[type="hidden"][name="' + param + '"]');
      if (!csv) {
        if (el) el.remove();
        return;
      }
      if (!el) {
        el = doc.createElement("input");
        el.type = "hidden";
        el.name = param;
        form.appendChild(el);
      }
      el.value = csv;
    }

    function syncBoxes(wrap) {
      var form = wrap.closest("form");
      var param = wrap.getAttribute("data-ht-ms-param");
      var have = {};
      appliedValue(form, param).split(",").forEach(function (v) {
        if (v) have[v] = true;
      });
      wrap.querySelectorAll(".ht-ms-list input[type=checkbox]").forEach(function (box) {
        box.checked = !!have[box.value];
      });
    }

    function checkedMembers(groupWrap) {
      var lists = [];
      groupWrap.querySelectorAll(".ht-ms-list input[type=checkbox]").forEach(function (box) {
        if (!box.checked) return;
        lists.push(
          (box.getAttribute("data-members") || "").split(",").filter(Boolean)
        );
      });
      return lists;
    }

    function applyFrom(wrap, reset) {
      var form = wrap.closest("form");
      var param = wrap.getAttribute("data-ht-ms-param");
      if (!form || !param) return;
      var boxes = wrap.querySelectorAll(".ht-ms-list input[type=checkbox]");
      var checked = [];
      if (!reset) {
        boxes.forEach(function (box) {
          if (box.checked) checked.push(box.value);
        });
      }
      setApplied(form, param, scopeCsv(param, checked, boxes.length, reset));
      if (param === "tenants") {
        ["account", "tenant"].forEach(function (k) {
          form.querySelectorAll('[name="' + k + '"]').forEach(function (n) {
            n.remove();
          });
        });
      }
      if (param === "groups") {
        var members = reset ? [] : checkedMembers(wrap);
        if (members.length) {
          setApplied(form, "tenants", pruneTenants(appliedValue(form, "tenants"), members));
        }
      }
      var fields = [];
      new FormData(form).forEach(function (value, name) {
        fields.push([name, value]);
      });
      var url = new URL(form.action || doc.location.href, doc.location.href);
      if (form.hasAttribute("data-ht-preserve-query")) {
        url.search = mergePreservedQuery(doc.location.search, fields);
      } else {
        url.search = searchFromFields(fields);
      }
      if (param === "tenants" || form.hasAttribute("data-ht-persist-tenants")) {
        var csv = "";
        fields.forEach(function (pair) {
          if (pair[0] === "tenants") csv = pair[1];
        });
        writeTenantCookie(doc, csv);
      }
      url.hash = "";
      doc.location.assign(url.toString());
    }

    doc.addEventListener("click", function (e) {
      var resetLink = e.target && e.target.closest &&
        e.target.closest("a.filter-reset");
      if (resetLink) writeTenantCookie(doc, "");
    }, true);

    doc.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest &&
        e.target.closest(".ht-ms-apply, .ht-ms-reset");
      if (!btn) return;
      var wrap = btn.closest("[data-ht-ms-dd]");
      if (!wrap) return;
      e.preventDefault();
      e.stopPropagation();
      applyFrom(wrap, btn.classList.contains("ht-ms-reset"));
    }, true);

    doc.addEventListener("hidden.bs.dropdown", function (e) {
      var toggle = e.target;
      var wrap = toggle && toggle.closest && toggle.closest("[data-ht-ms-dd]");
      if (wrap) syncBoxes(wrap);
    });
  }

  return {
    scopeCsv: scopeCsv,
    pruneTenants: pruneTenants,
    searchFromFields: searchFromFields,
    mergePreservedQuery: mergePreservedQuery,
    tenantCookieAssignment: tenantCookieAssignment,
    writeTenantCookie: writeTenantCookie,
    install: install,
  };
});
