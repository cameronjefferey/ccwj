const test = require("node:test");
const assert = require("node:assert/strict");
const {
  scopeCsv,
  pruneTenants,
  searchFromFields,
} = require("../../app/static/js/scope-filters.js");

test("group apply keeps the checked ids", () => {
  assert.equal(scopeCsv("groups", ["1"], 3, false), "1");
  assert.equal(scopeCsv("groups", ["1", "2"], 3, false), "1,2");
});

test("group reset clears the param", () => {
  assert.equal(scopeCsv("groups", ["1"], 3, true), "");
});

test("checking every account omits tenants", () => {
  assert.equal(scopeCsv("tenants", ["a", "b"], 2, false), "");
});

test("one account is kept", () => {
  assert.equal(
    scopeCsv("tenants", ["snaptrade:abc"], 3, false),
    "snaptrade:abc",
  );
});

test("empty checks do not invent a filter", () => {
  assert.equal(scopeCsv("groups", [], 3, false), "");
  assert.equal(scopeCsv("tenants", [], 3, false), "");
});

test("prune tenants to the checked group's members", () => {
  assert.equal(pruneTenants("a,b,c", [["a", "c"]]), "a,c");
  assert.equal(pruneTenants("a,b", []), "a,b");
  assert.equal(pruneTenants("", [["a"]]), "");
});

test("search string keeps other fields and drops an empty scope", () => {
  assert.equal(
    searchFromFields([
      ["groups", "1"],
      ["strategy", "Covered Call"],
      ["tenants", ""],
    ]),
    "groups=1&strategy=Covered+Call",
  );
});

test("header apply keeps the rest of the page query", () => {
  const { mergePreservedQuery } = require("../../app/static/js/scope-filters.js");
  assert.equal(
    mergePreservedQuery("?strategy=Wheel&tenant=old&range=1m", [
      ["tenants", "snaptrade:aaa"],
    ]),
    "strategy=Wheel&range=1m&tenants=snaptrade%3Aaaa",
  );
});

test("header reset drops the saved account query", () => {
  const { mergePreservedQuery } = require("../../app/static/js/scope-filters.js");
  assert.equal(
    mergePreservedQuery("?tenants=snaptrade:aaa&groups=2", [["tenants", ""]]),
    "groups=2",
  );
});

test("tenant cookie stores a subset and clears when empty", () => {
  const { tenantCookieAssignment } = require("../../app/static/js/scope-filters.js");
  const set = tenantCookieAssignment("snaptrade:aaa,snaptrade:bbb", false);
  assert.match(set, /^ht_tenants=snaptrade%3Aaaa%2Csnaptrade%3Abbb;/);
  assert.match(set, /Max-Age=31536000/);
  assert.match(set, /SameSite=Lax/);
  assert.doesNotMatch(set, /Secure/);
  const cleared = tenantCookieAssignment("", true);
  assert.match(cleared, /^ht_tenants=;/);
  assert.match(cleared, /Max-Age=0/);
  assert.match(cleared, /Secure/);
});
