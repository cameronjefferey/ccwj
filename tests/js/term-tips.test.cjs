const test = require("node:test");
const assert = require("node:assert/strict");
const { keepsClick, clickAction } = require("../../app/static/js/term-tips.js");

function el(tag, opts) {
  const classes = opts.classes || [];
  const closest = opts.closest || {};
  return {
    tagName: tag,
    classList: { contains: (name) => classes.includes(name) },
    closest: (sel) => closest[sel] || null,
  };
}

test("the compact i never keeps the click", () => {
  const mark = el("BUTTON", { classes: ["ht-term-mark"], closest: { "th.sortable": {} } });
  assert.equal(keepsClick(mark), false);
  assert.deepEqual(clickAction(mark, { coarse: false, open: false }), {
    prevent: true,
    stop: true,
    open: true,
    close: false,
  });
});

test("a plain button toggles and does not fall through", () => {
  const btn = el("BUTTON", {});
  assert.equal(keepsClick(btn), false);
  assert.equal(clickAction(btn, { coarse: true, open: true }).close, true);
  assert.equal(clickAction(btn, { coarse: true, open: true }).prevent, true);
});

test("fine pointer click on a header word sorts", () => {
  const word = el("BUTTON", { closest: { "th.sortable": {} } });
  const action = clickAction(word, { coarse: false, open: true });
  assert.equal(keepsClick(word), true);
  assert.equal(action.prevent, false);
  assert.equal(action.stop, false);
  assert.equal(action.close, true);
  assert.equal(action.open, false);
});

test("fine pointer click on a sort link navigates", () => {
  const link = el("A", {});
  const action = clickAction(link, { coarse: false, open: true });
  assert.equal(action.prevent, false);
  assert.equal(action.stop, false);
});

test("coarse first tap opens the definition and holds the sort", () => {
  const word = el("BUTTON", { closest: { "th.sortable": {} } });
  const link = el("A", {});
  for (const btn of [word, link]) {
    const action = clickAction(btn, { coarse: true, open: false });
    assert.equal(action.prevent, true);
    assert.equal(action.stop, true);
    assert.equal(action.open, true);
  }
});

test("coarse second tap lets the header act", () => {
  const word = el("BUTTON", { closest: { "th.sortable": {} } });
  const action = clickAction(word, { coarse: true, open: true });
  assert.equal(action.prevent, false);
  assert.equal(action.stop, false);
  assert.equal(action.close, true);
});
