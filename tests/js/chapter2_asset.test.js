const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '../..');
const assetPath = path.join(root, 'miniapp', 'chapter2.js');
const indexPath = path.join(root, 'miniapp', 'index.html');


test('Chapter 2 Mini App entry points at the canonical course key', () => {
  const source = fs.readFileSync(assetPath, 'utf8');
  assert.ok(source.includes("startQuiz('chapter2', mode.id, 10, false)"));
  assert.ok(source.includes('data-action="chapter2"'));
});


test('production index loads the Chapter 2 asset after app.js', () => {
  const html = fs.readFileSync(indexPath, 'utf8');
  const appIndex = html.indexOf('<script src="app.js"></script>');
  const chapter2Index = html.indexOf('<script src="chapter2.js"></script>');
  assert.ok(appIndex >= 0);
  assert.ok(chapter2Index > appIndex);
  assert.ok(html.includes('data-action="chapter2"'));
});
