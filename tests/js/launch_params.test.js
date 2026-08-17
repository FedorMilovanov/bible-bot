const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  parseStartParam,
  readStartParam,
  install,
} = require('../../miniapp/launch_params.js');

const fixtures = JSON.parse(
  fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'launch_params.json'), 'utf8'),
);

test('canonical launch fixtures match JavaScript parser', () => {
  for (const fixture of fixtures) {
    const parsed = parseStartParam(fixture.raw);
    assert.deepEqual(
      {
        kind: parsed.kind,
        source: parsed.source,
        destination: parsed.destination,
      },
      {
        kind: fixture.kind,
        source: fixture.source,
        destination: fixture.destination,
      },
      fixture.raw || '<empty>',
    );
  }
});

test('legacy destination remains untouched for backward compatibility', () => {
  assert.deepEqual(parseStartParam('chapter2'), {
    kind: 'legacy',
    source: null,
    destination: 'chapter2',
    raw: 'chapter2',
  });
  assert.deepEqual(parseStartParam('level_nero'), {
    kind: 'legacy',
    source: null,
    destination: 'level_nero',
    raw: 'level_nero',
  });
});

test('v1 parameter carries both reviewed source and destination', () => {
  assert.deepEqual(parseStartParam('v1_site_ch2__chapter2'), {
    kind: 'v1',
    source: 'site_ch2',
    destination: 'chapter2',
    raw: 'v1_site_ch2__chapter2',
  });
  assert.deepEqual(parseStartParam('v1_tg_pin__home'), {
    kind: 'v1',
    source: 'tg_pin',
    destination: 'home',
    raw: 'v1_tg_pin__home',
  });
});

test('unknown source, unknown version, and malformed values fail closed', () => {
  assert.equal(parseStartParam('v1_unknown__chapter2').kind, 'invalid');
  assert.equal(parseStartParam('v2_site_ch2__chapter2').kind, 'invalid');
  assert.equal(parseStartParam('v1_site_ch2_chapter2').kind, 'invalid');
  assert.equal(parseStartParam('v1_site_ch2__chapter2__extra').kind, 'invalid');
  assert.equal(parseStartParam('v1_site_ch2__Chapter 2').kind, 'invalid');
  assert.equal(parseStartParam('../chapter2').kind, 'invalid');
});

test('Telegram start parameter wins over browser fallback start parameter', () => {
  assert.deepEqual(readStartParam('?start=chapter3&tgWebAppStartParam=chapter2'), {
    key: 'tgWebAppStartParam',
    value: 'chapter2',
  });
});

function fakeNavigation(href) {
  const location = new URL(href);
  const history = {
    state: { preserved: true },
    lastUrl: null,
    replaceState(state, _title, url) {
      this.state = state;
      this.lastUrl = url;
      const updated = new URL(url, location.origin);
      location.href = updated.href;
    },
  };
  return { location, history };
}

test('install rewrites composite deep link to legacy destination before app.js reads it', () => {
  const { location, history } = fakeNavigation(
    'https://example.test/app?tgWebAppStartParam=v1_site_ch2__chapter2&tgWebAppVersion=9.0',
  );

  const context = install(location, history);

  assert.equal(context.source, 'site_ch2');
  assert.equal(context.destination, 'chapter2');
  assert.equal(location.searchParams.get('tgWebAppStartParam'), 'chapter2');
  assert.equal(location.searchParams.get('tgWebAppVersion'), '9.0');
  assert.ok(history.lastUrl.includes('tgWebAppStartParam=chapter2'));
});

test('home destination removes routing token so existing app stays on home', () => {
  const { location, history } = fakeNavigation(
    'https://example.test/app?tgWebAppStartParam=v1_tg_pin__home&foo=bar',
  );

  const context = install(location, history);

  assert.equal(context.source, 'tg_pin');
  assert.equal(context.destination, 'home');
  assert.equal(location.searchParams.has('tgWebAppStartParam'), false);
  assert.equal(location.searchParams.get('foo'), 'bar');
});

test('malformed versioned token is removed instead of falling through to legacy logic', () => {
  const { location, history } = fakeNavigation(
    'https://example.test/app?tgWebAppStartParam=v2_bad__chapter2',
  );

  const context = install(location, history);

  assert.equal(context.kind, 'invalid');
  assert.equal(context.source, null);
  assert.equal(context.destination, null);
  assert.equal(location.searchParams.has('tgWebAppStartParam'), false);
});

test('legacy link is not rewritten', () => {
  const { location, history } = fakeNavigation(
    'https://example.test/app?tgWebAppStartParam=chapter3',
  );

  const context = install(location, history);

  assert.equal(context.kind, 'legacy');
  assert.equal(context.destination, 'chapter3');
  assert.equal(history.lastUrl, null);
  assert.equal(location.searchParams.get('tgWebAppStartParam'), 'chapter3');
});
