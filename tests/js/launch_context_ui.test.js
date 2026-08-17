const assert = require('node:assert/strict');
const test = require('node:test');
const {
  normalizeReturnContext,
  install,
} = require('../../miniapp/launch_context_ui.js');

test('only exact reviewed site return URLs are accepted', () => {
  const reviewed = [
    ['Вернуться на сайт', 'https://gospod-bog.ru/app/'],
    ['Вернуться к статье', 'https://gospod-bog.ru/hard-texts/duhi-v-temnice-noi-kreshchenie-pobeda/'],
    ['Вернуться к статье', 'https://gospod-bog.ru/hard-texts/blagovestie-mertvym-1-petra-4-5-6/'],
  ];

  for (const [label, url] of reviewed) {
    assert.deepEqual(
      normalizeReturnContext({ kind: 'site', label, url }),
      { kind: 'site', label, url },
    );
  }

  for (const value of [
    null,
    { kind: 'telegram', label: 'Back', url: 'https://gospod-bog.ru/' },
    { kind: 'site', label: 'Back', url: 'https://evil.example/' },
    { kind: 'site', label: 'Back', url: 'javascript:alert(1)' },
    { kind: 'site', label: '', url: 'https://gospod-bog.ru/' },
    { kind: 'site', label: 'Back', url: 'https://gospod-bog.ru/hard-texts/unreviewed/' },
  ]) {
    assert.equal(normalizeReturnContext(value), null);
  }
});

test('install sends only signed initData authority and no client launch source', async () => {
  let request = null;
  const fetchImpl = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      async json() {
        return { return_context: null };
      },
    };
  };

  const result = await install({
    telegram: { initData: 'signed-telegram-payload' },
    fetchImpl,
    documentLike: {},
  });

  assert.equal(result, null);
  assert.equal(request.url, '/api/launch-context');
  assert.equal(request.options.method, 'GET');
  assert.deepEqual(request.options.headers, {
    'X-Telegram-Init-Data': 'signed-telegram-payload',
  });
  assert.equal('body' in request.options, false);
  assert.equal(request.url.includes('source='), false);
});

test('install is a no-op without signed Telegram initData', async () => {
  let called = false;
  const result = await install({
    telegram: { initData: '' },
    fetchImpl: async () => {
      called = true;
    },
    documentLike: {},
  });
  assert.equal(result, null);
  assert.equal(called, false);
});
