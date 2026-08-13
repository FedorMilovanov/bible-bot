const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function boot({ visible = true, answerable = true } = {}) {
  const documentListeners = {};
  const windowListeners = {};
  let reloads = 0;
  const quiz = { classList: { contains: (name) => name === 'active' } };
  const app = {};
  const document = {
    visibilityState: visible ? 'visible' : 'hidden',
    getElementById(id) {
      if (id === 'app') return app;
      if (id === 'screen-quiz') return quiz;
      return null;
    },
    querySelectorAll() {
      return [{ disabled: !answerable }];
    },
    addEventListener(name, callback) {
      documentListeners[name] = callback;
    },
  };
  const window = {
    Telegram: { WebApp: {
      enableClosingConfirmation() {},
      disableClosingConfirmation() {},
    } },
    addEventListener(name, callback) {
      windowListeners[name] = callback;
    },
    async loadCurrentQuestion() {
      reloads += 1;
    },
  };
  class MutationObserver {
    constructor(callback) { this.callback = callback; }
    observe() {}
  }

  const source = fs.readFileSync(
    path.join(__dirname, '../../miniapp/lifecycle.js'),
    'utf8',
  );
  vm.runInNewContext(source, { window, document, MutationObserver, Promise });
  return { document, documentListeners, windowListeners, reloads: () => reloads };
}

test('visible active unanswered quiz resyncs from server after resume', async () => {
  const env = boot();
  env.documentListeners.visibilitychange();
  await Promise.resolve();
  assert.equal(env.reloads(), 1);
});

test('hidden or non-answerable quiz does not trigger resume resync', async () => {
  const hidden = boot({ visible: false });
  hidden.documentListeners.visibilitychange();
  await Promise.resolve();
  assert.equal(hidden.reloads(), 0);

  const pending = boot({ answerable: false });
  pending.documentListeners.visibilitychange();
  await Promise.resolve();
  assert.equal(pending.reloads(), 0);
});
