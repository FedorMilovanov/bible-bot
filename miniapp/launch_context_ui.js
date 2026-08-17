(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.BibleAppReturnContext = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';

  const SAFE_RETURN_URLS = new Set([
    'https://gospod-bog.ru/',
    'https://gospod-bog.ru/app/',
    'https://gospod-bog.ru/hard-texts/duhi-v-temnice-noi-kreshchenie-pobeda/',
    'https://gospod-bog.ru/hard-texts/blagovestie-mertvym-1-petra-4-5-6/',
  ]);

  function normalizeReturnContext(value) {
    if (!value || typeof value !== 'object') return null;
    if (value.kind !== 'site') return null;
    if (typeof value.label !== 'string' || !value.label.trim() || value.label.length > 80) {
      return null;
    }
    if (typeof value.url !== 'string' || !SAFE_RETURN_URLS.has(value.url)) return null;
    return Object.freeze({
      kind: 'site',
      label: value.label.trim(),
      url: value.url,
    });
  }

  function renderReturnButton(context, documentLike = root?.document, telegram = root?.Telegram?.WebApp) {
    const normalized = normalizeReturnContext(context);
    if (!normalized || !documentLike) return null;
    if (documentLike.getElementById('launchReturnBtn')) return null;

    const anchor = documentLike.getElementById('openBotBtn');
    if (!anchor?.parentNode) return null;

    const button = documentLike.createElement('button');
    button.id = 'launchReturnBtn';
    button.className = 'btn btn-ghost';
    button.type = 'button';
    button.textContent = `↩ ${normalized.label}`;
    button.addEventListener('click', () => {
      if (telegram?.openLink) telegram.openLink(normalized.url);
      else root?.open?.(normalized.url, '_blank', 'noopener,noreferrer');
    });
    anchor.parentNode.insertBefore(button, anchor);
    return button;
  }

  async function install(options = {}) {
    const telegram = options.telegram || root?.Telegram?.WebApp;
    const fetchImpl = options.fetchImpl || root?.fetch;
    const documentLike = options.documentLike || root?.document;
    const initData = telegram?.initData || '';
    if (!initData || typeof fetchImpl !== 'function') return null;

    try {
      const response = await fetchImpl('/api/launch-context', {
        method: 'GET',
        headers: { 'X-Telegram-Init-Data': initData },
        credentials: 'same-origin',
        cache: 'no-store',
      });
      if (!response.ok) return null;
      const body = await response.json();
      return renderReturnButton(body.return_context, documentLike, telegram);
    } catch (_) {
      // Return navigation is optional; quiz bootstrap must remain independent.
      return null;
    }
  }

  return Object.freeze({
    SAFE_RETURN_URLS,
    normalizeReturnContext,
    renderReturnButton,
    install,
  });
});
