(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.BibleAppLaunchParams = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const VERSION_PREFIX = 'v1_';
  const COMPOSITE_SEPARATOR = '__';
  const SAFE_TOKEN = /^[a-z0-9_]{1,48}$/;
  const UNKNOWN_VERSION = /^v[0-9]+_/;

  const ALLOWED_SOURCES = new Set([
    'site_app',
    'site_home',
    'site_ch1',
    'site_ch2',
    'site_ch3',
    'site_ch4',
    'site_ch5',
    'tg_pin',
    'tg_profile',
    'tg_ch1',
    'tg_ch2',
    'tg_ch3',
    'tg_ch4',
    'tg_ch5',
    'yt_profile',
    'yt_ch1',
    'yt_ch2',
    'yt_ch3',
    'yt_ch4',
    'yt_ch5',
    'vk_pin',
    'vk_ch1',
    'vk_ch2',
    'vk_ch3',
    'vk_ch4',
    'vk_ch5',
  ]);

  function normalizeRaw(raw) {
    return typeof raw === 'string' ? raw.trim() : '';
  }

  function invalid(value) {
    return Object.freeze({ kind: 'invalid', source: null, destination: null, raw: value });
  }

  function parseStartParam(raw) {
    const value = normalizeRaw(raw);
    if (!value) {
      return Object.freeze({ kind: 'empty', source: null, destination: null, raw: '' });
    }

    if (!value.startsWith(VERSION_PREFIX)) {
      if (UNKNOWN_VERSION.test(value) || !SAFE_TOKEN.test(value)) return invalid(value);
      return Object.freeze({ kind: 'legacy', source: null, destination: value, raw: value });
    }

    const payload = value.slice(VERSION_PREFIX.length);
    const separatorIndex = payload.indexOf(COMPOSITE_SEPARATOR);
    if (separatorIndex <= 0 || separatorIndex !== payload.lastIndexOf(COMPOSITE_SEPARATOR)) {
      return invalid(value);
    }

    const source = payload.slice(0, separatorIndex);
    const destination = payload.slice(separatorIndex + COMPOSITE_SEPARATOR.length);
    if (!ALLOWED_SOURCES.has(source) || !SAFE_TOKEN.test(destination)) {
      return invalid(value);
    }

    return Object.freeze({ kind: 'v1', source, destination, raw: value });
  }

  function readStartParam(search) {
    const params = new URLSearchParams(search || '');
    if (params.has('tgWebAppStartParam')) {
      return { key: 'tgWebAppStartParam', value: params.get('tgWebAppStartParam') || '' };
    }
    if (params.has('start')) {
      return { key: 'start', value: params.get('start') || '' };
    }
    return { key: null, value: '' };
  }

  function rewriteSearchForDestination(locationLike, historyLike, parsed, sourceKey) {
    if (!locationLike || !historyLike || parsed.kind !== 'v1' && parsed.kind !== 'invalid') return false;

    const url = new URL(locationLike.href);
    url.searchParams.delete('tgWebAppStartParam');
    url.searchParams.delete('start');

    if (parsed.kind === 'v1' && parsed.destination !== 'home') {
      url.searchParams.set(sourceKey || 'tgWebAppStartParam', parsed.destination);
    }

    const next = `${url.pathname}${url.search}${url.hash}`;
    historyLike.replaceState(historyLike.state ?? null, '', next);
    return true;
  }

  function install(locationLike, historyLike) {
    const current = readStartParam(locationLike?.search || '');
    const parsed = parseStartParam(current.value);
    const context = Object.freeze({
      version: parsed.kind === 'v1' ? 1 : null,
      source: parsed.source,
      destination: parsed.destination,
      raw: parsed.raw,
      kind: parsed.kind,
    });

    if (parsed.kind === 'v1' || parsed.kind === 'invalid') {
      rewriteSearchForDestination(locationLike, historyLike, parsed, current.key);
    }

    if (typeof globalThis !== 'undefined') {
      globalThis.BibleAppLaunchContext = context;
    }
    return context;
  }

  return Object.freeze({
    ALLOWED_SOURCES,
    parseStartParam,
    readStartParam,
    rewriteSearchForDestination,
    install,
  });
});
