/* Telegram Mini App lifecycle refinements kept separate from quiz state logic. */
(() => {
  const tg = window.Telegram?.WebApp;
  const app = document.getElementById('app');
  if (!tg || !app) return;

  const quizActive = () => (
    document.getElementById('screen-quiz')?.classList.contains('active') === true
  );

  const syncClosingConfirmation = () => {
    try {
      if (quizActive()) tg.enableClosingConfirmation();
      else tg.disableClosingConfirmation();
    } catch (_) {
      // Older Telegram clients may not expose the lifecycle method.
    }
  };

  const resyncActiveQuestion = () => {
    if (document.visibilityState !== 'visible' || !quizActive()) return;
    const answerable = [...document.querySelectorAll('#quizOptions .opt')]
      .some((button) => !button.disabled);
    if (!answerable || typeof window.loadCurrentQuestion !== 'function') return;

    // Browser/WebView timers may be throttled while Telegram is backgrounded.
    // Reload the current question from the server so remaining_seconds is based
    // on the authoritative server timestamp instead of missed client intervals.
    Promise.resolve(window.loadCurrentQuestion()).catch(() => {
      // app.js owns user-visible retry/error handling for this request.
    });
  };

  syncClosingConfirmation();

  const observer = new MutationObserver(syncClosingConfirmation);
  observer.observe(app, {
    attributes: true,
    subtree: true,
    attributeFilter: ['class'],
  });

  document.addEventListener('visibilitychange', resyncActiveQuestion);
  window.addEventListener('pageshow', resyncActiveQuestion);
})();
