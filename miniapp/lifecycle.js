/* Telegram Mini App lifecycle refinements kept separate from quiz state logic. */
(() => {
  const tg = window.Telegram?.WebApp;
  const app = document.getElementById('app');
  if (!tg || !app) return;

  const syncClosingConfirmation = () => {
    const quizActive = document.getElementById('screen-quiz')?.classList.contains('active');
    try {
      if (quizActive) tg.enableClosingConfirmation();
      else tg.disableClosingConfirmation();
    } catch (_) {
      // Older Telegram clients may not expose the lifecycle method.
    }
  };

  syncClosingConfirmation();

  const observer = new MutationObserver(syncClosingConfirmation);
  observer.observe(app, {
    attributes: true,
    subtree: true,
    attributeFilter: ['class'],
  });
})();
