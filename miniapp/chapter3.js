/* Reviewed Chapter 3 learning entry for the Mini App. */
(() => {
  const courseModes = [
    { id: 'relaxed', label: '🧘 Спокойный', desc: 'без таймера' },
    { id: 'timed', label: '⏱ На время', desc: '30 сек на вопрос' },
    { id: 'speed', label: '⚡ Скоростной', desc: '15 сек на вопрос' },
  ];

  function openChapter3Course() {
    const title = document.querySelector('#levelsTitle');
    const container = document.querySelector('#levelsList');
    if (!title || !container || typeof showScreen !== 'function') return;

    title.textContent = '📙 1 Петра — Глава 3';
    container.replaceChildren();

    const info = document.createElement('div');
    info.className = 'card';
    const heading = document.createElement('b');
    heading.textContent = 'Исследовательский курс · 10 вопросов';
    const description = document.createElement('p');
    description.className = 'muted';
    description.textContent =
      'Текст · греческий · ВЗ/LXX · история · богословие · спорные места · применение. ' +
      'Учебный режим без рейтинга.';
    info.append(heading, description);
    container.appendChild(info);

    courseModes.forEach((mode) => {
      const button = document.createElement('button');
      button.className = 'level-btn';
      const left = document.createElement('span');
      const label = document.createElement('b');
      label.textContent = mode.label;
      const meta = document.createElement('span');
      meta.className = 'level-meta';
      meta.textContent = `${mode.desc} · учебный режим`;
      left.append(label, document.createElement('br'), meta);
      const play = document.createElement('span');
      play.textContent = '▶';
      button.append(left, play);
      button.addEventListener('click', () => startQuiz('chapter3', mode.id, 10, false));
      container.appendChild(button);
    });

    const boundary = document.createElement('p');
    boundary.className = 'muted';
    boundary.textContent =
      'Спорные толкования и позиции курса помечены в вопросах и не используются для рейтинга.';
    container.appendChild(boundary);

    const back = document.createElement('button');
    back.className = 'btn btn-ghost';
    back.textContent = '← Назад';
    back.addEventListener('click', () => showScreen('home'));
    container.appendChild(back);
    showScreen('levels');
  }

  document.addEventListener('click', (event) => {
    if (!event.target.closest('[data-action="chapter3"]')) return;
    openChapter3Course();
  });
})();
