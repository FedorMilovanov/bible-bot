/* Telegram Mini App — server-authoritative quiz client.
   The browser never receives future questions, never decides correctness,
   and never submits a self-reported score. */
const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
  try { tg.setHeaderColor('#0f0f1a'); } catch (_) {}
  try { tg.enableClosingConfirmation(); } catch (_) {}
}

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const LEVELS = [
  { key: 'easy_p1', name: '🟢 Легкий — 1 (ст. 1–16)', pts: 1 },
  { key: 'easy_p2', name: '🟢 Легкий — 2 (ст. 17–25)', pts: 1 },
  { key: 'medium_p1', name: '🟡 Средний — 1', pts: 2 },
  { key: 'medium_p2', name: '🟡 Средний — 2', pts: 2 },
  { key: 'hard_p1', name: '🔴 Сложный — 1', pts: 3 },
  { key: 'hard_p2', name: '🔴 Сложный — 2', pts: 3 },
  { key: 'practical_p1', name: '🙏 Применение — 1', pts: 2 },
  { key: 'practical_p2', name: '🙏 Применение — 2', pts: 2 },
  { key: 'linguistics_ch1', name: '🔬 Лингвистика — 1', pts: 3 },
  { key: 'linguistics_ch1_2', name: '🔬 Лингвистика — 2', pts: 3 },
  { key: 'linguistics_ch1_3', name: '🔬 Лингвистика — 3', pts: 3 },
];

const HIST = [
  { key: 'intro1', name: '📜 Введение: Авторство ч. 1', pts: 2 },
  { key: 'intro2', name: '📜 Введение: Авторство ч. 2', pts: 2 },
  { key: 'intro3', name: '📜 Введение: Структура', pts: 2 },
  { key: 'nero', name: '👑 Нерон', pts: 2 },
  { key: 'geography', name: '🌍 География', pts: 2 },
];

const MODES = [
  { id: 'relaxed', label: '🧘 Спокойный', desc: 'без таймера · ×1' },
  { id: 'timed', label: '⏱ На время', desc: '30 сек · ×1.5' },
  { id: 'speed', label: '⚡ Скоростной', desc: '15 сек · ×2' },
];

const state = {
  poolKey: null,
  mode: 'relaxed',
  challenge: false,
  requestedCount: 10,
  sessionId: null,
  question: null,
  idx: 0,
  total: 0,
  score: 0,
  answers: [],
  streak: 0,
  maxStreak: 0,
  timer: null,
  timeLeft: 0,
  timeLimit: null,
  answerPending: false,
  awardedPoints: 0,
  dailyBonus: 0,
  newAchievements: [],
};

function getInitData() {
  return tg?.initData || '';
}

function apiHeaders() {
  const headers = { 'Content-Type': 'application/json' };
  const initData = getInitData();
  if (initData) headers['X-Telegram-Init-Data'] = initData;
  return headers;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { ...apiHeaders(), ...(options.headers || {}) },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(body.error || `HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return body;
}

function getUser() {
  return tg?.initDataUnsafe?.user || null;
}

function showScreen(id) {
  $$('.screen').forEach((element) => element.classList.remove('active'));
  $(`#screen-${id}`)?.classList.add('active');
  window.scrollTo(0, 0);
  $('#progressBar').classList.toggle('hidden', id !== 'quiz');
}

function toast(message, ms = 2400) {
  const element = $('#toast');
  element.textContent = message;
  element.classList.remove('hidden');
  window.setTimeout(() => element.classList.add('hidden'), ms);
}

function haptic(kind) {
  try { tg?.HapticFeedback?.notificationOccurred(kind); } catch (_) {}
}

function stopTimer() {
  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
  }
}

function startTimer(remainingSeconds = null) {
  stopTimer();
  if (!state.timeLimit || state.answerPending) return;
  const initial = Number.isFinite(Number(remainingSeconds)) ? Number(remainingSeconds) : state.timeLimit;
  state.timeLeft = Math.max(0, Math.ceil(initial));
  updateTimer();
  if (state.timeLeft <= 0) {
    submitAnswer(-1, true);
    return;
  }
  state.timer = window.setInterval(() => {
    state.timeLeft -= 1;
    updateTimer();
    if (state.timeLeft <= 0) {
      stopTimer();
      submitAnswer(-1, true);
    }
  }, 1000);
}

function resumeTimer() {
  stopTimer();
  if (!state.timeLimit || state.timeLeft <= 0 || state.answerPending) return;
  state.timer = window.setInterval(() => {
    state.timeLeft -= 1;
    updateTimer();
    if (state.timeLeft <= 0) {
      stopTimer();
      submitAnswer(-1, true);
    }
  }, 1000);
}

function updateTimer() {
  const element = $('#quizTimer');
  element.textContent = `⏱ ${state.timeLeft}`;
  element.className = `timer${state.timeLeft <= 5 ? ' danger' : state.timeLeft <= 10 ? ' warn' : ''}`;
}

function resetQuizState(poolKey, mode, count, challenge) {
  stopTimer();
  Object.assign(state, {
    poolKey,
    mode,
    challenge,
    requestedCount: count,
    sessionId: null,
    question: null,
    idx: 0,
    total: 0,
    score: 0,
    answers: [],
    streak: 0,
    maxStreak: 0,
    timeLeft: 0,
    timeLimit: null,
    answerPending: false,
    awardedPoints: 0,
    dailyBonus: 0,
    newAchievements: [],
  });
}

async function updateUserBadge() {
  const user = getUser();
  const badge = $('#userBadge');
  if (!user) {
    badge.textContent = 'Открой в Telegram';
    badge.classList.remove('hidden');
    return;
  }

  badge.textContent = `👤 ${user.first_name || 'Игрок'}`;
  badge.classList.remove('hidden');
  $('#openBotBtn').style.display = 'none';

  try {
    const profile = await api('/api/me');
    const points = profile.entry?.total_points ?? profile.stats?.total_points ?? 0;
    const position = profile.position ? `#${profile.position}` : '';
    badge.textContent = `👤 ${user.first_name || 'Игрок'} · 💎${points} ${position}`.trim();
    const streak = profile.streak?.count || profile.entry?.daily_activity_streak || 0;
    if (streak) {
      $('#streakBadge').textContent = `🔥 ${streak}`;
      $('#streakBadge').classList.remove('hidden');
    }
  } catch (_) {
    // Profile data is non-critical to quiz rendering.
  }
}

async function ensureTelegramAuth() {
  if (getInitData()) return true;
  toast('Для прохождения открой приложение из Telegram-бота.', 3600);
  return false;
}

function openLevels(title, list) {
  $('#levelsTitle').textContent = title;
  const container = $('#levelsList');
  container.replaceChildren();

  list.forEach((level) => {
    const button = document.createElement('button');
    button.className = 'level-btn';

    const left = document.createElement('span');
    const name = document.createElement('b');
    name.textContent = level.name;
    const meta = document.createElement('span');
    meta.className = 'level-meta';
    meta.textContent = `${level.pts} балла за верный ответ · 10 вопросов`;
    left.append(name, document.createElement('br'), meta);

    const action = document.createElement('span');
    action.className = 'level-badge';
    action.textContent = 'Играть →';
    button.append(left, action);
    button.addEventListener('click', () => openModePicker(level));
    container.appendChild(button);
  });
  showScreen('levels');
}

function openModePicker(level) {
  $('#levelsTitle').textContent = level.name;
  const container = $('#levelsList');
  container.replaceChildren();

  const info = document.createElement('div');
  info.className = 'card';
  const title = document.createElement('b');
  title.textContent = level.name;
  const desc = document.createElement('p');
  desc.className = 'muted';
  desc.textContent = 'Результат проверяется сервером и синхронизируется с общей статистикой.';
  info.append(title, desc);
  container.appendChild(info);

  MODES.forEach((mode) => {
    const button = document.createElement('button');
    button.className = 'level-btn';
    const left = document.createElement('span');
    const label = document.createElement('b');
    label.textContent = mode.label;
    const meta = document.createElement('span');
    meta.className = 'level-meta';
    meta.textContent = mode.desc;
    left.append(label, document.createElement('br'), meta);
    const play = document.createElement('span');
    play.textContent = '▶';
    button.append(left, play);
    button.addEventListener('click', () => startQuiz(level.key, mode.id, 10, false));
    container.appendChild(button);
  });

  const back = document.createElement('button');
  back.className = 'btn btn-ghost';
  back.textContent = '← Назад';
  back.addEventListener('click', () => openLevels(
    HIST.some((item) => item.key === level.key) ? 'Исторический контекст' : 'Глава 1 — выбери уровень',
    HIST.some((item) => item.key === level.key) ? HIST : LEVELS,
  ));
  container.appendChild(back);
}

function openChallenge() {
  $('#levelsTitle').textContent = '🎲 Challenge 20';
  const container = $('#levelsList');
  container.replaceChildren();

  const info = document.createElement('div');
  info.className = 'card';
  info.textContent = '20 случайных вопросов. Сервер считает результат, дневной бонус и недельный рейтинг.';
  container.appendChild(info);

  [
    { mode: 'relaxed', label: '🎲 Normal · без таймера' },
    { mode: 'speed', label: '💀 Hardcore · 10 сек' },
  ].forEach((variant) => {
    const button = document.createElement('button');
    button.className = 'level-btn';
    const label = document.createElement('b');
    label.textContent = variant.label;
    const play = document.createElement('span');
    play.textContent = '▶';
    button.append(label, play);
    button.addEventListener('click', () => startQuiz('random_all', variant.mode, 20, true));
    container.appendChild(button);
  });
  showScreen('levels');
}

async function startQuiz(poolKey, mode = 'relaxed', count = 10, challenge = false) {
  if (!(await ensureTelegramAuth())) return;
  resetQuizState(poolKey, mode, count, challenge);
  showScreen('quiz');
  setQuizLoading('Готовлю вопросы…');

  try {
    const data = await api('/api/quiz/start', {
      method: 'POST',
      body: JSON.stringify({ pool_key: poolKey, mode, count, challenge }),
    });
    state.sessionId = data.session_id;
    applyCurrentQuestion(data);
  } catch (error) {
    showScreen('home');
    toast(error.status === 401 ? 'Открой приложение из Telegram-бота.' : `Не удалось начать тест: ${error.message}`, 4200);
  }
}

function setQuizLoading(message) {
  stopTimer();
  $('#quizQuestion').textContent = message;
  $('#quizOptions').replaceChildren();
  $('#quizFeedback').className = 'feedback hidden';
}

function applyCurrentQuestion(data) {
  if (!data?.question) throw new Error('Сервер не вернул вопрос');
  state.question = data.question;
  state.idx = Number(data.index || 0);
  state.total = Number(data.total || state.requestedCount || 0);
  state.timeLimit = data.time_limit || null;
  $('#quizTimer').classList.toggle('hidden', !state.timeLimit);
  renderQuestion(data.remaining_seconds);
}

async function loadCurrentQuestion() {
  if (!state.sessionId) return;
  setQuizLoading('Загружаю следующий вопрос…');
  try {
    const data = await api('/api/quiz/current', {
      method: 'POST',
      body: JSON.stringify({ session_id: state.sessionId }),
    });
    applyCurrentQuestion(data);
  } catch (error) {
    $('#quizQuestion').textContent = 'Не удалось загрузить следующий вопрос';
    const box = $('#quizOptions');
    box.replaceChildren();
    const retry = document.createElement('button');
    retry.className = 'btn btn-primary';
    retry.textContent = '↻ Повторить загрузку';
    retry.addEventListener('click', loadCurrentQuestion);
    box.appendChild(retry);
    toast(error.message, 3600);
  }
}

function renderQuestion(remainingSeconds = null) {
  stopTimer();
  const question = state.question;
  if (!question) return;

  const dots = $('#quizProgress');
  dots.replaceChildren();
  for (let index = 0; index < state.total; index += 1) {
    const dot = document.createElement('div');
    const answered = state.answers[index];
    dot.className = `qdot${answered ? (answered.ok ? ' done ok' : ' done bad') : index === state.idx ? ' current' : ''}`;
    dots.appendChild(dot);
  }

  $('#progressFill').style.width = `${(state.idx / Math.max(1, state.total)) * 100}%`;
  $('#progressText').textContent = `${state.idx + 1} / ${state.total}`;
  $('#quizQuestion').textContent = question.question;
  $('#quizFeedback').className = 'feedback hidden';

  const options = $('#quizOptions');
  options.replaceChildren();
  question.options.forEach((text, index) => {
    const button = document.createElement('button');
    button.className = 'opt';
    button.textContent = `${index + 1}. ${text}`;
    button.addEventListener('click', () => submitAnswer(index, false));
    options.appendChild(button);
  });

  startTimer(remainingSeconds);
}

function renderFeedback(question, chosen, result) {
  const optionButtons = [...document.querySelectorAll('.opt')];
  optionButtons.forEach((button, index) => {
    button.disabled = true;
    if (index === result.correct_index) button.classList.add('correct');
    if (index === chosen && !result.ok) button.classList.add('wrong');
  });

  const feedback = $('#quizFeedback');
  feedback.replaceChildren();
  feedback.className = `feedback ${result.ok ? 'ok' : 'bad'}`;

  const headline = document.createElement('div');
  if (result.timed_out) headline.textContent = `⏱ Время вышло. Правильно: ${question.options[result.correct_index]}`;
  else if (result.ok) headline.textContent = `✅ Верно! ${question.options[result.correct_index]}`;
  else headline.textContent = `❌ Неверно. Правильно: ${question.options[result.correct_index]}`;
  feedback.appendChild(headline);

  if (result.explanation) {
    const explanation = document.createElement('span');
    explanation.className = 'muted';
    explanation.textContent = result.explanation;
    feedback.append(document.createElement('br'), explanation);
  }
}

async function submitAnswer(chosen, localTimeout = false) {
  if (state.answerPending || !state.sessionId || !state.question) return;
  state.answerPending = true;
  stopTimer();
  const question = state.question;
  const questionIndex = state.idx;
  const buttons = [...document.querySelectorAll('.opt')];
  buttons.forEach((button) => { button.disabled = true; });

  try {
    const result = await api('/api/quiz/answer', {
      method: 'POST',
      body: JSON.stringify({ session_id: state.sessionId, question_id: question.id, chosen }),
    });

    state.score = result.score;
    state.maxStreak = result.max_streak || state.maxStreak;
    state.streak = result.ok ? state.streak + 1 : 0;
    state.answers[questionIndex] = {
      question,
      chosen,
      correct: result.correct_index,
      ok: result.ok,
      timedOut: result.timed_out || localTimeout,
      explanation: result.explanation || '',
    };
    state.awardedPoints = result.points || 0;
    state.dailyBonus = result.daily_bonus || 0;
    state.newAchievements = result.new_achievements || [];

    renderFeedback(question, chosen, result);
    haptic(result.ok ? 'success' : 'error');
    state.answerPending = false;

    window.setTimeout(() => {
      if (result.finished) showResult();
      else loadCurrentQuestion();
    }, result.ok ? 900 : 1800);
  } catch (error) {
    state.answerPending = false;
    buttons.forEach((button) => { button.disabled = false; });
    toast(`Не удалось сохранить ответ: ${error.message}`, 3600);
    resumeTimer();
  }
}

function showResult() {
  stopTimer();
  const total = state.total;
  const score = state.score;
  const percent = total ? Math.round((score / total) * 100) : 0;

  let emoji = '💪';
  let title = 'Попробуй ещё раз';
  if (percent === 100) { emoji = '🏆'; title = 'Идеально!'; }
  else if (percent >= 80) { emoji = '⭐'; title = 'Отлично!'; }
  else if (percent >= 60) { emoji = '👍'; title = 'Хорошо!'; }
  else if (percent >= 40) { emoji = '📚'; title = 'Неплохо'; }

  $('#resultEmoji').textContent = emoji;
  $('#resultTitle').textContent = title;
  $('#resultScore').textContent = `${score} / ${total} · ${percent}%`;
  $('#resultBarFill').style.width = '0%';
  window.setTimeout(() => { $('#resultBarFill').style.width = `${percent}%`; }, 80);

  const notes = [];
  if (state.maxStreak >= 3) notes.push(`🔥 Лучшая серия: ${state.maxStreak}`);
  if (state.dailyBonus) notes.push(`🎁 Дневной бонус: +${state.dailyBonus}`);
  if (state.newAchievements.length) notes.push(state.newAchievements.join(' · '));
  $('#resultDesc').textContent = notes.join(' · ');

  const stats = $('#resultStats');
  stats.replaceChildren();
  const points = document.createElement('span');
  points.className = 'stat';
  points.textContent = `💎 +${state.awardedPoints} баллов`;
  const mode = document.createElement('span');
  mode.className = 'stat';
  mode.textContent = state.challenge ? '🎲 Challenge 20' : `⏱ ${state.mode}`;
  stats.append(points, mode);

  showScreen('result');
  if (percent === 100) haptic('success');
  updateUserBadge();
}

function showReview() {
  const list = $('#reviewList');
  list.replaceChildren();

  state.answers.forEach((answer, index) => {
    if (!answer) return;
    const item = document.createElement('div');
    item.className = 'review-item';

    const title = document.createElement('div');
    title.className = 'review-q';
    title.textContent = `${index + 1}. ${answer.question.question}`;
    item.appendChild(title);

    answer.question.options.forEach((text, optionIndex) => {
      const option = document.createElement('div');
      option.className = 'review-opt';
      if (optionIndex === answer.correct) option.classList.add('correct');
      else if (optionIndex === answer.chosen && !answer.ok) option.classList.add('wrong');
      option.textContent = `${optionIndex + 1}. ${text}${optionIndex === answer.correct ? ' ✅' : optionIndex === answer.chosen ? ' ← твой' : ''}`;
      item.appendChild(option);
    });

    if (answer.explanation) {
      const explanation = document.createElement('div');
      explanation.className = 'review-exp';
      explanation.textContent = `💡 ${answer.explanation}`;
      item.appendChild(explanation);
    }
    list.appendChild(item);
  });
  showScreen('review');
}

async function openStats() {
  showScreen('leaderboard');
  document.querySelector('#screen-leaderboard h2').textContent = '📊 Моя статистика';
  $('.tabs').classList.add('hidden');
  const list = $('#lbList');
  list.textContent = 'Загрузка…';

  try {
    const profile = await api('/api/me');
    list.replaceChildren();
    const entry = profile.entry || {};
    const summary = document.createElement('div');
    summary.className = 'lb-row';
    summary.textContent = `💎 ${entry.total_points || 0} баллов · #${profile.position || '?'} · тестов: ${entry.total_tests || 0}`;
    list.appendChild(summary);

    (profile.history || []).forEach((history) => {
      const row = document.createElement('div');
      row.className = 'lb-row';
      const total = history.total_questions ?? '?';
      row.textContent = `${history.level_name || history.mode || 'Тест'} · ${history.correct_count || 0}/${total}`;
      list.appendChild(row);
    });
  } catch (error) {
    list.textContent = error.status === 401 ? 'Открой приложение из Telegram, чтобы увидеть статистику.' : 'Статистика временно недоступна.';
  }
}

async function openLeaderboard() {
  showScreen('leaderboard');
  document.querySelector('#screen-leaderboard h2').textContent = '🏆 Лидеры';
  $('.tabs').classList.remove('hidden');
  const list = $('#lbList');

  $$('.tab').forEach((tab) => {
    tab.onclick = async () => {
      $$('.tab').forEach((item) => item.classList.remove('active'));
      tab.classList.add('active');
      list.textContent = 'Загрузка…';
      try {
        const data = await api(`/api/leaderboard?cat=${encodeURIComponent(tab.dataset.tab)}`);
        list.replaceChildren();
        if (!data.users?.length) {
          list.textContent = 'Пока пусто — пройди первый тест!';
          return;
        }
        data.users.forEach((user) => {
          const row = document.createElement('div');
          row.className = 'lb-row';

          const rank = document.createElement('div');
          rank.className = `lb-rank${user.rank === 1 ? ' gold' : user.rank === 2 ? ' silver' : user.rank === 3 ? ' bronze' : ''}`;
          rank.textContent = user.rank;

          const info = document.createElement('div');
          info.className = 'lb-info';
          const name = document.createElement('div');
          name.className = 'lb-name';
          name.textContent = user.first_name || user.username || 'Игрок';
          const sub = document.createElement('div');
          sub.className = 'lb-sub';
          sub.textContent = user.total_tests ? `тестов: ${user.total_tests}` : '';
          info.append(name, sub);

          const score = document.createElement('div');
          score.className = 'lb-pts';
          score.textContent = `${tab.dataset.tab === 'general' ? '💎' : '✅'}${user.score || 0}`;
          row.append(rank, info, score);
          list.appendChild(row);
        });
      } catch (_) {
        list.textContent = 'Таблица лидеров временно недоступна.';
      }
    };
  });
  document.querySelector('.tab[data-tab="general"]')?.click();
}

async function openBot() {
  try {
    const info = await api('/api/botinfo');
    const username = info.username || '';
    if (!username) throw new Error('username missing');
    const url = `https://t.me/${username}`;
    if (tg) tg.openTelegramLink(url); else window.open(url, '_blank', 'noopener');
  } catch (_) {
    toast('BOT_USERNAME не настроен на сервере.');
  }
}

$$('[data-action]').forEach((button) => {
  button.addEventListener('click', () => {
    const action = button.dataset.action;
    if (action === 'chapter1') openLevels('Глава 1 — выбери уровень', LEVELS);
    else if (action === 'historical') openLevels('Исторический контекст', HIST);
    else if (action === 'challenge') openChallenge();
    else if (action === 'battle') toast('⚔️ PvP-битвы пока остаются в боте — там сохранена полная логика.');
    else if (action === 'leaderboard') openLeaderboard();
    else if (action === 'stats') openStats();
    else if (action === 'about') showScreen('about');
  });
});

$$('[data-back]').forEach((button) => button.addEventListener('click', () => showScreen(button.dataset.back)));
$('#openBotBtn').addEventListener('click', openBot);
$('#quizExit').addEventListener('click', () => {
  if (window.confirm('Выйти из теста? Незавершённый результат не попадёт в рейтинг.')) {
    stopTimer();
    showScreen('home');
  }
});
$('#resultHome').addEventListener('click', () => showScreen('home'));
$('#resultRetry').addEventListener('click', () => startQuiz(state.poolKey, state.mode, state.requestedCount, state.challenge));
$('#resultReview').addEventListener('click', showReview);
$('#resultShare').addEventListener('click', async () => {
  const total = state.total;
  const percent = total ? Math.round((state.score / total) * 100) : 0;
  const text = `📖 1 Петра — ${state.score}/${total} (${percent}%)`;
  try {
    if (navigator.share) await navigator.share({ title: 'Мой результат', text });
    else if (tg?.openTelegramLink) tg.openTelegramLink(`https://t.me/share/url?url=&text=${encodeURIComponent(text)}`);
    else {
      await navigator.clipboard.writeText(text);
      toast('Результат скопирован.');
    }
  } catch (_) {}
});

try {
  const params = new URLSearchParams(window.location.search);
  const startParam = params.get('tgWebAppStartParam') || params.get('start');
  if (startParam) {
    const key = startParam.replace(/^level_/, '');
    const level = [...LEVELS, ...HIST].find((item) => item.key === key);
    if (level) window.setTimeout(() => openModePicker(level), 250);
  }
} catch (_) {}

if (tg?.BackButton) {
  tg.BackButton.onClick(() => {
    const active = document.querySelector('.screen.active')?.id;
    if (active === 'screen-quiz') {
      if (window.confirm('Выйти из теста?')) {
        stopTimer();
        showScreen('home');
      }
    } else if (active === 'screen-home') tg.close();
    else showScreen('home');
  });
  const observer = new MutationObserver(() => {
    const homeActive = $('#screen-home').classList.contains('active');
    if (homeActive) tg.BackButton.hide(); else tg.BackButton.show();
  });
  observer.observe(document.getElementById('app'), { attributes: true, subtree: true });
}

updateUserBadge();
