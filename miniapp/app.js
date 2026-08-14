/* Telegram Mini App — server-authoritative quiz client.
   The browser never receives future questions, never decides correctness,
   and never submits a self-reported score or ranking policy. */
const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
  try { tg.setHeaderColor('#0f0f1a'); } catch (_) {}
}

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const quizFlow = new window.QuizFlowGuard();
const courseCatalogModel = window.CourseCatalog;
let courseCatalog = null;
let catalogRequest = null;

const state = {
  courseKey: null,
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

function quizScreenActive() {
  return Boolean($('#screen-quiz')?.classList.contains('active'));
}

function isCurrentQuizFlow(epoch, sessionId = null) {
  if (!quizFlow.isCurrent(epoch) || !quizScreenActive()) return false;
  return sessionId === null || state.sessionId === sessionId;
}

function invalidateQuizFlow() {
  stopTimer();
  quizFlow.invalidate();
  state.answerPending = false;
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

function resetQuizState(courseKey, mode, count, challenge) {
  stopTimer();
  Object.assign(state, {
    courseKey: challenge ? null : courseKey,
    poolKey: challenge ? 'random_all' : null,
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

function applySessionState(data) {
  state.sessionId = data.session_id;
  if (data.course_key) state.courseKey = data.course_key;
  if (data.pool_key) state.poolKey = data.pool_key;
  state.score = Number(data.score || 0);
  state.streak = Number(data.current_streak || 0);
  state.maxStreak = Number(data.max_streak || 0);
  state.answers = Array.isArray(data.answers) ? data.answers : [];
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

function coursePolicyText(course) {
  if (courseCatalogModel.isLearningOnly(course)) {
    return `учебный режим · без рейтинга · ${course.default_question_count} вопросов`;
  }
  return `${course.points_per_question} балл(а) за верный ответ · ${course.default_question_count} вопросов`;
}

function renderCourseMenu(catalog) {
  const container = $('#courseMenu');
  container.replaceChildren();
  container.setAttribute('aria-busy', 'false');

  const groups = courseCatalogModel.groups(catalog).filter((group) => group.home_card !== false);
  if (!groups.length) {
    const empty = document.createElement('div');
    empty.className = 'card';
    empty.textContent = 'Учебные модули сейчас недоступны.';
    container.appendChild(empty);
    return;
  }

  groups.forEach((group) => {
    const button = document.createElement('button');
    button.className = `card${group.key === 'chapter1' ? ' card-primary' : ''}`;
    button.type = 'button';
    button.setAttribute('aria-label', `${group.title}. ${group.description}`);

    const emoji = document.createElement('span');
    emoji.className = 'card-emoji';
    emoji.setAttribute('aria-hidden', 'true');
    emoji.textContent = group.icon || '📖';
    const title = document.createElement('span');
    title.className = 'card-title';
    title.textContent = group.title;
    const desc = document.createElement('span');
    desc.className = 'card-desc';
    desc.textContent = group.description;
    button.append(emoji, title, desc);
    button.addEventListener('click', () => openCourseGroup(group.key));
    container.appendChild(button);
  });
}

function renderCatalogFailure(message) {
  const container = $('#courseMenu');
  container.replaceChildren();
  container.setAttribute('aria-busy', 'false');
  const card = document.createElement('div');
  card.className = 'card';
  const text = document.createElement('p');
  text.className = 'muted';
  text.textContent = message;
  const retry = document.createElement('button');
  retry.className = 'btn btn-outline';
  retry.type = 'button';
  retry.textContent = '↻ Обновить курсы';
  retry.addEventListener('click', () => { void refreshCourseCatalog({ force: true }); });
  card.append(text, retry);
  container.appendChild(card);
}

async function refreshCourseCatalog({ force = false, quiet = false } = {}) {
  if (!force && courseCatalog) return courseCatalog;
  if (!force && catalogRequest) return catalogRequest;
  const container = $('#courseMenu');
  container?.setAttribute('aria-busy', 'true');
  catalogRequest = api('/api/catalog')
    .then((raw) => courseCatalogModel.validateCatalog(raw))
    .then((catalog) => {
      courseCatalog = catalog;
      renderCourseMenu(catalog);
      return catalog;
    })
    .catch((error) => {
      if (!quiet) renderCatalogFailure(`Не удалось загрузить учебные модули: ${error.message}`);
      throw error;
    })
    .finally(() => { catalogRequest = null; });
  return catalogRequest;
}

function openCourseGroup(groupKey) {
  const group = courseCatalogModel.getGroup(courseCatalog, groupKey);
  if (!group) {
    toast('Курс больше недоступен. Обновляю каталог…', 3200);
    void refreshCourseCatalog({ force: true });
    return;
  }
  const courses = Array.isArray(group.courses) ? group.courses : [];
  if (courses.length === 1) {
    openModePicker(courses[0]);
    return;
  }
  openLevels(group.title, courses, group.key);
}

function openLevels(title, list, groupKey) {
  $('#levelsTitle').textContent = title;
  const container = $('#levelsList');
  container.replaceChildren();

  list.forEach((course) => {
    const button = document.createElement('button');
    button.className = 'level-btn';
    button.type = 'button';
    button.setAttribute('aria-label', `${course.title}. ${coursePolicyText(course)}`);

    const left = document.createElement('span');
    const name = document.createElement('b');
    name.textContent = course.title;
    const meta = document.createElement('span');
    meta.className = 'level-meta';
    meta.textContent = coursePolicyText(course);
    left.append(name, document.createElement('br'), meta);

    const action = document.createElement('span');
    action.className = 'level-badge';
    action.textContent = 'Играть →';
    button.append(left, action);
    button.addEventListener('click', () => openModePicker(course));
    container.appendChild(button);
  });

  const back = document.createElement('button');
  back.className = 'btn btn-ghost';
  back.type = 'button';
  back.textContent = '← Назад';
  back.addEventListener('click', () => showScreen('home'));
  back.dataset.group = groupKey || '';
  container.appendChild(back);
  showScreen('levels');
}

function openModePicker(course) {
  if (!course || !courseCatalogModel.getCourse(courseCatalog, course.key)) {
    toast('Курс больше недоступен. Обновляю каталог…', 3200);
    void refreshCourseCatalog({ force: true });
    return;
  }
  $('#levelsTitle').textContent = course.title;
  const container = $('#levelsList');
  container.replaceChildren();

  const info = document.createElement('div');
  info.className = 'card';
  const title = document.createElement('b');
  title.textContent = course.title;
  const desc = document.createElement('p');
  desc.className = 'muted';
  desc.textContent = `${course.description}. ${coursePolicyText(course)}. Политика результата определяется сервером.`;
  info.append(title, desc);
  container.appendChild(info);

  course.modes.forEach((modeId) => {
    const mode = courseCatalogModel.getMode(courseCatalog, modeId);
    if (!mode) return;
    const button = document.createElement('button');
    button.className = 'level-btn';
    button.type = 'button';
    button.setAttribute('aria-label', `${mode.label}. ${mode.description}`);
    const left = document.createElement('span');
    const label = document.createElement('b');
    label.textContent = mode.label;
    const meta = document.createElement('span');
    meta.className = 'level-meta';
    meta.textContent = `${mode.description}${courseCatalogModel.isLearningOnly(course) ? ' · учебный режим' : ''}`;
    left.append(label, document.createElement('br'), meta);
    const play = document.createElement('span');
    play.textContent = '▶';
    button.append(left, play);
    button.addEventListener('click', () => startQuiz(course.key, mode.id, course.default_question_count, false));
    container.appendChild(button);
  });

  const back = document.createElement('button');
  back.className = 'btn btn-ghost';
  back.type = 'button';
  back.textContent = '← Назад';
  back.addEventListener('click', () => {
    const group = courseCatalogModel.getGroup(courseCatalog, course.group);
    if (!group || (group.courses || []).length <= 1) showScreen('home');
    else openLevels(group.title, group.courses, group.key);
  });
  container.appendChild(back);
  showScreen('levels');
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
    button.type = 'button';
    const label = document.createElement('b');
    label.textContent = variant.label;
    const play = document.createElement('span');
    play.textContent = '▶';
    button.append(label, play);
    button.addEventListener('click', () => startQuiz(null, variant.mode, 20, true));
    container.appendChild(button);
  });
  showScreen('levels');
}

async function startQuiz(courseKey, mode = 'relaxed', count = 10, challenge = false) {
  if (!(await ensureTelegramAuth())) return;
  let payload;
  if (challenge) {
    payload = { pool_key: 'random_all', mode, count: 20, challenge: true };
    count = 20;
  } else {
    const course = courseCatalogModel.getCourse(courseCatalog, courseKey);
    if (!course) {
      toast('Курс недоступен. Обновляю каталог…', 3200);
      try { await refreshCourseCatalog({ force: true }); } catch (_) { return; }
    }
    const current = courseCatalogModel.getCourse(courseCatalog, courseKey);
    if (!current) {
      toast('Этот курс больше не доступен.', 3200);
      return;
    }
    try {
      payload = courseCatalogModel.buildCourseStartPayload(current, mode);
    } catch (error) {
      toast(error.message, 3200);
      return;
    }
    count = current.default_question_count;
  }

  const flowEpoch = quizFlow.begin();
  resetQuizState(courseKey, mode, count, challenge);
  showScreen('quiz');
  setQuizLoading('Готовлю вопросы…');

  try {
    const data = await api('/api/quiz/start', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    if (!isCurrentQuizFlow(flowEpoch)) return;
    applySessionState(data);
    applyCurrentQuestion(data);
  } catch (error) {
    if (!quizFlow.isCurrent(flowEpoch)) return;
    invalidateQuizFlow();
    showScreen('home');
    if (error.status === 409 && /course unavailable/i.test(error.message)) {
      void refreshCourseCatalog({ force: true, quiet: true }).catch(() => {});
    }
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

async function restoreActiveQuiz() {
  if (!getInitData()) return false;
  const data = await api('/api/quiz/active');
  if (!data.active) {
    if (data.finalized) {
      toast('Завершённый тест сохранён в статистике.', 3200);
      void updateUserBadge();
    }
    return false;
  }

  const flowEpoch = quizFlow.begin();
  resetQuizState(
    data.course_key || null,
    data.mode || 'relaxed',
    Number(data.total || 10),
    Boolean(data.challenge),
  );
  showScreen('quiz');
  setQuizLoading('Восстанавливаю незавершённый тест…');
  if (!isCurrentQuizFlow(flowEpoch)) return false;
  applySessionState(data);
  applyCurrentQuestion(data);
  toast('Продолжаем незавершённый тест.', 2600);
  return true;
}

async function loadCurrentQuestion() {
  if (!state.sessionId) return;
  const flowEpoch = quizFlow.current();
  const sessionId = state.sessionId;
  if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
  setQuizLoading('Загружаю следующий вопрос…');
  try {
    const data = await api('/api/quiz/current', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId }),
    });
    if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
    applyCurrentQuestion(data);
  } catch (error) {
    if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
    $('#quizQuestion').textContent = 'Не удалось загрузить следующий вопрос';
    const box = $('#quizOptions');
    box.replaceChildren();
    const retry = document.createElement('button');
    retry.className = 'btn btn-primary';
    retry.type = 'button';
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
    button.type = 'button';
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
  if (state.answerPending || !state.sessionId || !state.question || !quizScreenActive()) return;
  const flowEpoch = quizFlow.current();
  const sessionId = state.sessionId;
  state.answerPending = true;
  stopTimer();
  const question = state.question;
  const questionIndex = state.idx;
  const buttons = [...document.querySelectorAll('.opt')];
  buttons.forEach((button) => { button.disabled = true; });

  try {
    const result = await api('/api/quiz/answer', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId, question_id: question.id, chosen }),
    });
    if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;

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

    quizFlow.schedule(flowEpoch, () => {
      if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
      if (result.finished) showResult();
      else loadCurrentQuestion();
    }, result.ok ? 900 : 1800);
  } catch (error) {
    if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
    state.answerPending = false;
    buttons.forEach((button) => { button.disabled = false; });
    toast(`Не удалось сохранить ответ: ${error.message}`, 3600);
    resumeTimer();
  }
}

async function exitQuiz() {
  if (!state.sessionId || !quizScreenActive()) return;
  if (state.answerPending) {
    toast('Сначала дождись сохранения текущего ответа.', 2800);
    return;
  }
  if (!window.confirm('Выйти из теста? Незавершённый результат не попадёт в рейтинг.')) return;

  const flowEpoch = quizFlow.current();
  const sessionId = state.sessionId;
  const buttons = [...document.querySelectorAll('.opt')];
  state.answerPending = true;
  stopTimer();
  buttons.forEach((button) => { button.disabled = true; });

  try {
    await api('/api/quiz/cancel', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId }),
    });
    if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
    invalidateQuizFlow();
    showScreen('home');
    void refreshCourseCatalog({ force: true, quiet: true }).catch(() => {});
    toast('Незавершённый тест отменён.', 2400);
  } catch (error) {
    if (!isCurrentQuizFlow(flowEpoch, sessionId)) return;
    if (error.status === 409) {
      try {
        const active = await api('/api/quiz/active');
        if (!active.active) {
          invalidateQuizFlow();
          showScreen('home');
          toast('Тест уже завершён — результат сохранён.', 3200);
          void updateUserBadge();
          void refreshCourseCatalog({ force: true, quiet: true }).catch(() => {});
          return;
        }
      } catch (_) {
        // Preserve the current screen below when server recovery cannot be confirmed.
      }
    }
    state.answerPending = false;
    toast(`Не удалось выйти из теста: ${error.message}`, 3600);
    await loadCurrentQuestion();
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
  const course = state.courseKey ? courseCatalogModel.getCourse(courseCatalog, state.courseKey) : null;
  const points = document.createElement('span');
  points.className = 'stat';
  if (course && courseCatalogModel.isLearningOnly(course)) {
    points.textContent = '📚 Учебный прогресс · без рейтинга';
  } else {
    points.textContent = `💎 +${state.awardedPoints} баллов`;
  }
  const mode = document.createElement('span');
  mode.className = 'stat';
  const modeMeta = courseCatalogModel.getMode(courseCatalog, state.mode);
  mode.textContent = state.challenge ? '🎲 Challenge 20' : (modeMeta?.label || `⏱ ${state.mode}`);
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
    if (action === 'challenge') openChallenge();
    else if (action === 'battle') toast('⚔️ PvP-битвы пока остаются в боте — там сохранена полная логика.');
    else if (action === 'leaderboard') openLeaderboard();
    else if (action === 'stats') openStats();
    else if (action === 'about') showScreen('about');
  });
});

$$('[data-back]').forEach((button) => button.addEventListener('click', () => showScreen(button.dataset.back)));
$('#openBotBtn').addEventListener('click', openBot);
$('#quizExit').addEventListener('click', () => { void exitQuiz(); });
$('#resultHome').addEventListener('click', () => {
  invalidateQuizFlow();
  showScreen('home');
  void refreshCourseCatalog({ force: true, quiet: true }).catch(() => {});
});
$('#resultRetry').addEventListener('click', () => startQuiz(state.courseKey, state.mode, state.requestedCount, state.challenge));
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

function openStartParam() {
  try {
    if (!courseCatalog) return;
    const params = new URLSearchParams(window.location.search);
    const startParam = params.get('tgWebAppStartParam') || params.get('start');
    if (!startParam) return;
    let course = courseCatalogModel.getCourse(courseCatalog, startParam);
    if (!course && !startParam.startsWith('level_')) {
      course = courseCatalogModel.getCourse(courseCatalog, `level_${startParam}`);
    }
    if (course) window.setTimeout(() => openModePicker(course), 250);
  } catch (_) {}
}

if (tg?.BackButton) {
  tg.BackButton.onClick(() => {
    const active = document.querySelector('.screen.active')?.id;
    if (active === 'screen-quiz') {
      void exitQuiz();
    } else if (active === 'screen-home') tg.close();
    else showScreen('home');
  });
  const observer = new MutationObserver(() => {
    const homeActive = $('#screen-home').classList.contains('active');
    if (homeActive) tg.BackButton.hide(); else tg.BackButton.show();
  });
  observer.observe(document.getElementById('app'), { attributes: true, subtree: true });
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState !== 'visible') return;
  if (!$('#screen-home')?.classList.contains('active')) return;
  void refreshCourseCatalog({ force: true, quiet: true }).catch(() => {});
});

async function bootstrapMiniApp() {
  void updateUserBadge();
  if (getInitData()) {
    try {
      if (await restoreActiveQuiz()) {
        // Active durable sessions resume even if a fresh catalog request fails.
        void refreshCourseCatalog({ quiet: true }).catch(() => {});
        return;
      }
    } catch (error) {
      toast(`Не удалось восстановить незавершённый тест: ${error.message}`, 4200);
      return;
    }
  }
  try {
    await refreshCourseCatalog();
    openStartParam();
  } catch (_) {
    // renderCatalogFailure already leaves a retry control on the home screen.
  }
}

void bootstrapMiniApp();
