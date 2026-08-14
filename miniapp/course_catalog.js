/* Pure helpers for the server-authoritative learning catalog. */
(function initCourseCatalog(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CourseCatalog = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, () => {
  function asArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function validateCatalog(raw) {
    if (!raw || typeof raw !== 'object') throw new Error('Некорректный каталог курсов');
    const groups = asArray(raw.groups);
    const modes = raw.modes && typeof raw.modes === 'object' ? raw.modes : {};
    const seenGroups = new Set();
    const seenCourses = new Set();

    for (const group of groups) {
      if (!group || typeof group.key !== 'string' || !group.key) {
        throw new Error('Каталог содержит группу без ключа');
      }
      if (seenGroups.has(group.key)) throw new Error(`Дублирующаяся группа: ${group.key}`);
      seenGroups.add(group.key);
      for (const course of asArray(group.courses)) {
        if (!course || typeof course.key !== 'string' || !course.key) {
          throw new Error('Каталог содержит курс без ключа');
        }
        if (seenCourses.has(course.key)) throw new Error(`Дублирующийся курс: ${course.key}`);
        seenCourses.add(course.key);
        if (course.group !== group.key) throw new Error(`Курс ${course.key} находится не в своей группе`);
        if (!Number.isInteger(course.default_question_count) || course.default_question_count <= 0) {
          throw new Error(`Некорректное число вопросов: ${course.key}`);
        }
        if (!Array.isArray(course.modes) || course.modes.length === 0) {
          throw new Error(`У курса нет разрешённых режимов: ${course.key}`);
        }
        for (const mode of course.modes) {
          if (!modes[mode]) throw new Error(`Неизвестный режим ${mode} для ${course.key}`);
        }
        if (!['scored', 'learning'].includes(course.scoring_mode)) {
          throw new Error(`Неизвестная scoring policy для ${course.key}`);
        }
      }
    }
    return raw;
  }

  function groups(catalog) {
    return [...asArray(catalog?.groups)].sort((a, b) => (a.order || 0) - (b.order || 0) || a.key.localeCompare(b.key));
  }

  function courses(catalog) {
    return groups(catalog).flatMap((group) => asArray(group.courses));
  }

  function getGroup(catalog, key) {
    return groups(catalog).find((group) => group.key === key) || null;
  }

  function getCourse(catalog, key) {
    return courses(catalog).find((course) => course.key === key) || null;
  }

  function getMode(catalog, id) {
    const mode = catalog?.modes?.[id];
    return mode && typeof mode === 'object' ? mode : null;
  }

  function buildCourseStartPayload(course, modeId) {
    if (!course || !course.key) throw new Error('Курс недоступен');
    if (!Array.isArray(course.modes) || !course.modes.includes(modeId)) {
      throw new Error('Режим недоступен для курса');
    }
    return {
      course_key: course.key,
      mode: modeId,
      count: course.default_question_count,
      challenge: false,
    };
  }

  function isLearningOnly(course) {
    return course?.scoring_mode === 'learning';
  }

  return {
    validateCatalog,
    groups,
    courses,
    getGroup,
    getCourse,
    getMode,
    buildCourseStartPayload,
    isLearningOnly,
  };
});
