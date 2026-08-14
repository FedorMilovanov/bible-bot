const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '../..');
const catalogModel = require(path.join(root, 'miniapp', 'course_catalog.js'));
const indexPath = path.join(root, 'miniapp', 'index.html');
const appPath = path.join(root, 'miniapp', 'app.js');

const catalog = catalogModel.validateCatalog({
  version: 1,
  modes: {
    relaxed: { id: 'relaxed', label: 'Спокойный', description: 'без таймера' },
    speed: { id: 'speed', label: 'Скоростной', description: '10 сек' },
  },
  groups: [
    {
      key: 'chapter2',
      title: 'Глава 2',
      order: 20,
      courses: [
        {
          key: 'chapter2',
          title: '1 Петра — Глава 2',
          group: 'chapter2',
          order: 10,
          default_question_count: 10,
          modes: ['relaxed', 'speed'],
          scoring_mode: 'learning',
          points_per_question: 0,
        },
      ],
    },
    {
      key: 'chapter3',
      title: 'Глава 3',
      order: 30,
      courses: [
        {
          key: 'chapter3',
          title: '1 Петра — Глава 3',
          group: 'chapter3',
          order: 10,
          default_question_count: 10,
          modes: ['relaxed'],
          scoring_mode: 'learning',
          points_per_question: 0,
        },
      ],
    },
  ],
});

test('course catalog validates Chapter 2/3 and keeps deterministic group order', () => {
  assert.deepEqual(catalogModel.groups(catalog).map((item) => item.key), ['chapter2', 'chapter3']);
  assert.equal(catalogModel.getCourse(catalog, 'chapter2').scoring_mode, 'learning');
  assert.equal(catalogModel.getCourse(catalog, 'chapter3').scoring_mode, 'learning');
});

test('normal course start payload cannot carry pool/ranked/scoring overrides', () => {
  const course = catalogModel.getCourse(catalog, 'chapter3');
  assert.deepEqual(catalogModel.buildCourseStartPayload(course, 'relaxed'), {
    course_key: 'chapter3',
    mode: 'relaxed',
    count: 10,
    challenge: false,
  });
  assert.throws(() => catalogModel.buildCourseStartPayload(course, 'speed'), /Режим недоступен/);
});

test('catalog rejects duplicate course keys and unknown modes', () => {
  const duplicate = JSON.parse(JSON.stringify(catalog));
  duplicate.groups[1].courses[0].key = 'chapter2';
  assert.throws(() => catalogModel.validateCatalog(duplicate), /Дублирующийся курс/);

  const unknownMode = JSON.parse(JSON.stringify(catalog));
  unknownMode.groups[0].courses[0].modes = ['ranked'];
  assert.throws(() => catalogModel.validateCatalog(unknownMode), /Неизвестный режим/);
});

test('production Mini App loads one catalog model and no chapter-specific script truth', () => {
  const html = fs.readFileSync(indexPath, 'utf8');
  const app = fs.readFileSync(appPath, 'utf8');
  assert.ok(html.includes('<div id="courseMenu"'));
  assert.ok(html.includes('<script src="course_catalog.js"></script>'));
  assert.equal(html.includes('chapter2.js'), false);
  assert.equal(html.includes('chapter3.js'), false);
  assert.ok(app.includes("api('/api/catalog')"));
  assert.ok(app.includes('buildCourseStartPayload'));
  assert.equal(app.includes("ranked: true"), false);
});
