const assert = require('node:assert/strict');
const test = require('node:test');
const { QuizFlowGuard } = require('../../miniapp/flow_guard.js');

class FakeClock {
  constructor() {
    this.nextId = 1;
    this.callbacks = new Map();
  }

  setTimeout(callback) {
    const id = this.nextId;
    this.nextId += 1;
    this.callbacks.set(id, callback);
    return id;
  }

  clearTimeout(id) {
    this.callbacks.delete(id);
  }

  run(id) {
    const callback = this.callbacks.get(id);
    this.callbacks.delete(id);
    if (callback) callback();
  }
}

test('starting a new flow invalidates the previous generation', () => {
  const guard = new QuizFlowGuard(new FakeClock());
  const first = guard.begin();
  const second = guard.begin();

  assert.equal(guard.isCurrent(first), false);
  assert.equal(guard.isCurrent(second), true);
});

test('invalidating a flow cancels its delayed transition', () => {
  const clock = new FakeClock();
  const guard = new QuizFlowGuard(clock);
  const epoch = guard.begin();
  let transitions = 0;

  const timerId = guard.schedule(epoch, () => { transitions += 1; }, 900);
  guard.invalidate();
  clock.run(timerId);

  assert.equal(transitions, 0);
});

test('only the latest scheduled transition can fire', () => {
  const clock = new FakeClock();
  const guard = new QuizFlowGuard(clock);
  const epoch = guard.begin();
  const fired = [];

  const firstTimer = guard.schedule(epoch, () => fired.push('first'), 900);
  const secondTimer = guard.schedule(epoch, () => fired.push('second'), 1800);
  clock.run(firstTimer);
  clock.run(secondTimer);

  assert.deepEqual(fired, ['second']);
});
