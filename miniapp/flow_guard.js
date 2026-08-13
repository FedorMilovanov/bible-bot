/* Cancel stale Mini App quiz transitions without depending on DOM state. */
(() => {
  class QuizFlowGuard {
    constructor(clock = globalThis) {
      this.clock = clock;
      this.epoch = 0;
      this.transitionTimer = null;
    }

    current() {
      return this.epoch;
    }

    begin() {
      return this.invalidate();
    }

    invalidate() {
      this.cancelTransition();
      this.epoch += 1;
      return this.epoch;
    }

    isCurrent(epoch) {
      return epoch === this.epoch;
    }

    cancelTransition() {
      if (this.transitionTimer !== null) {
        this.clock.clearTimeout(this.transitionTimer);
        this.transitionTimer = null;
      }
    }

    schedule(epoch, callback, delayMs) {
      this.cancelTransition();
      this.transitionTimer = this.clock.setTimeout(() => {
        this.transitionTimer = null;
        if (this.isCurrent(epoch)) callback();
      }, delayMs);
      return this.transitionTimer;
    }
  }

  const root = typeof window !== 'undefined' ? window : globalThis;
  root.QuizFlowGuard = QuizFlowGuard;

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { QuizFlowGuard };
  }
})();
