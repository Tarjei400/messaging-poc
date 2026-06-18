import { IMessageScheduler, NotSupportedError } from '../abstractions';
import {
  Scenario,
  ScenarioResult,
  fail,
  nonce,
  pass,
  unsupported,
} from './scenario';

/** S4 — observability: schedule two messages far in the future and confirm the
 *  broker can report them as pending (Artemis QueueControl.listScheduledMessages).
 *  Brokers without an inspection API report ⊘. */
export const listPending: Scenario = {
  name: 'S4 list pending',
  description: 'Two far-future messages are scheduled and counted as pending.',
  async run(s: IMessageScheduler): Promise<ScenarioResult> {
    const t0 = performance.now();
    const dest = `mbc.s4.${nonce()}`;
    const farFuture = () => new Date(Date.now() + 60_000);
    try {
      await s.schedule(dest, `a-${nonce()}`, farFuture());
      await s.schedule(dest, `b-${nonce()}`, farFuture());

      let pending;
      try {
        pending = await s.listScheduled(dest);
      } catch (err) {
        if (err instanceof NotSupportedError) {
          return unsupported(this.name, 'no inspection API on this broker', t0);
        }
        throw err;
      }

      return pending.length >= 2
        ? pass(this.name, `reported ${pending.length} pending`, t0)
        : fail(this.name, `expected >=2 pending, saw ${pending.length}`, t0);
    } finally {
      // best-effort cleanup so far-future messages don't accumulate
      try {
        const left = await s.listScheduled(dest);
        for (const p of left) {
          await s.cancel({ id: p.id, destination: dest, deliverAt: new Date() });
        }
      } catch {
        /* cleanup is best-effort; unsupported brokers simply skip it */
      }
    }
  },
};
