import { IMessageScheduler } from '../abstractions';
import {
  MessageCollector,
  Scenario,
  ScenarioResult,
  delay,
  fail,
  nonce,
  pass,
  waitUntil,
} from './scenario';

const DELAY_MS = 3000;

/** S2 — the core requirement (R1): a scheduled message must NOT arrive early,
 *  and MUST arrive at (or just after) its delivery time. */
export const scheduledDelivery: Scenario = {
  name: 'S2 scheduled delivery',
  description: `A message scheduled +${DELAY_MS}ms is withheld, then delivered.`,
  async run(s: IMessageScheduler): Promise<ScenarioResult> {
    const t0 = performance.now();
    const dest = `mbc.s2.${nonce()}`;
    const token = `sched-${nonce()}`;
    const collector = await MessageCollector.start(s, dest);
    try {
      const deliverAt = new Date(Date.now() + DELAY_MS);
      await s.schedule(dest, token, deliverAt);

      // Must still be withheld well before the deadline.
      await delay(DELAY_MS * 0.4);
      if (collector.bodies().includes(token)) {
        return fail(this.name, 'message delivered early (not withheld)', t0);
      }

      // Must arrive within a tolerance window after the deadline.
      const arrived = await waitUntil(
        () => collector.bodies().includes(token),
        DELAY_MS + 4000,
      );
      return arrived
        ? pass(this.name, `withheld then delivered after ~${DELAY_MS}ms`, t0)
        : fail(this.name, 'scheduled message never arrived', t0);
    } finally {
      await collector.stop();
    }
  },
};
