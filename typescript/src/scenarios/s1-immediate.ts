import { IMessageScheduler } from '../abstractions';
import {
  MessageCollector,
  Scenario,
  ScenarioResult,
  fail,
  nonce,
  pass,
  waitUntil,
} from './scenario';

/** S1 — baseline: a message sent now is received now. Proves connectivity,
 *  publish, and consume before we layer scheduling on top. */
export const immediateDelivery: Scenario = {
  name: 'S1 immediate send/receive',
  description: 'A message published for immediate delivery is consumed.',
  async run(s: IMessageScheduler): Promise<ScenarioResult> {
    const t0 = performance.now();
    const dest = `mbc.s1.${nonce()}`;
    const token = `now-${nonce()}`;
    const collector = await MessageCollector.start(s, dest);
    try {
      await s.sendNow(dest, token);
      const got = await waitUntil(
        () => collector.bodies().includes(token),
        5000,
      );
      return got
        ? pass(this.name, 'delivered immediately', t0)
        : fail(this.name, 'message never arrived within 5s', t0);
    } finally {
      await collector.stop();
    }
  },
};
