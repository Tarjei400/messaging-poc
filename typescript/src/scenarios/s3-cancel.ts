import { IMessageScheduler, NotSupportedError } from '../abstractions';
import {
  MessageCollector,
  Scenario,
  ScenarioResult,
  delay,
  fail,
  nonce,
  pass,
  unsupported,
} from './scenario';

const DELAY_MS = 3000;

/** S3 — the key discriminator from the research: cancel a scheduled message
 *  before it fires and prove it never arrives. Adapters that cannot do this
 *  surface a NotSupportedError, which we record as an honest ⊘ rather than ✗. */
export const cancelScheduled: Scenario = {
  name: 'S3 cancel scheduled',
  description: 'A scheduled message is cancelled and never delivered.',
  async run(s: IMessageScheduler): Promise<ScenarioResult> {
    const t0 = performance.now();
    const dest = `mbc.s3.${nonce()}`;
    const token = `cancel-${nonce()}`;
    const collector = await MessageCollector.start(s, dest);
    try {
      const handle = await s.schedule(
        dest,
        token,
        new Date(Date.now() + DELAY_MS),
      );

      try {
        await s.cancel(handle);
      } catch (err) {
        if (err instanceof NotSupportedError) {
          return unsupported(this.name, 'no cancel API on this broker', t0);
        }
        throw err;
      }

      // Wait past the original delivery time; the message must never appear.
      await delay(DELAY_MS + 2000);
      return collector.bodies().includes(token)
        ? fail(this.name, 'cancel returned but message still delivered', t0)
        : pass(this.name, 'scheduled message successfully cancelled', t0);
    } finally {
      await collector.stop();
    }
  },
};
