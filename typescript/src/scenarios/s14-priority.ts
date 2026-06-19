import { AckHandler, IMessageBus, IncomingMessage } from '../abstractions';
import {
  BusScenario,
  ScenarioResult,
  delay,
  fail,
  nonce,
  pass,
  waitUntil,
} from './scenario';

const LOW_COUNT = 8; // backlog of low-priority messages
const HANDLER_DELAY_MS = 60; // slow ack so a backlog actually forms

/**
 * S14 — Message priority. A high-priority message must overtake a backlog of
 * low-priority ones already waiting on the queue. A single slow consumer lets a
 * backlog build; we publish several `priority:1` messages, then one
 * `priority:9`, and assert the high-priority body is delivered near the FRONT
 * (within the first two received), not at the back.
 *
 * Artemis honours priority natively. RabbitMQ needs the queue declared with
 * `x-max-priority` — surfaced here via the `priorityQueue` subscribe option. The
 * in-memory reference selects the highest-priority pending message (FIFO on a
 * tie), which leaves un-prioritised scenarios unaffected.
 */
export const priority: BusScenario = {
  name: 'S14 priority',
  description: `A priority:9 message overtakes a backlog of ${LOW_COUNT} priority:1 messages.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    const topic = `mbc.s14.${nonce()}`;
    const received: string[] = [];
    let settled = 0;

    // A deliberately slow handler: each ack takes HANDLER_DELAY_MS, so the
    // publishes below pile up as a real backlog the broker must order. The ack is
    // guarded because the scenario may unsubscribe (closing the channel) while a
    // late handler is still draining — settling a closed channel is harmless here.
    const handler: AckHandler = async (m: IncomingMessage) => {
      received.push(m.body);
      await delay(HANDLER_DELAY_MS);
      try {
        await m.ack();
      } catch {
        /* channel may already be closing on the way out */
      }
      settled += 1;
    };
    const sub = await bus.subscribe(topic, handler, {
      subscriberId: `prio-${nonce()}`,
      priorityQueue: true, // RabbitMQ: declare x-max-priority; native elsewhere
    });
    try {
      // Fill the backlog first, then drop in the high-priority message.
      for (let i = 0; i < LOW_COUNT; i++) {
        await bus.publish(topic, `low-${i}`, undefined, { priority: 1 });
      }
      await bus.publish(topic, 'HIGH', undefined, { priority: 9 });

      const ok = await waitUntil(() => received.length >= LOW_COUNT + 1, 8000);
      if (!ok) {
        return fail(
          this.name,
          `received ${received.length}/${LOW_COUNT + 1}`,
          t0,
        );
      }
      // Let the in-flight handlers settle before we close the channel below.
      await waitUntil(() => settled >= LOW_COUNT + 1, 2000);

      const highIndex = received.indexOf('HIGH');
      // Robust assertion: the high-priority message arrives near the front — not
      // an exact slot (the very first low message is usually already in-flight
      // before HIGH is published, so index 0 or 1 are both correct).
      if (highIndex < 0 || highIndex > 1) {
        return fail(
          this.name,
          `high-priority message arrived at index ${highIndex} (order: ${received.join(',')})`,
          t0,
        );
      }
      return pass(
        this.name,
        `priority:9 overtook the backlog (arrived at index ${highIndex})`,
        t0,
      );
    } finally {
      await sub.unsubscribe();
    }
  },
};
