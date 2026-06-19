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

const GAP_COUNT = 5; // messages published while the subscriber is away

/**
 * S17 — Durable subscription. A named subscriber attaches, then disconnects.
 * Messages published while it is gone must be retained on its (durable, non
 * auto-delete) queue and delivered when a consumer with the SAME `subscriberId`
 * reattaches — nothing is lost across the gap.
 *
 * This is the pub/sub-with-memory story: unlike a transient consumer (whose
 * queue vanishes on disconnect), a durable subscription keeps accumulating.
 * Needs no API change — all adapters declare durable, auto-delete=false queues;
 * the in-memory reference keeps the queue + its pending messages across
 * unsubscribe and replays them to the reattached consumer.
 */
export const durableSubscription: BusScenario = {
  name: 'S17 durable subscription',
  description: `${GAP_COUNT} messages published while a durable subscriber is offline are delivered on reattach.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    const topic = `mbc.s17.${nonce()}`;
    const subscriberId = `durable-${nonce()}`;

    // 1. Attach the durable subscriber so its queue is declared and bound, then
    //    drop the consumer (the durable queue survives with no one attached).
    const warmup = await bus.subscribe(topic, async (m) => m.ack(), {
      subscriberId,
    });
    await warmup.unsubscribe();
    await delay(200); // let the unsubscribe settle on the broker

    // 2. Publish while the subscription has no live consumer.
    for (let i = 0; i < GAP_COUNT; i++) {
      await bus.publish(topic, `gap-${i}`);
    }
    await delay(200);

    // 3. Reattach with the SAME id — the retained messages must arrive.
    const received: string[] = [];
    const handler: AckHandler = async (m: IncomingMessage) => {
      received.push(m.body);
      await m.ack();
    };
    const sub = await bus.subscribe(topic, handler, { subscriberId });
    try {
      const ok = await waitUntil(() => received.length >= GAP_COUNT, 8000);
      if (!ok) {
        return fail(
          this.name,
          `received ${received.length}/${GAP_COUNT} after reattach`,
          t0,
        );
      }
      const expected = new Set(
        Array.from({ length: GAP_COUNT }, (_, i) => `gap-${i}`),
      );
      const got = new Set(received);
      for (const e of expected) {
        if (!got.has(e)) {
          return fail(this.name, `lost message ${e} across the gap`, t0);
        }
      }
      return pass(
        this.name,
        `all ${GAP_COUNT} offline messages retained and delivered on reattach`,
        t0,
      );
    } finally {
      await sub.unsubscribe();
    }
  },
};
