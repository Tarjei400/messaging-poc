import { IMessageBus, IncomingMessage } from '../abstractions';
import {
  AckCollector,
  BusScenario,
  ScenarioResult,
  delay,
  fail,
  nonce,
  pass,
  unsupported,
  waitUntil,
} from './scenario';

/**
 * S7 — Explicit acknowledgement. Three observable behaviors prove the consumer
 * (not the broker) controls settlement:
 *   (a) ack removes the message — it is not redelivered;
 *   (b) nack(requeue) redelivers it — and the delivery count climbs;
 *   (c) a consumer that drops without acking causes redelivery to a fresh one.
 * (c) is also the "consumer crash → redelivery" fault-tolerance story.
 */
export const explicitAck: BusScenario = {
  name: 'S7 explicit ack',
  description: 'ack removes; nack requeues; a crashed consumer triggers redelivery.',
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsManualAck) {
      return unsupported(this.name, 'no manual ack on this broker', t0);
    }
    const notes: string[] = [];

    // (a) ack removes — message delivered exactly once.
    {
      const topic = `mbc.s7a.${nonce()}`;
      const c = await AckCollector.start(bus, topic, {
        subscriberId: `ack-${nonce()}`,
      });
      try {
        await bus.publish(topic, 'ack-1');
        await waitUntil(() => c.count() >= 1, 4000);
        await delay(500); // give any erroneous redelivery a chance to show up
        if (c.count() !== 1) {
          return fail(this.name, `(a) acked msg delivered ${c.count()}×`, t0);
        }
        notes.push('ack→once');
      } finally {
        await c.stop();
      }
    }

    // (b) nack(requeue) redelivers.
    {
      const topic = `mbc.s7b.${nonce()}`;
      let attempts = 0;
      let secondCount: number | undefined;
      const c = await AckCollector.start(bus, topic, {
        subscriberId: `nack-${nonce()}`,
        autoAck: false,
        onMessage: async (m: IncomingMessage) => {
          attempts += 1;
          if (attempts === 1) {
            await m.nack(true); // requeue
          } else {
            secondCount = m.deliveryCount;
            await m.ack();
          }
        },
      });
      try {
        await bus.publish(topic, 'nack-1');
        const ok = await waitUntil(() => attempts >= 2, 5000);
        if (!ok) return fail(this.name, '(b) nacked msg was not redelivered', t0);
        if (bus.busCapabilities.reportsDeliveryCount && secondCount !== 2) {
          return fail(
            this.name,
            `(b) expected deliveryCount 2, got ${secondCount}`,
            t0,
          );
        }
        notes.push(
          bus.busCapabilities.reportsDeliveryCount
            ? 'nack→redelivered (count=2)'
            : 'nack→redelivered (count n/a)',
        );
      } finally {
        await c.stop();
      }
    }

    // (c) crashed consumer (drops without acking) → redelivery to a fresh one.
    {
      const topic = `mbc.s7c.${nonce()}`;
      const queueId = `crash-${nonce()}`; // the fresh consumer reuses this queue
      const crashed = await AckCollector.start(bus, topic, {
        subscriberId: queueId,
        autoAck: false,
        onMessage: async () => {
          /* receive but never settle — simulate a crash */
        },
      });
      await bus.publish(topic, 'crash-1');
      await waitUntil(() => crashed.count() >= 1, 4000);
      await crashed.stop(); // drop the consumer with the message un-acked

      // A fresh consumer on the SAME queue must get the un-acked message back.
      const fresh = await AckCollector.start(bus, topic, { subscriberId: queueId });
      try {
        const got = await waitUntil(() => fresh.count() >= 1, 5000);
        if (!got) {
          return fail(this.name, '(c) crashed consumer msg was lost', t0);
        }
        notes.push('crash→redelivered');
      } finally {
        await fresh.stop();
      }
    }

    return pass(this.name, notes.join('; '), t0);
  },
};
