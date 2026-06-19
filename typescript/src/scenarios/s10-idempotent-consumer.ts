import { IMessageBus } from '../abstractions';
import { idempotentHandler, InMemoryIdempotencyStore } from '../idempotency';
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
 * S10 — At-least-once → idempotent consumer. Three observable sub-proofs show
 * the full problem/solution/resilience cycle:
 *
 *   (a) Without idempotency: a message is delivered twice because the first
 *       consumer crashes before acking. The business-logic side-effect (a
 *       counter) runs twice — duplicates are real, not theoretical.
 *
 *   (b) With an idempotent consumer: the same crash+redelivery happens, but the
 *       `InMemoryIdempotencyStore` suppresses the duplicate. The counter stays
 *       at 1 regardless of how many times the broker delivers the message.
 *
 *   (c) Store-down resilience (Redis-outage simulation): the idempotency store
 *       always throws. The consumer degrades gracefully — it processes the
 *       message anyway (fail-open) rather than blocking. This proves that a
 *       Redis outage cannot halt message processing; it merely removes the
 *       deduplication guarantee temporarily.
 *
 * Idempotency key = message body. In production, use a publisher-assigned
 * correlation ID in a header so the key survives broker serialisation round-trips.
 */
export const idempotentConsumer: BusScenario = {
  name: 'S10 idempotent consumer',
  description:
    'at-least-once causes duplicates; idempotency store deduplicates; store-down degrades gracefully.',

  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsManualAck) {
      return unsupported(this.name, 'no manual ack on this broker', t0);
    }
    const notes: string[] = [];

    // -----------------------------------------------------------------------
    // (a) Without idempotency — duplicate side-effect is observable.
    // -----------------------------------------------------------------------
    {
      const topic = `mbc.s10a.${nonce()}`;
      const queueId = `idm-a-${nonce()}`;
      let processCount = 0;

      // First consumer: counts the side-effect but drops without acking.
      const first = await AckCollector.start(bus, topic, {
        subscriberId: queueId,
        autoAck: false,
        onMessage: async () => {
          processCount += 1;
          // Never ack → simulates a crash; the broker redelivers.
        },
      });
      await bus.publish(topic, `order-a-${nonce()}`);
      await waitUntil(() => first.count() >= 1, 4000);
      await first.stop();

      // Fresh consumer on the same queue picks up the redelivered message.
      const second = await AckCollector.start(bus, topic, {
        subscriberId: queueId,
        autoAck: false,
        onMessage: async (m) => {
          processCount += 1;
          await m.ack();
        },
      });
      const redelivered = await waitUntil(() => second.count() >= 1, 5000);
      await second.stop();

      if (!redelivered) return fail(this.name, '(a) redelivery did not happen', t0);
      if (processCount !== 2) {
        return fail(this.name, `(a) expected 2 processings, got ${processCount}`, t0);
      }
      notes.push(`no-store→processed×${processCount}`);
    }

    // -----------------------------------------------------------------------
    // (b) With idempotency — duplicate delivery, single processing.
    // -----------------------------------------------------------------------
    {
      const topic = `mbc.s10b.${nonce()}`;
      const queueId = `idm-b-${nonce()}`;
      const msgBody = `order-b-${nonce()}`;
      let processCount = 0;
      const store = new InMemoryIdempotencyStore();

      // First consumer: processes once via idempotent handler, then drops.
      const firstHandler = idempotentHandler(store, async (_m) => {
        processCount += 1;
        // Don't ack — simulate crash so the broker redelivers.
      });
      const first = await AckCollector.start(bus, topic, {
        subscriberId: queueId,
        autoAck: false,
        onMessage: firstHandler,
      });
      await bus.publish(topic, msgBody);
      await waitUntil(() => first.count() >= 1, 4000);
      await first.stop();

      // Fresh consumer with the SAME store sees the key already recorded.
      const secondHandler = idempotentHandler(store, async (m) => {
        processCount += 1;
        await m.ack();
      });
      const second = await AckCollector.start(bus, topic, {
        subscriberId: queueId,
        autoAck: false,
        onMessage: secondHandler,
      });
      const redelivered = await waitUntil(() => second.count() >= 1, 5000);
      await second.stop();

      if (!redelivered) return fail(this.name, '(b) redelivery did not happen', t0);
      if (processCount !== 1) {
        return fail(this.name, `(b) expected 1 processing, got ${processCount}`, t0);
      }
      notes.push('with-store→processed×1');
    }

    // -----------------------------------------------------------------------
    // (c) Store throws (Redis-down simulation) → fail-open, still processes.
    // -----------------------------------------------------------------------
    {
      const topic = `mbc.s10c.${nonce()}`;
      const queueId = `idm-c-${nonce()}`;
      let processCount = 0;
      let caughtError: unknown;

      const brokenStore = {
        tryMarkSeen: async (_key: string): Promise<boolean> => {
          throw new Error('Redis connection refused');
        },
      };

      const handler = idempotentHandler(brokenStore, async (m) => {
        processCount += 1;
        await m.ack();
      });

      const sub = await AckCollector.start(bus, topic, {
        subscriberId: queueId,
        autoAck: false,
        onMessage: async (m) => {
          try {
            await handler(m);
          } catch (e) {
            caughtError = e;
            await m.ack();
          }
        },
      });
      try {
        await bus.publish(topic, `order-c-${nonce()}`);
        const ok = await waitUntil(() => sub.count() >= 1, 4000);
        if (!ok) return fail(this.name, '(c) message was not delivered', t0);
        await delay(200); // brief wait to confirm no second delivery
        if (caughtError !== undefined) {
          return fail(this.name, `(c) store error leaked to caller: ${caughtError}`, t0);
        }
        if (processCount !== 1) {
          return fail(this.name, `(c) expected fail-open processing, got ${processCount}`, t0);
        }
        notes.push('store-down→fail-open,processed×1');
      } finally {
        await sub.stop();
      }
    }

    return pass(this.name, notes.join('; '), t0);
  },
};
