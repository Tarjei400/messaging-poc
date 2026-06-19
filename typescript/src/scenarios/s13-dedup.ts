import { IMessageBus } from '../abstractions';
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
 * S13 — Broker-native producer deduplication. The producer stamps the SAME
 * `dedupId` on two publishes; the broker drops the repeat within its dedup
 * window, so a single subscriber sees exactly one delivery. This is the
 * broker-side counterpart of the app-level idempotent consumer (S10): the same
 * "exactly-once effect" goal, but enforced by the infrastructure rather than the
 * application.
 *
 * Artemis honours `_AMQ_DUPL_ID` (duplicate detection is enabled on the AMQP
 * acceptor in broker.xml) → ✓. RabbitMQ has no native producer dedup, so it
 * declares `supportsDedup=false` and this scenario reports ⊘ — the honest gap
 * that motivates S10.
 */
export const brokerNativeDedup: BusScenario = {
  name: 'S13 broker-native dedup',
  description: 'Publishing the same dedupId twice is delivered exactly once.',
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsDedup) {
      return unsupported(this.name, 'no broker-native producer dedup', t0);
    }
    const topic = `mbc.s13.${nonce()}`;
    const dedupId = `dup-${nonce()}`;

    const sub = await AckCollector.start(bus, topic, {
      subscriberId: `dedup-${nonce()}`,
    });
    try {
      // Same dedupId, two publishes: the broker must collapse them to one.
      await bus.publish(topic, 'order-42', undefined, { dedupId });
      await bus.publish(topic, 'order-42', undefined, { dedupId });

      // Wait for the first to arrive, then give the (suppressed) second ample
      // time to show up if dedup were not working.
      const arrived = await waitUntil(() => sub.count() >= 1, 6000);
      if (!arrived) {
        return fail(this.name, 'no delivery at all', t0);
      }
      await delay(800); // window for a (wrongly) un-deduped second copy

      if (sub.count() !== 1) {
        return fail(this.name, `expected 1 delivery, got ${sub.count()}`, t0);
      }
      return pass(this.name, 'duplicate dropped by the broker (1 delivery)', t0);
    } finally {
      await sub.stop();
    }
  },
};
