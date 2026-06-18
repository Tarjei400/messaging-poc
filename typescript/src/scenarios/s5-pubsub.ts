import { IMessageBus } from '../abstractions';
import {
  AckCollector,
  BusScenario,
  ScenarioResult,
  fail,
  nonce,
  pass,
  unsupported,
  waitUntil,
} from './scenario';

/**
 * S5 — Pub/Sub with topic routing. Two independent subscribers bind to the same
 * topic with different routing-key filters; a publish reaches exactly the
 * subscribers whose filter matches. Proves selective fan-out, not just delivery.
 */
export const pubSub: BusScenario = {
  name: 'S5 pub/sub (topic)',
  description: 'Two filtered subscribers each receive only their matching events.',
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsTopic) {
      return unsupported(this.name, 'no topic routing on this broker', t0);
    }
    const topic = `mbc.s5.${nonce()}`;
    const created = await AckCollector.start(bus, topic, {
      kind: 'topic',
      routingKey: 'order.created',
      subscriberId: `created-${nonce()}`,
    });
    const all = await AckCollector.start(bus, topic, {
      kind: 'topic',
      routingKey: 'order.#',
      subscriberId: `all-${nonce()}`,
    });
    try {
      await bus.publish(topic, 'created-1', 'order.created');
      await bus.publish(topic, 'shipped-1', 'order.shipped');

      const ok = await waitUntil(
        () => created.count() >= 1 && all.count() >= 2,
        5000,
      );
      if (!ok) {
        return fail(
          this.name,
          `timed out (created=${created.count()}, all=${all.count()})`,
          t0,
        );
      }
      if (created.count() !== 1 || created.bodies()[0] !== 'created-1') {
        return fail(this.name, 'filtered subscriber saw the wrong events', t0);
      }
      return pass(
        this.name,
        `order.created→1 sub, order.#→both (${all.count()})`,
        t0,
      );
    } finally {
      await created.stop();
      await all.stop();
    }
  },
};
