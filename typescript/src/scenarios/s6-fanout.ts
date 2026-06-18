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

const SUBSCRIBERS = 3;

/**
 * S6 — Fanout multicast. One publish is delivered, in full, to N independent
 * subscriber queues. This is the cleanest cross-broker capability: Artemis
 * multicast addresses and RabbitMQ fanout exchanges both express it natively.
 */
export const fanout: BusScenario = {
  name: 'S6 fanout multicast',
  description: `One publish reaches all ${SUBSCRIBERS} independent subscribers.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsFanout) {
      return unsupported(this.name, 'no fanout on this broker', t0);
    }
    const topic = `mbc.s6.${nonce()}`;
    const subs = await Promise.all(
      Array.from({ length: SUBSCRIBERS }, (_, i) =>
        AckCollector.start(bus, topic, {
          kind: 'fanout',
          subscriberId: `s${i}-${nonce()}`,
        }),
      ),
    );
    try {
      await bus.publish(topic, 'broadcast-1');
      const ok = await waitUntil(
        () => subs.every((s) => s.count() >= 1),
        5000,
      );
      const counts = subs.map((s) => s.count());
      if (!ok || counts.some((n) => n !== 1)) {
        return fail(this.name, `subscriber counts were [${counts.join(',')}]`, t0);
      }
      return pass(
        this.name,
        `1 publish → ${SUBSCRIBERS} subscribers each received it`,
        t0,
      );
    } finally {
      await Promise.all(subs.map((s) => s.stop()));
    }
  },
};
