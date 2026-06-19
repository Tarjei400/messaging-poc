import { AckHandler, IMessageBus, IncomingMessage } from '../abstractions';
import {
  BusScenario,
  ScenarioResult,
  fail,
  nonce,
  pass,
  unsupported,
  waitUntil,
} from './scenario';

const COUNT = 6; // messages; each throws once, then succeeds on redelivery

/**
 * S20 — Handler fault isolation. A message handler that THROWS (a random or
 * transient application error: a null dereference, a downstream 500, a bad parse)
 * must NOT crash the consumer or take the process down via an unhandled rejection.
 * The broker adapter has to contain the exception, treat the delivery as failed,
 * and redeliver — so a later attempt succeeds and other messages keep flowing.
 *
 * Each message here throws on its FIRST delivery and is acked on its second. The
 * scenario passes only if all COUNT messages are eventually processed — proving
 * the consumer survived COUNT exceptions and every failure was retried, not fatal.
 *
 * The in-memory reference treats "handler threw" as a crashed consumer and nacks
 * for redelivery; the Artemis/RabbitMQ/Kafka adapters wrap the handler the same
 * way (see each adapter's deliver/onMessage path).
 */
export const handlerFaultIsolation: BusScenario = {
  name: 'S20 handler fault isolation',
  description: `${COUNT} messages each throw once inside the handler, then succeed on redelivery — the consumer never crashes.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsManualAck) {
      return unsupported(
        this.name,
        'needs manual ack/redelivery to recover from a thrown handler',
        t0,
      );
    }
    const topic = `mbc.s20.${nonce()}`;
    const subscriberId = `fault-${nonce()}`;
    const attempts = new Map<string, number>();
    const acked = new Set<string>();
    let thrown = 0;

    const handler: AckHandler = async (m: IncomingMessage) => {
      const n = (attempts.get(m.body) ?? 0) + 1;
      attempts.set(m.body, n);
      if (n === 1) {
        // Simulate a random/transient failure: throw WITHOUT settling. The
        // adapter must catch this and redeliver rather than crash the consumer.
        thrown += 1;
        throw new Error(`simulated handler failure for ${m.body}`);
      }
      await m.ack();
      acked.add(m.body);
    };

    const sub = await bus.subscribe(topic, handler, { subscriberId });
    try {
      for (let i = 0; i < COUNT; i++) {
        await bus.publish(topic, `msg-${i}`);
      }
      const ok = await waitUntil(() => acked.size >= COUNT, 30000);
      return ok
        ? pass(
            this.name,
            `consumer survived ${thrown} handler exceptions; all ${COUNT} messages processed on retry`,
            t0,
          )
        : fail(
            this.name,
            `only ${acked.size}/${COUNT} recovered after a thrown handler (exceptions=${thrown})`,
            t0,
          );
    } finally {
      await sub.unsubscribe();
    }
  },
};
