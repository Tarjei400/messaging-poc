import { IMessageBus, expiryAddress } from '../abstractions';
import {
  AckCollector,
  BusScenario,
  ScenarioResult,
  fail,
  nonce,
  pass,
  waitUntil,
} from './scenario';

const TTL_MS = 600; // short enough to expire well within the wait window

/**
 * S16 — TTL → expiry. A message published with a short `ttlMs` to a queue that
 * has no active consumer must expire un-consumed and land on the expiry address
 * (distinct from the dead-letter address, which is for poison messages). A
 * subscriber on `expiryAddress(topic)` observes it.
 *
 * Pattern: declare the main queue by briefly subscribing then dropping the
 * consumer (the durable queue persists and holds the message), subscribe to the
 * expiry address, then publish with a TTL and assert arrival on expiry.
 *
 * Artemis: broker.xml routes `mbc.s16.#` to the multicast address `mbc.EXPIRY`;
 * the adapter maps the `.expiry` suffix onto it. RabbitMQ: the per-subscriber
 * queue carries `x-message-ttl` + a dead-letter-exchange wired to an expiry
 * fanout the `.expiry` subscriber binds to. In-memory: a per-message timer drops
 * the un-consumed message to the expiry address.
 */
export const ttlExpiry: BusScenario = {
  name: 'S16 TTL → expiry',
  description: `An unconsumed message with ttl=${TTL_MS}ms lands on the expiry address.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    const topic = `mbc.s16.${nonce()}`;
    const subscriberId = `expiring-${nonce()}`;

    // Declare the main per-subscriber queue (so it exists with its TTL/expiry
    // wiring), then drop the consumer — the durable queue stays and holds the
    // message with no one to consume it, so it can expire.
    const warmup = await bus.subscribe(topic, async (m) => m.ack(), {
      subscriberId,
      ttlMs: TTL_MS,
    });
    await warmup.unsubscribe();

    // Watch the expiry address.
    const expiry = await AckCollector.start(bus, expiryAddress(topic), {
      kind: 'fanout',
      subscriberId: `expiry-watch-${nonce()}`,
    });
    try {
      // Publish with a short TTL; with no active consumer it must expire.
      await bus.publish(topic, 'perishable', undefined, { ttlMs: TTL_MS });

      const landed = await waitUntil(() => expiry.count() >= 1, 8000);
      if (!landed) {
        return fail(this.name, 'message never reached the expiry address', t0);
      }
      if (expiry.bodies()[0] !== 'perishable') {
        return fail(
          this.name,
          `unexpected expiry body "${expiry.bodies()[0]}"`,
          t0,
        );
      }
      return pass(this.name, 'expired message landed on the expiry address', t0);
    } finally {
      await expiry.stop();
    }
  },
};
