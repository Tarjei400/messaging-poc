import { AckHandler, IMessageBus, IncomingMessage } from '../abstractions';
import {
  BusScenario,
  ScenarioResult,
  delay,
  fail,
  nonce,
  pass,
  unsupported,
  waitUntil,
} from './scenario';

const N = 8; // messages published into the stream before the replay subscriber attaches

/**
 * S19 — Stream replay from the beginning. A brand-new subscriber replays the
 * FULL history of a topic — including messages published (and already consumed
 * by others) before it attached. This is the honest gap that separates RabbitMQ
 * (✓ streams) from Artemis (⊘): an append-only log can be re-read from offset 0,
 * a classic queue cannot.
 *
 * RabbitMQ: a stream queue (`x-queue-type=stream`) bound to the topic exchange;
 * a fresh consumer with `x-stream-offset=first` re-reads the whole log. The
 * stream must be bound BEFORE the publishes to capture them, so we establish it
 * with an initial streamReplay subscription, drain it, publish N, then attach a
 * SECOND fresh streamReplay subscriber and prove it replays all N from offset 0.
 * In-memory: a per-topic append-only log seeded into each streamReplay queue.
 * Artemis: `supportsStreamReplay=false` → ⊘ (no offset-replay of consumed
 * history).
 */
export const streamReplay: BusScenario = {
  name: 'S19 stream replay',
  description: `A fresh subscriber replays all ${N} messages from offset 0, including ones published & consumed before it attached.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsStreamReplay) {
      return unsupported(
        this.name,
        'broker cannot replay consumed history (no stream/offset support)',
        t0,
      );
    }
    const topic = `mbc.s19.${nonce()}`;

    // 1. Establish the stream so it captures publishes. On RabbitMQ this declares
    //    + binds the stream queue; the message log only captures from now on, so
    //    this MUST happen before the publishes. A first streamReplay subscriber
    //    that consumes everything it sees doubles as proof the early messages were
    //    really delivered and acked, not merely "still pending".
    const firstSeen: string[] = [];
    const firstHandler: AckHandler = async (m: IncomingMessage) => {
      firstSeen.push(m.body);
      await m.ack();
    };
    const first = await bus.subscribe(topic, firstHandler, {
      streamReplay: true,
    });
    await delay(300); // let the stream queue/binding settle before publishing

    // 2. Publish N messages into the established stream. The first subscriber
    //    consumes (and acks) them live.
    for (let i = 0; i < N; i++) {
      await bus.publish(topic, `evt-${i}`);
    }
    const firstOk = await waitUntil(() => firstSeen.length >= N, 10000);
    if (!firstOk) {
      await first.unsubscribe();
      return fail(
        this.name,
        `establishing subscriber saw ${firstSeen.length}/${N} live messages`,
        t0,
      );
    }
    // Drop the first subscriber — its messages are gone from a classic queue's
    // point of view; only an append-only log can hand them to a newcomer.
    await first.unsubscribe();
    await delay(300);

    // 3. Attach a BRAND-NEW subscriber AFTER the publishes (and after they were
    //    consumed) and assert it replays the entire history from offset 0.
    const replayed: string[] = [];
    const replayHandler: AckHandler = async (m: IncomingMessage) => {
      replayed.push(m.body);
      await m.ack();
    };
    const replay = await bus.subscribe(topic, replayHandler, {
      streamReplay: true,
    });
    try {
      const ok = await waitUntil(() => replayed.length >= N, 12000);
      if (!ok) {
        return fail(
          this.name,
          `fresh subscriber replayed ${replayed.length}/${N} from offset 0`,
          t0,
        );
      }
      // Every published message must be present in the replay.
      const got = new Set(replayed);
      for (let i = 0; i < N; i++) {
        if (!got.has(`evt-${i}`)) {
          return fail(this.name, `replay missing evt-${i}`, t0);
        }
      }
      return pass(
        this.name,
        `fresh subscriber replayed all ${N} messages from offset 0 (full history)`,
        t0,
      );
    } finally {
      await replay.unsubscribe();
    }
  },
};
