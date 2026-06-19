import { IMessageBus, IncomingMessage, deadLetterAddress } from '../abstractions';
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

const MAX_DELIVERIES = 6; // initial attempt + 5 retries, then dead-letter
const RETRY_DELAY_MS = 250; // backoff parked in the retry queue between attempts
const GOOD_COUNT = 8; // healthy messages that must flow past the parked poison

/**
 * S11 — Non-blocking retry queue → dead-letter. Unlike S8 (which requeues in
 * place and can head-of-line-block), a failing message is parked in a dedicated
 * retry queue and redelivered after a short delay, up to 5 retries, then
 * dead-lettered. Meanwhile a batch of healthy messages must drain immediately —
 * proving the poison message does not block the main queue.
 *
 * The same observable behaviour is wired natively per broker: RabbitMQ uses a
 * DLX + TTL retry queue that bounces back to the main queue; Artemis uses
 * `redelivery-delay` + `max-delivery-attempts` (configured for `mbc.s11.#` in
 * broker.xml); the in-memory reference parks the message on a timer.
 */
export const retryQueue: BusScenario = {
  name: 'S11 retry queue',
  description: `${GOOD_COUNT} good messages flow while a poison message is parked in a retry queue and dead-lettered after ${MAX_DELIVERIES - 1} retries.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsDeadLetter) {
      return unsupported(this.name, 'no dead-letter support on this broker', t0);
    }
    const topic = `mbc.s11.${nonce()}`; // the prefix selects the Artemis retry policy
    const good: string[] = [];
    let attempts = 0;
    let lastCount: number | undefined;
    let goodDrainedAt: number | undefined;
    let dlqAt: number | undefined;

    const main = await AckCollector.start(bus, topic, {
      subscriberId: `worker-${nonce()}`,
      deadLetter: true,
      maxDeliveries: MAX_DELIVERIES,
      retryDelayMs: RETRY_DELAY_MS,
      autoAck: false,
      onMessage: async (m: IncomingMessage) => {
        if (m.body.startsWith('poison')) {
          attempts += 1;
          lastCount = m.deliveryCount;
          await m.nack(true); // always fail → park in retry queue, eventually DLQ
        } else {
          good.push(m.body);
          if (good.length === GOOD_COUNT) goodDrainedAt = performance.now();
          await m.ack();
        }
      },
    });
    const dlq = await AckCollector.start(bus, deadLetterAddress(topic), {
      kind: 'fanout',
      subscriberId: `dlq-${nonce()}`,
      onMessage: async (m: IncomingMessage) => {
        dlqAt ??= performance.now();
        await m.ack();
      },
    });
    try {
      // Interleave the poison among the healthy messages: if it blocked the
      // queue, the later good messages would be stuck behind its retries.
      for (let i = 0; i < 3; i++) await bus.publish(topic, `job-${i}`);
      await bus.publish(topic, 'poison-1');
      for (let i = 3; i < GOOD_COUNT; i++) await bus.publish(topic, `job-${i}`);

      const timeout = MAX_DELIVERIES * RETRY_DELAY_MS + 4000;
      const done = await waitUntil(
        () => good.length >= GOOD_COUNT && dlq.count() >= 1,
        timeout,
      );
      if (!done) {
        return fail(
          this.name,
          `pipeline did not drain (good=${good.length}/${GOOD_COUNT}, dlq=${dlq.count()}, attempts=${attempts})`,
          t0,
        );
      }
      const unique = new Set(good);
      if (unique.size !== GOOD_COUNT) {
        return fail(
          this.name,
          `good messages not delivered exactly once (${unique.size} unique of ${good.length})`,
          t0,
        );
      }
      // Bounded retries, then dead-lettered — not an infinite loop. The exact
      // count can vary by ±1 across brokers; report the actual for comparison.
      if (attempts < 2 || attempts > MAX_DELIVERIES + 1) {
        return fail(
          this.name,
          `retry attempts out of range: ${attempts} (limit ${MAX_DELIVERIES})`,
          t0,
        );
      }
      // Non-blocking proof: the healthy batch must finish before the poison
      // exhausts its retries and lands in the DLQ.
      if (goodDrainedAt === undefined || dlqAt === undefined || goodDrainedAt >= dlqAt) {
        return fail(
          this.name,
          `main queue was blocked by the poison message (good done ${fmt(goodDrainedAt, t0)}, dlq ${fmt(dlqAt, t0)})`,
          t0,
        );
      }
      const goodMs = Math.round(goodDrainedAt - t0);
      const dlqMs = Math.round(dlqAt - t0);
      const gap = Math.round(dlqAt - goodDrainedAt);
      const countNote =
        bus.busCapabilities.reportsDeliveryCount && lastCount !== undefined
          ? ` (final deliveryCount=${lastCount})`
          : '';
      return pass(
        this.name,
        `${GOOD_COUNT} ok in ${goodMs}ms; poison→DLQ after ${attempts} attempts${countNote} in ${dlqMs}ms; main unblocked (ok done ${gap}ms before DLQ)`,
        t0,
      );
    } finally {
      await main.stop();
      await dlq.stop();
    }
  },
};

function fmt(at: number | undefined, t0: number): string {
  return at === undefined ? 'n/a' : `${Math.round(at - t0)}ms`;
}
