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

const BATCH = 5; // messages published before, then after, the failover
const TOTAL = BATCH * 2;

/**
 * S18 — Single active consumer / failover. Two consumers attach to the SAME
 * queue with `singleActiveConsumer`. Only ONE may receive while it is up; the
 * other is a hot standby. We publish a first batch (only the active consumer may
 * get it), drop the active consumer, then publish a second batch — which must be
 * picked up by the standby that has now been promoted. Order is preserved and
 * nothing is lost. This is leader/standby failover without a separate election.
 *
 * RabbitMQ: `x-single-active-consumer` queue arg. Artemis: an exclusive queue
 * (`default-exclusive-queue` in broker.xml for `mbc.s18.#`). In-memory: deliver
 * to one active consumer and promote a standby on unsubscribe.
 */
export const singleActiveConsumer: BusScenario = {
  name: 'S18 single active consumer',
  description: `${TOTAL} messages: an active consumer takes the first ${BATCH}, then on failover a standby takes over the rest (order preserved).`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsManualAck) {
      return unsupported(this.name, 'no manual ack on this broker', t0);
    }
    const topic = `mbc.s18.${nonce()}`;
    const queueId = `sac-${nonce()}`; // shared queue → one active consumer
    const byConsumer: number[][] = [[], []];

    const make = (i: number): AckHandler => async (m: IncomingMessage) => {
      byConsumer[i].push(Number(m.body.split('-')[1]));
      try {
        await m.ack();
      } catch {
        /* channel may be closing on the way out */
      }
    };
    const subA = await bus.subscribe(topic, make(0), {
      subscriberId: queueId,
      singleActiveConsumer: true,
    });
    const subB = await bus.subscribe(topic, make(1), {
      subscriberId: queueId,
      singleActiveConsumer: true,
    });
    try {
      // First batch — only the single active consumer may receive these.
      for (let i = 0; i < BATCH; i++) await bus.publish(topic, `m-${i}`);

      const firstBatchDone = await waitUntil(
        () => byConsumer[0].length + byConsumer[1].length >= BATCH,
        8000,
      );
      if (!firstBatchDone) {
        return fail(
          this.name,
          `only ${byConsumer[0].length + byConsumer[1].length}/${BATCH} of the first batch delivered`,
          t0,
        );
      }
      await delay(200); // let any stray second delivery surface

      // Exactly one consumer must have handled the whole first batch.
      const activeIdx = byConsumer[0].length >= byConsumer[1].length ? 0 : 1;
      const standbyIdx = activeIdx === 0 ? 1 : 0;
      if (byConsumer[standbyIdx].length !== 0) {
        return fail(
          this.name,
          `both consumers were active before failover (${byConsumer[0].length}/${byConsumer[1].length})`,
          t0,
        );
      }

      // Fail the active consumer over; the standby must be promoted.
      await (activeIdx === 0 ? subA : subB).unsubscribe();
      await delay(300); // give the broker time to promote the standby

      // Second batch — these can only be served by the promoted standby.
      for (let i = BATCH; i < TOTAL; i++) await bus.publish(topic, `m-${i}`);

      const ok = await waitUntil(
        () => new Set([...byConsumer[0], ...byConsumer[1]]).size >= TOTAL,
        8000,
      );
      if (!ok) {
        const seen = new Set([...byConsumer[0], ...byConsumer[1]]);
        return fail(
          this.name,
          `only ${seen.size}/${TOTAL} distinct messages delivered after failover`,
          t0,
        );
      }
      await delay(200);

      if (byConsumer[standbyIdx].length === 0) {
        return fail(this.name, 'standby never took over after failover', t0);
      }
      // Nothing lost: every sequence number present.
      const union = new Set([...byConsumer[activeIdx], ...byConsumer[standbyIdx]]);
      for (let i = 0; i < TOTAL; i++) {
        if (!union.has(i)) {
          return fail(this.name, `message m-${i} was lost across failover`, t0);
        }
      }
      // Per-consumer order: each consumer saw its messages in ascending order.
      for (const idx of [activeIdx, standbyIdx]) {
        const seq = byConsumer[idx];
        for (let k = 1; k < seq.length; k++) {
          if (seq[k] <= seq[k - 1]) {
            return fail(
              this.name,
              `consumer ${idx} out of order: ${seq.join(',')}`,
              t0,
            );
          }
        }
      }
      // The standby must have served the post-failover batch.
      const standbyMax = Math.max(...byConsumer[standbyIdx]);
      if (standbyMax < BATCH) {
        return fail(
          this.name,
          `standby did not take over the post-failover batch (got ${byConsumer[standbyIdx].join(',')})`,
          t0,
        );
      }
      return pass(
        this.name,
        `active served ${byConsumer[activeIdx].length}, standby took over ${byConsumer[standbyIdx].length} after failover, order preserved`,
        t0,
      );
    } finally {
      await subA.unsubscribe().catch(() => {});
      await subB.unsubscribe().catch(() => {});
    }
  },
};
