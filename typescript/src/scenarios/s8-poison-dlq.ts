import { IMessageBus, deadLetterAddress } from '../abstractions';
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

const MAX_DELIVERIES = 3;

/**
 * S8 — Poison message → dead-letter after N attempts. A message that always
 * fails must not loop forever: after `maxDeliveries` attempts the broker moves
 * it to the dead-letter destination, where an operator (here, a DLQ subscriber)
 * can inspect it. Artemis does this with `max-delivery-attempts`; RabbitMQ with
 * a delivery-limit + dead-letter exchange.
 */
export const poisonDlq: BusScenario = {
  name: 'S8 poison → dead-letter',
  description: `A always-failing message is dead-lettered after ${MAX_DELIVERIES} attempts.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsDeadLetter) {
      return unsupported(this.name, 'no dead-letter support on this broker', t0);
    }
    const topic = `mbc.s8.${nonce()}`;
    let attempts = 0;
    let lastCount: number | undefined;

    const main = await AckCollector.start(bus, topic, {
      subscriberId: `poison-${nonce()}`,
      deadLetter: true,
      maxDeliveries: MAX_DELIVERIES,
      autoAck: false,
      onMessage: async (m) => {
        attempts += 1;
        lastCount = m.deliveryCount;
        await m.nack(true); // always fail → forces redelivery then dead-letter
      },
    });
    const dlq = await AckCollector.start(bus, deadLetterAddress(topic), {
      kind: 'fanout',
      subscriberId: `dlq-${nonce()}`,
    });
    try {
      await bus.publish(topic, 'poison-1');
      const landed = await waitUntil(() => dlq.count() >= 1, 8000);
      if (!landed) {
        return fail(
          this.name,
          `never dead-lettered (attempts=${attempts}, dlq=${dlq.count()})`,
          t0,
        );
      }
      // The key property is "bounded, then dead-lettered" — not an exact count.
      // Artemis & the in-memory reference dead-letter at exactly maxDeliveries;
      // RabbitMQ quorum queues dead-letter when the count *exceeds* the limit
      // (maxDeliveries + 1). Both are correct; an infinite loop is not.
      if (attempts < 2 || attempts > MAX_DELIVERIES + 1) {
        return fail(
          this.name,
          `delivery attempts out of range: ${attempts} (limit ${MAX_DELIVERIES})`,
          t0,
        );
      }
      const countNote =
        bus.busCapabilities.reportsDeliveryCount && lastCount !== undefined
          ? ` (final deliveryCount=${lastCount})`
          : '';
      return pass(
        this.name,
        `dead-lettered after ${attempts} attempts${countNote}`,
        t0,
      );
    } finally {
      await main.stop();
      await dlq.stop();
    }
  },
};
