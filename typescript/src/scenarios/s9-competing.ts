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

const MESSAGE_COUNT = 10;

/**
 * S9 — Competing consumers (work sharing, at-least-once). Two consumers attached
 * to the SAME queue share the load: each message is handled by exactly one of
 * them. This is the counterpoint to S6 fanout — same publish API, opposite
 * delivery semantics — and is the foundation of horizontal worker scaling.
 */
export const competingConsumers: BusScenario = {
  name: 'S9 competing consumers',
  description: `${MESSAGE_COUNT} messages are shared across 2 consumers, no duplicates.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsManualAck) {
      return unsupported(this.name, 'no manual ack on this broker', t0);
    }
    const topic = `mbc.s9.${nonce()}`;
    const queueId = `workers-${nonce()}`; // shared queue → competing consumers
    const byConsumer: string[][] = [[], []];

    const make = (i: number): AckHandler => async (m: IncomingMessage) => {
      byConsumer[i].push(m.body);
      await m.ack();
    };
    const subA = await bus.subscribe(topic, make(0), { subscriberId: queueId });
    const subB = await bus.subscribe(topic, make(1), { subscriberId: queueId });
    try {
      for (let i = 0; i < MESSAGE_COUNT; i++) {
        await bus.publish(topic, `job-${i}`);
      }
      const total = () => byConsumer[0].length + byConsumer[1].length;
      const ok = await waitUntil(() => total() >= MESSAGE_COUNT, 6000);

      const all = [...byConsumer[0], ...byConsumer[1]];
      const unique = new Set(all);
      if (!ok || all.length !== MESSAGE_COUNT) {
        return fail(this.name, `received ${all.length}/${MESSAGE_COUNT}`, t0);
      }
      if (unique.size !== MESSAGE_COUNT) {
        return fail(this.name, `duplicate delivery (${unique.size} unique)`, t0);
      }
      if (byConsumer[0].length === 0 || byConsumer[1].length === 0) {
        return fail(this.name, 'one consumer was starved (not balanced)', t0);
      }
      return pass(
        this.name,
        `split ${byConsumer[0].length}/${byConsumer[1].length}, no dupes`,
        t0,
      );
    } finally {
      await subA.unsubscribe();
      await subB.unsubscribe();
    }
  },
};
