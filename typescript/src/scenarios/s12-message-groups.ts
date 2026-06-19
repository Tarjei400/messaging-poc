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

const GROUPS = ['a', 'b', 'c'];
const PER_GROUP = 6; // ordered sequence 0..PER_GROUP-1 per group
const TOTAL = GROUPS.length * PER_GROUP;

/**
 * S12 — Ordered delivery / message groups. Messages of several groups are
 * published *interleaved* to ONE topic consumed by two competing consumers
 * (same `subscriberId`) with `partitionByGroup`. The broker must pin each group
 * to a single consumer so that, despite work-sharing, every group's messages
 * arrive in their published order and a group is never split across consumers.
 *
 * Artemis: native message groups (`_AMQ_GROUP_ID`) pin a group to one consumer
 * on the shared multicast queue. RabbitMQ: a consistent-hash exchange routes a
 * `groupId` to a fixed per-consumer queue (needs the bundled plugin) — gated by
 * `supportsMessageGroups`. In-memory: group→consumer affinity on first sight.
 */
export const messageGroups: BusScenario = {
  name: 'S12 message groups',
  description: `${TOTAL} messages across ${GROUPS.length} groups keep per-group order, each pinned to one of 2 consumers.`,
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    if (!bus.busCapabilities.supportsManualAck) {
      return unsupported(this.name, 'no manual ack on this broker', t0);
    }
    if (bus.busCapabilities.supportsMessageGroups === false) {
      return unsupported(this.name, 'no broker-native message grouping', t0);
    }
    const topic = `mbc.s12.${nonce()}`;
    const queueId = `groups-${nonce()}`; // shared queue → competing consumers

    // Record, per consumer, the (group, seq) of each message it handled, in
    // arrival order — enough to assert both per-group FIFO and group→consumer
    // pinning.
    const byConsumer: { group: string; seq: number }[][] = [[], []];

    const make = (i: number): AckHandler => async (m: IncomingMessage) => {
      const [group, seqStr] = m.body.split(':');
      byConsumer[i].push({ group, seq: Number(seqStr) });
      await m.ack();
    };
    const subA = await bus.subscribe(topic, make(0), {
      subscriberId: queueId,
      partitionByGroup: true,
    });
    const subB = await bus.subscribe(topic, make(1), {
      subscriberId: queueId,
      partitionByGroup: true,
    });
    try {
      // Publish interleaved: a0,b0,c0,a1,b1,c1,… so ordering is only preserved
      // if the broker actually pins each group to one consumer.
      for (let seq = 0; seq < PER_GROUP; seq++) {
        for (const g of GROUPS) {
          await bus.publish(topic, `${g}:${seq}`, undefined, { groupId: g });
        }
      }

      const total = () => byConsumer[0].length + byConsumer[1].length;
      const ok = await waitUntil(() => total() >= TOTAL, 10000);
      if (!ok) {
        return fail(this.name, `received ${total()}/${TOTAL}`, t0);
      }

      // Which consumer(s) handled each group, and in what order per consumer.
      const ownersOf = new Map<string, Set<number>>();
      for (let i = 0; i < byConsumer.length; i++) {
        // Per-group sequences as this consumer saw them, in arrival order.
        const perGroupSeqs = new Map<string, number[]>();
        for (const { group, seq } of byConsumer[i]) {
          if (!ownersOf.has(group)) ownersOf.set(group, new Set());
          ownersOf.get(group)!.add(i);
          if (!perGroupSeqs.has(group)) perGroupSeqs.set(group, []);
          perGroupSeqs.get(group)!.push(seq);
        }
        // Within a consumer, each group's sequences must be strictly increasing.
        for (const [group, seqs] of perGroupSeqs) {
          for (let k = 1; k < seqs.length; k++) {
            if (seqs[k] <= seqs[k - 1]) {
              return fail(
                this.name,
                `group ${group} out of order on consumer ${i}: ${seqs.join(',')}`,
                t0,
              );
            }
          }
        }
      }

      // Every group must have been handled by exactly one consumer (pinned).
      for (const g of GROUPS) {
        const owners = ownersOf.get(g);
        if (!owners || owners.size === 0) {
          return fail(this.name, `group ${g} was never delivered`, t0);
        }
        if (owners.size > 1) {
          return fail(this.name, `group ${g} split across consumers`, t0);
        }
        // And the consumer must have seen the full 0..PER_GROUP-1 run.
        const seen = byConsumer[[...owners][0]]
          .filter((m) => m.group === g)
          .map((m) => m.seq);
        if (seen.length !== PER_GROUP) {
          return fail(
            this.name,
            `group ${g} got ${seen.length}/${PER_GROUP} messages`,
            t0,
          );
        }
      }

      const split = byConsumer.map((c) => c.length).join('/');
      return pass(
        this.name,
        `per-group order preserved, each group pinned to one consumer (split ${split})`,
        t0,
      );
    } finally {
      await subA.unsubscribe();
      await subB.unsubscribe();
    }
  },
};
