import { describe, expect, it } from 'vitest';
import { IncomingMessage, deadLetterAddress } from '../src/abstractions';
import { InMemoryScheduler } from '../src/adapters/in-memory';
import { topicMatch } from '../src/adapters/in-memory-bus';

async function freshBus(): Promise<InMemoryScheduler> {
  const bus = new InMemoryScheduler();
  await bus.connectBus();
  return bus;
}

const settle = (ms = 60) => new Promise((r) => setTimeout(r, ms));

describe('topic matching (RabbitMQ-style wildcards)', () => {
  it('matches * (one word) and # (zero or more words)', () => {
    expect(topicMatch('order.created', 'order.created')).toBe(true);
    expect(topicMatch('order.*', 'order.created')).toBe(true);
    expect(topicMatch('order.*', 'order.created.eu')).toBe(false);
    expect(topicMatch('order.#', 'order.created.eu')).toBe(true);
    expect(topicMatch('order.#', 'order')).toBe(true);
    expect(topicMatch('order.created', 'order.shipped')).toBe(false);
  });
});

describe('in-memory bus', () => {
  it('fans a single publish out to every independent subscriber', async () => {
    const bus = await freshBus();
    const got: number[] = [0, 0, 0];
    await Promise.all(
      [0, 1, 2].map((i) =>
        bus.subscribe('t.fan', async (m) => {
          got[i]++;
          await m.ack();
        }, { kind: 'fanout', subscriberId: `s${i}` }),
      ),
    );
    await bus.publish('t.fan', 'hello');
    await settle();
    expect(got).toEqual([1, 1, 1]);
  });

  it('shares load across competing consumers on the same queue', async () => {
    const bus = await freshBus();
    const seen = new Set<string>();
    const counts = [0, 0];
    for (const i of [0, 1]) {
      await bus.subscribe('t.work', async (m) => {
        counts[i]++;
        seen.add(m.body);
        await m.ack();
      }, { subscriberId: 'shared' });
    }
    for (let n = 0; n < 10; n++) await bus.publish('t.work', `job-${n}`);
    await settle(120);
    expect(seen.size).toBe(10); // every job handled exactly once
    expect(counts[0]).toBeGreaterThan(0);
    expect(counts[1]).toBeGreaterThan(0);
  });

  it('redelivers a nacked message and increments the delivery count', async () => {
    const bus = await freshBus();
    let attempts = 0;
    let lastCount = 0;
    await bus.subscribe('t.nack', async (m: IncomingMessage) => {
      attempts++;
      lastCount = m.deliveryCount ?? 0;
      if (attempts === 1) await m.nack(true);
      else await m.ack();
    }, { subscriberId: 's' });
    await bus.publish('t.nack', 'x');
    await settle(120);
    expect(attempts).toBe(2);
    expect(lastCount).toBe(2);
  });

  it('dead-letters a poison message after maxDeliveries', async () => {
    const bus = await freshBus();
    let attempts = 0;
    const dead: string[] = [];
    await bus.subscribe('t.poison', async (m) => {
      attempts++;
      await m.nack(true);
    }, { subscriberId: 'main', deadLetter: true, maxDeliveries: 3 });
    await bus.subscribe(deadLetterAddress('t.poison'), async (m) => {
      dead.push(m.body);
      await m.ack();
    }, { kind: 'fanout', subscriberId: 'dlq' });
    await bus.publish('t.poison', 'rotten');
    await settle(200);
    expect(attempts).toBe(3);
    expect(dead).toEqual(['rotten']);
  });

  it('routes topic messages only to matching routing keys', async () => {
    const bus = await freshBus();
    const created: string[] = [];
    const all: string[] = [];
    await bus.subscribe('t.topic', async (m) => { created.push(m.body); await m.ack(); }, {
      kind: 'topic', routingKey: 'order.created', subscriberId: 'c',
    });
    await bus.subscribe('t.topic', async (m) => { all.push(m.body); await m.ack(); }, {
      kind: 'topic', routingKey: 'order.#', subscriberId: 'a',
    });
    await bus.publish('t.topic', 'c1', 'order.created');
    await bus.publish('t.topic', 's1', 'order.shipped');
    await settle();
    expect(created).toEqual(['c1']);
    expect(all.sort()).toEqual(['c1', 's1']);
  });
});
