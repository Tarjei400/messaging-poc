import { describe, expect, it } from 'vitest';
import { InMemoryScheduler } from '../src/adapters/in-memory';
import { FaultInjectingBus } from '../src/fault/fault-injecting-bus';
import { ResilienceEvent, ResilientScheduler } from '../src/resilience';

const settle = (ms = 60) => new Promise((r) => setTimeout(r, ms));

describe('fault tolerance', () => {
  it('absorbs transient faults via retry, then surfaces resilience events', async () => {
    const events: ResilienceEvent[] = [];
    const faulted = new FaultInjectingBus(new InMemoryScheduler());
    const resilient = new ResilientScheduler(
      faulted,
      {
        maxRetryAttempts: 3,
        initialDelayMs: 5,
        maxDelayMs: 20,
        consecutiveFailures: 5, // higher than the injected faults → breaker stays closed
        halfOpenAfterMs: 50,
      },
      (e) => events.push(e),
    );
    await resilient.connect();
    await resilient.connectBus();

    const received: string[] = [];
    await resilient.subscribe('t.fault', async (m) => {
      received.push(m.body);
      await m.ack();
    }, { subscriberId: 's' });

    // Two transient faults are absorbed by retries; the publish still succeeds.
    faulted.injectFailures(2);
    await resilient.publish('t.fault', 'survives');
    await settle(120);

    expect(received).toContain('survives');
    expect(events.some((e) => e.kind === 'retry')).toBe(true);
  });

  it('opens the circuit breaker under a sustained outage', async () => {
    const events: ResilienceEvent[] = [];
    const faulted = new FaultInjectingBus(new InMemoryScheduler());
    const resilient = new ResilientScheduler(
      faulted,
      {
        maxRetryAttempts: 2,
        initialDelayMs: 5,
        maxDelayMs: 20,
        consecutiveFailures: 2,
        halfOpenAfterMs: 50,
      },
      (e) => events.push(e),
    );
    await resilient.connectBus();

    faulted.injectFailures(100);
    for (let i = 0; i < 6; i++) {
      await resilient.publish('t.out', 'x').catch(() => undefined);
    }
    expect(events.some((e) => e.kind === 'breaker-open')).toBe(true);
  });

  it('redelivers an un-acked message when a consumer drops (crash)', async () => {
    const bus = new InMemoryScheduler();
    await bus.connectBus();

    const crashed = await bus.subscribe('t.crash', async () => {
      /* receive but never ack — simulate a crash */
    }, { subscriberId: 'q' });
    await bus.publish('t.crash', 'job');
    await settle();
    await crashed.unsubscribe(); // drop the consumer with the message un-acked

    const got: string[] = [];
    await bus.subscribe('t.crash', async (m) => {
      got.push(m.body);
      await m.ack();
    }, { subscriberId: 'q' });
    await settle();
    expect(got).toEqual(['job']);
  });
});
