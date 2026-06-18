import { describe, expect, it } from 'vitest';
import {
  Capabilities,
  Destination,
  IMessageScheduler,
  MessageHandler,
  NotSupportedError,
  ScheduleHandle,
  ScheduledInfo,
  Subscription,
} from '../src/abstractions';
import { InMemoryScheduler } from '../src/adapters/in-memory';
import { ResilientScheduler } from '../src/resilience';
import { ALL_BUS_SCENARIOS, ALL_SCENARIOS } from '../src/scenarios';
import { runSuite } from '../src/runner';

describe('scenario suite against the in-memory reference adapter', () => {
  it('passes every scenario (no broker required)', async () => {
    const scheduler = new InMemoryScheduler();
    await scheduler.connect();
    const report = await runSuite(scheduler);
    await scheduler[Symbol.asyncDispose]();

    const failed = report.results.filter((r) => r.status === 'fail');
    expect(failed, JSON.stringify(failed, null, 2)).toHaveLength(0);
    expect(report.results.every((r) => r.status === 'pass')).toBe(true);
  });
});

/**
 * A deliberately limited adapter: it can send/schedule/consume but cannot cancel
 * or list. It proves that scenarios treat a declared gap as `unsupported`, not a
 * failure — and that nothing in the runner is coupled to a real broker.
 */
class CancelUnsupportedFake implements IMessageScheduler {
  readonly name = 'Fake (no cancel/list)';
  readonly capabilities: Capabilities = {
    protocol: 'fake',
    nativeScheduling: false,
    supportsCancel: false,
    supportsList: false,
  };
  private readonly handlers = new Map<Destination, Set<MessageHandler>>();

  async connect(): Promise<void> {}
  async sendNow(d: Destination, p: string): Promise<void> {
    queueMicrotask(() => this.fire(d, p));
  }
  async schedule(d: Destination, p: string, at: Date): Promise<ScheduleHandle> {
    setTimeout(() => this.fire(d, p), Math.max(0, at.getTime() - Date.now()));
    return { id: 'x', destination: d, deliverAt: at };
  }
  async cancel(): Promise<void> {
    throw new NotSupportedError('cancel', this.name);
  }
  async listScheduled(): Promise<ScheduledInfo[]> {
    throw new NotSupportedError('listScheduled', this.name);
  }
  async consume(d: Destination, h: MessageHandler): Promise<Subscription> {
    const set = this.handlers.get(d) ?? new Set();
    set.add(h);
    this.handlers.set(d, set);
    return { unsubscribe: async () => void set.delete(h) };
  }
  async [Symbol.asyncDispose](): Promise<void> {}
  private fire(d: Destination, body: string) {
    for (const h of this.handlers.get(d) ?? [])
      void h({ id: 'm', destination: d, body, headers: {} });
  }
}

describe('graceful degradation', () => {
  it('reports cancel/list as unsupported rather than failed', async () => {
    const scheduler = new CancelUnsupportedFake();
    await scheduler.connect();
    const report = await runSuite(scheduler);

    const byName = (n: string) =>
      report.results.find((r) => r.name.startsWith(n))!;
    expect(byName('S1').status).toBe('pass');
    expect(byName('S2').status).toBe('pass');
    expect(byName('S3').status).toBe('unsupported');
    expect(byName('S4').status).toBe('unsupported');
  });

  it('reports every bus scenario as unsupported when the adapter has no bus port', async () => {
    // CancelUnsupportedFake implements only IMessageScheduler (no `bus`
    // capability), so the runner must report S5–S9 as ⊘ n/a, not failures.
    const scheduler = new CancelUnsupportedFake();
    await scheduler.connect();
    const report = await runSuite(scheduler);

    for (const s of ALL_BUS_SCENARIOS) {
      const r = report.results.find((x) => x.name === s.name)!;
      expect(r.status, `${s.name} should be unsupported`).toBe('unsupported');
    }
  });
});

describe('scenario registry', () => {
  it('exposes the expected ordered scheduling suite', () => {
    expect(ALL_SCENARIOS.map((s) => s.name.slice(0, 2))).toEqual([
      'S1',
      'S2',
      'S3',
      'S4',
    ]);
  });

  it('exposes the expected ordered bus suite', () => {
    expect(ALL_BUS_SCENARIOS.map((s) => s.name.slice(0, 2))).toEqual([
      'S5',
      'S6',
      'S7',
      'S8',
      'S9',
    ]);
  });
});

describe('resilience decorator (cockatiel)', () => {
  it('still passes every scenario when wrapping the in-memory adapter', async () => {
    const scheduler = new ResilientScheduler(new InMemoryScheduler());
    await scheduler.connect();
    const report = await runSuite(scheduler);
    await scheduler[Symbol.asyncDispose]();

    expect(report.results.every((r) => r.status === 'pass')).toBe(true);
    expect(scheduler.name.endsWith('+ Cockatiel')).toBe(true);
  });

  it('does not trip the breaker on unsupported operations', async () => {
    const scheduler = new ResilientScheduler(new CancelUnsupportedFake());
    await scheduler.connect();
    const report = await runSuite(scheduler);
    await scheduler[Symbol.asyncDispose]();

    const byName = (n: string) =>
      report.results.find((r) => r.name.startsWith(n))!;
    expect(byName('S1').status).toBe('pass');
    expect(byName('S2').status).toBe('pass');
    expect(byName('S3').status).toBe('unsupported');
    expect(byName('S4').status).toBe('unsupported');
  });
});
