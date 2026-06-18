import {
  AckHandler,
  IMessageBus,
  IMessageScheduler,
  IncomingMessage,
  MessageHandler,
  ReceivedMessage,
  SubscribeOptions,
  Subscription,
} from '../abstractions';

export type ScenarioStatus = 'pass' | 'fail' | 'unsupported' | 'skipped';

export interface ScenarioResult {
  readonly name: string;
  readonly status: ScenarioStatus;
  readonly detail: string;
  readonly durationMs: number;
}

/**
 * A single observable behavior we want every broker to demonstrate (or honestly
 * fail to demonstrate). Scenarios are pure with respect to the broker: they only
 * ever touch `IMessageScheduler`, so the exact same scenario list runs against
 * Artemis, RabbitMQ, or the in-memory fake.
 */
export interface Scenario {
  readonly name: string;
  readonly description: string;
  run(scheduler: IMessageScheduler): Promise<ScenarioResult>;
}

/**
 * A scenario that exercises the pub/sub + ack surface (`IMessageBus`) instead of
 * the scheduling surface. Kept as a distinct type so the runner can route it to
 * the right port and so adapters without a bus report `⊘ n/a` uniformly.
 */
export interface BusScenario {
  readonly name: string;
  readonly description: string;
  run(bus: IMessageBus): Promise<ScenarioResult>;
}

// ---------------------------------------------------------------------------
// Small, dependency-free helpers shared by scenarios (kept here to stay DRY).
// ---------------------------------------------------------------------------

let counter = 0;
/** Unique-enough token so concurrent scenarios never read each other's mail. */
export function nonce(): string {
  counter += 1;
  return `${Date.now().toString(36)}-${counter}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Poll `predicate` until it returns true or the timeout elapses. */
export async function waitUntil(
  predicate: () => boolean,
  timeoutMs: number,
  intervalMs = 50,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await delay(intervalMs);
  }
  return predicate();
}

/** Subscribe and accumulate received messages for later assertions. */
export class MessageCollector {
  readonly received: ReceivedMessage[] = [];
  private sub?: Subscription;

  static async start(
    scheduler: IMessageScheduler,
    destination: string,
  ): Promise<MessageCollector> {
    const c = new MessageCollector();
    const handler: MessageHandler = (m) => {
      c.received.push(m);
    };
    c.sub = await scheduler.consume(destination, handler);
    return c;
  }

  bodies(): string[] {
    return this.received.map((m) => m.body);
  }

  async stop(): Promise<void> {
    await this.sub?.unsubscribe();
  }
}

/**
 * Subscribe to a topic and accumulate received messages, auto-acking each by
 * default. The `onMessage` hook lets a scenario take explicit control (nack,
 * crash, delay) for the ack/redelivery/poison cases.
 */
export class AckCollector {
  readonly received: IncomingMessage[] = [];
  private sub?: Subscription;

  static async start(
    bus: IMessageBus,
    topic: string,
    options?: SubscribeOptions & {
      onMessage?: (m: IncomingMessage) => void | Promise<void>;
      autoAck?: boolean;
    },
  ): Promise<AckCollector> {
    const collector = new AckCollector();
    const autoAck = options?.autoAck ?? true;
    const handler: AckHandler = async (m) => {
      collector.received.push(m);
      if (options?.onMessage) await options.onMessage(m);
      else if (autoAck) await m.ack();
    };
    const { onMessage, autoAck: _a, ...subOpts } = options ?? {};
    collector.sub = await bus.subscribe(topic, handler, subOpts);
    return collector;
  }

  bodies(): string[] {
    return this.received.map((m) => m.body);
  }

  count(): number {
    return this.received.length;
  }

  async stop(): Promise<void> {
    await this.sub?.unsubscribe();
  }
}

// Result constructors keep scenario bodies readable.
export const pass = (name: string, detail: string, t0: number): ScenarioResult => ({
  name,
  status: 'pass',
  detail,
  durationMs: Math.round(performance.now() - t0),
});
export const fail = (name: string, detail: string, t0: number): ScenarioResult => ({
  name,
  status: 'fail',
  detail,
  durationMs: Math.round(performance.now() - t0),
});
export const unsupported = (
  name: string,
  detail: string,
  t0: number,
): ScenarioResult => ({
  name,
  status: 'unsupported',
  detail,
  durationMs: Math.round(performance.now() - t0),
});
