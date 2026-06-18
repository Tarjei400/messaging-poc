import {
  AckHandler,
  BusCapabilities,
  Capabilities,
  Destination,
  IMessageBus,
  IMessageScheduler,
  MessageHandler,
  ScheduleHandle,
  ScheduledInfo,
  SubscribeOptions,
  Subscription,
} from '../abstractions';

/** A transient fault — deliberately NOT a NotSupportedError, so the resilience
 *  decorator treats it as retryable (the whole point of the disconnect demo). */
export class TransientFault extends Error {
  constructor(message = 'simulated broker disconnect') {
    super(message);
    this.name = 'TransientFault';
  }
}

/**
 * A TEST-ONLY decorator that injects transient faults, layered UNDER the
 * resilience decorator so the retry/circuit-breaker behavior is exercised
 * against a real adapter without crashing the broker. It implements both ports
 * and forwards everything to the inner adapter; only `publish`/`sendNow` can be
 * made to fail, on demand, to simulate a broker becoming briefly unreachable.
 *
 * Keeping fault injection here (not in the production adapters) keeps the
 * adapters honest — no test hooks leak into the shipped code.
 */
export class FaultInjectingBus implements IMessageScheduler, IMessageBus {
  readonly name: string;
  private failuresRemaining = 0;

  constructor(private readonly inner: IMessageScheduler & IMessageBus) {
    this.name = `${inner.name} (fault-injectable)`;
  }

  /** Make the next `count` publish/send calls throw a transient fault. */
  injectFailures(count: number): void {
    this.failuresRemaining = count;
  }

  private maybeFail(): void {
    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1;
      throw new TransientFault();
    }
  }

  get capabilities(): Capabilities {
    return this.inner.capabilities;
  }
  get busCapabilities(): BusCapabilities {
    return this.inner.busCapabilities;
  }

  connect(): Promise<void> {
    return this.inner.connect();
  }
  connectBus(): Promise<void> {
    return this.inner.connectBus();
  }

  async sendNow(destination: Destination, payload: string): Promise<void> {
    this.maybeFail();
    return this.inner.sendNow(destination, payload);
  }
  schedule(d: Destination, p: string, at: Date): Promise<ScheduleHandle> {
    return this.inner.schedule(d, p, at);
  }
  cancel(handle: ScheduleHandle): Promise<void> {
    return this.inner.cancel(handle);
  }
  listScheduled(destination: Destination): Promise<ScheduledInfo[]> {
    return this.inner.listScheduled(destination);
  }
  consume(destination: Destination, handler: MessageHandler): Promise<Subscription> {
    return this.inner.consume(destination, handler);
  }

  async publish(topic: Destination, payload: string, routingKey?: string): Promise<void> {
    this.maybeFail();
    return this.inner.publish(topic, payload, routingKey);
  }
  subscribe(
    topic: Destination,
    handler: AckHandler,
    options?: SubscribeOptions,
  ): Promise<Subscription> {
    return this.inner.subscribe(topic, handler, options);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.inner[Symbol.asyncDispose]();
  }
}
