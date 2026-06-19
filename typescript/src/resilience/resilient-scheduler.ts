import {
  ConsecutiveBreaker,
  ExponentialBackoff,
  circuitBreaker,
  handleWhen,
  retry,
  wrap,
} from 'cockatiel';
import {
  AckHandler,
  BusCapabilities,
  Capabilities,
  Destination,
  IMessageBus,
  IMessageScheduler,
  MessageHandler,
  NotSupportedError,
  PublishOptions,
  ScheduleHandle,
  ScheduledInfo,
  SubscribeOptions,
  Subscription,
} from '../abstractions';

export interface ResilienceOptions {
  maxRetryAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  consecutiveFailures: number;
  halfOpenAfterMs: number;
}

export const DEFAULT_RESILIENCE: ResilienceOptions = {
  maxRetryAttempts: 3,
  initialDelayMs: 200,
  maxDelayMs: 3000,
  consecutiveFailures: 5,
  halfOpenAfterMs: 5000,
};

/** A resilience lifecycle event, surfaced so the fault-tolerance narrator can
 *  print a real timeline (retries, breaker open/half-open/close) rather than a
 *  faked one. Default is a no-op, so normal runs are unaffected. */
export type ResilienceEvent =
  | { kind: 'retry'; attempt: number; error: string }
  | { kind: 'breaker-open' }
  | { kind: 'breaker-half-open' }
  | { kind: 'breaker-close' };

export type ResilienceListener = (event: ResilienceEvent) => void;

const ALL_BUS_FALSE: BusCapabilities = {
  supportsTopic: false,
  supportsFanout: false,
  supportsManualAck: false,
  supportsDeadLetter: false,
  reportsDeliveryCount: false,
  supportsDedup: false,
  supportsStreamReplay: false,
};

/**
 * A resilience **decorator** over any adapter, the TypeScript mirror of the .NET
 * Polly decorator. It adds retry-with-backoff and a circuit breaker (via
 * cockatiel) around discrete broker calls, without any adapter knowing it
 * exists. It implements BOTH ports: scheduling calls and bus publish/subscribe
 * setup run through the same pipeline.
 *
 * Like the .NET side, it treats `NotSupportedError` as a *contract outcome*, not
 * a transient fault: never retried, never trips the breaker, so an honestly
 * unsupported operation still surfaces as "unsupported", not an outage.
 */
export class ResilientScheduler implements IMessageScheduler, IMessageBus {
  readonly name: string;
  private readonly inner: IMessageScheduler;
  private readonly run: <T>(op: () => Promise<T>) => Promise<T>;

  constructor(
    inner: IMessageScheduler,
    options: ResilienceOptions = DEFAULT_RESILIENCE,
    private readonly onEvent?: ResilienceListener,
  ) {
    this.inner = inner;
    this.name = `${inner.name} + Cockatiel`;

    // A transient fault is anything EXCEPT a declared "not supported" outcome.
    const policy = handleWhen((err) => !(err instanceof NotSupportedError));

    const retryPolicy = retry(policy, {
      maxAttempts: options.maxRetryAttempts,
      backoff: new ExponentialBackoff({
        initialDelay: options.initialDelayMs,
        maxDelay: options.maxDelayMs,
      }),
    });
    const breaker = circuitBreaker(policy, {
      halfOpenAfter: options.halfOpenAfterMs,
      breaker: new ConsecutiveBreaker(options.consecutiveFailures),
    });

    let attempt = 0;
    retryPolicy.onRetry((data) => {
      attempt += 1;
      this.onEvent?.({
        kind: 'retry',
        attempt,
        error: 'reason' in data ? String((data as { reason?: unknown }).reason) : 'transient',
      });
    });
    breaker.onBreak(() => this.onEvent?.({ kind: 'breaker-open' }));
    breaker.onHalfOpen(() => this.onEvent?.({ kind: 'breaker-half-open' }));
    breaker.onReset(() => this.onEvent?.({ kind: 'breaker-close' }));

    // Retry first, then the breaker — failed retries feed the breaker's count.
    const pipeline = wrap(retryPolicy, breaker);
    this.run = (op) => pipeline.execute(() => op());
  }

  get capabilities(): Capabilities {
    return this.inner.capabilities;
  }

  // --- scheduler port -----------------------------------------------------

  connect(): Promise<void> {
    return this.run(() => this.inner.connect());
  }

  sendNow(destination: Destination, payload: string): Promise<void> {
    return this.run(() => this.inner.sendNow(destination, payload));
  }

  schedule(
    destination: Destination,
    payload: string,
    deliverAt: Date,
  ): Promise<ScheduleHandle> {
    return this.run(() => this.inner.schedule(destination, payload, deliverAt));
  }

  cancel(handle: ScheduleHandle): Promise<void> {
    return this.run(() => this.inner.cancel(handle));
  }

  listScheduled(destination: Destination): Promise<ScheduledInfo[]> {
    return this.run(() => this.inner.listScheduled(destination));
  }

  // The consume *setup* is protected; the long-lived message loop itself is not
  // wrapped (a circuit breaker belongs around discrete calls, not a stream).
  consume(
    destination: Destination,
    handler: MessageHandler,
  ): Promise<Subscription> {
    return this.run(() => this.inner.consume(destination, handler));
  }

  // --- bus port (forwarded only when the inner adapter is a bus) ----------

  private get innerBus(): IMessageBus {
    return this.inner as unknown as IMessageBus;
  }

  get busCapabilities(): BusCapabilities {
    return this.inner.capabilities.bus ?? ALL_BUS_FALSE;
  }

  connectBus(): Promise<void> {
    return this.run(() => this.innerBus.connectBus());
  }

  publish(
    topic: Destination,
    payload: string,
    routingKey?: string,
    options?: PublishOptions,
  ): Promise<void> {
    return this.run(() => this.innerBus.publish(topic, payload, routingKey, options));
  }

  subscribe(
    topic: Destination,
    handler: AckHandler,
    options?: SubscribeOptions,
  ): Promise<Subscription> {
    return this.run(() => this.innerBus.subscribe(topic, handler, options));
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.inner[Symbol.asyncDispose]();
  }
}
