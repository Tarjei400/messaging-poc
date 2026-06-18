import { Destination, ReceivedMessage, Subscription } from './types';

/**
 * The second seam of the project (a sibling of `IMessageScheduler`).
 *
 * Scheduling is point-to-point and time-shifted; pub/sub, fanout and explicit
 * acknowledgement are about *fan-out* and *settlement*. Those are different
 * reasons to change, so they live on a different port (Interface Segregation):
 * an adapter that only schedules need not grow topic/ack methods, and the
 * scheduling scenarios (S1–S4) are untouched.
 *
 * Every concrete adapter (in-memory, Artemis, RabbitMQ) implements BOTH ports;
 * the resilience decorator implements both and forwards through one pipeline.
 */
export type TopologyKind = 'topic' | 'fanout';

/**
 * A message delivered to a bus subscriber. Unlike `ReceivedMessage` (auto-ack on
 * the scheduler port), the consumer here controls settlement explicitly — which
 * is exactly what makes redelivery, poison-handling and at-least-once delivery
 * observable.
 */
export interface IncomingMessage extends ReceivedMessage {
  /** Settle positively: the broker removes the message. */
  ack(): Promise<void>;
  /** Settle negatively. `requeue=true` → redeliver (subject to maxDeliveries);
   *  `requeue=false` → dead-letter (if configured) or drop. */
  nack(requeue: boolean): Promise<void>;
  /** 1-based broker-reported delivery attempt, when the broker exposes it.
   *  Absent on brokers that only report a redelivered boolean (RabbitMQ classic
   *  queues) — see `BusCapabilities.reportsDeliveryCount`. */
  readonly deliveryCount?: number;
}

export type AckHandler = (message: IncomingMessage) => void | Promise<void>;

export interface SubscribeOptions {
  /** `topic` = routing-key filtered; `fanout` = every subscriber gets a copy. */
  readonly kind?: TopologyKind;
  /** Topic routing-key filter, e.g. `order.*`. Ignored for fanout. */
  readonly routingKey?: string;
  /** Names the subscriber's queue. Distinct ids → independent copies (pub/sub,
   *  fanout). Re-using an id → consumers share one queue (competing consumers). */
  readonly subscriberId?: string;
  /** Provision dead-letter wiring so poison messages can be inspected. */
  readonly deadLetter?: boolean;
  /** Number of delivery attempts before a message is dead-lettered. */
  readonly maxDeliveries?: number;
}

/**
 * Self-declared bus capabilities, scored by the runner the same way the
 * scheduling `Capabilities` are: an honest gap prints `⊘ n/a`, a real break `✗`.
 */
export interface BusCapabilities {
  /** Routing-key filtered pub/sub. */
  readonly supportsTopic: boolean;
  /** One publish → N independent subscriber queues. */
  readonly supportsFanout: boolean;
  /** ack / nack / requeue under consumer control. */
  readonly supportsManualAck: boolean;
  /** Dead-letter after N attempts. */
  readonly supportsDeadLetter: boolean;
  /** Reports a precise per-message delivery count (vs. a redelivered flag only). */
  readonly reportsDeliveryCount: boolean;
}

export interface IMessageBus extends AsyncDisposable {
  /** Human-readable adapter name (shared with the scheduler port). */
  readonly name: string;
  /** What this adapter's bus surface can and cannot do. */
  readonly busCapabilities: BusCapabilities;

  /** Establish the connection and provision any required topic topology. */
  connectBus(): Promise<void>;

  /** Publish to a topic/fanout address (NOT a single point-to-point queue). */
  publish(topic: Destination, payload: string, routingKey?: string): Promise<void>;

  /** Subscribe to a topic. Each distinct `subscriberId` is an independent queue. */
  subscribe(
    topic: Destination,
    handler: AckHandler,
    options?: SubscribeOptions,
  ): Promise<Subscription>;
}

/** Default number of delivery attempts before dead-lettering. */
export const DEFAULT_MAX_DELIVERIES = 3;

/**
 * The conventional dead-letter destination for a topic. Every adapter maps this
 * single logical name onto its native dead-letter concept (Artemis
 * `dead-letter-address`, RabbitMQ `dlq.{topic}` via a DLX), so a scenario can
 * subscribe here to prove a poison message was dead-lettered.
 */
export function deadLetterAddress(topic: Destination): Destination {
  return `${topic}.dlq`;
}

/** Narrow an adapter to the bus port without a hard dependency on the class. */
export function isMessageBus(value: unknown): value is IMessageBus {
  return (
    typeof value === 'object' &&
    value !== null &&
    'busCapabilities' in value &&
    typeof (value as IMessageBus).subscribe === 'function'
  );
}
