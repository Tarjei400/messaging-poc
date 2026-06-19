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
  /** Address a reply should be sent to (request/reply — S15). */
  readonly replyTo?: string;
  /** Correlates a reply with its request (request/reply — S15). */
  readonly correlationId?: string;
  /** Ordering/affinity key — messages of the same group keep their order and
   *  are pinned to one consumer (S12). */
  readonly groupId?: string;
  /** Broker-reported message priority, when exposed (S14). */
  readonly priority?: number;
}

export type AckHandler = (message: IncomingMessage) => void | Promise<void>;

/**
 * Per-message publish metadata. Each adapter maps these onto its native AMQP
 * properties (Artemis `_AMQ_GROUP_ID`/`_AMQ_DUPL_ID`/priority/reply-to/ttl;
 * RabbitMQ `BasicProperties` + headers). All fields are optional — a bare
 * `publish(topic, body)` keeps working unchanged.
 */
export interface PublishOptions {
  /** Broker priority (higher = sooner). RabbitMQ 0–9; Artemis 0–9 (native). */
  readonly priority?: number;
  /** Ordering/affinity key — same group → ordered, pinned to one consumer (S12). */
  readonly groupId?: string;
  /** Producer dedup key — the broker drops a repeat within its window (S13). */
  readonly dedupId?: string;
  /** Where a reply should be sent (request/reply — S15). */
  readonly replyTo?: string;
  /** Correlates a reply with its request (request/reply — S15). */
  readonly correlationId?: string;
  /** Time-to-live before the message expires to the expiry address (S16). */
  readonly ttlMs?: number;
  /** Arbitrary application headers. */
  readonly headers?: Readonly<Record<string, string>>;
}

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
  /** Delay (ms) before a nacked message is redelivered. When set (with
   *  `deadLetter`), the adapter parks the failed message in a dedicated retry
   *  queue instead of requeuing it in place, so the main queue keeps flowing
   *  (non-blocking retry). Adapters that drive retry from broker config (Artemis
   *  `redelivery-delay`) treat this as advisory — see the adapter notes. */
  readonly retryDelayMs?: number;
  /** Preserve per-group order across competing consumers — each `groupId` is
   *  pinned to one consumer (S12). Artemis message groups / RabbitMQ
   *  consistent-hash exchange / in-memory group affinity. */
  readonly partitionByGroup?: boolean;
  /** Only one consumer on the shared queue is active at a time; a standby takes
   *  over if it drops, preserving order (S18). */
  readonly singleActiveConsumer?: boolean;
  /** Replay the whole retained log from the beginning rather than only new
   *  messages (S19). Requires `BusCapabilities.supportsStreamReplay`. */
  readonly streamReplay?: boolean;
  /** Transient per-connection subscription (exclusive + auto-delete) — the queue
   *  vanishes when the subscriber disconnects. Used by the SSE cluster so each
   *  client connection doesn't leak a durable queue. */
  readonly transient?: boolean;
  /** Declare the queue as priority-capable so `PublishOptions.priority` is
   *  honoured (RabbitMQ `x-max-priority`; no-op where priority is native). */
  readonly priorityQueue?: boolean;
  /** Declare the queue so an unconsumed message expires to the expiry address
   *  (S16). On RabbitMQ this wires `x-dead-letter-exchange` → the expiry fanout
   *  (per-message `expiration` then routes the expired message there). Artemis
   *  drives expiry from broker.xml (`mbc.s16.#` → `mbc.EXPIRY`), so this is a
   *  no-op there; the value mirrors the publish-side `ttlMs`. */
  readonly ttlMs?: number;
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
  /** Broker-native producer deduplication (Artemis duplicate detection). When
   *  false the broker has no native dedup (RabbitMQ) — see app-level S10. */
  readonly supportsDedup: boolean;
  /** Offset-based replay of a retained log (RabbitMQ streams). When false the
   *  broker cannot replay consumed history (Artemis). */
  readonly supportsStreamReplay: boolean;
  /** Broker-native ordered message groups — a `groupId` is pinned to one
   *  consumer so per-group order survives competing consumers (S12). Artemis
   *  message groups / RabbitMQ consistent-hash exchange / in-memory affinity.
   *  Optional: absent ≡ supported (only an adapter that genuinely lacks it sets
   *  this false). */
  readonly supportsMessageGroups?: boolean;
}

export interface IMessageBus extends AsyncDisposable {
  /** Human-readable adapter name (shared with the scheduler port). */
  readonly name: string;
  /** What this adapter's bus surface can and cannot do. */
  readonly busCapabilities: BusCapabilities;

  /** Establish the connection and provision any required topic topology. */
  connectBus(): Promise<void>;

  /** Publish to a topic/fanout address (NOT a single point-to-point queue).
   *  `options` carries per-message metadata (priority, group, dedup, reply-to,
   *  ttl, headers); omit it for a plain publish. */
  publish(
    topic: Destination,
    payload: string,
    routingKey?: string,
    options?: PublishOptions,
  ): Promise<void>;

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

/**
 * The conventional expiry destination for a topic — where a message that lives
 * past its TTL lands (distinct from the dead-letter address, which is for poison
 * messages). Every adapter maps this onto its native expiry concept (Artemis
 * `expiry-address`, RabbitMQ per-queue `x-message-ttl` + an expiry exchange).
 */
export function expiryAddress(topic: Destination): Destination {
  return `${topic}.expiry`;
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
