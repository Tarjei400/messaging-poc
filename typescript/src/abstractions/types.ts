/**
 * Core domain types shared by every broker adapter.
 *
 * These types intentionally describe ONLY the semantics the project needs
 * (send-now, schedule, cancel, list, consume). Nothing broker-specific leaks
 * in here — that is what keeps application code portable across brokers and is
 * exactly the "internal scheduling abstraction" the research report recommends
 * standing up *before* committing to a broker.
 */

/** A logical destination. Each adapter maps this to its native concept
 *  (Artemis address/queue, RabbitMQ exchange+queue). */
export type Destination = string;

/** Opaque handle returned when a message is scheduled. The `id` is the only
 *  thing application code should rely on; `native` is adapter-private detail. */
export interface ScheduleHandle {
  readonly id: string;
  readonly destination: Destination;
  readonly deliverAt: Date;
  /** Adapter-private payload (e.g. broker message id or native handle). */
  readonly native?: unknown;
}

/** A message handed to a consumer. */
export interface ReceivedMessage {
  readonly id: string;
  readonly destination: Destination;
  readonly body: string;
  readonly headers: Readonly<Record<string, string>>;
}

/** Summary of a still-pending scheduled message, for inspection/observability. */
export interface ScheduledInfo {
  readonly id: string;
  readonly destination: Destination;
  readonly deliverAt?: Date;
}

/** An active subscription; dispose to stop consuming. */
export interface Subscription {
  unsubscribe(): Promise<void>;
}

export type MessageHandler = (message: ReceivedMessage) => void | Promise<void>;

/**
 * Self-declared capabilities of an adapter.
 *
 * The scenario runner reads these to decide whether an unsupported operation is
 * an *expected* honest gap (printed as ⊘ "unsupported") or a genuine failure
 * (printed as ✗). This is what turns the comparison docs' claims into something
 * the runner can actually verify rather than assert.
 */
export interface Capabilities {
  /** Wire protocol, for the report ("AMQP 1.0", "AMQP 0.9.1"). */
  readonly protocol: string;
  /** Broker schedules natively vs. via a bolted-on plugin/workaround. */
  readonly nativeScheduling: boolean;
  /** A pending scheduled message can be cancelled before it fires. */
  readonly supportsCancel: boolean;
  /** Pending scheduled messages can be listed/inspected. */
  readonly supportsList: boolean;
  /** Pub/sub, fanout and explicit-ack capabilities — present when the adapter
   *  also implements `IMessageBus`. Additive, so scheduler-only constructions
   *  (e.g. test fakes) stay valid. Imported lazily to avoid a type cycle. */
  readonly bus?: import('./message-bus').BusCapabilities;
}
