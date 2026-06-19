/**
 * Outbound ports of the SSE hexagon.
 *
 * These are the ONLY contracts the domain (`SseHub`) knows about. They are
 * deliberately minimal and transport/broker-agnostic: the broker is hidden
 * behind `MessageBusPort`, and the HTTP/SSE response is hidden behind
 * `ConnectionSink`. Adapters in `sse/adapters/` implement them.
 */

import { SseFrame } from './domain/contract';

/** Tear down a subscription created via `MessageBusPort.subscribe`. */
export type Unsubscribe = () => Promise<void>;

/**
 * What the hub needs from a message broker — nothing more. The adapter
 * (`BusMessagePort`) maps these onto the project's `IMessageBus`, hiding the
 * `mbc.sse` topic, transient-subscription flags and manual ack.
 */
export interface MessageBusPort {
  /**
   * Subscribe to a routing key on the SSE channel. `onMessage` receives the raw
   * string body; the adapter owns acking. Returns a handle that ends the
   * subscription.
   */
  subscribe(routingKey: string, onMessage: (body: string) => void): Promise<Unsubscribe>;

  /** Publish a payload to a routing key on the SSE channel. */
  publish(routingKey: string, payload: string): Promise<void>;
}

/**
 * Where the hub fans broker messages to — one per live client connection. The
 * HTTP adapter implements this over a `ServerResponse` (SSE framing); a test
 * could implement it over an array.
 */
export interface ConnectionSink {
  /** Deliver one SSE frame to the connected client. */
  send(frame: SseFrame): void;
  /** Close the underlying connection. */
  close(): void;
}
