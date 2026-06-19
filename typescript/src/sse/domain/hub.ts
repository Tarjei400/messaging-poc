/**
 * SseHub — the core application logic of the SSE cluster, with NO knowledge of
 * `node:http` or any broker. It depends only on the outbound ports
 * (`MessageBusPort`, `ConnectionSink`), which is what makes the hexagon testable
 * and broker-agnostic.
 *
 * Responsibilities:
 *   - turn a publish intent (`PublishTarget` + envelope fields) into a routing
 *     key and publish it (`publish`);
 *   - bind a client connection's two transient subscriptions (user-direct +
 *     org-broadcast) and fan matching broker messages into its `ConnectionSink`
 *     as `SseFrame`s, returning a single teardown handle (`connect`).
 */

import {
  PublishTarget,
  routingKeyFor,
  Scope,
  SseEnvelope,
} from './contract';
import { ConnectionSink, MessageBusPort, Unsubscribe } from '../ports';

/** Identifies the user + org a single SSE connection is for. */
export interface ConnectionIdentity {
  readonly userId: string;
  readonly orgId: string;
}

/** The fields a caller supplies when publishing; the hub stamps `via`/`at`. */
export interface PublishIntent {
  readonly to: PublishTarget;
  readonly from: string;
  readonly text: string;
}

export class SseHub {
  constructor(
    private readonly bus: MessageBusPort,
    /** Id of THIS instance — stamped into envelopes (`via`) and frames. */
    private readonly instanceId: string,
  ) {}

  /**
   * Publish a message to its target's routing key. Returns the routing key it
   * resolved to (handy for the HTTP `202` response body).
   */
  async publish(intent: PublishIntent): Promise<string> {
    const routingKey = routingKeyFor(intent.to);
    const envelope: SseEnvelope = {
      from: intent.from || 'anon',
      text: intent.text ?? '',
      to: intent.to,
      via: this.instanceId,
      at: new Date().toISOString(),
    };
    await this.bus.publish(routingKey, JSON.stringify(envelope));
    return routingKey;
  }

  /**
   * Bind a connection's user-direct and org-broadcast subscriptions, fanning
   * each matching broker message into `sink` as an `SseFrame`. Returns a handle
   * that tears down both subscriptions (call it when the client disconnects).
   */
  async connect(identity: ConnectionIdentity, sink: ConnectionSink): Promise<Unsubscribe> {
    const userUnsub = await this.bindScope('user', identity.userId, sink);
    const orgUnsub = await this.bindScope('org', identity.orgId, sink);
    return async () => {
      await Promise.allSettled([userUnsub(), orgUnsub()]);
    };
  }

  /** Subscribe one scope's routing key and forward matches to the sink. */
  private bindScope(scope: Scope, id: string, sink: ConnectionSink): Promise<Unsubscribe> {
    const routingKey = routingKeyFor({ type: scope, id });
    return this.bus.subscribe(routingKey, (body) => {
      sink.send({ scope, instanceId: this.instanceId, message: this.parse(body) });
    });
  }

  /** Best-effort parse of a broker body into an envelope; tolerates raw text. */
  private parse(body: string): SseEnvelope {
    try {
      return JSON.parse(body) as SseEnvelope;
    } catch {
      // Non-JSON body — surface it as text so the client still sees something.
      return { from: 'anon', text: body, to: { type: 'user', id: '' }, via: this.instanceId, at: '' };
    }
  }
}
