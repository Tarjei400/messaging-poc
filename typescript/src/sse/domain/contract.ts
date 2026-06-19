/**
 * SSE event contract — the SINGLE SOURCE OF TRUTH for the cross-instance SSE
 * cluster's wire shape.
 *
 * Both the runtime (hub + adapters) and the documentation generator
 * (`sse/docs/generate.ts`) consume THIS module: the runtime imports the types
 * and the `routingKeyFor` function; the doc generator reads the `SSE_CONTRACT`
 * metadata object. Because there is exactly one definition, the generated
 * `docs/sse-events.md` and `docs/sse-asyncapi.yaml` can never drift from code.
 *
 * Nothing here depends on `node:http`, the broker, or any adapter — the contract
 * is pure data + types, the innermost ring of the hexagon.
 */

// ---------------------------------------------------------------------------
// Wire constants & addressing
// ---------------------------------------------------------------------------

/** The single broker topic every SSE connection publishes to and subscribes on. */
export const SSE_CHANNEL = 'mbc.sse';

/** The two scopes an SSE frame (and a publish target) can carry. */
export type Scope = 'user' | 'org';

/** Who a publish is aimed at: a single user, or every member of an org. */
export interface PublishTarget {
  readonly type: Scope;
  readonly id: string;
}

/**
 * Map a publish/subscribe target onto its broker routing key. This is the ONE
 * place the `user.{id}` / `org.{id}` address schemes are defined; the hub, the
 * bus adapter, and the doc generator all route through it.
 */
export function routingKeyFor(target: PublishTarget): string {
  return `${target.type}.${target.id}`;
}

// ---------------------------------------------------------------------------
// Message & frame types (the wire payloads)
// ---------------------------------------------------------------------------

/**
 * The envelope published to the broker on `POST /publish` and re-emitted to
 * clients as the `message` of an SSE frame.
 */
export interface SseEnvelope {
  /** Display name of the sender (defaults to `anon` when omitted). */
  readonly from: string;
  /** Free-text body of the message. */
  readonly text: string;
  /** Who the message was addressed to (the publish target). */
  readonly to: PublishTarget;
  /** Id of the instance that accepted and published the message. */
  readonly via: string;
  /** ISO-8601 timestamp set when the message was published. */
  readonly at: string;
}

/**
 * The SSE `data:` frame delivered to a connected client. Wraps the envelope
 * with the scope it matched and the instance that delivered it (which may differ
 * from `message.via` — that is the whole point of the cross-instance demo).
 */
export interface SseFrame {
  /** Which subscription matched: `user`-direct or `org`-broadcast. */
  readonly scope: Scope;
  /** Id of the instance whose SSE stream delivered this frame. */
  readonly instanceId: string;
  /** The published envelope. */
  readonly message: SseEnvelope;
}

// ---------------------------------------------------------------------------
// Self-describing metadata (consumed by the doc generator)
// ---------------------------------------------------------------------------

/** Direction of an operation relative to the SSE server. */
export type OperationDirection = 'publish' | 'subscribe';

/** A documented field of a message/frame type. */
export interface FieldDoc {
  readonly name: string;
  readonly type: string;
  readonly description: string;
}

/** A documented operation on the channel. */
export interface OperationDoc {
  readonly id: string;
  readonly summary: string;
  readonly direction: OperationDirection;
  /** The routing-key pattern this operation uses, e.g. `user.{userId}`. */
  readonly routingKeyPattern: string;
  /** When this operation fires / how it is triggered. */
  readonly when: string;
}

/** A documented address scheme on the channel. */
export interface AddressSchemeDoc {
  readonly scope: Scope;
  readonly pattern: string;
  readonly description: string;
}

/**
 * The whole contract, as a self-describing data object. The doc generator
 * renders Markdown and AsyncAPI purely from this — no hand-written prose is
 * duplicated between code and docs.
 */
export interface SseContract {
  readonly channel: string;
  readonly channelDescription: string;
  readonly addressSchemes: readonly AddressSchemeDoc[];
  readonly operations: readonly OperationDoc[];
  readonly envelopeFields: readonly FieldDoc[];
  readonly frameFields: readonly FieldDoc[];
}

export const SSE_CONTRACT: SseContract = {
  channel: SSE_CHANNEL,
  channelDescription:
    'Single broker topic backing the cross-instance SSE cluster. Every browser ' +
    'connection opens two transient (exclusive + auto-delete) subscriptions on ' +
    'this topic — one user-direct, one org-broadcast — so the broker fans ' +
    'messages across server instances with no presence registry.',
  addressSchemes: [
    {
      scope: 'user',
      pattern: 'user.{userId}',
      description:
        'User-direct routing key. A message published here reaches only the ' +
        'connection(s) for that single user.',
    },
    {
      scope: 'org',
      pattern: 'org.{orgId}',
      description:
        'Org-broadcast routing key. A message published here fans out to every ' +
        'connection in that org, across all server instances.',
    },
  ],
  operations: [
    {
      id: 'publish-to-user',
      summary: 'Publish a message directly to one user.',
      direction: 'publish',
      routingKeyPattern: 'user.{userId}',
      when: 'On `POST /publish` with `{ to: { type: "user", id } }`.',
    },
    {
      id: 'publish-to-org',
      summary: 'Broadcast a message to every member of an org.',
      direction: 'publish',
      routingKeyPattern: 'org.{orgId}',
      when: 'On `POST /publish` with `{ to: { type: "org", id } }`.',
    },
    {
      id: 'subscribe-user-stream',
      summary: 'Receive user-direct messages on an SSE connection.',
      direction: 'subscribe',
      routingKeyPattern: 'user.{userId}',
      when:
        'When a client opens `GET /events?userId&orgId`; bound as a transient ' +
        'subscription for the connection lifetime.',
    },
    {
      id: 'subscribe-org-stream',
      summary: 'Receive org-broadcast messages on an SSE connection.',
      direction: 'subscribe',
      routingKeyPattern: 'org.{orgId}',
      when:
        'When a client opens `GET /events?userId&orgId`; bound as a transient ' +
        'subscription for the connection lifetime.',
    },
  ],
  envelopeFields: [
    { name: 'from', type: 'string', description: 'Display name of the sender (defaults to `anon`).' },
    { name: 'text', type: 'string', description: 'Free-text body of the message.' },
    { name: 'to', type: 'object', description: 'The publish target: `{ type: "user" | "org", id }`.' },
    { name: 'via', type: 'string', description: 'Id of the instance that accepted and published the message.' },
    { name: 'at', type: 'string (ISO-8601)', description: 'Timestamp set when the message was published.' },
  ],
  frameFields: [
    { name: 'scope', type: '"user" | "org"', description: 'Which subscription matched: user-direct or org-broadcast.' },
    { name: 'instanceId', type: 'string', description: 'Id of the instance whose SSE stream delivered this frame.' },
    { name: 'message', type: 'SseEnvelope', description: 'The published message envelope.' },
  ],
};
