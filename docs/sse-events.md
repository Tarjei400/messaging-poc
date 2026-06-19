# SSE event catalog

> Generated from `typescript/src/sse/domain/contract.ts` via `npm run sse:docs`. Do not edit by hand.

## Channel `mbc.sse`

Single broker topic backing the cross-instance SSE cluster. Every browser connection opens two transient (exclusive + auto-delete) subscriptions on this topic — one user-direct, one org-broadcast — so the broker fans messages across server instances with no presence registry.

## Address schemes

| Scope | Routing key | Description |
| --- | --- | --- |
| `user` | `user.{userId}` | User-direct routing key. A message published here reaches only the connection(s) for that single user. |
| `org` | `org.{orgId}` | Org-broadcast routing key. A message published here fans out to every connection in that org, across all server instances. |

## Operations

### `publish-to-user` (publish)

Publish a message directly to one user.

- **Direction:** publish
- **Routing key:** `user.{userId}`
- **When:** On `POST /publish` with `{ to: { type: "user", id } }`.

### `publish-to-org` (publish)

Broadcast a message to every member of an org.

- **Direction:** publish
- **Routing key:** `org.{orgId}`
- **When:** On `POST /publish` with `{ to: { type: "org", id } }`.

### `subscribe-user-stream` (subscribe)

Receive user-direct messages on an SSE connection.

- **Direction:** subscribe
- **Routing key:** `user.{userId}`
- **When:** When a client opens `GET /events?userId&orgId`; bound as a transient subscription for the connection lifetime.

### `subscribe-org-stream` (subscribe)

Receive org-broadcast messages on an SSE connection.

- **Direction:** subscribe
- **Routing key:** `org.{orgId}`
- **When:** When a client opens `GET /events?userId&orgId`; bound as a transient subscription for the connection lifetime.

## Message envelope

The payload published to the channel and re-emitted as the `message` of an SSE frame.

| Field | Type | Description |
| --- | --- | --- |
| `from` | `string` | Display name of the sender (defaults to `anon`). |
| `text` | `string` | Free-text body of the message. |
| `to` | `object` | The publish target: `{ type: "user" | "org", id }`. |
| `via` | `string` | Id of the instance that accepted and published the message. |
| `at` | `string (ISO-8601)` | Timestamp set when the message was published. |

## SSE frame

The `data:` frame delivered to a connected client.

| Field | Type | Description |
| --- | --- | --- |
| `scope` | `"user" | "org"` | Which subscription matched: user-direct or org-broadcast. |
| `instanceId` | `string` | Id of the instance whose SSE stream delivered this frame. |
| `message` | `SseEnvelope` | The published message envelope. |
