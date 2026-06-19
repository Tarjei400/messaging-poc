# Artemis vs RabbitMQ — capability comparison (executed, not asserted)

Every row below is produced by **running the scenario suite**, identically, in
**both TypeScript and .NET**, against each broker. `pass` = behavior
demonstrated; `⊘ n-a` = an *honest, declared* gap (not a failure); `✗` = a real
break. Reproduce with `scripts/run-all.sh` and the fault demos with
`scripts/run-faults.sh`.

For why this is a hand-rolled port rather than Wolverine/MassTransit, see
[framework-findings.md](./framework-findings.md).

## Declared capabilities (printed in each run's header)

| Capability | In-Memory (ref) | Apache ActiveMQ Artemis | RabbitMQ |
|---|---|---|---|
| Protocol | in-memory | **AMQP 1.0** | AMQP 0.9.1 |
| Native scheduling | yes | **yes** (`x-opt-delivery-time`) | no (delayed-message **plugin**) |
| Cancel scheduled | yes | **yes** (Jolokia mgmt) | **no** (plugin has no API) |
| List scheduled | yes | **yes** (Jolokia mgmt) | **no** (node-local Mnesia) |
| Pub/sub (topic) | yes | yes (multicast + selector) | yes (`topic` exchange) |
| Fanout | yes | yes (multicast queues) | yes (`fanout` exchange) |
| Manual ack | yes | yes (accept/modify/reject) | yes (ack / nack) |
| Dead-letter | yes | yes (`max-delivery-attempts` + DLA) | yes (quorum `x-delivery-limit` + DLX) |
| Reports delivery count | yes | **yes** (AMQP `delivery-count`) | **no** (classic = `redelivered` flag only) |
| Broker-native producer dedup | yes | **yes** (`amqpDuplicateDetection`, `_AMQ_DUPL_ID`) | **no** (no native producer dedup) |
| Stream replay from offset | yes | **no** (no offset replay of consumed history) | **yes** (`x-queue-type=stream`) |

## Scenario outcomes (TypeScript + .NET, identical)

| # | Scenario | In-Memory | Artemis | RabbitMQ |
|---|---|---|---|---|
| S1 | Immediate send/receive | pass | pass | pass |
| S2 | Scheduled delivery (withheld, then delivered) | pass | **pass** | pass |
| S3 | Cancel scheduled before it fires | pass | **pass** | **⊘ n-a** |
| S4 | List pending scheduled | pass | **pass** | **⊘ n-a** |
| S5 | Pub/sub with routing-key filter | pass | pass | pass |
| S6 | Fanout multicast (1 → N) | pass | pass | pass |
| S7 | Explicit ack / nack-requeue / crash-redelivery | pass | pass | pass¹ |
| S8 | Poison → dead-letter after N attempts | pass | pass² | pass³ |
| S9 | Competing consumers (work sharing, no dupes) | pass | pass | pass |
| S11 | Non-blocking retry queue → dead-letter after 5 retries | pass | pass⁴ | pass⁴ |
| S12 | Ordered delivery / message groups (per-group FIFO, pinned consumer) | pass | pass⁵ | pass⁵ |
| S13 | Broker-native producer dedup (same id twice → delivered once) | pass | pass⁶ | **⊘ n-a**⁶ |
| S14 | Priority (high-priority overtakes a low-priority backlog) | pass | pass⁷ | pass⁷ |
| S15 | Request/reply (RPC over reply-to + correlation-id) | pass | pass⁸ | pass⁸ |
| S16 | TTL → expiry destination (distinct from DLQ) | pass | pass⁹ | pass⁹ |
| S17 | Durable subscription (offline messages retained, replayed on reattach) | pass | pass¹⁰ | pass¹⁰ |
| S18 | Single active consumer / failover (standby takes over, order preserved) | pass | pass¹¹ | pass¹¹ |
| S19 | Stream replay from offset (fresh subscriber replays full history) | pass | **⊘ n-a**¹² | pass¹² |

S10 (idempotent consumer, app-level dedup over an external store) is a
deliberate work-in-progress and is intentionally omitted from this matrix.

Fault-tolerance demos (`scripts/run-faults.sh`): consumer-crash → redelivery,
broker-disconnect → retry → circuit-breaker → reconnect, and poison →
dead-letter, all narrated live on both brokers.

The two `⊘ n-a` cells (S13 on RabbitMQ, S19 on Artemis) are **deliberate, honest
capability gaps**, declared up front via `BusCapabilities` (`supportsDedup`,
`supportsStreamReplay`) — not failures. Each broker is green for the feature the
*other* one lacks; the gaps are the point of the exercise.

¹ RabbitMQ classic queues report only a `redelivered` boolean, so S7 shows
"redelivered (count n/a)" — a *measured* difference, still a pass.
² Artemis dead-letters at **exactly** `max-delivery-attempts` (3).
³ RabbitMQ quorum queues dead-letter when the count **exceeds** the limit (4
attempts). Both are correct; the scenario asserts "bounded, then dead-lettered,"
not an exact count.
⁴ S11 wires the same observable behaviour two ways: RabbitMQ uses an explicit
**retry queue** (a parking queue with `x-message-ttl` + a dead-letter exchange
that bounces the message back to the main queue), while Artemis uses native
`redelivery-delay` + `max-delivery-attempts` configured for `mbc.s11.#` in
broker.xml. Both park the failing message off the main queue, so a batch of
healthy messages drains immediately while the poison message is retried 5× and
then dead-lettered — the scenario detail reports both timings for comparison.
⁵ S12 pins each message **group** to a single consumer for per-group FIFO.
Artemis uses native **message groups** (`_AMQ_GROUP_ID`): the broker pins a group
to one consumer for as long as it is active. RabbitMQ has no equivalent, so it
routes through a **consistent-hash exchange** (`rabbitmq_consistent_hash_exchange`
plugin) that hashes `groupId` to one of N per-consumer queues — same observable
guarantee, different mechanism. (Hashing can land every group on one consumer; the
scenario asserts per-group order, not an even split.)
⁶ S13 publishes the same `dedupId` twice and asserts a single delivery. Artemis
has **broker-native duplicate detection** (`_AMQ_DUPL_ID`, `amqpDuplicateDetection`
already on) → the duplicate is dropped at the broker. RabbitMQ has **no native
producer dedup**, so it declares `supportsDedup=false` and the scenario reports
`⊘ n-a` — honestly, rather than faking it (contrast S10, which is *app-level*
dedup over an external store).
⁷ S14: Artemis honours the AMQP **message priority** header natively. RabbitMQ
requires the queue to be declared with `x-max-priority` (the `priorityQueue`
option) plus `BasicProperties.Priority`. Both let a priority-9 message overtake a
low-priority backlog.
⁸ S15 (RPC) is portable: the responder echoes to the request's **`reply-to`**
with the same **`correlation-id`**; both brokers carry these as first-class
message properties.
⁹ S16 routes an expired message to a dedicated **expiry** destination (distinct
from the DLQ). Artemis maps `mbc.s16.#` → `expiry-address=mbc.EXPIRY` in
`broker.xml` (a new multicast address mirroring `mbc.DLQ`). RabbitMQ uses a queue
`x-message-ttl` plus a dead-letter exchange that fans the expired message to the
expiry destination.
¹⁰ S17: both brokers retain messages for an absent subscriber on a **durable**
queue (`auto-delete=false`); resubscribing with the same id replays everything
published while offline. No API change was needed — the queues were already
durable.
¹¹ S18 (failover): only one of two consumers on a shared queue is active until it
drops, then the standby takes over with order preserved. Artemis uses an
**exclusive** consumer; RabbitMQ uses **`x-single-active-consumer`**.
¹² S19 replays the full history to a fresh subscriber from offset 0. RabbitMQ
uses **stream queues** (`x-queue-type=stream`, consumed with
`x-stream-offset=first`, manual ack + qos) — no extra plugin. Artemis has **no
stream/offset replay** of already-consumed history, so it declares
`supportsStreamReplay=false` and reports `⊘ n-a`. This is the mirror of S13: each
broker covers the gap the other leaves.

## The decisive differences

1. **Scheduling is broker-native on Artemis, bolted-on for RabbitMQ.** Artemis
   uses the `x-opt-delivery-time` AMQP annotation — the same family Azure Service
   Bus uses — so migration is conceptually a connection-string-and-client swap.
   RabbitMQ needs the `rabbitmq_delayed_message_exchange` plugin, whose own docs
   warn it is not a long-term scheduler.
2. **Cancellation & inspection of scheduled work.** Artemis exposes
   `listScheduledMessages` / `removeMessages` via management (Jolokia), giving a
   real "schedule now, cancel later" story. The RabbitMQ plugin stores pending
   messages in a **node-local Mnesia table with no enumerate/remove API** — so
   cancel and list are genuinely impossible (reported as `⊘ n-a`, not faked).
3. **Delivery-count fidelity.** Artemis reports a precise per-message
   `delivery-count`; RabbitMQ classic queues report only `redelivered`. Use
   **quorum queues** on RabbitMQ when you need a deterministic delivery limit
   (we do, for dead-lettering).

## Scale & operational trade-offs

| Dimension | Apache ActiveMQ Artemis | RabbitMQ |
|---|---|---|
| Scheduling at scale | Broker journal holds scheduled messages durably; `scheduled.message.count` is a first-class metric to alert on. | Delayed-plugin messages live in a **single node-local Mnesia replica**; lost if that node is lost or the plugin is disabled. Intended for short delays. |
| Throughput / fan-out | Multicast addresses, paging to disk, mature clustering. | Very high throughput; quorum queues for durable HA; classic queues cheap but less safe. |
| HA model | Replication or shared-store primary/backup; only persisted data survives failover. | Quorum queues (Raft) for the live path; the delayed plugin is **not** covered by quorum guarantees. |
| Cancellation story | First-class via management API. | None for scheduled messages. |
| Ops burden | Medium — addresses/queues, HA topology, management API. | Low day-one; the scheduling path is the weak spot to monitor with app-level metrics. |
| Protocol fit vs Azure Service Bus | **High** — AMQP 1.0, scheduled-enqueue + cancel semantics line up. | Medium — AMQP 0.9.1 ecosystem; scheduling is a constrained extension. |
| Framework support (.NET) | No first-class framework (Wolverine/MassTransit can't schedule on it — see findings). | Wolverine & MassTransit both support it (framework-managed scheduling). |

## Recommendation

For **replacing Azure Service Bus scheduled messaging**, **Apache ActiveMQ
Artemis** is the cleanest landing zone: broker-native scheduling over AMQP 1.0
with a real cancel/inspect story — demonstrated here end-to-end in both
languages. **RabbitMQ** remains excellent for pub/sub, fanout, competing
consumers and dead-lettering (all green above), but its scheduled-message
support is a plugin with material limitations and **no cancellation**, which is
backwards for a system whose core purpose is firing actions at a future time.

> Open items to validate before production (per the founding research): Artemis
> behavior of scheduled messages across failover in your chosen HA mode, and the
> maximum practical schedule horizon.
