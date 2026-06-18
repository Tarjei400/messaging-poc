# Scheduled-Messaging Broker Comparison — runnable code

A side-by-side, **runnable** comparison of two candidate brokers for replacing
Azure Service Bus scheduled messaging — **Apache ActiveMQ Artemis** and
**RabbitMQ** — implemented twice, once in **TypeScript** and once in **.NET 8**,
behind two shared abstractions, and exercised through a production-style
**retry + circuit-breaker** resilience layer. It covers scheduling **and**
pub/sub, fanout, explicit acknowledgement, and fault tolerance.

| Broker | Protocol | Native scheduling | Cancel scheduled | Pub/sub · Fanout · Manual-ack · Dead-letter |
|---|---|---|---|---|
| **Apache ActiveMQ Artemis** | AMQP 1.0 | yes | yes (via management) | yes · yes · yes · yes |
| **RabbitMQ** (delayed-message plugin) | AMQP 0.9.1 | no (plugin) | **no** | yes · yes · yes · yes |

The point of this repo is not prose claims — it is to **run the same scenarios
against each broker and watch the outcomes print in the terminal**, including the
places where a broker honestly cannot do something. See
[docs/comparison.md](docs/comparison.md) for the full executed matrix and
[docs/framework-findings.md](docs/framework-findings.md) for why this is a
hand-rolled port rather than Wolverine or MassTransit.

---

## What it demonstrates

Every requirement is turned into an executable scenario that runs identically
against both brokers in both languages. Two suites, two ports:

**Scheduling suite** (`IMessageScheduler` — the Azure Service Bus replacement core):

- **S1 — immediate send/receive**: baseline connectivity.
- **S2 — scheduled delivery**: a message is withheld until its delivery time
  (the core requirement).
- **S3 — cancel scheduled**: the key discriminator — cancel before firing.
  RabbitMQ cannot do this and reports `⊘ n/a`.
- **S4 — list pending**: inspect still-pending scheduled messages (RabbitMQ `⊘`).

**Pub/sub suite** (`IMessageBus` — fan-out + settlement):

- **S5 — pub/sub (topic)**: routing-key-filtered subscribers each get only their
  matching events.
- **S6 — fanout multicast**: one publish reaches N independent subscribers.
- **S7 — explicit ack**: manual ack removes; nack requeues; a crashed consumer
  triggers redelivery.
- **S8 — poison → dead-letter**: an always-failing message is dead-lettered after
  N attempts.
- **S9 — competing consumers**: work is shared across consumers on one queue, no
  duplicates.

**Fault-tolerance demos** (`scripts/run-faults.sh`, narrated live timeline):
consumer crash → redelivery, broker disconnect → retry → circuit-breaker →
reconnect, and poison → dead-letter — on both brokers.

A broker that lacks a capability does **not** silently no-op and does **not**
count as a failure. The adapter throws a typed `NotSupported` error (or declares
it via capabilities), the runner reads the self-declared capabilities, and the
result prints as an expected gap (`⊘`) — distinct from a real failure (`✗`).

---

## How the design maps to the principles

The same shape exists in both languages. One interface is the only thing the
application, the scenarios, the runner, and the tests ever depend on.

- **Dependency Inversion** — everything depends on the `IMessageScheduler` port
  (scheduling) and its sibling `IMessageBus` port (pub/sub + ack), never on a
  concrete broker client. Those ports *are* the "internal abstraction" the
  research recommends building before any cutover, so this code doubles as the
  migration seam.
- **Interface Segregation** — scheduling and pub/sub are separate ports, so a
  scheduler-only adapter never grows topic/ack methods, and the scheduling
  scenarios are untouched by the bus work.
- **Single Responsibility** — each adapter has exactly one reason to change: its
  broker. Resilience is a separate concern in a separate type.
- **Open/Closed** — adding a broker is "write an adapter, add one line to the
  factory." Adding a scenario is "write it, add one line to the registry."
- **DRY** — the scenario suite is written once and executed against every
  adapter, including the in-memory reference.
- **YAGNI** — exactly four scenarios, mapping to the actual requirements.
- **Liskov** — the in-memory reference, every real adapter, and the resilience
  decorator are interchangeable behind the port; the suite cannot tell which one
  it is driving.

```
            ┌─────────────────────────────────────────────┐
            │   Scenario suite  +  Runner  +  Unit tests   │
            │     (know only the port, never a broker)     │
            └───────────────────────┬─────────────────────┘
                                    │ depends on
                       ┌────────────▼────────────┐
                       │     IMessageScheduler    │   ← the one seam
                       └────────────┬────────────┘
                  decorated by      │
                ┌──────────────────▼───────────────────┐
                │  Resilient*Scheduler (Decorator)      │
                │  retry + circuit breaker, and treats  │
                │  "not supported" as a contract result │
                │  .NET: Polly v8   TS: cockatiel       │
                └──────────────────┬───────────────────┘
                  wraps            │
        ┌───────────────┬─────────┼───────────────┐
        ▼               ▼         ▼               ▼
    InMemory        Artemis    RabbitMQ      (next broker:
   (reference)    AMQP1.0 +   delayed-msg     add adapter +
                  Jolokia mgmt   plugin       one factory line)
```

---

## Resilience layer (retry + circuit breaker)

Every real broker is wrapped in a resilience **decorator** that adds
retry-with-backoff and a circuit breaker around the broker calls — the same
shape you would run in production — without any adapter knowing it exists.

- **.NET** uses **Polly v8** (`ResiliencePipelineBuilder` with `AddRetry` +
  `AddCircuitBreaker`) in `Messaging.Resilience/ResilientMessageScheduler.cs`.
- **TypeScript** uses **cockatiel** (a Polly-inspired, TypeScript-first library)
  in `src/resilience/resilient-scheduler.ts`.

The important design point in both: a `NotSupported` error is treated as a
**contract outcome, not a transient fault**. It is never retried and never trips
the breaker, so RabbitMQ's missing cancel still surfaces cleanly as
"unsupported" instead of looking like an outage. Both test suites assert this.

The factory wraps adapters by default; pass `resilient: false`
(.NET `BrokerFactory.Create(id, resilient: false)`) or `createScheduler(id, false)`
(TS) to get a raw adapter.

> **Why hand-rolled and not Wolverine / MassTransit?** This is the load-bearing
> finding of the evaluation, covered in full in
> [docs/framework-findings.md](docs/framework-findings.md). In short: **Wolverine**
> has no Artemis/AMQP-1.0 transport at all (so it can't evaluate the top
> candidate), and **MassTransit**'s ActiveMQ transport **can't schedule on Artemis**
> — its scheduler uses the ActiveMQ *Classic* `AMQ_SCHEDULED_DELAY` header, which
> Artemis ignores (messages fire immediately, GitHub #3213) — while its v9 line is
> commercial and dropped ActiveMQ entirely. The hand-rolled port is the only
> approach that schedules **at the broker** on Artemis (`x-opt-delivery-time`,
> verified live) with no paid/EOL dependency.

---

## Repository layout

```
typescript/
  src/
    abstractions/      both ports (message-scheduler, message-bus), types, errors
    adapters/          in-memory(+bus engine), artemis, rabbitmq (+ factory)
    resilience/        cockatiel retry + circuit-breaker decorator (+ onEvent hook)
    scenarios/         s1–s4 (scheduling) + s5–s9 (bus), helpers, registries
    interactive/       publish · subscribe · worker · schedule (multi-terminal demos)
    fault/             fault-injecting bus + timeline narrator
    runner.ts          runs both suites, prints the capability-aware table
    ansi.ts            shared terminal palette/glyphs
    index.ts / fault.ts  CLI entry points (suite / fault demos)
  test/                vitest: suites + bus + fault + graceful-degradation
dotnet/                mirror of the above (Messaging.* projects, xUnit tests)
infra/
  docker-compose.yml          both brokers (Artemis 5672/8161, RabbitMQ 5673/15672)
  rabbitmq.Dockerfile         RabbitMQ image with the delayed-message plugin
  artemis/broker.xml          DLA-aware broker config (mounted over etc/broker.xml)
scripts/
  run-all.sh                  up the brokers, run every broker × both languages
  run-faults.sh               narrated fault-tolerance demos × both languages
  demo-tmux.sh                tmux layout for the live multi-terminal pub/sub demo
docs/
  comparison.md               the executed capability matrix + scale trade-offs
  framework-findings.md       why hand-rolled, not Wolverine / MassTransit
```

---

## Running it

### Prerequisites
- Docker + Docker Compose
- Node.js ≥ 20 (for the TypeScript suite)
- .NET SDK 8 (for the .NET suite)

### 1. Start the brokers
```bash
docker compose -f infra/docker-compose.yml up -d --build
```
Artemis listens for AMQP on `5672` with the console/Jolokia on `8161`; RabbitMQ
(with the delayed-message plugin) is on AMQP `5673` and the management UI on
`15672`.

### 2a. Run a single broker, TypeScript
```bash
cd typescript
npm install
npm run scenarios -- artemis        # or: rabbitmq | in-memory | all
```

### 2b. Run a single broker, .NET
```bash
cd dotnet
dotnet run --project src/Messaging.Runner -- artemis   # or rabbitmq | in-memory | all
```

### 3. Run everything at once (both languages, both brokers)
```bash
scripts/run-all.sh
```

### 4. Narrated fault-tolerance demos
```bash
scripts/run-faults.sh             # all brokers, both languages
scripts/run-faults.sh in-memory   # no broker needed (faults are injected)
```

### 5. Live multi-terminal demo (pub/sub, fanout, competing, DLQ)

The interactive processes are thin CLIs over the same adapters — open several
terminals and watch behavior happen live. `scripts/demo-tmux.sh` lays this out
automatically, or do it by hand (TypeScript shown; `.NET` mirrors via
`dotnet run --project src/Messaging.Interactive -- <verb> …`):

```bash
# Terminal 1 & 2 — two fanout subscribers (each gets every message):
npm run demo:subscribe -- artemis orders --id A --kind fanout
npm run demo:subscribe -- artemis orders --id B --kind fanout
# Terminal 3 — publish 5; both A and B receive all 5:
npm run demo:publish   -- artemis orders --count 5 --kind fanout

# Competing consumers — same --id ⇒ the load is SHARED (no duplicates):
npm run demo:subscribe -- artemis tasks --id pool   # run twice
npm run demo:publish   -- artemis tasks --count 10

# Poison → dead-letter — a flaky worker + a DLQ watcher:
npm run demo:subscribe -- artemis jobs.dlq --kind fanout --id dlq
npm run demo:worker    -- artemis jobs --fail-rate 1
npm run demo:publish   -- artemis jobs --count 1

# Scheduling (the core) — schedule, list, and cancel, in one terminal:
npm run demo:schedule  -- artemis reminders --in 5 --list
npm run demo:schedule  -- artemis reminders --in 5 --cancel
```

### Tear down
```bash
docker compose -f infra/docker-compose.yml down -v
```

---

## Expected outcomes

Artemis passes all nine scenarios. RabbitMQ passes everything except S3–S4
(reported `⊘ n/a`), because the delayed-message plugin keeps pending messages in
a node-local store with no API to enumerate or remove an individual one — the
exact limitation the comparison flags, now demonstrated rather than asserted. An
actual Artemis run (TypeScript):

```
Apache ActiveMQ Artemis + Cockatiel
protocol=AMQP 1.0  native-scheduling=yes  cancel=yes  list=yes
pub/sub=yes  fanout=yes  manual-ack=yes  dead-letter=yes  delivery-count=yes
────────────────────────────────────────────────────────────────────────────
  ✓ pass  S1 immediate send/receive        55ms  delivered immediately
  ✓ pass  S2 scheduled delivery          3010ms  withheld then delivered after ~3000ms
  ✓ pass  S3 cancel scheduled            5049ms  scheduled message successfully cancelled
  ✓ pass  S4 list pending                  31ms  reported 2 pending
  ✓ pass  S5 pub/sub (topic)               61ms  order.created→1 sub, order.#→both (2)
  ✓ pass  S6 fanout multicast             77ms  1 publish → 3 subscribers each received it
  ✓ pass  S7 explicit ack                752ms  ack→once; nack→redelivered (count=2); crash→redelivered
  ✓ pass  S8 poison → dead-letter          73ms  dead-lettered after 3 attempts (final deliveryCount=3)
  ✓ pass  S9 competing consumers           72ms  split 5/5, no dupes
────────────────────────────────────────────────────────────────────────────
  9 passed, 0 unsupported, 0 failed
```

For RabbitMQ, S3/S4 read `⊘ n/a … no cancel/inspection API`, S7 notes
"redelivered (count n/a)" (classic queues expose only a `redelivered` flag), and
S8 dead-letters after 4 attempts (quorum queues dead-letter when the count
*exceeds* the limit) — the summary is `7 passed, 2 unsupported, 0 failed`. The
process exits non-zero only on a real failure (`✗`); `⊘ unsupported` never fails
the run.

---

## Testability (no broker required)

The whole suite runs against an in-memory reference adapter with zero
infrastructure, which is what CI uses:

```bash
cd typescript && npm test          # vitest
cd dotnet && dotnet test           # xUnit
```

These tests prove, without any container running: (1) both suites (scheduling
S1–S4 and pub/sub S5–S9) pass end-to-end against the reference implementation;
(2) a deliberately limited fake that cannot cancel/list — and has no bus port —
is reported as `unsupported` rather than `failed`; (3) the resilience decorator
preserves both behaviours and does not trip the breaker on an honestly-
unsupported operation; and (4) the fault-tolerance behaviours (retry absorbs a
blip, the breaker opens under a sustained outage, a crashed consumer's message is
redelivered) hold, driven by an injectable fault decorator over the in-memory
reference.

---

## Broker-specific notes

- **Artemis** schedules with the AMQP annotation `x-opt-delivery-time` (absolute
  epoch-ms) — the same annotation family Azure Service Bus uses, which is what
  makes it the low-friction target. Cancel and list have no AMQP verb, so they go
  through the broker's `QueueControl` MBean over Jolokia (HTTP/JSON). Each
  scheduled message is tagged with a `scheduleId` application property so cancel
  can target it precisely.
- **RabbitMQ** requires the `rabbitmq_delayed_message_exchange` plugin, baked
  into `infra/rabbitmq.Dockerfile`. Scheduling is publish-to-`x-delayed-message`
  exchange with an `x-delay` header. Cancellation and enumeration of pending
  delayed messages are genuinely unsupported by the plugin, which is why those
  scenarios report `⊘`.

### Bus (pub/sub) mappings

- **Artemis** — pub/sub and fanout use **multicast addresses**; independent
  subscribers attach to the FQQN `address::subscriberId` (the `topic` capability
  forces multicast on auto-create), and topic routing-key filters become AMQP/JMS
  **selectors** on a `routingKey` property. Manual ack = `accept`; nack-requeue =
  `modified(deliveryFailed)`; reject → DLA. Dead-lettering is broker-native via
  `max-delivery-attempts` + a multicast `dead-letter-address` (see
  `infra/artemis/broker.xml`). The scheduler keeps using **anycast** (the `queue`
  capability) so the Jolokia cancel/list lookups work.
- **RabbitMQ** — pub/sub uses a `topic` exchange and fanout a `fanout` exchange
  per topic; each subscriber gets its own queue (distinct `subscriberId`) or
  shares one (same `subscriberId`, for competing consumers). **Each subscription
  uses its own channel** so closing it requeues unacked messages (real
  crash→redelivery). Dead-lettering uses a **quorum queue** with `x-delivery-limit`
  + a dead-letter exchange. Classic queues report only a `redelivered` flag, so
  precise delivery counts are `n/a`.
