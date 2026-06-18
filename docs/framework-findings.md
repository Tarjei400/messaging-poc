# Framework findings — why this PoC is hand-rolled (and not Wolverine or MassTransit)

> This is the load-bearing finding of the whole evaluation. The brief asked for a
> "state of the art" .NET messaging experience. We evaluated the two leading .NET
> messaging frameworks — **Wolverine** and **MassTransit** — as the foundation
> and **rejected both** for the *specific* requirement that drives this project:
> **replacing Azure Service Bus _scheduled_ messaging, with Apache ActiveMQ
> Artemis as the preferred broker.**

## TL;DR

| Framework | Targets Artemis? | Broker-native scheduling on Artemis? | License (mid-2026) | Verdict for this project |
|---|---|---|---|---|
| **Wolverine** | ❌ no Artemis / AMQP-1.0 transport at all | ❌ n/a — can't reach Artemis | OSS | **Rejected** — cannot target the preferred broker |
| **MassTransit** | ⚠️ via the ActiveMQ transport (`EnableArtemisCompatibility()`) | ❌ **broker scheduling is broken on Artemis** | v8 OSS / **v9 commercial, no ActiveMQ** | **Rejected** — the one feature we need most doesn't work |
| **Hand-rolled `IMessageScheduler` (this repo)** | ✅ AMQP 1.0 (AMQPNetLite / rhea) | ✅ `x-opt-delivery-time`, verified live | OSS, no dependency | **Chosen** |

The hand-rolled port is the **only** approach that demonstrates Artemis
broker-native scheduling — the same `x-opt-delivery-time` annotation family Azure
Service Bus uses — across **both** languages and **both** brokers, with no paid
or end-of-life dependency.

## Wolverine

Wolverine is an excellent application framework (mediator + messaging + durable
inbox/outbox). Its native transports are **RabbitMQ, Azure Service Bus, Amazon
SQS/SNS, Kafka, MQTT, SQL-table, and an in-process `local`** transport.

- **No Apache Artemis transport, and no generic AMQP 1.0 transport.** Artemis is
  AMQP-1.0-native; Wolverine's RabbitMQ transport speaks AMQP 0.9.1. There is no
  supported way to point Wolverine at Artemis short of writing a custom transport
  over AMQPNetLite (weeks of unsupported work — a YAGNI violation and a permanent
  maintenance liability).
- **Scheduling is framework-managed, not broker-native.** Wolverine's
  `ScheduleAsync` / `DeliverAt` delegate to broker-native scheduling only when the
  transport supports it. RabbitMQ core does not, so on RabbitMQ Wolverine holds
  the scheduled envelope in **durable storage (Marten/EF Core) polled by the
  running nodes**. That is a perfectly good pattern — but it keeps the scheduling
  responsibility (and a database) inside your application, which is the opposite
  of "drop the dependency, let the broker schedule."

**Consequence:** Wolverine can't evaluate Artemis at all, so it cannot be the
basis of an Artemis-vs-RabbitMQ comparison.

## MassTransit

MassTransit *does* have an ActiveMQ transport (`MassTransit.ActiveMQ`) with an
`EnableArtemisCompatibility()` switch, and on it pub/sub, fanout, competing
consumers, retry/redelivery, `_error`/`_skipped` poison queues, and circuit
breakers all work against both RabbitMQ and Artemis. But three findings rule it
out for *this* project:

1. **Broker-native scheduling is broken on Artemis (the decisive one).**
   MassTransit's ActiveMQ scheduler sets the `AMQ_SCHEDULED_DELAY` header — an
   **ActiveMQ _Classic_ (5.x)** broker feature. **Artemis does not implement it**,
   so MassTransit-scheduled messages on Artemis are **delivered immediately, not
   delayed** (GitHub issue #3213). MassTransit does *not* target Artemis's
   `x-opt-delivery-time` / `_AMQ_SCHED_DELIVERY`. The only portable workaround is a
   transport-independent scheduler (Quartz.NET / Hangfire with its own persistent
   store) — which, like Wolverine, moves scheduling back into the application and
   defeats the "schedule at the broker" goal.
2. **Licensing.** **v8** (Apache-2.0, .NET 8) is the only free, ActiveMQ-capable
   line, but it loses support at the end of 2026. **v9** (released mid-2026, the
   latest line) is **commercial** (~$4k/yr for SMB) **and dropped the ActiveMQ
   transport entirely.** Building the PoC on MassTransit means pinning to the
   soon-unsupported v8.
3. **No manual-acknowledgement API.** MassTransit owns acknowledgement based on
   consumer-pipeline success; you express intent through retry/redelivery
   policies, not explicit `ack`/`nack`. That's fine in general, but it makes
   "explicit acknowledgement" a framework concept rather than the broker-level
   primitive this comparison wants to show side-by-side.

## What we kept instead

A thin `IMessageScheduler` port plus a sibling `IMessageBus` port (see
[comparison.md](./comparison.md)), with three interchangeable adapters per
language. This:

- Schedules **at the broker** on Artemis (`x-opt-delivery-time`) and supports
  cancel + list via the Artemis management API — the exact Azure Service Bus
  replacement story, **verified live** (scenarios S2/S3/S4).
- Has **no framework dependency**, so there is no licensing cliff and no hidden
  database requirement for scheduling.
- Keeps the comparison honest: every capability is *executed* and reported as
  `pass / ⊘ n-a / ✗`, including the places RabbitMQ genuinely cannot follow
  Artemis (cancel/list of scheduled messages).

> If a future requirement drops Artemis or relaxes broker-native scheduling, a
> framework becomes attractive again — Wolverine on RabbitMQ for its ergonomics,
> or MassTransit v8 on RabbitMQ. For *this* requirement set, hand-rolling the
> single seam is the smaller, more honest, dependency-free choice.
