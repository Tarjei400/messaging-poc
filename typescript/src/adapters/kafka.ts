import {
  Admin,
  Consumer,
  EachMessagePayload,
  Kafka,
  Producer,
  logLevel,
} from 'kafkajs';
import {
  AckHandler,
  BusCapabilities,
  Capabilities,
  Destination,
  IMessageBus,
  IMessageScheduler,
  IncomingMessage,
  MessageHandler,
  NotSupportedError,
  PublishOptions,
  ScheduleHandle,
  ScheduledInfo,
  SubscribeOptions,
  Subscription,
} from '../abstractions';

export interface KafkaConfig {
  brokers: string[]; // e.g. ['localhost:9092']
  clientId: string;
}

export function kafkaConfigFromEnv(): KafkaConfig {
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
  return {
    brokers,
    clientId: process.env.KAFKA_CLIENT_ID ?? 'messaging-poc',
  };
}

/**
 * Apache Kafka adapter — a deliberately faithful, minimal mapping.
 *
 * Kafka is a partitioned, append-only commit log, NOT a queue or a scheduler, so
 * several features the suite probes simply do not exist natively. Rather than
 * emulate them in the adapter (which would flatter Kafka and defeat the point of
 * the comparison), this adapter implements only what Kafka does on its own and
 * declares the rest honestly:
 *
 *  - scheduling (S2) / cancel (S3) / list (S4): Kafka has no broker-side timer
 *    and no API to remove a written record. `schedule` appends immediately
 *    (ignoring `deliverAt`), so S2 — which has no capability gate — surfaces as a
 *    real `✗` on early delivery; `cancel` and `listScheduled` throw
 *    `NotSupportedError`, so S3/S4 report the honest `⊘ unsupported`.
 *  - topic routing-key filtering (S5): no server-side selector → `supportsTopic`
 *    is false (⊘). A "topic" here is just a Kafka topic; we don't filter by key.
 *  - fanout (S6): each distinct `subscriberId` becomes its own consumer GROUP, so
 *    every group receives every record — true fan-out.
 *  - competing consumers (S9): a shared `subscriberId` → one group whose members
 *    split the topic's partitions.
 *  - message groups (S12): the publish `groupId` becomes the record KEY, which
 *    Kafka hashes to a fixed partition → per-key order, pinned to one consumer.
 *  - manual ack (S7/S18): consumers run with autoCommit off; `ack()` commits the
 *    offset, `nack(requeue)` seeks back to redeliver (best-effort — Kafka has no
 *    per-message redelivery counter, so `reportsDeliveryCount` is false).
 *  - dead-letter (S8/S11) / native dedup (S13): no native support → false (⊘).
 *  - stream replay (S19): reading from the earliest offset is core Kafka → true.
 *
 * Consumers read `fromBeginning` (earliest) so a subscriber that joins a moment
 * after a publish still sees the record — Kafka's "join at latest" default would
 * otherwise drop messages in these fast scenarios. Each scenario uses a unique
 * (nonce) topic, so reading from the start never replays unrelated data.
 */
export class KafkaScheduler implements IMessageScheduler, IMessageBus {
  readonly name = 'Apache Kafka';
  readonly busCapabilities: BusCapabilities = {
    supportsTopic: false, // no server-side routing-key/selector filtering
    supportsFanout: true, // distinct consumer groups each get every record
    supportsManualAck: true, // manual offset commit
    supportsDeadLetter: false, // no native DLQ (would need an app-managed topic)
    reportsDeliveryCount: false, // offsets, not a per-message delivery counter
    supportsDedup: false, // idempotent producer ≠ message-level dedup
    supportsStreamReplay: true, // reset to earliest offset is core to Kafka
    supportsMessageGroups: true, // record key → partition affinity
  };
  readonly capabilities: Capabilities = {
    protocol: 'Kafka',
    nativeScheduling: false, // no broker-side timer
    supportsCancel: false,
    supportsList: false,
    bus: this.busCapabilities,
  };

  private kafka?: Kafka;
  private producer?: Producer;
  private admin?: Admin;
  private readonly consumers: Consumer[] = [];
  private readonly topics = new Set<Destination>();
  private readonly partitions = 3; // spread for competing consumers (S9) + groups (S12)

  constructor(private readonly cfg: KafkaConfig) {}

  private async ensureClient(): Promise<void> {
    if (!this.kafka) {
      this.kafka = new Kafka({
        clientId: this.cfg.clientId,
        brokers: this.cfg.brokers,
        logLevel: logLevel.NOTHING,
      });
    }
    if (!this.producer) {
      this.producer = this.kafka.producer({ allowAutoTopicCreation: true });
      await this.producer.connect();
    }
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
    }
  }

  async connect(): Promise<void> {
    await this.ensureClient();
  }

  async connectBus(): Promise<void> {
    await this.ensureClient();
  }

  /** Create a topic on demand (idempotent). Kafka would auto-create with a
   *  single partition; we ask for several so partition-spread scenarios work. */
  private async ensureTopic(topic: Destination): Promise<void> {
    if (this.topics.has(topic)) return;
    await this.admin!.createTopics({
      topics: [{ topic, numPartitions: this.partitions }],
      waitForLeaders: true,
    });
    this.topics.add(topic);
  }

  // --- scheduler port -----------------------------------------------------

  async sendNow(destination: Destination, payload: string): Promise<void> {
    await this.ensureTopic(destination);
    await this.producer!.send({
      topic: destination,
      messages: [{ value: payload }],
    });
  }

  async schedule(
    destination: Destination,
    payload: string,
    deliverAt: Date,
  ): Promise<ScheduleHandle> {
    // Kafka has no broker-side delayed delivery: a record is appended (and so
    // becomes consumable) immediately, ignoring `deliverAt`. We honestly model
    // that rather than throwing — S2 then FAILS on early delivery (the real gap),
    // while S3/S4 can still reach cancel()/listScheduled() and report those as
    // `unsupported`. `nativeScheduling: false` already advertises the gap.
    await this.sendNow(destination, payload);
    const id = `kafka-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    return { id, destination, deliverAt };
  }

  async cancel(_handle: ScheduleHandle): Promise<void> {
    throw new NotSupportedError(
      'cancel',
      this.name,
      'a written log record cannot be removed before consumption',
    );
  }

  async listScheduled(_destination: Destination): Promise<ScheduledInfo[]> {
    throw new NotSupportedError(
      'listScheduled',
      this.name,
      'Kafka has no notion of pending, not-yet-delivered messages',
    );
  }

  async consume(
    destination: Destination,
    handler: MessageHandler,
  ): Promise<Subscription> {
    // Scheduler-port consume is auto-ack: a unique group reads every record.
    return this.startConsumer(destination, randomGroup('sched'), async (consumer, p) => {
      await handler({
        id: p.message.offset,
        destination,
        body: p.message.value?.toString('utf8') ?? '',
        headers: normalizeHeaders(p.message.headers),
      });
      await this.commit(consumer, p);
    });
  }

  // --- bus port -----------------------------------------------------------

  async publish(
    topic: Destination,
    payload: string,
    _routingKey?: string,
    options?: PublishOptions,
  ): Promise<void> {
    await this.ensureTopic(topic);
    const headers: Record<string, string> = { ...(options?.headers ?? {}) };
    if (options?.correlationId) headers.correlationId = options.correlationId;
    if (options?.replyTo) headers.replyTo = options.replyTo;
    await this.producer!.send({
      topic,
      messages: [
        {
          value: payload,
          // S12: groupId → record key → stable partition → one consumer.
          key: options?.groupId,
          headers,
        },
      ],
    });
  }

  async subscribe(
    topic: Destination,
    handler: AckHandler,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    await this.ensureTopic(topic);
    // The crux of the Kafka mapping: subscriberId IS the consumer group id.
    // Distinct ids → independent groups (fanout S6); a shared id → competing
    // consumers on one group (S9).
    const groupId = options.subscriberId ?? randomGroup('bus');
    return this.startConsumer(topic, groupId, async (consumer, p) => {
      const incoming = this.toIncoming(topic, consumer, p);
      // A handler that throws (a random/transient application error) must not
      // crash the consumer. Contain it and nack (seek-to-redeliver) rather than
      // let the exception propagate into kafkajs's run loop (S20). Mirrors the
      // in-memory reference's "throwing handler = redeliver".
      try {
        await handler(incoming);
      } catch {
        try {
          await incoming.nack(true);
        } catch {
          /* already settled */
        }
      }
    });
  }

  /** Wire up one kafkajs consumer for a (topic, groupId) and run the loop. */
  private async startConsumer(
    topic: Destination,
    groupId: string,
    onMessage: (consumer: Consumer, p: EachMessagePayload) => Promise<void>,
  ): Promise<Subscription> {
    const consumer = this.kafka!.consumer({
      groupId,
      // Short timeouts so group join / rebalance keep up with the suite's
      // few-second wait windows rather than Kafka's slower production defaults.
      sessionTimeout: 6000,
      heartbeatInterval: 2000,
      rebalanceTimeout: 6000,
    });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    this.consumers.push(consumer);
    await consumer.run({
      autoCommit: false, // we settle explicitly via ack()/nack()
      eachMessage: (p) => onMessage(consumer, p),
    });
    return {
      unsubscribe: async () => {
        try {
          await consumer.disconnect();
        } catch {
          /* already disconnected */
        }
        const i = this.consumers.indexOf(consumer);
        if (i >= 0) this.consumers.splice(i, 1);
      },
    };
  }

  /** Commit the offset AFTER this record (Kafka commits the next offset to read). */
  private async commit(consumer: Consumer, p: EachMessagePayload): Promise<void> {
    await consumer.commitOffsets([
      {
        topic: p.topic,
        partition: p.partition,
        offset: (Number(p.message.offset) + 1).toString(),
      },
    ]);
  }

  private toIncoming(
    topic: Destination,
    consumer: Consumer,
    p: EachMessagePayload,
  ): IncomingMessage {
    const headers = normalizeHeaders(p.message.headers);
    let settled = false;
    return {
      id: p.message.offset,
      destination: topic,
      body: p.message.value?.toString('utf8') ?? '',
      headers,
      deliveryCount: undefined, // Kafka has no per-message redelivery counter
      replyTo: headers.replyTo,
      correlationId: headers.correlationId,
      groupId: p.message.key?.toString('utf8') ?? undefined,
      priority: undefined, // no native priority
      ack: async () => {
        if (settled) return;
        settled = true;
        await this.commit(consumer, p);
      },
      nack: async (requeue: boolean) => {
        if (settled) return;
        settled = true;
        if (requeue) {
          // Best-effort redelivery: rewind this partition to re-read the record
          // on the next poll. There is no broker-side requeue or attempt counter.
          consumer.seek({
            topic: p.topic,
            partition: p.partition,
            offset: p.message.offset,
          });
        } else {
          // Give up: advance past it (no dead-letter address exists).
          await this.commit(consumer, p);
        }
      },
    };
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await Promise.allSettled(this.consumers.map((c) => c.disconnect()));
    this.consumers.length = 0;
    await this.producer?.disconnect();
    await this.admin?.disconnect();
  }
}

function randomGroup(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeHeaders(
  headers?: EachMessagePayload['message']['headers'],
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers ?? {})) {
    if (v == null) continue;
    out[k] = Buffer.isBuffer(v) ? v.toString('utf8') : String(v);
  }
  return out;
}
