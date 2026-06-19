import amqp, { Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import {
  AckHandler,
  BusCapabilities,
  Capabilities,
  DEFAULT_MAX_DELIVERIES,
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
  deadLetterAddress,
  expiryAddress,
} from '../abstractions';

export interface RabbitConfig {
  url: string; // amqp://user:pass@host:5672
  exchange: string; // delayed-message exchange name
}

export function rabbitConfigFromEnv(): RabbitConfig {
  const host = process.env.RABBITMQ_HOST ?? 'localhost';
  const port = process.env.RABBITMQ_PORT ?? '5673';
  const user = process.env.RABBITMQ_USER ?? 'guest';
  const pass = process.env.RABBITMQ_PASSWORD ?? 'guest';
  return {
    url: process.env.RABBITMQ_URL ?? `amqp://${user}:${pass}@${host}:${port}`,
    exchange: process.env.RABBITMQ_EXCHANGE ?? 'mbc.delayed',
  };
}

/**
 * RabbitMQ adapter.
 *
 * Scheduling (S1–S4) uses the `rabbitmq_delayed_message_exchange` plugin, whose
 * pending messages live in a node-local Mnesia table with no enumerate/remove
 * API — so cancel/list are honestly unsupported.
 *
 * The bus surface (S5–S9) maps onto core RabbitMQ:
 *  - pub/sub  → a `topic` exchange per logical topic, per-subscriber queues;
 *  - fanout   → a `fanout` exchange per topic;
 *  - ack/nack → manual acknowledgement (`noAck:false`), `basicNack(requeue)`;
 *  - DLQ      → quorum queue + `x-delivery-limit` + a dead-letter exchange.
 *
 * RabbitMQ classic queues report only a `redelivered` boolean (not a precise
 * count), so `reportsDeliveryCount` is false — surfaced as a measured difference
 * from Artemis, not a failure.
 */
export class RabbitMqScheduler implements IMessageScheduler, IMessageBus {
  readonly name = 'RabbitMQ (delayed-message plugin)';
  readonly busCapabilities: BusCapabilities = {
    supportsTopic: true,
    supportsFanout: true,
    supportsManualAck: true,
    supportsDeadLetter: true,
    reportsDeliveryCount: false, // classic queues expose only a redelivered flag
    supportsDedup: false, // no native producer dedup (cf. app-level S10)
    supportsStreamReplay: true, // stream queues replay from an offset
    supportsMessageGroups: true, // consistent-hash exchange pins groupId→one queue
  };
  readonly capabilities: Capabilities = {
    protocol: 'AMQP 0.9.1',
    nativeScheduling: false, // plugin, not core broker
    supportsCancel: false,
    supportsList: false,
    bus: this.busCapabilities,
  };

  private model?: ChannelModel;
  private channel?: Channel;
  private readonly topology = new Set<Destination>();
  private readonly busTopology = new Set<Destination>();
  private readonly hashTopology = new Set<Destination>();

  constructor(private readonly cfg: RabbitConfig) {}

  private async ensureChannel(): Promise<Channel> {
    if (!this.model) this.model = await amqp.connect(this.cfg.url);
    if (!this.channel) {
      this.channel = await this.model.createChannel();
      await this.channel.prefetch(1); // fair dispatch for competing consumers
    }
    return this.channel;
  }

  async connect(): Promise<void> {
    const ch = await this.ensureChannel();
    await ch.assertExchange(this.cfg.exchange, 'x-delayed-message', {
      durable: true,
      arguments: { 'x-delayed-type': 'direct' },
    });
  }

  async connectBus(): Promise<void> {
    await this.ensureChannel();
  }

  // --- scheduler port -----------------------------------------------------

  private queueName(destination: Destination): string {
    return `q.${destination}`;
  }

  private async ensureTopology(destination: Destination): Promise<string> {
    const queue = this.queueName(destination);
    if (!this.topology.has(destination)) {
      await this.channel!.assertQueue(queue, { durable: true });
      await this.channel!.bindQueue(queue, this.cfg.exchange, destination);
      this.topology.add(destination);
    }
    return queue;
  }

  async sendNow(destination: Destination, payload: string): Promise<void> {
    await this.ensureTopology(destination);
    this.channel!.publish(this.cfg.exchange, destination, Buffer.from(payload), {
      persistent: true,
    });
  }

  async schedule(
    destination: Destination,
    payload: string,
    deliverAt: Date,
  ): Promise<ScheduleHandle> {
    await this.ensureTopology(destination);
    const delay = Math.max(0, deliverAt.getTime() - Date.now());
    const id = `rmq-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    this.channel!.publish(this.cfg.exchange, destination, Buffer.from(payload), {
      persistent: true,
      headers: { 'x-delay': delay, scheduleId: id },
    });
    return { id, destination, deliverAt };
  }

  async cancel(_handle: ScheduleHandle): Promise<void> {
    throw new NotSupportedError(
      'cancel',
      this.name,
      'the delayed-message plugin has no API to remove a pending message',
    );
  }

  async listScheduled(_destination: Destination): Promise<ScheduledInfo[]> {
    throw new NotSupportedError(
      'listScheduled',
      this.name,
      'pending delayed messages live in a node-local Mnesia table, not a queue',
    );
  }

  async consume(
    destination: Destination,
    handler: MessageHandler,
  ): Promise<Subscription> {
    const queue = await this.ensureTopology(destination);
    const { consumerTag } = await this.channel!.consume(
      queue,
      (msg) => {
        if (!msg) return;
        void handler({
          id: msg.properties.messageId ?? '',
          destination,
          body: msg.content.toString('utf8'),
          headers: normalizeHeaders(msg.properties.headers),
        });
        this.channel!.ack(msg);
      },
      { noAck: false },
    );
    return {
      unsubscribe: async () => {
        await this.channel!.cancel(consumerTag);
      },
    };
  }

  // --- bus port -----------------------------------------------------------

  private topicExchange(topic: Destination): string {
    return `x.t.${topic}`;
  }

  private fanoutExchange(topic: Destination): string {
    return `x.f.${topic}`;
  }

  /** S12: the consistent-hash exchange that pins each `groupId` to one queue. */
  private hashExchange(topic: Destination): string {
    return `x.h.${topic}`;
  }

  /** Declare the topic + fanout exchanges for a logical topic (idempotent). */
  private async ensureBusExchanges(topic: Destination): Promise<void> {
    if (this.busTopology.has(topic)) return;
    const ch = this.channel!;
    await ch.assertExchange(this.topicExchange(topic), 'topic', { durable: true });
    await ch.assertExchange(this.fanoutExchange(topic), 'fanout', { durable: true });
    this.busTopology.add(topic);
  }

  /**
   * S12: declare the per-topic consistent-hash exchange (idempotent). The
   * `rabbitmq_consistent_hash_exchange` plugin hashes the routing key (we publish
   * with routing key = `groupId`) and routes each message to ONE of the bound
   * queues by weight — so the same group always lands on the same queue, and thus
   * the same competing consumer, preserving per-group order.
   */
  private async ensureHashExchange(topic: Destination): Promise<void> {
    if (this.hashTopology.has(topic)) return;
    await this.channel!.assertExchange(this.hashExchange(topic), 'x-consistent-hash', {
      durable: true,
    });
    this.hashTopology.add(topic);
  }

  async publish(
    topic: Destination,
    payload: string,
    routingKey?: string,
    options?: PublishOptions,
  ): Promise<void> {
    await this.ensureBusExchanges(topic);
    const body = Buffer.from(payload);
    const props = this.publishProps(options);
    // Publish to both exchanges; a queue is bound to exactly one, so each
    // subscriber receives exactly one copy regardless of its topology kind.
    this.channel!.publish(this.topicExchange(topic), routingKey ?? topic, body, props);
    this.channel!.publish(this.fanoutExchange(topic), '', body, props);
    // S12: when the message carries a groupId, also route it through the
    // consistent-hash exchange (routing key = groupId) so a partition-by-group
    // subscriber's per-consumer queues each own a stable subset of groups. The
    // hash exchange has no bound queues unless such a subscriber exists, so this
    // is a no-op for ordinary topics.
    if (options?.groupId) {
      await this.ensureHashExchange(topic);
      this.channel!.publish(this.hashExchange(topic), options.groupId, body, props);
    }
  }

  /** Map PublishOptions onto amqplib publish options / BasicProperties. */
  private publishProps(options?: PublishOptions): Record<string, unknown> {
    const headers: Record<string, unknown> = { ...(options?.headers ?? {}) };
    if (options?.groupId) headers['x-group-id'] = options.groupId;
    if (options?.dedupId) headers['x-dedup-id'] = options.dedupId;
    const props: Record<string, unknown> = { persistent: true, headers };
    if (options?.priority !== undefined) props.priority = options.priority;
    if (options?.replyTo) props.replyTo = options.replyTo;
    if (options?.correlationId) props.correlationId = options.correlationId;
    if (options?.ttlMs !== undefined) props.expiration = String(options.ttlMs);
    return props;
  }

  /** S19: the per-topic stream queue — an append-only log bound to the topic
   *  exchange. Shared by every `streamReplay` subscriber so each can re-read the
   *  whole history from offset 0. */
  private streamQueueName(topic: Destination): string {
    return `stream.${topic}`;
  }

  /**
   * S19: subscribe by replaying a topic's full history from the beginning.
   *
   * A RabbitMQ stream queue (`x-queue-type=stream`) is an append-only log: a
   * fresh consumer with `x-stream-offset=first` re-reads every message ever
   * published to it, even ones already consumed by other consumers. We declare
   * one durable stream queue per topic and bind it to the topic exchange, so it
   * captures publishes — the queue must exist (and be bound) BEFORE the publish
   * to capture it, which the scenario guarantees by establishing the stream with
   * an initial streamReplay subscription first.
   *
   * Stream queues refuse a consumer without a QoS (prefetch), so we set one and
   * use manual ack.
   */
  private async subscribeStreamReplay(
    topic: Destination,
    handler: AckHandler,
  ): Promise<Subscription> {
    const queue = this.streamQueueName(topic);
    // Each consumer gets its own channel + its own offset cursor.
    const ch = await this.model!.createChannel();
    await ch.prefetch(10); // stream queues REQUIRE a QoS before consuming
    await ch.assertQueue(queue, {
      durable: true,
      arguments: { 'x-queue-type': 'stream' },
    });
    await ch.bindQueue(queue, this.topicExchange(topic), '#');
    await ch.consume(
      queue,
      (msg) => {
        if (!msg) return;
        this.deliver(this.toStreamIncoming(ch, topic, msg), handler);
      },
      { noAck: false, arguments: { 'x-stream-offset': 'first' } },
    );
    return {
      unsubscribe: async () => {
        try {
          await ch.close();
        } catch {
          /* already closed */
        }
      },
    };
  }

  /** S19: a stream delivery. Streams track a per-consumer offset cursor; ack just
   *  advances it (no requeue/redelivery semantics). */
  private toStreamIncoming(
    ch: Channel,
    topic: Destination,
    msg: ConsumeMessage,
  ): IncomingMessage {
    let settled = false;
    return {
      id: msg.properties.messageId ?? '',
      destination: topic,
      body: msg.content.toString('utf8'),
      headers: normalizeHeaders(msg.properties.headers),
      deliveryCount: undefined,
      ack: async () => {
        if (settled) return;
        settled = true;
        ch.ack(msg);
      },
      nack: async () => {
        // Streams don't redeliver; a nack just advances the offset like an ack.
        if (settled) return;
        settled = true;
        ch.ack(msg);
      },
    };
  }

  async subscribe(
    topic: Destination,
    handler: AckHandler,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    await this.ensureBusExchanges(topic);
    // S19: stream replay takes a dedicated append-only-log path.
    if (options.streamReplay) {
      return this.subscribeStreamReplay(topic, handler);
    }
    const kind = options.kind ?? 'topic';
    const subscriberId = options.subscriberId ?? `sub-${Math.random().toString(36).slice(2, 8)}`;
    // S12: each partition-by-group consumer needs its OWN queue bound to the
    // consistent-hash exchange (the hash routes a group to exactly one queue).
    // So even when they share a subscriberId, give each a unique queue name.
    const queue = options.partitionByGroup
      ? `bus.${topic}.${subscriberId}.${Math.random().toString(36).slice(2, 8)}`
      : `bus.${topic}.${subscriberId}`;

    // Each subscription gets its own channel. Closing it on unsubscribe requeues
    // any un-acked message — which is precisely how a crashed consumer (dropped
    // connection) surfaces as redelivery to a surviving consumer (S7c). A shared
    // channel would hold the message unacked and never redeliver it.
    const ch = await this.model!.createChannel();
    await ch.prefetch(1); // fair dispatch for competing consumers (S9)

    const maxDeliveries = options.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
    const retryEnabled = !!options.deadLetter && !!options.retryDelayMs && options.retryDelayMs > 0;
    const dlaExchange = options.deadLetter ? this.fanoutExchange(deadLetterAddress(topic)) : '';

    const args: Record<string, unknown> = {};
    if (retryEnabled) {
      // Non-blocking retry: the main queue dead-letters a nacked message into a
      // dedicated retry (parking) queue, so the head of the main queue is free
      // immediately. The retry queue holds it for `retryDelayMs` then dead-letters
      // it BACK to the main queue (default exchange routes by queue name), which
      // redelivers it. The adapter counts cycles via the x-death header and routes
      // to the real DLQ once `maxDeliveries` is reached (see toIncoming.nack).
      const dla = deadLetterAddress(topic);
      await this.ensureBusExchanges(dla); // the DLQ destination is itself a topic
      const retryQueue = `${queue}.retry`;
      args['x-dead-letter-exchange'] = ''; // default exchange → route by queue name
      args['x-dead-letter-routing-key'] = retryQueue;
      await ch.assertQueue(retryQueue, {
        durable: true,
        arguments: {
          'x-message-ttl': options.retryDelayMs,
          'x-dead-letter-exchange': '',
          'x-dead-letter-routing-key': queue, // bounce back to the main queue
        },
      });
    } else if (options.deadLetter) {
      // Quorum queue gives a deterministic delivery-limit → dead-letter path.
      const dla = deadLetterAddress(topic);
      await this.ensureBusExchanges(dla); // the DLQ destination is itself a topic
      args['x-queue-type'] = 'quorum';
      args['x-delivery-limit'] = maxDeliveries;
      args['x-dead-letter-exchange'] = this.fanoutExchange(dla);
    }
    // S14: a priority-capable queue honours per-message `priority` (0..9).
    if (options.priorityQueue) {
      args['x-max-priority'] = 10;
    }
    // S18: a single-active-consumer queue dispatches to one consumer at a time and
    // promotes a standby when the active one drops (order preserved).
    if (options.singleActiveConsumer) {
      args['x-single-active-consumer'] = true;
    }
    // SSE: a transient per-connection subscription gets an exclusive, auto-delete,
    // non-durable queue — RabbitMQ removes it the instant the connection's channel
    // closes (browser disconnect), so each SSE client leaves no queue behind.
    if (options.transient) {
      await ch.assertQueue(queue, {
        durable: false,
        exclusive: true,
        autoDelete: true,
      });
      await ch.bindQueue(queue, this.topicExchange(topic), options.routingKey ?? '#');
      await ch.consume(
        queue,
        (msg) => {
          if (!msg) return;
          this.deliver(this.toIncoming(ch, topic, msg), handler);
        },
        { noAck: false },
      );
      return {
        unsubscribe: async () => {
          try {
            await ch.close();
          } catch {
            /* already closed */
          }
        },
      };
    }
    // S16: an expiry-capable queue dead-letters an expired (TTL-elapsed) message
    // to the expiry fanout exchange the `${topic}.expiry` subscriber binds to.
    // (Per-message `expiration` on publish supplies the TTL; the DLX routes the
    // expired message — this mirrors the `.dlq` path but to a distinct address.)
    if (options.ttlMs !== undefined) {
      const expiry = expiryAddress(topic);
      await this.ensureBusExchanges(expiry); // the expiry destination is itself a topic
      args['x-dead-letter-exchange'] = this.fanoutExchange(expiry);
    }
    await ch.assertQueue(queue, { durable: true, arguments: args });

    if (options.partitionByGroup) {
      // S12: bind this consumer's queue to the consistent-hash exchange with an
      // equal weight ("1"). The plugin spreads groups across the bound queues by
      // hashing the routing key (groupId), so the same group always reaches the
      // same queue → the same consumer → per-group order.
      await this.ensureHashExchange(topic);
      await ch.bindQueue(queue, this.hashExchange(topic), '1');
    } else if (kind === 'fanout') {
      await ch.bindQueue(queue, this.fanoutExchange(topic), '');
    } else {
      await ch.bindQueue(queue, this.topicExchange(topic), options.routingKey ?? '#');
    }

    const retry = retryEnabled ? { mainQueue: queue, maxDeliveries, dlaExchange } : undefined;
    await ch.consume(
      queue,
      (msg) => {
        if (!msg) return;
        this.deliver(this.toIncoming(ch, topic, msg, retry), handler);
      },
      { noAck: false },
    );
    return {
      unsubscribe: async () => {
        try {
          await ch.close();
        } catch {
          /* already closed */
        }
      },
    };
  }

  private toIncoming(
    ch: Channel,
    topic: Destination,
    msg: ConsumeMessage,
    retry?: { mainQueue: string; maxDeliveries: number; dlaExchange: string },
  ): IncomingMessage {
    // Retry path: count main-queue rejections accumulated in the x-death header.
    // Non-retry path: quorum queues expose x-delivery-count; classic queues do
    // not, so deliveryCount is best-effort and absent for the classic path.
    let deliveryCount: number | undefined;
    if (retry) {
      deliveryCount = xDeathCount(msg.properties.headers, retry.mainQueue) + 1;
    } else {
      const deaths = msg.properties.headers?.['x-delivery-count'];
      deliveryCount =
        typeof deaths === 'number' ? deaths + 1 : msg.fields.redelivered ? undefined : 1;
    }
    let settled = false;
    return {
      id: msg.properties.messageId ?? '',
      destination: topic,
      body: msg.content.toString('utf8'),
      headers: normalizeHeaders(msg.properties.headers),
      deliveryCount,
      replyTo: msg.properties.replyTo ?? undefined,
      correlationId: msg.properties.correlationId ?? undefined,
      groupId: msg.properties.headers?.['x-group-id'] as string | undefined,
      priority: msg.properties.priority ?? undefined,
      ack: async () => {
        if (settled) return;
        settled = true;
        ch.ack(msg);
      },
      nack: async (requeue: boolean) => {
        if (settled) return;
        settled = true;
        if (retry) {
          // requeue=false (give up now) or the retry budget is spent → publish
          // to the DLQ fanout and ack the original. Otherwise nack(requeue=false)
          // dead-letters it into the parking/retry queue for a delayed retry.
          if (!requeue || (deliveryCount ?? 1) >= retry.maxDeliveries) {
            ch.publish(retry.dlaExchange, '', msg.content, { persistent: true });
            ch.ack(msg);
          } else {
            ch.nack(msg, false, false);
          }
          return;
        }
        // requeue=false with a DLX configured → the message is dead-lettered.
        ch.nack(msg, false, requeue);
      },
    };
  }

  /**
   * Invoke a bus handler with fault isolation. A handler that throws (a random or
   * transient application error) is a failed delivery, not a crashed broker
   * connection: we contain the exception and nack for redelivery rather than let
   * it surface as an unhandled promise rejection that could take down the process
   * (S20). Mirrors the in-memory reference's "throwing handler = redeliver".
   */
  private deliver(incoming: IncomingMessage, handler: AckHandler): void {
    void (async () => {
      try {
        await handler(incoming);
      } catch {
        try {
          await incoming.nack(true);
        } catch {
          /* already settled or channel closed */
        }
      }
    })();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.channel?.close();
    await this.model?.close();
  }
}

function normalizeHeaders(
  headers?: Record<string, unknown>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers ?? {})) out[k] = String(v);
  return out;
}

/**
 * How many times the message was rejected from the main queue, read from the
 * `x-death` header RabbitMQ accumulates as a message bounces main → retry →
 * main. Zero on the first delivery (no x-death yet).
 */
function xDeathCount(headers: Record<string, unknown> | undefined, mainQueue: string): number {
  const xd = headers?.['x-death'];
  if (!Array.isArray(xd)) return 0;
  const entry = xd.find(
    (e) => e && typeof e === 'object' && (e as { queue?: string }).queue === mainQueue,
  ) as { count?: number } | undefined;
  return typeof entry?.count === 'number' ? entry.count : 0;
}
