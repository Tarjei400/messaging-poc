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
  ScheduleHandle,
  ScheduledInfo,
  SubscribeOptions,
  Subscription,
  deadLetterAddress,
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

  /** Declare the topic + fanout exchanges for a logical topic (idempotent). */
  private async ensureBusExchanges(topic: Destination): Promise<void> {
    if (this.busTopology.has(topic)) return;
    const ch = this.channel!;
    await ch.assertExchange(this.topicExchange(topic), 'topic', { durable: true });
    await ch.assertExchange(this.fanoutExchange(topic), 'fanout', { durable: true });
    this.busTopology.add(topic);
  }

  async publish(
    topic: Destination,
    payload: string,
    routingKey?: string,
  ): Promise<void> {
    await this.ensureBusExchanges(topic);
    const body = Buffer.from(payload);
    // Publish to both exchanges; a queue is bound to exactly one, so each
    // subscriber receives exactly one copy regardless of its topology kind.
    this.channel!.publish(this.topicExchange(topic), routingKey ?? topic, body, {
      persistent: true,
    });
    this.channel!.publish(this.fanoutExchange(topic), '', body, {
      persistent: true,
    });
  }

  async subscribe(
    topic: Destination,
    handler: AckHandler,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    await this.ensureBusExchanges(topic);
    const kind = options.kind ?? 'topic';
    const subscriberId = options.subscriberId ?? `sub-${Math.random().toString(36).slice(2, 8)}`;
    const queue = `bus.${topic}.${subscriberId}`;

    // Each subscription gets its own channel. Closing it on unsubscribe requeues
    // any un-acked message — which is precisely how a crashed consumer (dropped
    // connection) surfaces as redelivery to a surviving consumer (S7c). A shared
    // channel would hold the message unacked and never redeliver it.
    const ch = await this.model!.createChannel();
    await ch.prefetch(1); // fair dispatch for competing consumers (S9)

    const args: Record<string, unknown> = {};
    if (options.deadLetter) {
      // Quorum queue gives a deterministic delivery-limit → dead-letter path.
      const dla = deadLetterAddress(topic);
      await this.ensureBusExchanges(dla); // the DLQ destination is itself a topic
      args['x-queue-type'] = 'quorum';
      args['x-delivery-limit'] = options.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
      args['x-dead-letter-exchange'] = this.fanoutExchange(dla);
    }
    await ch.assertQueue(queue, { durable: true, arguments: args });

    if (kind === 'fanout') {
      await ch.bindQueue(queue, this.fanoutExchange(topic), '');
    } else {
      await ch.bindQueue(queue, this.topicExchange(topic), options.routingKey ?? '#');
    }

    await ch.consume(
      queue,
      (msg) => {
        if (!msg) return;
        void handler(this.toIncoming(ch, topic, msg));
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
  ): IncomingMessage {
    // Quorum queues expose x-death/x-delivery-count; classic queues do not, so
    // deliveryCount is best-effort and absent for the classic path.
    const deaths = msg.properties.headers?.['x-delivery-count'];
    const deliveryCount =
      typeof deaths === 'number' ? deaths + 1 : msg.fields.redelivered ? undefined : 1;
    let settled = false;
    return {
      id: msg.properties.messageId ?? '',
      destination: topic,
      body: msg.content.toString('utf8'),
      headers: normalizeHeaders(msg.properties.headers),
      deliveryCount,
      ack: async () => {
        if (settled) return;
        settled = true;
        ch.ack(msg);
      },
      nack: async (requeue: boolean) => {
        if (settled) return;
        settled = true;
        // requeue=false with a DLX configured → the message is dead-lettered.
        ch.nack(msg, false, requeue);
      },
    };
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
