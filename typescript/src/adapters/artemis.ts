import * as rhea from 'rhea'; // type namespace (Connection, Sender, …)
import rheaContainer from 'rhea'; // runtime container instance (prototype methods)
import {
  AckHandler,
  BusCapabilities,
  Capabilities,
  Destination,
  IMessageBus,
  IMessageScheduler,
  IncomingMessage,
  MessageHandler,
  ScheduleHandle,
  ScheduledInfo,
  SubscribeOptions,
  Subscription,
} from '../abstractions';
import { JolokiaClient } from './artemis-jolokia';

/** The broker-configured multicast dead-letter address for `mbc.#` (see
 *  infra/artemis broker.xml). Our logical `${topic}.dlq` subscriptions read
 *  from here. */
const ARTEMIS_DLA = process.env.ARTEMIS_DLA ?? 'mbc.DLQ';
const ROUTING_KEY_PROP = 'routingKey';

export interface ArtemisConfig {
  host: string;
  port: number; // AMQP, default 5672
  username: string;
  password: string;
  jolokiaUrl: string; // http://host:8161/console/jolokia
}

export function artemisConfigFromEnv(): ArtemisConfig {
  const host = process.env.ARTEMIS_HOST ?? 'localhost';
  return {
    host,
    port: Number(process.env.ARTEMIS_PORT ?? 5672),
    username: process.env.ARTEMIS_USER ?? 'admin',
    password: process.env.ARTEMIS_PASSWORD ?? 'admin',
    jolokiaUrl:
      process.env.ARTEMIS_JOLOKIA_URL ??
      `http://${host}:8161/console/jolokia`,
  };
}

const SCHEDULE_ID = 'scheduleId';
const DELIVERY_TIME_ANNOTATION = 'x-opt-delivery-time';

/**
 * Apache ActiveMQ Artemis adapter.
 *
 *  - sendNow / schedule / consume go over AMQP 1.0 (rhea).
 *  - Scheduling uses the AMQP message annotation `x-opt-delivery-time`
 *    (absolute epoch-ms) which Artemis honors natively — the same annotation
 *    family Azure Service Bus uses, which is what makes this a low-friction
 *    migration target.
 *  - cancel / listScheduled go through broker management (Jolokia) because the
 *    AMQP protocol itself has no cancel verb. We tag each scheduled message with
 *    an application property `scheduleId` so we can cancel precisely by id.
 *
 * Destinations are addressed via the fully-qualified queue name `dest::dest`
 * to force deterministic ANYCAST routing onto a queue whose name we know.
 */
export class ArtemisScheduler implements IMessageScheduler, IMessageBus {
  readonly name = 'Apache ActiveMQ Artemis';
  readonly busCapabilities: BusCapabilities = {
    supportsTopic: true,
    supportsFanout: true,
    supportsManualAck: true,
    supportsDeadLetter: true,
    reportsDeliveryCount: true, // AMQP header delivery-count is precise
  };
  readonly capabilities: Capabilities = {
    protocol: 'AMQP 1.0',
    nativeScheduling: true,
    supportsCancel: true,
    supportsList: true,
    bus: this.busCapabilities,
  };

  private connection?: rhea.Connection;
  private readonly senders = new Map<string, rhea.Sender>();
  private readonly busSenders = new Map<string, rhea.Sender>();
  private readonly jolokia: JolokiaClient;

  constructor(private readonly cfg: ArtemisConfig) {
    this.jolokia = new JolokiaClient({
      baseUrl: cfg.jolokiaUrl,
      username: cfg.username,
      password: cfg.password,
    });
  }

  private async ensureConnection(): Promise<rhea.Connection> {
    if (this.connection && this.connection.is_open()) return this.connection;
    const container = rheaContainer.create_container();
    this.connection = container.connect({
      host: this.cfg.host,
      port: this.cfg.port,
      username: this.cfg.username,
      password: this.cfg.password,
      reconnect: false,
    });
    await once(this.connection, 'connection_open');
    return this.connection;
  }

  async connect(): Promise<void> {
    await this.ensureConnection();
  }

  async connectBus(): Promise<void> {
    await this.ensureConnection();
  }

  private fqqn(destination: Destination): string {
    return `${destination}::${destination}`;
  }

  private async getSender(destination: Destination): Promise<rhea.Sender> {
    const address = this.fqqn(destination);
    let sender = this.senders.get(address);
    if (sender && sender.is_open()) return sender;
    // The `queue` capability forces ANYCAST auto-create. Artemis defaults
    // auto-created addresses to MULTICAST, which would make the scheduling queue
    // a topic and break the Jolokia (anycast) cancel/list management lookups.
    sender = this.connection!.open_sender({
      target: { address, capabilities: 'queue' } as unknown as rhea.Source,
    });
    await once(sender, 'sendable');
    this.senders.set(address, sender);
    return sender;
  }

  async sendNow(destination: Destination, payload: string): Promise<void> {
    const sender = await this.getSender(destination);
    sender.send({ body: payload });
  }

  async schedule(
    destination: Destination,
    payload: string,
    deliverAt: Date,
  ): Promise<ScheduleHandle> {
    const id = `art-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const sender = await this.getSender(destination);
    sender.send({
      body: payload,
      message_annotations: { [DELIVERY_TIME_ANNOTATION]: deliverAt.getTime() },
      application_properties: { [SCHEDULE_ID]: id },
    });
    return { id, destination, deliverAt };
  }

  async cancel(handle: ScheduleHandle): Promise<void> {
    const filter = `${SCHEDULE_ID} = '${handle.id}'`;
    await this.jolokia.removeMessages(handle.destination, filter);
  }

  async listScheduled(destination: Destination): Promise<ScheduledInfo[]> {
    const raw = await this.jolokia.listScheduledMessages(destination);
    return raw.map((m) => ({
      id: String(m[SCHEDULE_ID] ?? m.messageID ?? ''),
      destination,
      deliverAt: m.scheduledDeliveryTime
        ? new Date(Number(m.scheduledDeliveryTime))
        : undefined,
    }));
  }

  async consume(
    destination: Destination,
    handler: MessageHandler,
  ): Promise<Subscription> {
    const receiver = this.connection!.open_receiver({
      source: {
        address: this.fqqn(destination),
        capabilities: 'queue',
      } as unknown as rhea.Source,
    });
    const onMessage = (ctx: rhea.EventContext) => {
      const msg = ctx.message!;
      void handler({
        id: String(msg.message_id ?? ''),
        destination,
        body: bodyToString(msg.body),
        headers: stringifyProps(msg.application_properties),
      });
    };
    receiver.on('message', onMessage);
    await once(receiver, 'receiver_open');
    return {
      unsubscribe: async () => {
        receiver.removeListener('message', onMessage);
        receiver.close();
      },
    };
  }

  // --- bus port -----------------------------------------------------------

  private async getBusSender(address: Destination): Promise<rhea.Sender> {
    let sender = this.busSenders.get(address);
    if (sender && sender.is_open()) return sender;
    // The `topic` capability makes Artemis treat the address as MULTICAST on
    // auto-create, which is what gives independent subscribers their own copy.
    sender = this.connection!.open_sender({
      target: { address, capabilities: ['topic'] as unknown as string },
    });
    await once(sender, 'sendable');
    this.busSenders.set(address, sender);
    return sender;
  }

  async publish(
    topic: Destination,
    payload: string,
    routingKey?: string,
  ): Promise<void> {
    const sender = await this.getBusSender(topic);
    sender.send({
      body: payload,
      application_properties: { [ROUTING_KEY_PROP]: routingKey ?? topic },
    });
  }

  async subscribe(
    topic: Destination,
    handler: AckHandler,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    const kind = options.kind ?? 'topic';
    const subscriberId =
      options.subscriberId ?? `sub-${Math.random().toString(36).slice(2, 8)}`;

    // A `${topic}.dlq` subscription reads from the broker's multicast DLA, where
    // Artemis routes messages after `max-delivery-attempts` (set in broker.xml).
    const address = topic.endsWith('.dlq') ? ARTEMIS_DLA : topic;
    const fqqn = `${address}::${subscriberId}`;

    const source = {
      address: fqqn,
      capabilities: 'topic',
    } as unknown as rhea.Source;
    // Topic routing-key filtering uses an AMQP/JMS selector on a message
    // property — Artemis's mechanism differs from RabbitMQ's exchange bindings,
    // same observable outcome.
    if (kind === 'topic' && options.routingKey) {
      (source as unknown as { filter: unknown }).filter =
        rheaContainer.filter.selector(toSelector(options.routingKey));
    }

    const receiver = this.connection!.open_receiver({
      source,
      autoaccept: false, // the consumer settles explicitly (ack/nack)
    });
    const onMessage = (ctx: rhea.EventContext) => {
      const msg = ctx.message!;
      const delivery = ctx.delivery!;
      void handler(this.toIncoming(address, msg, delivery));
    };
    receiver.on('message', onMessage);
    await once(receiver, 'receiver_open');
    return {
      unsubscribe: async () => {
        receiver.removeListener('message', onMessage);
        receiver.close();
      },
    };
  }

  private toIncoming(
    address: Destination,
    msg: rhea.Message,
    delivery: rhea.Delivery,
  ): IncomingMessage {
    let settled = false;
    return {
      id: String(msg.message_id ?? ''),
      destination: address,
      body: bodyToString(msg.body),
      headers: stringifyProps(msg.application_properties),
      // AMQP `delivery-count` is the number of prior (failed) deliveries.
      deliveryCount: (msg.delivery_count ?? 0) + 1,
      ack: async () => {
        if (settled) return;
        settled = true;
        delivery.accept();
      },
      nack: async (requeue: boolean) => {
        if (settled) return;
        settled = true;
        if (requeue) {
          // Counts as a failed delivery; after max-delivery-attempts Artemis
          // routes the message to the dead-letter address.
          (
            delivery as unknown as {
              modified: (o: {
                delivery_failed: boolean;
                undeliverable_here: boolean;
              }) => void;
            }
          ).modified({ delivery_failed: true, undeliverable_here: false });
        } else {
          delivery.reject(); // straight to the dead-letter address
        }
      },
    };
  }

  async [Symbol.asyncDispose](): Promise<void> {
    for (const s of this.senders.values()) s.close();
    for (const s of this.busSenders.values()) s.close();
    this.connection?.close();
  }
}

/**
 * Translate a RabbitMQ-style topic pattern into an Artemis JMS selector on the
 * routingKey property. `*`/`#` become SQL `LIKE` wildcards; an exact key uses
 * equality.
 */
function toSelector(routingKey: string): string {
  if (routingKey.includes('*') || routingKey.includes('#')) {
    const like = routingKey.replace(/[*#]/g, '%');
    return `${ROUTING_KEY_PROP} LIKE '${like}'`;
  }
  return `${ROUTING_KEY_PROP} = '${routingKey}'`;
}

// --- helpers -------------------------------------------------------------

function once(emitter: any, event: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const ok = () => {
      cleanup();
      resolve();
    };
    const err = (e: any) => {
      cleanup();
      reject(e?.error ?? new Error(`error on '${event}'`));
    };
    const cleanup = () => {
      emitter.removeListener(event, ok);
      emitter.removeListener('connection_error', err);
      emitter.removeListener('error', err);
    };
    emitter.once(event, ok);
    emitter.once('connection_error', err);
    emitter.once('error', err);
  });
}

function bodyToString(body: unknown): string {
  if (body == null) return '';
  if (typeof body === 'string') return body;
  if (Buffer.isBuffer(body)) return body.toString('utf8');
  // rhea may wrap typed bodies; fall back to JSON for objects.
  return typeof body === 'object' ? JSON.stringify(body) : String(body);
}

function stringifyProps(
  props?: Record<string, unknown>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(props ?? {})) out[k] = String(v);
  return out;
}
