import {
  AckHandler,
  BusCapabilities,
  Capabilities,
  Destination,
  IMessageBus,
  IMessageScheduler,
  MessageHandler,
  PublishOptions,
  ReceivedMessage,
  ScheduleHandle,
  ScheduledInfo,
  SubscribeOptions,
  Subscription,
} from '../abstractions';
import { InMemoryBus } from './in-memory-bus';

interface PendingSchedule {
  id: string;
  destination: Destination;
  body: string;
  deliverAt: Date;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * A fully in-process scheduler. It speaks no wire protocol and needs no broker,
 * which makes it the perfect test double: the scenario suite and the runner can
 * be exercised in milliseconds in CI with zero infrastructure. It also serves as
 * the executable specification of what "correct" behavior looks like, so the
 * real adapters can be diffed against it.
 */
export class InMemoryScheduler implements IMessageScheduler, IMessageBus {
  readonly name = 'In-Memory (reference)';
  readonly busCapabilities: BusCapabilities = {
    supportsTopic: true,
    supportsFanout: true,
    supportsManualAck: true,
    supportsDeadLetter: true,
    reportsDeliveryCount: true,
    supportsDedup: true,
    supportsStreamReplay: true,
  };
  readonly capabilities: Capabilities = {
    protocol: 'in-memory',
    nativeScheduling: true,
    supportsCancel: true,
    supportsList: true,
    bus: this.busCapabilities,
  };

  private readonly handlers = new Map<Destination, Set<MessageHandler>>();
  private readonly pending = new Map<string, PendingSchedule>();
  private readonly bus = new InMemoryBus();
  private seq = 0;

  async connect(): Promise<void> {
    /* nothing to connect to */
  }

  async connectBus(): Promise<void> {
    /* nothing to connect to */
  }

  async publish(
    topic: Destination,
    payload: string,
    routingKey?: string,
    options?: PublishOptions,
  ): Promise<void> {
    this.bus.publish(topic, payload, routingKey, options);
  }

  async subscribe(
    topic: Destination,
    handler: AckHandler,
    options?: SubscribeOptions,
  ): Promise<Subscription> {
    return this.bus.subscribe(topic, handler, options);
  }

  async sendNow(destination: Destination, payload: string): Promise<void> {
    this.dispatch(destination, payload);
  }

  async schedule(
    destination: Destination,
    payload: string,
    deliverAt: Date,
  ): Promise<ScheduleHandle> {
    const id = `mem-${(this.seq += 1)}`;
    const ms = Math.max(0, deliverAt.getTime() - Date.now());
    const timer = setTimeout(() => {
      this.pending.delete(id);
      this.dispatch(destination, payload);
    }, ms);
    // Don't keep the event loop alive solely for a pending schedule.
    if (typeof timer.unref === 'function') timer.unref();
    this.pending.set(id, { id, destination, body: payload, deliverAt, timer });
    return { id, destination, deliverAt };
  }

  async cancel(handle: ScheduleHandle): Promise<void> {
    const entry = this.pending.get(handle.id);
    if (entry) {
      clearTimeout(entry.timer);
      this.pending.delete(handle.id);
    }
  }

  async listScheduled(destination: Destination): Promise<ScheduledInfo[]> {
    return [...this.pending.values()]
      .filter((p) => p.destination === destination)
      .map((p) => ({ id: p.id, destination: p.destination, deliverAt: p.deliverAt }));
  }

  async consume(
    destination: Destination,
    handler: MessageHandler,
  ): Promise<Subscription> {
    let set = this.handlers.get(destination);
    if (!set) {
      set = new Set();
      this.handlers.set(destination, set);
    }
    set.add(handler);
    return {
      unsubscribe: async () => {
        set!.delete(handler);
      },
    };
  }

  async [Symbol.asyncDispose](): Promise<void> {
    for (const p of this.pending.values()) clearTimeout(p.timer);
    this.pending.clear();
    this.handlers.clear();
    this.bus.dispose();
  }

  private dispatch(destination: Destination, body: string): void {
    const message: ReceivedMessage = {
      id: `mem-msg-${(this.seq += 1)}`,
      destination,
      body,
      headers: {},
    };
    // Deliver asynchronously, mirroring real broker push semantics.
    queueMicrotask(() => {
      for (const h of this.handlers.get(destination) ?? []) void h(message);
    });
  }
}
