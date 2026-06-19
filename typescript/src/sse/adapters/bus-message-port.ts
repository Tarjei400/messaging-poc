/**
 * Outbound broker adapter: adapts the project's `IMessageBus` to the hexagon's
 * `MessageBusPort`. This is where ALL broker detail lives — the `mbc.sse` topic,
 * the transient (exclusive + auto-delete) topic subscriptions, the routing-key
 * filtering and the manual ack — none of which the domain (`SseHub`) sees.
 */

import { IMessageBus } from '../../abstractions';
import { SSE_CHANNEL } from '../domain/contract';
import { MessageBusPort, Unsubscribe } from '../ports';

export class BusMessagePort implements MessageBusPort {
  private seq = 0;

  constructor(
    private readonly bus: IMessageBus,
    /** Used only to build unique transient subscriber ids. */
    private readonly instanceId: string,
  ) {}

  async subscribe(routingKey: string, onMessage: (body: string) => void): Promise<Unsubscribe> {
    const subscriberId = `${this.instanceId}.${routingKey}.${Date.now()}.${this.seq++}`;
    const subscription = await this.bus.subscribe(
      SSE_CHANNEL,
      async (m) => {
        onMessage(m.body);
        await m.ack();
      },
      { kind: 'topic', routingKey, subscriberId, transient: true },
    );
    return () => subscription.unsubscribe();
  }

  async publish(routingKey: string, payload: string): Promise<void> {
    await this.bus.publish(SSE_CHANNEL, payload, routingKey);
  }
}
