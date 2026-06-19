import { AckHandler, IMessageBus, IncomingMessage } from '../abstractions';
import {
  BusScenario,
  ScenarioResult,
  fail,
  nonce,
  pass,
  waitUntil,
} from './scenario';

/**
 * S15 — Request/reply (RPC over messaging). A responder subscribes to a request
 * topic; for each request it echoes a reply to the message's `replyTo` address,
 * stamping the same `correlationId`. A requester subscribes to its own unique
 * reply topic, publishes a request carrying `replyTo` + `correlationId`, and
 * asserts the correlated reply comes back. This is the messaging substrate for
 * synchronous-feeling calls over an async bus — all brokers support it because
 * it only needs `replyTo`/`correlationId`, which every adapter maps to native
 * AMQP properties.
 */
export const requestReply: BusScenario = {
  name: 'S15 request/reply',
  description: 'A request carrying replyTo + correlationId gets a correlated reply.',
  async run(bus: IMessageBus): Promise<ScenarioResult> {
    const t0 = performance.now();
    const id = nonce();
    const reqTopic = `mbc.s15.req.${id}`;
    const replyTopic = `mbc.s15.reply.${id}`;
    const correlationId = `corr-${id}`;

    // The responder: echo each request back to its replyTo, preserving the id.
    const responder: AckHandler = async (m: IncomingMessage) => {
      if (m.replyTo) {
        await bus.publish(m.replyTo, `echo:${m.body}`, undefined, {
          correlationId: m.correlationId,
        });
      }
      await m.ack();
    };
    const respSub = await bus.subscribe(reqTopic, responder, {
      subscriberId: `responder-${id}`,
    });

    // The requester: collect correlated replies.
    const replies: IncomingMessage[] = [];
    const requester: AckHandler = async (m: IncomingMessage) => {
      replies.push(m);
      await m.ack();
    };
    const reqSub = await bus.subscribe(replyTopic, requester, {
      subscriberId: `requester-${id}`,
    });
    try {
      await bus.publish(reqTopic, 'ping', undefined, {
        replyTo: replyTopic,
        correlationId,
      });

      const got = await waitUntil(() => replies.length >= 1, 6000);
      if (!got) {
        return fail(this.name, 'no reply received', t0);
      }
      const reply = replies[0];
      if (reply.body !== 'echo:ping') {
        return fail(this.name, `unexpected reply body "${reply.body}"`, t0);
      }
      if (reply.correlationId !== correlationId) {
        return fail(
          this.name,
          `correlationId mismatch: "${reply.correlationId}" != "${correlationId}"`,
          t0,
        );
      }
      return pass(
        this.name,
        `correlated reply "${reply.body}" (correlationId matched)`,
        t0,
      );
    } finally {
      await respSub.unsubscribe();
      await reqSub.unsubscribe();
    }
  },
};
