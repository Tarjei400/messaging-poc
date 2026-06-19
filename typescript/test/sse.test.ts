import http from 'node:http';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { IMessageBus, IMessageScheduler } from '../src/abstractions';
import { BrokerId, createScheduler } from '../src/adapters';
import { SseServer, startSseServer } from '../src/sse/server';

/**
 * Bullet-proof SSE cross-instance test.
 *
 * For each real broker (Artemis, RabbitMQ): start TWO server instances on two
 * ports against the SAME broker, connect THREE SSE clients (alice@acme on
 * inst1, bob@acme on inst2, carol@globex on inst1) and assert:
 *   1. user-direct: publish to user alice (from inst2) → only alice.
 *   2. org-broadcast CROSS-INSTANCE: publish to org acme (from inst1) → BOTH
 *      alice (inst1) AND bob (inst2); carol does not. This is the proof.
 *   3. org isolation: publish to org globex → only carol.
 *   4. reconnect: a fresh stream still receives subsequent messages.
 *
 * The whole suite is SKIPPED gracefully when the broker is unreachable, so it
 * never runs in the no-broker unit run.
 */

const BROKERS: BrokerId[] = ['artemis', 'rabbitmq'];
const PROBE_TIMEOUT_MS = 4000;
const RECEIVE_WAIT_MS = 2500;

/** Try to connect+provision a bus; resolve null if the broker is unreachable. */
async function tryConnect(
  broker: BrokerId,
): Promise<(IMessageScheduler & IMessageBus) | null> {
  const adapter = createScheduler(broker, false) as IMessageScheduler & IMessageBus;
  try {
    await Promise.race([
      (async () => {
        await adapter.connect();
        await adapter.connectBus();
      })(),
      new Promise((_r, rej) =>
        setTimeout(() => rej(new Error('connect timeout')), PROBE_TIMEOUT_MS),
      ),
    ]);
    return adapter;
  } catch {
    try {
      await adapter[Symbol.asyncDispose]();
    } catch {
      /* ignore */
    }
    return null;
  }
}

interface SseClient {
  /** All `data:` frames received so far, parsed. */
  readonly frames: Array<{ scope?: string; instanceId?: string; message?: any }>;
  close(): void;
}

/** Open an SSE stream over raw http and collect parsed `data:` frames. */
function openSse(baseUrl: string, userId: string, orgId: string): Promise<SseClient> {
  const url = new URL(
    `${baseUrl}/events?userId=${encodeURIComponent(userId)}&orgId=${encodeURIComponent(orgId)}`,
  );
  return new Promise((resolve, reject) => {
    const frames: SseClient['frames'] = [];
    const req = http.get(
      { hostname: url.hostname, port: url.port, path: url.pathname + url.search },
      (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`SSE status ${res.statusCode}`));
          res.resume();
          return;
        }
        let buf = '';
        res.setEncoding('utf8');
        res.on('data', (chunk: string) => {
          buf += chunk;
          let idx: number;
          // SSE events are separated by a blank line.
          while ((idx = buf.indexOf('\n\n')) !== -1) {
            const rawEvent = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const dataLines = rawEvent
              .split('\n')
              .filter((l) => l.startsWith('data:'))
              .map((l) => l.slice(5).trim());
            if (dataLines.length === 0) continue; // comment / heartbeat / hello event
            const isHello = rawEvent.includes('event: hello');
            try {
              const parsed = JSON.parse(dataLines.join('\n'));
              if (!isHello) frames.push(parsed);
            } catch {
              /* ignore non-JSON frame */
            }
          }
        });
        // Resolve once the stream is established (headers received).
        resolve({ frames, close: () => req.destroy() });
      },
    );
    req.on('error', reject);
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

for (const broker of BROKERS) {
  describe(`SSE cross-instance cluster — ${broker}`, () => {
    let bus1: (IMessageScheduler & IMessageBus) | null = null;
    let bus2: (IMessageScheduler & IMessageBus) | null = null;
    let inst1: SseServer | undefined;
    let inst2: SseServer | undefined;
    let reachable = false;

    beforeAll(async () => {
      bus1 = await tryConnect(broker);
      if (!bus1) return;
      // Second independent connection — a genuinely separate "instance".
      bus2 = await tryConnect(broker);
      if (!bus2) {
        await bus1[Symbol.asyncDispose]();
        bus1 = null;
        return;
      }
      reachable = true;
      const base = 7100 + Math.floor(Math.random() * 300) * 2;
      inst1 = await startSseServer({ port: base, instanceId: 'inst1', bus: bus1 });
      inst2 = await startSseServer({ port: base + 1, instanceId: 'inst2', bus: bus2 });
    }, 30000);

    afterAll(async () => {
      await inst1?.close();
      await inst2?.close();
      if (bus1) await bus1[Symbol.asyncDispose]();
      if (bus2) await bus2[Symbol.asyncDispose]();
    });

    const url1 = () => `http://localhost:${inst1!.port}`;
    const url2 = () => `http://localhost:${inst2!.port}`;

    it('routes user-direct, broadcasts org cross-instance, and isolates other orgs', async () => {
      if (!reachable) {
        console.warn(`[sse] ${broker} unreachable — skipping`);
        return;
      }

      // alice@acme on inst1, bob@acme on inst2, carol@globex on inst1.
      const alice = await openSse(url1(), 'alice', 'acme');
      const bob = await openSse(url2(), 'bob', 'acme');
      const carol = await openSse(url1(), 'carol', 'globex');
      // Give the broker time to bind all six transient subscriptions.
      await sleep(800);

      // (1) user-direct: publish to alice FROM inst2 → only alice.
      await post(url2(), { to: { type: 'user', id: 'alice' }, from: 'sys', text: 'just-alice' });
      await sleep(RECEIVE_WAIT_MS);

      const aliceUser = alice.frames.filter((f) => f.message?.text === 'just-alice');
      expect(aliceUser.length, 'alice should receive her direct message').toBe(1);
      expect(aliceUser[0].scope).toBe('user');
      expect(bob.frames.some((f) => f.message?.text === 'just-alice')).toBe(false);
      expect(carol.frames.some((f) => f.message?.text === 'just-alice')).toBe(false);

      // (2) org-broadcast CROSS-INSTANCE: publish to org acme FROM inst1 →
      //     BOTH alice (inst1) and bob (inst2); carol (globex) does not.
      await post(url1(), { to: { type: 'org', id: 'acme' }, from: 'sys', text: 'acme-broadcast' });
      await sleep(RECEIVE_WAIT_MS);

      const aliceOrg = alice.frames.filter((f) => f.message?.text === 'acme-broadcast');
      const bobOrg = bob.frames.filter((f) => f.message?.text === 'acme-broadcast');
      expect(aliceOrg.length, 'alice (inst1) receives the org broadcast').toBe(1);
      expect(bobOrg.length, 'bob (inst2) receives the org broadcast CROSS-INSTANCE').toBe(1);
      expect(aliceOrg[0].instanceId).toBe('inst1');
      expect(bobOrg[0].instanceId).toBe('inst2');
      expect(aliceOrg[0].scope).toBe('org');
      expect(bobOrg[0].scope).toBe('org');
      expect(carol.frames.some((f) => f.message?.text === 'acme-broadcast')).toBe(false);

      // (3) org isolation: publish to org globex → only carol.
      await post(url2(), { to: { type: 'org', id: 'globex' }, from: 'sys', text: 'globex-only' });
      await sleep(RECEIVE_WAIT_MS);
      expect(carol.frames.some((f) => f.message?.text === 'globex-only')).toBe(true);
      expect(alice.frames.some((f) => f.message?.text === 'globex-only')).toBe(false);
      expect(bob.frames.some((f) => f.message?.text === 'globex-only')).toBe(false);

      alice.close();
      bob.close();
      carol.close();

      // (4) reconnect: a fresh stream still receives subsequent messages.
      await sleep(400);
      const alice2 = await openSse(url1(), 'alice', 'acme');
      await sleep(800);
      await post(url2(), { to: { type: 'user', id: 'alice' }, from: 'sys', text: 'after-reconnect' });
      await sleep(RECEIVE_WAIT_MS);
      expect(
        alice2.frames.some((f) => f.message?.text === 'after-reconnect'),
        'a reconnected stream still receives messages',
      ).toBe(true);
      alice2.close();
    }, 30000);
  });
}

function post(baseUrl: string, body: unknown): Promise<void> {
  const url = new URL(`${baseUrl}/publish`);
  const data = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'POST',
        headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) },
      },
      (res) => {
        res.resume();
        res.on('end', () => resolve());
      },
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}
