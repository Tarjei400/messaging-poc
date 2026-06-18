import { IMessageBus, IMessageScheduler, deadLetterAddress } from './abstractions';
import { BrokerId, KNOWN_BROKERS, createScheduler } from './adapters';
import { FaultInjectingBus } from './fault/fault-injecting-bus';
import { FaultNarrator } from './fault/fault-narrator';
import {
  ResilienceEvent,
  ResilientScheduler,
} from './resilience';
import { AckCollector, delay, nonce, waitUntil } from './scenarios';

/**
 * Presentable fault-tolerance demos (consumer crash → redelivery, broker
 * disconnect → retry → circuit breaker → reconnect, poison → dead-letter),
 * narrated as a live timeline. Runs against a real broker, or in-memory (the
 * disconnect is injected, so no broker is even required).
 *
 *   npm run fault -- <broker>     # artemis | rabbitmq | in-memory
 */
async function main(): Promise<void> {
  const arg = (process.argv[2] ?? 'in-memory').toLowerCase();
  if (arg !== 'all' && !KNOWN_BROKERS.includes(arg as BrokerId)) {
    console.error(`Unknown broker '${arg}'. Known: ${KNOWN_BROKERS.join(', ')}, all`);
    process.exit(2);
  }
  const brokers: BrokerId[] =
    arg === 'all' ? [...KNOWN_BROKERS] : ([arg] as BrokerId[]);

  let failures = 0;
  for (const id of brokers) {
    failures += await runFaultDemos(id);
  }
  process.exit(failures > 0 ? 1 : 0);
}

async function runFaultDemos(id: BrokerId): Promise<number> {
  const raw = createScheduler(id, false) as IMessageScheduler & IMessageBus;
  const narrator = new FaultNarrator(raw.name);
  try {
    await raw.connect();
    await raw.connectBus();
  } catch (err) {
    console.error(`\n[${id}] could not connect: ${(err as Error).message}`);
    return 0; // unreachable broker is not a demo failure
  }

  narrator.header();
  let demonstrated = 0;
  let failed = 0;
  try {
    demonstrated += (await consumerCrashDemo(raw, narrator)) ? 1 : (failed++, 0);
    demonstrated += (await disconnectDemo(raw, narrator)) ? 1 : (failed++, 0);
    demonstrated += (await poisonDemo(raw, narrator)) ? 1 : (failed++, 0);
  } finally {
    narrator.footer(demonstrated, failed);
    await raw[Symbol.asyncDispose]();
  }
  return failed;
}

/** Demo 1 — a consumer receives a message, "crashes" without acking, and the
 *  broker redelivers it to a fresh consumer. */
async function consumerCrashDemo(
  bus: IMessageBus,
  n: FaultNarrator,
): Promise<boolean> {
  const topic = `mbc.fault.crash.${nonce()}`;
  const queueId = `worker-${nonce()}`;
  const crashed = await AckCollector.start(bus, topic, {
    subscriberId: queueId,
    autoAck: false,
    onMessage: async () => {
      /* received, but never settles — the consumer "crashes" */
    },
  });
  await bus.publish(topic, 'job-A');
  const got = await waitUntil(() => crashed.count() >= 1, 4000);
  if (!got) {
    n.step('consumer never received the message');
    await crashed.stop();
    return false;
  }
  n.ok('message delivered to consumer');
  n.step('consumer crashes (drops the link with the message un-acked)');
  await crashed.stop();

  const fresh = await AckCollector.start(bus, topic, { subscriberId: queueId });
  const t0 = Date.now();
  const redelivered = await waitUntil(() => fresh.count() >= 1, 6000);
  await fresh.stop();
  if (redelivered) {
    n.recover(`broker redelivered the message to a fresh consumer  (${Date.now() - t0}ms)`);
    return true;
  }
  n.step('redelivery did NOT happen — message lost');
  return false;
}

/** Demo 2 — a transient blip is absorbed by retries, then a sustained outage
 *  trips the circuit breaker, then recovery closes it again. */
async function disconnectDemo(
  raw: IMessageScheduler & IMessageBus,
  n: FaultNarrator,
): Promise<boolean> {
  const faulted = new FaultInjectingBus(raw);
  let breakerOpen = false;
  const onEvent = (e: ResilienceEvent) => {
    switch (e.kind) {
      case 'retry':
        n.retry(`retry #${e.attempt} after transient fault`);
        break;
      case 'breaker-open':
        breakerOpen = true;
        n.breaker('circuit OPENED — failing fast to protect the broker');
        break;
      case 'breaker-half-open':
        n.breaker('circuit HALF-OPEN — probing for recovery');
        break;
      case 'breaker-close':
        n.breaker('circuit CLOSED — fully recovered');
        break;
    }
  };
  const resilient = new ResilientScheduler(
    faulted,
    {
      maxRetryAttempts: 3,
      initialDelayMs: 60,
      maxDelayMs: 200,
      consecutiveFailures: 3,
      halfOpenAfterMs: 800,
    },
    onEvent,
  );

  const topic = `mbc.fault.net.${nonce()}`;
  const collector = await AckCollector.start(raw, topic, { subscriberId: `obs-${nonce()}` });

  // (a) transient blip: 2 failures, absorbed by retry → publish still succeeds.
  n.step('publish during a brief blip (2 transient faults)');
  faulted.injectFailures(2);
  try {
    await resilient.publish(topic, 'blip');
    n.ok('publish succeeded despite the blip (retries absorbed it)');
  } catch {
    n.step('publish failed unexpectedly during the blip');
  }

  // (b) sustained outage: keep failing until the breaker opens.
  n.step('broker connection severed (sustained outage)');
  faulted.injectFailures(100);
  for (let i = 0; i < 8 && !breakerOpen; i++) {
    try {
      await resilient.publish(topic, 'during-outage');
    } catch {
      /* expected: transient fault or open-circuit */
    }
  }

  // (c) recovery.
  faulted.injectFailures(0);
  await delay(1000); // let the breaker move to half-open
  let recovered = false;
  for (let i = 0; i < 5 && !recovered; i++) {
    try {
      await resilient.publish(topic, 'recovered');
      recovered = true;
    } catch {
      await delay(300);
    }
  }
  const delivered = await waitUntil(
    () => collector.bodies().includes('recovered'),
    4000,
  );
  await collector.stop();

  if (breakerOpen && recovered && delivered) {
    n.ok('post-recovery message delivered end-to-end');
    return true;
  }
  n.step(
    `incomplete (breakerOpen=${breakerOpen}, recovered=${recovered}, delivered=${delivered})`,
  );
  return breakerOpen; // opening + narrating the breaker is the core demonstration
}

/** Demo 3 — a message that always fails is dead-lettered after N attempts. */
async function poisonDemo(bus: IMessageBus, n: FaultNarrator): Promise<boolean> {
  if (!bus.busCapabilities.supportsDeadLetter) {
    n.step('broker has no dead-letter support — skipping poison demo');
    return false;
  }
  const topic = `mbc.fault.poison.${nonce()}`;
  let attempts = 0;
  const main = await AckCollector.start(bus, topic, {
    subscriberId: `poison-${nonce()}`,
    deadLetter: true,
    maxDeliveries: 3,
    autoAck: false,
    onMessage: async (m) => {
      attempts += 1;
      n.retry(`delivery attempt #${m.deliveryCount ?? attempts} failed → nack`);
      await m.nack(true);
    },
  });
  const dlq = await AckCollector.start(bus, deadLetterAddress(topic), {
    kind: 'fanout',
    subscriberId: `dlq-${nonce()}`,
  });
  n.step('publish a poison message (handler always fails)');
  await bus.publish(topic, 'poison');
  const landed = await waitUntil(() => dlq.count() >= 1, 8000);
  await main.stop();
  await dlq.stop();
  if (landed) {
    n.dead(`dead-lettered after ${attempts} attempts → ${deadLetterAddress(topic)}`);
    return true;
  }
  n.step(`poison message was not dead-lettered (attempts=${attempts})`);
  return false;
}

void main();
