import { DEFAULT_MAX_DELIVERIES, deadLetterAddress } from '../abstractions';
import { c } from '../ansi';
import { connect, keepAlive, label, parseCli, ts } from './common';

/**
 * A flaky worker for the dead-letter demo: it processes messages but fails a
 * configurable fraction of the time (nack→requeue). After `--max` failed
 * attempts the broker dead-letters the message. Watch the DLQ in another
 * terminal with:
 *
 *   npm run demo:subscribe -- <broker> <topic>.dlq --kind fanout --id dlq
 *
 * Usage:
 *   npm run demo:worker -- <broker> <topic> [--id name] [--fail-rate 0..1]
 *                          [--max N] [--work ms]
 */
async function main(): Promise<void> {
  const cli = parseCli('demo:worker <broker> <topic> [--id name] [--fail-rate 0..1] [--max N] [--work ms]');
  const topic = cli.positionals[0] ?? 'orders';
  const id = cli.flag('id') ?? `worker-${Math.random().toString(36).slice(2, 6)}`;
  const failRate = cli.num('fail-rate', 1); // default: always fail (pure poison demo)
  const max = cli.num('max', DEFAULT_MAX_DELIVERIES);
  const work = cli.num('work', 100);

  const bus = await connect(cli.broker);
  const sub = await bus.subscribe(
    topic,
    async (m) => {
      const dc = m.deliveryCount ? ` ${c.dim}(attempt ${m.deliveryCount})${c.reset}` : '';
      if (work > 0) await new Promise((r) => setTimeout(r, work));
      // Deterministic-ish failure: hash the body so a given message is stable.
      const fail = pseudoRandom(m.body) < failRate;
      if (fail) {
        console.log(`  ${c.yellow}↺${c.reset} ${ts()} [${id}] ${m.body}${dc} → nack`);
        await m.nack(true);
      } else {
        console.log(`  ${c.green}✓${c.reset} ${ts()} [${id}] ${m.body}${dc} → ack`);
        await m.ack();
      }
    },
    { subscriberId: id, deadLetter: true, maxDeliveries: max },
  );

  console.log(
    `${label(bus.name)}  ${c.bold}[${id}]${c.reset} working ${c.bold}${topic}${c.reset} ` +
      `(fail-rate=${failRate}, max=${max} → ${deadLetterAddress(topic)})  ${c.dim}Ctrl-C to stop${c.reset}`,
  );
  keepAlive(async () => {
    await sub.unsubscribe();
    await bus[Symbol.asyncDispose]();
  });
}

/** Stable [0,1) value from a string so the same message fails consistently. */
function pseudoRandom(s: string): number {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return ((h >>> 0) % 1000) / 1000;
}

void main();
