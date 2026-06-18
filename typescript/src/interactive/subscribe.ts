import { TopologyKind } from '../abstractions';
import { c } from '../ansi';
import { connect, keepAlive, label, parseCli, ts } from './common';

/**
 * A long-running subscriber for live demos. Run several with distinct --id to
 * see fanout (each gets every message); run several with the SAME --id to see
 * competing consumers (the load is shared).
 *
 *   npm run demo:subscribe -- <broker> <topic> [--id name] [--kind topic|fanout]
 *                             [--key rk] [--nack-every N] [--crash-after N] [--work ms]
 */
async function main(): Promise<void> {
  const cli = parseCli('demo:subscribe <broker> <topic> [--id name] [--kind topic|fanout] [--key rk] [--nack-every N] [--crash-after N] [--work ms]');
  const topic = cli.positionals[0] ?? 'orders';
  const id = cli.flag('id') ?? `sub-${Math.random().toString(36).slice(2, 6)}`;
  const kind = (cli.flag('kind') ?? 'topic') as TopologyKind;
  const routingKey = cli.flag('key');
  const nackEvery = cli.num('nack-every', 0);
  const crashAfter = cli.num('crash-after', 0);
  const work = cli.num('work', 0);

  const bus = await connect(cli.broker);
  let n = 0;

  const sub = await bus.subscribe(
    topic,
    async (m) => {
      n += 1;
      const dc = m.deliveryCount ? ` ${c.dim}(delivery ${m.deliveryCount})${c.reset}` : '';
      console.log(
        `  ${c.green}↓${c.reset} ${ts()}  ${c.bold}[${id}]${c.reset} ` +
          `${m.body}${dc}`,
      );
      if (work > 0) await new Promise((r) => setTimeout(r, work));

      if (crashAfter > 0 && n >= crashAfter) {
        console.log(`  ${c.red}✖ [${id}] crashing (leaving msg un-acked)${c.reset}`);
        await bus[Symbol.asyncDispose]();
        process.exit(1);
      }
      if (nackEvery > 0 && n % nackEvery === 0) {
        console.log(`  ${c.yellow}↺ [${id}] nack (requeue)${c.reset}`);
        await m.nack(true);
      } else {
        await m.ack();
      }
    },
    { kind, subscriberId: id, routingKey },
  );

  console.log(
    `${label(bus.name)}  ${c.bold}[${id}]${c.reset} subscribed to ` +
      `${c.bold}${topic}${c.reset} (kind=${kind}${routingKey ? `, key=${routingKey}` : ''})  ` +
      `${c.dim}Ctrl-C to stop${c.reset}`,
  );
  keepAlive(async () => {
    await sub.unsubscribe();
    await bus[Symbol.asyncDispose]();
  });
}

void main();
