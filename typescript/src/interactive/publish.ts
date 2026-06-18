import { TopologyKind } from '../abstractions';
import { c } from '../ansi';
import { connect, label, parseCli, ts } from './common';

/**
 * Publish messages to a topic, for live demos.
 *
 *   npm run demo:publish -- <broker> <topic> [--count N] [--kind topic|fanout]
 *                           [--key routingKey] [--rate ms]
 */
async function main(): Promise<void> {
  const cli = parseCli('demo:publish <broker> <topic> [--count N] [--kind topic|fanout] [--key rk] [--rate ms]');
  const topic = cli.positionals[0] ?? 'orders';
  const count = cli.num('count', 1);
  const kind = (cli.flag('kind') ?? 'topic') as TopologyKind;
  const rate = cli.num('rate', 250);
  const key = cli.flag('key') ?? topic;

  const bus = await connect(cli.broker);
  console.log(
    `${label(bus.name)}  publishing ${count} message(s) to ` +
      `${c.bold}${topic}${c.reset} (kind=${kind}, key=${key})`,
  );
  for (let i = 1; i <= count; i++) {
    const body = `${topic}#${i}`;
    await bus.publish(topic, body, kind === 'topic' ? key : undefined);
    console.log(`  ${c.green}↑${c.reset} ${ts()}  sent ${c.bold}${body}${c.reset}`);
    if (i < count && rate > 0) await new Promise((r) => setTimeout(r, rate));
  }
  await bus[Symbol.asyncDispose]();
  process.exit(0);
}

void main();
