import { NotSupportedError } from '../abstractions';
import { c } from '../ansi';
import { connect, label, parseCli, ts } from './common';

/**
 * Demonstrate broker-native scheduled delivery (and cancellation) live. The
 * process schedules a message for the future, consumes the destination, and
 * shows it arrive — or, with --cancel, proves it never does. This is the core
 * Azure Service Bus replacement behavior.
 *
 *   npm run demo:schedule -- <broker> <dest> [--in seconds] [--cancel] [--list]
 */
async function main(): Promise<void> {
  const cli = parseCli('demo:schedule <broker> <dest> [--in seconds] [--cancel] [--list]');
  const dest = cli.positionals[0] ?? 'reminders';
  const seconds = cli.num('in', 5);
  const doCancel = cli.bool('cancel');
  const doList = cli.bool('list');

  const sched = await connect(cli.broker);
  console.log(`${label(sched.name)}  scheduling on ${c.bold}${dest}${c.reset} (+${seconds}s)`);

  const arrivals: string[] = [];
  const sub = await sched.consume(dest, (m) => {
    arrivals.push(m.body);
    console.log(`  ${c.green}↓${c.reset} ${ts()}  delivered ${c.bold}${m.body}${c.reset}`);
  });

  const body = `reminder-${Date.now()}`;
  const deliverAt = new Date(Date.now() + seconds * 1000);
  const handle = await sched.schedule(dest, body, deliverAt);
  console.log(`  ${c.cyan}⏰${c.reset} ${ts()}  scheduled ${c.bold}${body}${c.reset} for ${deliverAt.toISOString().slice(11, 19)}`);

  if (doList) {
    try {
      const pending = await sched.listScheduled(dest);
      console.log(`  ${c.dim}list →${c.reset} ${pending.length} pending: ${pending.map((p) => p.id).join(', ')}`);
    } catch (err) {
      reportUnsupported('list', err);
    }
  }

  if (doCancel) {
    try {
      await sched.cancel(handle);
      console.log(`  ${c.yellow}✖${c.reset} ${ts()}  cancelled ${c.bold}${body}${c.reset}`);
    } catch (err) {
      reportUnsupported('cancel', err);
    }
  }

  // Wait past the delivery time and report the outcome.
  await new Promise((r) => setTimeout(r, seconds * 1000 + 2500));
  const arrived = arrivals.includes(body);
  if (doCancel) {
    console.log(
      arrived
        ? `  ${c.red}✗ cancel failed — message was delivered anyway${c.reset}`
        : `  ${c.green}✓ cancelled message never arrived${c.reset}`,
    );
  } else {
    console.log(
      arrived
        ? `  ${c.green}✓ scheduled message delivered on time${c.reset}`
        : `  ${c.red}✗ scheduled message never arrived${c.reset}`,
    );
  }

  await sub.unsubscribe();
  await sched[Symbol.asyncDispose]();
  process.exit(0);
}

function reportUnsupported(op: string, err: unknown): void {
  if (err instanceof NotSupportedError) {
    console.log(`  ${c.yellow}⊘ ${op} is not supported by this broker${c.reset}`);
  } else {
    throw err;
  }
}

void main();
