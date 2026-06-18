import { IMessageBus, IMessageScheduler } from '../abstractions';
import { BrokerId, KNOWN_BROKERS, createScheduler } from '../adapters';
import { c } from '../ansi';

/**
 * Tiny argv helper shared by the interactive demo processes. These processes are
 * deliberately thin compositions over the adapters — no business logic — so a
 * presenter can open several terminals and watch pub/sub, fanout, competing
 * consumers, redelivery and dead-lettering happen live.
 */
export interface Cli {
  readonly broker: BrokerId;
  readonly positionals: string[];
  flag(name: string): string | undefined;
  bool(name: string): boolean;
  num(name: string, fallback: number): number;
}

export function parseCli(usage: string): Cli {
  const argv = process.argv.slice(2);
  const positionals: string[] = [];
  const flags = new Map<string, string>();
  const bools = new Set<string>();

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith('--')) {
        flags.set(key, next);
        i++;
      } else {
        bools.add(key);
      }
    } else {
      positionals.push(a);
    }
  }

  const broker = (positionals.shift() ?? '') as BrokerId;
  if (!KNOWN_BROKERS.includes(broker)) {
    console.error(`${c.red}Usage:${c.reset} ${usage}`);
    console.error(`First argument must be a broker: ${KNOWN_BROKERS.join(' | ')}`);
    process.exit(2);
  }

  return {
    broker,
    positionals,
    flag: (name) => flags.get(name),
    bool: (name) => bools.has(name),
    num: (name, fallback) => {
      const v = flags.get(name);
      return v === undefined ? fallback : Number(v);
    },
  };
}

/** HH:MM:SS.mmm timestamp for the live logs. */
export function ts(): string {
  return new Date().toISOString().slice(11, 23);
}

/** Build a connected adapter (resilient-wrapped) as both ports. */
export async function connect(
  broker: BrokerId,
): Promise<IMessageScheduler & IMessageBus> {
  const adapter = createScheduler(broker) as IMessageScheduler & IMessageBus;
  await adapter.connect();
  await adapter.connectBus();
  return adapter;
}

/** Run `onStop` on Ctrl-C, then exit. Keeps a subscriber process alive. */
export function keepAlive(onStop: () => Promise<void>): void {
  const stop = async () => {
    console.log(`\n${c.dim}shutting down…${c.reset}`);
    try {
      await onStop();
    } finally {
      process.exit(0);
    }
  };
  process.on('SIGINT', () => void stop());
  process.on('SIGTERM', () => void stop());
}

export const label = (text: string) => `${c.bold}${c.cyan}${text}${c.reset}`;
