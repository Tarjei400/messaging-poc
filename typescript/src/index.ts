import { IMessageScheduler } from './abstractions';
import { BrokerId, KNOWN_BROKERS, createScheduler } from './adapters';
import { RunReport, printReport, runSuite } from './runner';

/**
 * Usage:
 *   npm run scenarios -- <broker>      e.g. artemis | rabbitmq | in-memory
 *   npm run scenarios -- all           run every broker that is reachable
 *
 * Exit code is non-zero if any scenario *failed* (unsupported does not count as
 * a failure — it is an expected, declared gap).
 */
async function main(): Promise<void> {
  const arg = (process.argv[2] ?? 'in-memory').toLowerCase();
  const brokers: BrokerId[] =
    arg === 'all'
      ? KNOWN_BROKERS.filter((b) => b !== 'in-memory')
      : ([arg] as BrokerId[]);

  if (arg !== 'all' && !KNOWN_BROKERS.includes(arg as BrokerId)) {
    console.error(`Unknown broker '${arg}'. Known: ${KNOWN_BROKERS.join(', ')}, all`);
    process.exit(2);
  }

  const reports: RunReport[] = [];
  for (const id of brokers) {
    const scheduler = createScheduler(id);
    const report = await runAgainst(id, scheduler);
    if (report) {
      reports.push(report);
      printReport(report);
    }
  }

  const failed = reports.some((r) =>
    r.results.some((x) => x.status === 'fail'),
  );
  process.exit(failed ? 1 : 0);
}

async function runAgainst(
  id: BrokerId,
  scheduler: IMessageScheduler,
): Promise<RunReport | undefined> {
  try {
    await scheduler.connect();
  } catch (err) {
    console.error(
      `\n[${id}] could not connect: ${(err as Error).message}\n` +
        `  Is the broker running? Try: docker compose up -d ${id}`,
    );
    return undefined;
  }
  try {
    return await runSuite(scheduler);
  } finally {
    await scheduler[Symbol.asyncDispose]();
  }
}

void main();
