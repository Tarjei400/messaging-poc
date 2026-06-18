import { Capabilities, IMessageBus, IMessageScheduler } from './abstractions';
import { GLYPH, HR, c, yn } from './ansi';
import {
  ALL_BUS_SCENARIOS,
  ALL_SCENARIOS,
  BusScenario,
  Scenario,
  ScenarioResult,
} from './scenarios';

export interface RunReport {
  readonly broker: string;
  readonly capabilities: Capabilities;
  readonly results: readonly ScenarioResult[];
}

/**
 * Runs the full suite (scheduling S1–S4 + pub/sub S5–S9) against a single,
 * already-constructed adapter.
 *
 * The runner depends on `IMessageScheduler` (+ optionally `IMessageBus`) and
 * nothing else — it has no idea which broker it is exercising. That is the
 * payoff of the abstraction: one runner, every broker. If an adapter does not
 * implement the bus port, every bus scenario is reported as `⊘ n/a`.
 */
export async function runSuite(scheduler: IMessageScheduler): Promise<RunReport> {
  const results: ScenarioResult[] = [];
  for (const scenario of ALL_SCENARIOS) {
    results.push(await runOne(scenario, scheduler));
  }

  // The bus suite runs only when the adapter declares a bus surface. Adapters
  // that don't (a scheduler-only fake) report every bus scenario as ⊘ n/a.
  if (scheduler.capabilities.bus) {
    const bus = scheduler as unknown as IMessageBus;
    let connectError: unknown;
    try {
      await bus.connectBus();
    } catch (err) {
      connectError = err;
    }
    for (const scenario of ALL_BUS_SCENARIOS) {
      results.push(
        connectError
          ? failResult(scenario.name, connectError)
          : await runOneBus(scenario, bus),
      );
    }
  } else {
    for (const scenario of ALL_BUS_SCENARIOS) {
      results.push(naResult(scenario.name, 'adapter has no bus port'));
    }
  }

  return {
    broker: scheduler.name,
    capabilities: scheduler.capabilities,
    results,
  };
}

async function runOne(
  scenario: Scenario,
  scheduler: IMessageScheduler,
): Promise<ScenarioResult> {
  try {
    return await scenario.run(scheduler);
  } catch (err) {
    return failResult(scenario.name, err);
  }
}

async function runOneBus(
  scenario: BusScenario,
  bus: IMessageBus,
): Promise<ScenarioResult> {
  try {
    return await scenario.run(bus);
  } catch (err) {
    return failResult(scenario.name, err);
  }
}

function naResult(name: string, detail: string): ScenarioResult {
  return { name, status: 'unsupported', detail, durationMs: 0 };
}

function failResult(name: string, err: unknown): ScenarioResult {
  return {
    name,
    status: 'fail',
    detail: `threw: ${(err as Error).message}`,
    durationMs: 0,
  };
}

export function printReport(report: RunReport): void {
  const cap = report.capabilities;
  const capLine =
    `protocol=${cap.protocol}  ` +
    `native-scheduling=${yn(cap.nativeScheduling)}  ` +
    `cancel=${yn(cap.supportsCancel)}  ` +
    `list=${yn(cap.supportsList)}`;
  const busLine = cap.bus
    ? `pub/sub=${yn(cap.bus.supportsTopic)}  ` +
      `fanout=${yn(cap.bus.supportsFanout)}  ` +
      `manual-ack=${yn(cap.bus.supportsManualAck)}  ` +
      `dead-letter=${yn(cap.bus.supportsDeadLetter)}  ` +
      `delivery-count=${yn(cap.bus.reportsDeliveryCount)}`
    : undefined;

  console.log('');
  console.log(`${c.bold}${c.cyan}${report.broker}${c.reset}`);
  console.log(`${c.dim}${capLine}${c.reset}`);
  if (busLine) console.log(`${c.dim}${busLine}${c.reset}`);
  console.log(HR);
  for (const r of report.results) {
    const name = r.name.padEnd(28);
    const time = `${String(r.durationMs).padStart(5)}ms`;
    console.log(
      `  ${GLYPH[r.status]}  ${name} ${c.dim}${time}${c.reset}  ${r.detail}`,
    );
  }
  const passed = countBy(report, 'pass');
  const na = countBy(report, 'unsupported');
  const failed = countBy(report, 'fail');
  console.log(HR);
  console.log(
    `  ${c.green}${passed} passed${c.reset}, ` +
      `${c.yellow}${na} unsupported${c.reset}, ` +
      `${failed ? c.red : c.dim}${failed} failed${c.reset}`,
  );
}

const countBy = (r: RunReport, s: ScenarioResult['status']) =>
  r.results.filter((x) => x.status === s).length;
