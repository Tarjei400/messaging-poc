import { BusScenario, Scenario } from './scenario';
import { immediateDelivery } from './s1-immediate';
import { scheduledDelivery } from './s2-scheduled';
import { cancelScheduled } from './s3-cancel';
import { listPending } from './s4-list';
import { pubSub } from './s5-pubsub';
import { fanout } from './s6-fanout';
import { explicitAck } from './s7-explicit-ack';
import { poisonDlq } from './s8-poison-dlq';
import { competingConsumers } from './s9-competing';

/**
 * The scheduling suite (S1–S4) — exercises the `IMessageScheduler` port.
 * Order matters only for readability of output. Adding a scenario here
 * automatically runs it against every adapter (Open/Closed).
 */
export const ALL_SCENARIOS: readonly Scenario[] = [
  immediateDelivery,
  scheduledDelivery,
  cancelScheduled,
  listPending,
];

/**
 * The pub/sub suite (S5–S9) — exercises the `IMessageBus` port. Disconnect/
 * reconnect and broker-restart durability are demonstrated in the dedicated
 * fault-tolerance mode (they need controlled outages, not pass/fail assertions).
 */
export const ALL_BUS_SCENARIOS: readonly BusScenario[] = [
  pubSub,
  fanout,
  explicitAck,
  poisonDlq,
  competingConsumers,
];

export * from './scenario';
