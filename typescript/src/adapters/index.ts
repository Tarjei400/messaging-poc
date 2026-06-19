import { IMessageScheduler } from '../abstractions';
import { ResilientScheduler } from '../resilience';
import { ArtemisScheduler, artemisConfigFromEnv } from './artemis';
import { InMemoryScheduler } from './in-memory';
import { KafkaScheduler, kafkaConfigFromEnv } from './kafka';
import { RabbitMqScheduler, rabbitConfigFromEnv } from './rabbitmq';

export * from './in-memory';
export * from './artemis';
export * from './kafka';
export * from './rabbitmq';

export type BrokerId = 'artemis' | 'rabbitmq' | 'kafka' | 'in-memory';

/**
 * The single registration point for brokers. Adding a broker is: write an
 * adapter, add one line here. Nothing else in the codebase changes — that is
 * the Open/Closed principle made concrete.
 */
const REGISTRY: Record<BrokerId, () => IMessageScheduler> = {
  artemis: () => new ArtemisScheduler(artemisConfigFromEnv()),
  rabbitmq: () => new RabbitMqScheduler(rabbitConfigFromEnv()),
  kafka: () => new KafkaScheduler(kafkaConfigFromEnv()),
  'in-memory': () => new InMemoryScheduler(),
};

export const KNOWN_BROKERS = Object.keys(REGISTRY) as BrokerId[];

/**
 * Creates a broker adapter. Real brokers are wrapped in `ResilientScheduler` so
 * the comparison runs through the same retry + circuit-breaker layer you would
 * use in production. Pass `resilient = false` to get the raw adapter.
 */
export function createScheduler(id: BrokerId, resilient = true): IMessageScheduler {
  const factory = REGISTRY[id];
  if (!factory) {
    throw new Error(
      `Unknown broker '${id}'. Known: ${KNOWN_BROKERS.join(', ')}`,
    );
  }
  const scheduler = factory();
  return resilient ? new ResilientScheduler(scheduler) : scheduler;
}
