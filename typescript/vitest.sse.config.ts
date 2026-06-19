import { defineConfig } from 'vitest/config';

/**
 * Dedicated config for the SSE cross-instance cluster test. It needs live
 * brokers (Artemis + RabbitMQ), so it is NOT part of the no-broker unit run
 * (`npm test`, which excludes it). Run it explicitly via `npm run test:sse`.
 */
export default defineConfig({
  test: {
    include: ['test/sse.test.ts'],
    testTimeout: 60000,
    hookTimeout: 60000,
  },
});
