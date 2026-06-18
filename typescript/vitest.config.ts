import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // The scheduling scenarios (S2 +3s, S3 +5s) use real timers against the
    // in-memory reference, so the end-to-end suite needs more than the 5s
    // default. Individual fast tests are unaffected.
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
