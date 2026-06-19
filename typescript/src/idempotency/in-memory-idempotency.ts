import { IIdempotencyStore } from './idempotency-store';

/**
 * In-process, TTL-aware idempotency store. Uses the JS single-threaded event
 * loop for atomicity — no locks needed. Suitable for tests and single-process
 * deployments; replace with `RedisIdempotencyStore` when you need cross-process
 * deduplication.
 */
export class InMemoryIdempotencyStore implements IIdempotencyStore {
  /** key → expiry timestamp (ms since epoch) */
  private readonly seen = new Map<string, number>();

  async tryMarkSeen(key: string, ttlSeconds = 300): Promise<boolean> {
    const now = Date.now();
    const expiry = this.seen.get(key);
    if (expiry !== undefined && expiry > now) {
      return true; // duplicate within the time window
    }
    this.seen.set(key, now + ttlSeconds * 1000);
    return false; // first time (or expired)
  }
}
