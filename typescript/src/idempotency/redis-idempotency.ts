import { IIdempotencyStore } from './idempotency-store';

/**
 * Redis-backed idempotency store. Uses `SET key 1 EX <ttl> NX`, which is an
 * atomic check-and-set in a single round-trip. Returns `true` (duplicate) when
 * the key already existed, `false` (fresh) when it was newly inserted.
 *
 * Reads connection details from `REDIS_URL` (default `redis://localhost:6379`).
 *
 * This implementation is intentionally lazy-imported so that the `ioredis`
 * package is only resolved at runtime when the class is actually instantiated.
 * Test paths using `InMemoryIdempotencyStore` never trigger the import.
 */
export class RedisIdempotencyStore implements IIdempotencyStore {
  // Typed as `unknown` here to avoid a hard compile-time dependency on ioredis
  // types. The actual Redis client is created dynamically below.
  private client: unknown = null;

  constructor(private readonly url = process.env.REDIS_URL ?? 'redis://localhost:6379') {}

  private async ensureClient(): Promise<{
    set(key: string, value: string, exMode: 'EX', ttl: number, flag: 'NX'): Promise<string | null>;
    quit(): Promise<unknown>;
  }> {
    if (this.client) return this.client as ReturnType<typeof this.ensureClient> extends Promise<infer T> ? T : never;
    const { default: Redis } = await import('ioredis');
    this.client = new Redis(this.url);
    return this.client as ReturnType<typeof this.ensureClient> extends Promise<infer T> ? T : never;
  }

  async tryMarkSeen(key: string, ttlSeconds = 300): Promise<boolean> {
    const redis = await this.ensureClient();
    // SET NX returns null when the key already exists → duplicate.
    const result = await redis.set(key, '1', 'EX', ttlSeconds, 'NX');
    return result === null; // null = key existed (duplicate); 'OK' = newly set (fresh)
  }
}
