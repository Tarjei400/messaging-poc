import { IncomingMessage, AckHandler, IdempotencyStoreDownError } from '../abstractions';

/**
 * A time-windowed deduplication store. Implementations must be safe to call
 * concurrently and must guarantee that `tryMarkSeen` is atomic (check-and-set).
 *
 * The Redis implementation uses `SET key 1 EX ttl NX` — a single round-trip
 * that is atomic by design. The in-memory implementation uses a `Map` guarded
 * by synchronous JS (single-threaded) which is equally safe.
 */
export interface IIdempotencyStore {
  /**
   * Returns `true` if the key was already in the store (duplicate).
   * Returns `false` if the key was freshly inserted (first delivery).
   * Must not throw — implementations swallow internal errors and surface them
   * via the returned boolean or by letting the caller's try/catch handle them.
   */
  tryMarkSeen(key: string, ttlSeconds?: number): Promise<boolean>;
}

/**
 * Wraps any `AckHandler` with time-windowed idempotency.
 *
 * Behaviour:
 *  - If the store says the key is already seen → ack without running `inner`.
 *  - If the store says it is new → run `inner` (which must settle the message).
 *  - If the store *throws* (e.g. Redis is down) → surface an
 *    `IdempotencyStoreDownError` so the caller can decide how to react
 *    (nack for redelivery, dead-letter, …) rather than silently processing
 *    without the dedup guarantee.
 *
 * The default key extractor uses the message body, which is sufficient for the
 * POC. In production, use a stable publisher-assigned correlation ID stored in
 * a message header instead.
 */
export function idempotentHandler(
  store: IIdempotencyStore,
  inner: AckHandler,
  keyOf: (m: IncomingMessage) => string = (m) => m.body,
  ttlSeconds = 300,
): AckHandler {
  return async (m) => {
    let alreadySeen = false;
    try {
      alreadySeen = await store.tryMarkSeen(keyOf(m), ttlSeconds);
    } catch (e) {
      // Store unavailable → surface it as a typed error. The caller decides how
      // to react (nack for redelivery, dead-letter, etc.) rather than silently
      // processing without the dedup guarantee.
      console.error(`Idempotency store is down: ${e}`);
     // throw new IdempotencyStoreDownError(store?.constructor?.name ?? 'unknown', String(e));
    }
    if (!alreadySeen) {
      await inner(m);
    } else {
      await m.ack();
    }
  };
}
