using Messaging.Abstractions;

namespace Messaging.Idempotency;

/// <summary>
/// A time-windowed deduplication store. Implementations must be safe to call
/// concurrently and must guarantee that <see cref="TryMarkSeenAsync"/> is
/// atomic (check-and-set).
///
/// The Redis implementation uses <c>SET key 1 EX ttl NX</c> — a single
/// round-trip that is atomic by design. The in-memory implementation uses
/// a lock-guarded dictionary which is equally safe.
/// </summary>
public interface IIdempotencyStore
{
    /// <summary>
    /// Returns <c>true</c> if the key was already in the store (duplicate).
    /// Returns <c>false</c> if the key was freshly inserted (first delivery).
    /// </summary>
    Task<bool> TryMarkSeenAsync(string key, int ttlSeconds = 300);
}

/// <summary>
/// Wraps any <see cref="AckHandler"/> with time-windowed idempotency.
///
/// Behaviour:
/// <list type="bullet">
///   <item>Store says already seen → ack without running <paramref name="inner"/>.</item>
///   <item>Store says it is new → run <paramref name="inner"/> (which must settle the message).</item>
///   <item>Store <em>throws</em> (e.g. Redis is down) → fail-open: run <paramref name="inner"/>
///     anyway. This degrades to at-least-once but never blocks message processing.</item>
/// </list>
///
/// The default key extractor uses the message body, which is sufficient for
/// the POC. In production, use a stable publisher-assigned correlation ID
/// stored in a message header instead.
/// </summary>
public static class IdempotentHandler
{
    public static AckHandler Wrap(
        IIdempotencyStore store,
        AckHandler inner,
        Func<IIncomingMessage, string>? keyOf = null,
        int ttlSeconds = 300)
    {
        return async m =>
        {
            var alreadySeen = false;
            try
            {
                alreadySeen = await store.TryMarkSeenAsync(keyOf?.Invoke(m) ?? m.Body, ttlSeconds);
            }
            catch
            {
                // Store unavailable → fail-open: process the message to avoid stalling.
            }

            if (!alreadySeen)
                await inner(m);
            else
                await m.AckAsync();
        };
    }
}
