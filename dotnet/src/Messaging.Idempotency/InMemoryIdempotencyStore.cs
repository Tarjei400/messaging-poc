namespace Messaging.Idempotency;

/// <summary>
/// In-process, TTL-aware idempotency store. Uses a lock-guarded dictionary
/// for atomicity. Suitable for tests and single-process deployments; replace
/// with <see cref="RedisIdempotencyStore"/> when you need cross-process
/// deduplication.
/// </summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    /// <summary>key → expiry (UTC ticks).</summary>
    private readonly Dictionary<string, long> _seen = new();
    private readonly object _gate = new();

    public Task<bool> TryMarkSeenAsync(string key, int ttlSeconds = 300)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_gate)
        {
            if (_seen.TryGetValue(key, out var expiryTicks) &&
                expiryTicks > now.UtcTicks)
            {
                return Task.FromResult(true); // duplicate within the time window
            }
            _seen[key] = (now + TimeSpan.FromSeconds(ttlSeconds)).UtcTicks;
        }
        return Task.FromResult(false); // first time (or expired)
    }
}
