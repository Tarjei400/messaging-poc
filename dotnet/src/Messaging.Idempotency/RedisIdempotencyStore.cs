using StackExchange.Redis;

namespace Messaging.Idempotency;

/// <summary>
/// Redis-backed idempotency store using <c>SET key 1 EX ttl NX</c>, which is
/// an atomic check-and-set in a single round-trip. Returns <c>true</c>
/// (duplicate) when the key already existed, <c>false</c> (fresh) when it was
/// newly inserted.
///
/// Reads the connection string from <c>REDIS_URL</c>
/// (default <c>localhost:6379</c>).
/// </summary>
public sealed class RedisIdempotencyStore : IIdempotencyStore, IAsyncDisposable
{
    private readonly Lazy<Task<IDatabase>> _db;
    private ConnectionMultiplexer? _multiplexer;

    public RedisIdempotencyStore(string? connectionString = null)
    {
        var cs = connectionString
            ?? Environment.GetEnvironmentVariable("REDIS_URL")
            ?? "localhost:6379";

        _db = new Lazy<Task<IDatabase>>(async () =>
        {
            _multiplexer = await ConnectionMultiplexer.ConnectAsync(cs);
            return _multiplexer.GetDatabase();
        });
    }

    public async Task<bool> TryMarkSeenAsync(string key, int ttlSeconds = 300)
    {
        var db = await _db.Value;
        // SET NX returns false when the key already exists → duplicate.
        var inserted = await db.StringSetAsync(
            key, "1", TimeSpan.FromSeconds(ttlSeconds), When.NotExists);
        return !inserted; // !inserted = key existed = duplicate
    }

    public async ValueTask DisposeAsync()
    {
        if (_multiplexer is not null)
            await _multiplexer.DisposeAsync();
    }
}
