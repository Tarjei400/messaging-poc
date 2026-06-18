using Messaging.Abstractions;

namespace Messaging.Faults;

/// <summary>
/// A test decorator over any <see cref="IMessageBus"/> that injects controlled
/// faults so the resilience story can be <i>demonstrated</i>, not just asserted:
///
///  - <see cref="FailNext"/> makes the next N broker calls throw, exercising the
///    resilience decorator's retry → circuit-breaker pipeline (when this is
///    wrapped by <c>ResilientMessageScheduler</c>);
///  - <see cref="Disconnect"/> / <see cref="Reconnect"/> simulate a broker outage
///    so every call fails until the connection is restored.
///
/// It is deliberately transparent when no fault is armed, so a healthy run is
/// unaffected (Decorator / Liskov).
/// </summary>
public sealed class FaultInjectingBus : IMessageBus
{
    private readonly IMessageBus _inner;
    private int _failNext;
    private volatile bool _connected = true;

    public FaultInjectingBus(IMessageBus inner) => _inner = inner;

    public string Name => $"{_inner.Name} (fault-injecting)";
    public BusCapabilities BusCapabilities => _inner.BusCapabilities;

    /// <summary>Arm the next <paramref name="count"/> broker calls to throw a transient fault.</summary>
    public void FailNext(int count) => Interlocked.Exchange(ref _failNext, count);

    /// <summary>Simulate a broker outage: every call throws until <see cref="Reconnect"/>.</summary>
    public void Disconnect() => _connected = false;

    /// <summary>Restore the connection after a simulated outage.</summary>
    public void Reconnect() => _connected = true;

    private void Guard(string op)
    {
        if (!_connected)
            throw new IOException($"{op}: broker connection is down (injected outage)");
        // Consume one armed fault, if any, atomically.
        int current;
        do
        {
            current = Volatile.Read(ref _failNext);
            if (current <= 0) return;
        }
        while (Interlocked.CompareExchange(ref _failNext, current - 1, current) != current);
        throw new IOException($"{op}: injected transient fault");
    }

    public Task ConnectBusAsync(CancellationToken ct = default)
    {
        Guard(nameof(ConnectBusAsync));
        return _inner.ConnectBusAsync(ct);
    }

    public Task PublishAsync(string topic, string payload, string? routingKey = null, CancellationToken ct = default)
    {
        Guard(nameof(PublishAsync));
        return _inner.PublishAsync(topic, payload, routingKey, ct);
    }

    public Task<ISubscription> SubscribeAsync(
        string topic, AckHandler handler, SubscribeOptions? options = null, CancellationToken ct = default)
    {
        Guard(nameof(SubscribeAsync));
        return _inner.SubscribeAsync(topic, handler, options, ct);
    }

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
