using System.Collections.Concurrent;
using Messaging.Abstractions;

namespace Messaging.InMemory;

/// <summary>
/// A fully in-process scheduler. It speaks no wire protocol and needs no broker,
/// which makes it the perfect test double: the scenario suite and the runner can
/// be exercised in milliseconds in CI with zero infrastructure. It also serves as
/// the executable specification of what "correct" behaviour looks like, so the
/// real adapters can be diffed against it.
/// </summary>
public sealed class InMemoryScheduler : IMessageScheduler, IMessageBus
{
    public string Name => "In-Memory (reference)";

    public BusCapabilities BusCapabilities { get; } = new(
        SupportsTopic: true,
        SupportsFanout: true,
        SupportsManualAck: true,
        SupportsDeadLetter: true,
        ReportsDeliveryCount: true,
        SupportsDedup: true,
        SupportsStreamReplay: true);

    public Capabilities Capabilities { get; }

    private readonly ConcurrentDictionary<string, HashSet<MessageHandler>> _handlers = new();
    private readonly ConcurrentDictionary<string, Pending> _pending = new();
    private readonly InMemoryBus _bus = new();
    private int _seq;

    public InMemoryScheduler()
    {
        Capabilities = new Capabilities(
            Protocol: "in-memory",
            NativeScheduling: true,
            SupportsCancel: true,
            SupportsList: true,
            Bus: BusCapabilities);
    }

    public Task ConnectAsync(CancellationToken ct = default) => Task.CompletedTask;

    // --- bus port -----------------------------------------------------------

    public Task ConnectBusAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task PublishAsync(
        string topic, string payload, string? routingKey = null,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        _bus.Publish(topic, payload, routingKey, options);
        return Task.CompletedTask;
    }

    public Task<ISubscription> SubscribeAsync(
        string topic, AckHandler handler, SubscribeOptions? options = null, CancellationToken ct = default)
    {
        return Task.FromResult(_bus.Subscribe(topic, handler, options));
    }

    public Task SendNowAsync(string destination, string payload, CancellationToken ct = default)
    {
        Dispatch(destination, payload);
        return Task.CompletedTask;
    }

    public Task<ScheduleHandle> ScheduleAsync(
        string destination, string payload, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        var id = $"mem-{Interlocked.Increment(ref _seq)}";
        var due = deliverAt - DateTimeOffset.UtcNow;
        var ms = due < TimeSpan.Zero ? TimeSpan.Zero : due;

        var cts = new CancellationTokenSource();
        var pending = new Pending(id, destination, payload, deliverAt, cts);
        _pending[id] = pending;

        _ = Task.Delay(ms, cts.Token).ContinueWith(t =>
        {
            if (t.IsCanceled) return;
            _pending.TryRemove(id, out _);
            Dispatch(destination, payload);
        }, TaskScheduler.Default);

        return Task.FromResult(new ScheduleHandle(id, destination, deliverAt));
    }

    public Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default)
    {
        if (_pending.TryRemove(handle.Id, out var p))
            p.Cts.Cancel();
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(
        string destination, CancellationToken ct = default)
    {
        IReadOnlyList<ScheduledInfo> list = _pending.Values
            .Where(p => p.Destination == destination)
            .Select(p => new ScheduledInfo(p.Id, p.Destination, p.DeliverAt))
            .ToList();
        return Task.FromResult(list);
    }

    public Task<ISubscription> ConsumeAsync(
        string destination, MessageHandler handler, CancellationToken ct = default)
    {
        var set = _handlers.GetOrAdd(destination, _ => new HashSet<MessageHandler>());
        lock (set) set.Add(handler);
        ISubscription sub = new Subscription(() =>
        {
            lock (set) set.Remove(handler);
            return ValueTask.CompletedTask;
        });
        return Task.FromResult(sub);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var p in _pending.Values) p.Cts.Cancel();
        _pending.Clear();
        _handlers.Clear();
        _bus.Dispose();
        return ValueTask.CompletedTask;
    }

    private void Dispatch(string destination, string body)
    {
        var msg = new ReceivedMessage(
            $"mem-msg-{Interlocked.Increment(ref _seq)}",
            destination,
            body,
            new Dictionary<string, string>());

        // Deliver asynchronously, mirroring real broker push semantics.
        MessageHandler[] targets;
        if (!_handlers.TryGetValue(destination, out var set)) return;
        lock (set) targets = set.ToArray();

        _ = Task.Run(async () =>
        {
            foreach (var h in targets) await h(msg);
        });
    }

    private sealed record Pending(
        string Id,
        string Destination,
        string Body,
        DateTimeOffset DeliverAt,
        CancellationTokenSource Cts);
}

/// <summary>A trivial <see cref="ISubscription"/> backed by a dispose callback.</summary>
internal sealed class Subscription(Func<ValueTask> onDispose) : ISubscription
{
    public ValueTask DisposeAsync() => onDispose();
}
