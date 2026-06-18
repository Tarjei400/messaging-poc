using System.Diagnostics;
using Messaging.Abstractions;

namespace Messaging.Scenarios;

public enum ScenarioStatus
{
    Pass,
    Fail,
    Unsupported,
    Skipped,
}

public sealed record ScenarioResult(
    string Name,
    ScenarioStatus Status,
    string Detail,
    long DurationMs);

/// <summary>
/// A single observable behaviour we want every broker to demonstrate (or
/// honestly fail to demonstrate). Scenarios touch only <see cref="IMessageScheduler"/>,
/// so the exact same list runs against Artemis, RabbitMQ, or the in-memory
/// fake — written once, run everywhere (DRY / Open-Closed).
/// </summary>
public interface IScenario
{
    string Name { get; }
    string Description { get; }
    Task<ScenarioResult> RunAsync(IMessageScheduler scheduler);
}

/// <summary>
/// A scenario that exercises the pub/sub + ack surface (<see cref="IMessageBus"/>)
/// instead of the scheduling surface. Kept as a distinct type so the runner can
/// route it to the right port and so adapters without a bus report ⊘ n/a uniformly.
/// </summary>
public interface IBusScenario
{
    string Name { get; }
    string Description { get; }
    Task<ScenarioResult> RunAsync(IMessageBus bus);
}

/// <summary>Small, dependency-free helpers shared by scenarios (kept here to stay DRY).</summary>
public static class ScenarioHelpers
{
    private static int _counter;

    /// <summary>Unique-enough token so concurrent scenarios never read each other's mail.</summary>
    public static string Nonce()
    {
        var n = Interlocked.Increment(ref _counter);
        return $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds():x}-{n}-{Guid.NewGuid():N}".Substring(0, 24);
    }

    /// <summary>Poll <paramref name="predicate"/> until true or the timeout elapses.</summary>
    public static async Task<bool> WaitUntilAsync(
        Func<bool> predicate, TimeSpan timeout, TimeSpan? interval = null)
    {
        var step = interval ?? TimeSpan.FromMilliseconds(50);
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (predicate()) return true;
            await Task.Delay(step);
        }
        return predicate();
    }

    public static ScenarioResult Pass(string name, string detail, long t0) =>
        new(name, ScenarioStatus.Pass, detail, Elapsed(t0));

    public static ScenarioResult Fail(string name, string detail, long t0) =>
        new(name, ScenarioStatus.Fail, detail, Elapsed(t0));

    public static ScenarioResult Unsupported(string name, string detail, long t0) =>
        new(name, ScenarioStatus.Unsupported, detail, Elapsed(t0));

    public static long StartClock() => Stopwatch.GetTimestamp();

    private static long Elapsed(long t0) =>
        (long)Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
}

/// <summary>Subscribe and accumulate received messages for later assertions.</summary>
public sealed class MessageCollector : IAsyncDisposable
{
    private readonly List<ReceivedMessage> _received = new();
    private readonly object _gate = new();
    private ISubscription? _sub;

    public static async Task<MessageCollector> StartAsync(
        IMessageScheduler scheduler, string destination)
    {
        var c = new MessageCollector();
        c._sub = await scheduler.ConsumeAsync(destination, msg =>
        {
            lock (c._gate) c._received.Add(msg);
            return Task.CompletedTask;
        });
        return c;
    }

    public IReadOnlyList<string> Bodies()
    {
        lock (_gate) return _received.Select(m => m.Body).ToList();
    }

    public async ValueTask DisposeAsync()
    {
        if (_sub is not null) await _sub.DisposeAsync();
    }
}

/// <summary>
/// Subscribe to a topic and accumulate received messages, auto-acking each by
/// default. The <c>onMessage</c> hook lets a scenario take explicit control
/// (nack, crash, delay) for the ack/redelivery/poison cases.
/// </summary>
public sealed class AckCollector : IAsyncDisposable
{
    private readonly List<IIncomingMessage> _received = new();
    private readonly object _gate = new();
    private ISubscription? _sub;

    public static async Task<AckCollector> StartAsync(
        IMessageBus bus,
        string topic,
        SubscribeOptions? options = null,
        Func<IIncomingMessage, Task>? onMessage = null,
        bool autoAck = true)
    {
        var collector = new AckCollector();
        AckHandler handler = async m =>
        {
            lock (collector._gate) collector._received.Add(m);
            if (onMessage is not null) await onMessage(m);
            else if (autoAck) await m.AckAsync();
        };
        collector._sub = await bus.SubscribeAsync(topic, handler, options);
        return collector;
    }

    public IReadOnlyList<IIncomingMessage> Received
    {
        get { lock (_gate) return _received.ToList(); }
    }

    public IReadOnlyList<string> Bodies()
    {
        lock (_gate) return _received.Select(m => m.Body).ToList();
    }

    public int Count()
    {
        lock (_gate) return _received.Count;
    }

    public async ValueTask DisposeAsync()
    {
        if (_sub is not null) await _sub.DisposeAsync();
    }
}
