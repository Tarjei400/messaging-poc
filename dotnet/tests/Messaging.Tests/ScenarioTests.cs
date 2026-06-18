using Messaging.Abstractions;
using Messaging.InMemory;
using Messaging.Resilience;
using Messaging.Scenarios;
using Xunit;

namespace Messaging.Tests;

public class ScenarioSuiteTests
{
    [Fact]
    public async Task Passes_every_scenario_against_in_memory_reference()
    {
        var scheduler = new InMemoryScheduler();
        await scheduler.ConnectAsync();
        var report = await SuiteRunner.RunAsync(scheduler);
        await scheduler.DisposeAsync();

        var failed = report.Results.Where(r => r.Status == ScenarioStatus.Fail).ToList();
        Assert.True(failed.Count == 0,
            "unexpected failures: " + string.Join("; ", failed.Select(f => $"{f.Name}: {f.Detail}")));
        Assert.All(report.Results, r => Assert.Equal(ScenarioStatus.Pass, r.Status));
    }
}

public class GracefulDegradationTests
{
    [Fact]
    public async Task Reports_cancel_and_list_as_unsupported_not_failed()
    {
        var scheduler = new CancelUnsupportedFake();
        await scheduler.ConnectAsync();
        var report = await SuiteRunner.RunAsync(scheduler);
        await scheduler.DisposeAsync();

        ScenarioResult ByName(string n) => report.Results.First(r => r.Name.StartsWith(n));
        Assert.Equal(ScenarioStatus.Pass, ByName("S1").Status);
        Assert.Equal(ScenarioStatus.Pass, ByName("S2").Status);
        Assert.Equal(ScenarioStatus.Unsupported, ByName("S3").Status);
        Assert.Equal(ScenarioStatus.Unsupported, ByName("S4").Status);
    }
}

public class ScenarioRegistryTests
{
    [Fact]
    public void Exposes_the_expected_ordered_suite()
    {
        var prefixes = ScenarioRegistry.All.Select(s => s.Name[..2]).ToArray();
        Assert.Equal(new[] { "S1", "S2", "S3", "S4" }, prefixes);
    }
}

public class ResilienceDecoratorTests
{
    [Fact]
    public async Task Wrapped_in_memory_still_passes_every_scenario()
    {
        var scheduler = new ResilientMessageScheduler(new InMemoryScheduler());
        await scheduler.ConnectAsync();
        var report = await SuiteRunner.RunAsync(scheduler);
        await scheduler.DisposeAsync();

        Assert.All(report.Results, r => Assert.Equal(ScenarioStatus.Pass, r.Status));
        Assert.EndsWith("+ Polly", scheduler.Name);
    }

    [Fact]
    public async Task Does_not_trip_the_breaker_on_unsupported_operations()
    {
        // The decorator must treat OperationNotSupportedException as a contract
        // outcome, not a transient fault — so cancel/list stay "unsupported".
        var scheduler = new ResilientMessageScheduler(new CancelUnsupportedFake());
        await scheduler.ConnectAsync();
        var report = await SuiteRunner.RunAsync(scheduler);
        await scheduler.DisposeAsync();

        ScenarioResult ByName(string n) => report.Results.First(r => r.Name.StartsWith(n));
        Assert.Equal(ScenarioStatus.Pass, ByName("S1").Status);
        Assert.Equal(ScenarioStatus.Pass, ByName("S2").Status);
        Assert.Equal(ScenarioStatus.Unsupported, ByName("S3").Status);
        Assert.Equal(ScenarioStatus.Unsupported, ByName("S4").Status);
    }
}

/// <summary>
/// A deliberately limited adapter: it can send/schedule/consume but cannot
/// cancel or list. It proves that scenarios treat a declared gap as
/// <see cref="ScenarioStatus.Unsupported"/>, not a failure — and that nothing in
/// the runner is coupled to a real broker.
/// </summary>
internal sealed class CancelUnsupportedFake : IMessageScheduler
{
    public string Name => "Fake (no cancel/list)";

    public Capabilities Capabilities { get; } = new("fake", false, false, false);

    private readonly Dictionary<string, HashSet<MessageHandler>> _handlers = new();

    public Task ConnectAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task SendNowAsync(string destination, string payload, CancellationToken ct = default)
    {
        Fire(destination, payload);
        return Task.CompletedTask;
    }

    public Task<ScheduleHandle> ScheduleAsync(
        string destination, string payload, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        var due = deliverAt - DateTimeOffset.UtcNow;
        _ = Task.Delay(due < TimeSpan.Zero ? TimeSpan.Zero : due)
            .ContinueWith(_ => Fire(destination, payload));
        return Task.FromResult(new ScheduleHandle("x", destination, deliverAt));
    }

    public Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default) =>
        throw new OperationNotSupportedException("cancel", Name);

    public Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(
        string destination, CancellationToken ct = default) =>
        throw new OperationNotSupportedException("listScheduled", Name);

    public Task<ISubscription> ConsumeAsync(
        string destination, MessageHandler handler, CancellationToken ct = default)
    {
        if (!_handlers.TryGetValue(destination, out var set))
            _handlers[destination] = set = new HashSet<MessageHandler>();
        set.Add(handler);
        ISubscription sub = new FakeSub(() => set.Remove(handler));
        return Task.FromResult(sub);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private void Fire(string destination, string body)
    {
        if (!_handlers.TryGetValue(destination, out var set)) return;
        foreach (var h in set.ToArray())
            _ = h(new ReceivedMessage("m", destination, body, new Dictionary<string, string>()));
    }

    private sealed class FakeSub(Action onDispose) : ISubscription
    {
        public ValueTask DisposeAsync()
        {
            onDispose();
            return ValueTask.CompletedTask;
        }
    }
}
