using Messaging.Abstractions;
using Messaging.Faults;
using Messaging.InMemory;
using Messaging.Resilience;
using Messaging.Scenarios;
using Xunit;

namespace Messaging.Tests;

public class BusSuiteTests
{
    [Fact]
    public async Task Passes_every_bus_scenario_against_in_memory_reference()
    {
        var scheduler = new InMemoryScheduler();
        await scheduler.ConnectAsync();
        var report = await SuiteRunner.RunAsync(scheduler);
        await scheduler.DisposeAsync();

        // Every bus scenario (S5–S11) must pass against the reference engine.
        foreach (var name in new[] { "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17", "S18", "S19" })
        {
            var r = report.Results.First(x => x.Name.StartsWith(name));
            Assert.True(r.Status == ScenarioStatus.Pass, $"{r.Name}: {r.Detail}");
        }
    }

    [Fact]
    public void Exposes_the_expected_ordered_bus_suite()
    {
        var prefixes = ScenarioRegistry.AllBus.Select(s => s.Name.Split(" ")[0]).ToArray();
        Assert.Equal(new[] { "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17", "S18", "S19" }, prefixes);
    }
}

public class BusUnsupportedTests
{
    [Fact]
    public async Task A_scheduler_only_adapter_reports_every_bus_scenario_as_unsupported()
    {
        var scheduler = new BusUnsupportedFake();
        await scheduler.ConnectAsync();
        var report = await SuiteRunner.RunAsync(scheduler);
        await scheduler.DisposeAsync();

        // The bus port is absent → every bus scenario (S5–S11) is ⊘ n/a, never ✗.
        foreach (var name in new[] { "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17", "S18", "S19" })
        {
            var r = report.Results.First(x => x.Name.StartsWith(name));
            Assert.Equal(ScenarioStatus.Unsupported, r.Status);
        }
    }
}

public class FaultRedeliveryTests
{
    [Fact]
    public async Task Consumer_crash_redelivers_to_a_surviving_consumer()
    {
        await using var scheduler = new InMemoryScheduler();
        IMessageBus bus = scheduler;
        await bus.ConnectBusAsync();

        var topic = $"fault.crash.{Guid.NewGuid():N}";
        var queueId = $"workers-{Guid.NewGuid():N}";
        var redelivered = new TaskCompletionSource<string>();

        var crashed = await bus.SubscribeAsync(topic,
            _ => Task.CompletedTask, // receive but never settle → crash
            new SubscribeOptions { SubscriberId = queueId });

        await bus.PublishAsync(topic, "job-1");
        await Task.Delay(150);
        await crashed.DisposeAsync(); // drop with the message un-acked

        var fresh = await bus.SubscribeAsync(topic, m =>
        {
            redelivered.TrySetResult(m.Body);
            return m.AckAsync();
        }, new SubscribeOptions { SubscriberId = queueId });

        var done = await Task.WhenAny(redelivered.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        await fresh.DisposeAsync();

        Assert.True(done == redelivered.Task, "crashed consumer's message was never redelivered");
        Assert.Equal("job-1", await redelivered.Task);
    }

    [Fact]
    public async Task Broker_outage_fires_the_resilience_onEvent_hook()
    {
        // Wrap a fault-injecting bus over the in-memory reference in the production
        // resilience pipeline; an injected outage must surface as retry events.
        var events = new List<ResilienceEvent>();
        var inner = new InMemoryScheduler();
        var faulty = new FaultInjectingBus(inner);
        var resilient = new ResilientMessageScheduler(
            new FaultAdapter(inner, faulty),
            ResilienceOptions.Default with { BaseDelay = TimeSpan.FromMilliseconds(10) },
            onEvent: e => { lock (events) events.Add(e); });
        await using var _ = resilient;

        await resilient.ConnectBusAsync();

        // Two transient faults, then success: the pipeline must retry and recover.
        faulty.FailNext(2);
        await resilient.PublishAsync("fault.retry.topic", "payload");

        lock (events)
        {
            var retries = events.OfType<ResilienceEvent.Retry>().ToList();
            Assert.True(retries.Count >= 2, $"expected >=2 retry events, got {retries.Count}");
        }
    }
}

/// <summary>
/// A deliberately limited adapter: it schedules but does NOT implement
/// <see cref="IMessageBus"/> and declares no bus capability. It proves the runner
/// reports every bus scenario as <see cref="ScenarioStatus.Unsupported"/> (⊘),
/// not a failure, when an adapter lacks the bus port.
/// </summary>
internal sealed class BusUnsupportedFake : IMessageScheduler
{
    public string Name => "Fake (scheduler only, no bus)";

    public Capabilities Capabilities { get; } = new("fake", false, false, false, Bus: null);

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
        _ = Task.Delay(due < TimeSpan.Zero ? TimeSpan.Zero : due).ContinueWith(_ => Fire(destination, payload));
        return Task.FromResult(new ScheduleHandle("x", destination, deliverAt));
    }

    public Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default) =>
        throw new OperationNotSupportedException("cancel", Name);

    public Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(string destination, CancellationToken ct = default) =>
        throw new OperationNotSupportedException("listScheduled", Name);

    public Task<ISubscription> ConsumeAsync(string destination, MessageHandler handler, CancellationToken ct = default)
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
        public ValueTask DisposeAsync() { onDispose(); return ValueTask.CompletedTask; }
    }
}

/// <summary>Routes scheduler calls to the inner adapter and bus calls through the fault injector.</summary>
internal sealed class FaultAdapter(IMessageScheduler scheduler, IMessageBus bus)
    : IMessageScheduler, IMessageBus
{
    public string Name => scheduler.Name;
    public Capabilities Capabilities => scheduler.Capabilities;
    public BusCapabilities BusCapabilities => bus.BusCapabilities;

    public Task ConnectAsync(CancellationToken ct = default) => scheduler.ConnectAsync(ct);
    public Task SendNowAsync(string d, string p, CancellationToken ct = default) => scheduler.SendNowAsync(d, p, ct);
    public Task<ScheduleHandle> ScheduleAsync(string d, string p, DateTimeOffset at, CancellationToken ct = default) =>
        scheduler.ScheduleAsync(d, p, at, ct);
    public Task CancelAsync(ScheduleHandle h, CancellationToken ct = default) => scheduler.CancelAsync(h, ct);
    public Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(string d, CancellationToken ct = default) =>
        scheduler.ListScheduledAsync(d, ct);
    public Task<ISubscription> ConsumeAsync(string d, MessageHandler h, CancellationToken ct = default) =>
        scheduler.ConsumeAsync(d, h, ct);

    public Task ConnectBusAsync(CancellationToken ct = default) => bus.ConnectBusAsync(ct);
    public Task PublishAsync(string t, string p, string? rk = null, PublishOptions? o = null, CancellationToken ct = default) =>
        bus.PublishAsync(t, p, rk, o, ct);
    public Task<ISubscription> SubscribeAsync(string t, AckHandler h, SubscribeOptions? o = null, CancellationToken ct = default) =>
        bus.SubscribeAsync(t, h, o, ct);

    public ValueTask DisposeAsync() => bus.DisposeAsync();
}
