using Messaging.Abstractions;
using Messaging.Resilience;
using Messaging.Scenarios;

namespace Messaging.Faults;

/// <summary>
/// Narrates the fault-tolerance story as a real timeline (not a faked one). Each
/// act drives a controlled outage through <see cref="FaultInjectingBus"/> and
/// prints the resilience lifecycle events emitted by the
/// <see cref="ResilientMessageScheduler"/> onEvent hook, alongside the observable
/// messaging outcome (redelivery, dead-letter).
///
/// The three acts mirror the production failure modes the comparison cares about:
///   1. consumer crash → redelivery to a surviving consumer;
///   2. broker disconnect → retry → circuit-open → reconnect → recovery;
///   3. poison message → dead-letter after N attempts.
/// </summary>
public sealed class FaultNarrator
{
    private readonly Func<IMessageScheduler> _innerFactory;

    /// <param name="innerFactory">
    /// Builds a fresh underlying adapter (in-memory or a real broker) for each act.
    /// </param>
    public FaultNarrator(Func<IMessageScheduler> innerFactory) => _innerFactory = innerFactory;

    public async Task RunAsync()
    {
        Title("Fault-tolerance timeline");
        await ConsumerCrashAsync();
        await BrokerOutageAsync();
        await PoisonDeadLetterAsync();
        Console.WriteLine();
    }

    // Act 1 — a consumer that drops without acking; the message must come back.
    private async Task ConsumerCrashAsync()
    {
        Act("1. consumer crash → redelivery");
        var inner = _innerFactory();
        await using var scheduler = inner;
        var bus = (IMessageBus)inner;
        await bus.ConnectBusAsync();

        var topic = $"fault.crash.{Guid.NewGuid():N}";
        var queueId = $"workers-{Guid.NewGuid():N}";
        var redelivered = new TaskCompletionSource<string>();

        var crashedSub = await bus.SubscribeAsync(topic, _ =>
        {
            Step("consumer A received the job, then crashed (never acked)");
            return Task.CompletedTask; // never settle → simulate a crash
        }, new SubscribeOptions { SubscriberId = queueId });

        await bus.PublishAsync(topic, "shipment-42");
        await Task.Delay(200);
        Step("consumer A connection dropped");
        await crashedSub.DisposeAsync(); // drop with the message un-acked

        var freshSub = await bus.SubscribeAsync(topic, m =>
        {
            redelivered.TrySetResult(m.Body);
            return m.AckAsync();
        }, new SubscribeOptions { SubscriberId = queueId });

        var body = await Race(redelivered.Task, TimeSpan.FromSeconds(5));
        Outcome(body is not null
            ? $"consumer B picked up '{body}' — no message lost"
            : "FAILED: message was lost");
        await freshSub.DisposeAsync();
    }

    // Act 2 — broker goes away; the resilience pipeline retries, the breaker opens,
    // then the broker comes back and the call succeeds.
    private async Task BrokerOutageAsync()
    {
        Act("2. broker disconnect → retry → circuit-open → reconnect");
        var inner = _innerFactory();
        var faulty = new FaultInjectingBus((IMessageBus)inner);

        // Wrap the faulty bus in the production resilience pipeline and subscribe to
        // its lifecycle events so the timeline is real.
        var breakDuration = TimeSpan.FromMilliseconds(500);
        var resilient = new ResilientMessageScheduler(
            new BusOnlyScheduler(inner, faulty),
            ResilienceOptions.Default with
            {
                BaseDelay = TimeSpan.FromMilliseconds(50),
                BreakDuration = breakDuration,
            },
            onEvent: e => Step(Describe(e)));
        await using var _ = resilient;

        await resilient.ConnectBusAsync();
        var topic = $"fault.outage.{Guid.NewGuid():N}";

        Step("broker goes down");
        faulty.Disconnect();
        var publishFailed = false;
        try
        {
            await resilient.PublishAsync(topic, "while-down");
        }
        catch (Exception ex)
        {
            publishFailed = true;
            Step($"publish gave up after retries: {ex.GetType().Name}");
        }

        Step("broker comes back");
        faulty.Reconnect();
        // Wait for the breaker's cool-down so the next call probes (half-open).
        await Task.Delay(breakDuration + TimeSpan.FromMilliseconds(100));
        await resilient.PublishAsync(topic, "after-recovery");
        Outcome(publishFailed
            ? "retries exhausted during the outage, but the call succeeded once the broker recovered"
            : "the call rode out the blip transparently");
    }

    // Act 3 — a message that always fails is bounded and dead-lettered.
    private async Task PoisonDeadLetterAsync()
    {
        Act("3. poison message → dead-letter");
        var inner = _innerFactory();
        await using var scheduler = inner;
        var bus = (IMessageBus)inner;
        if (!bus.BusCapabilities.SupportsDeadLetter)
        {
            Outcome("dead-letter not supported on this broker — skipped");
            return;
        }
        await bus.ConnectBusAsync();

        // Use the `mbc.` prefix so Artemis's broker-side dead-letter policy (which
        // matches `mbc.#` in broker.xml) applies — same convention the S8 scenario
        // relies on. In-memory and RabbitMQ dead-letter from the subscribe options.
        var topic = $"mbc.fault.poison.{Guid.NewGuid():N}";
        var attempts = 0;
        var landed = new TaskCompletionSource<int>();

        var mainSub = await bus.SubscribeAsync(topic, m =>
        {
            attempts += 1;
            Step($"delivery attempt #{attempts} failed → nack(requeue)");
            return m.NackAsync(true);
        }, new SubscribeOptions
        {
            SubscriberId = $"poison-{Guid.NewGuid():N}",
            DeadLetter = true,
            MaxDeliveries = MessageBus.DefaultMaxDeliveries,
        });
        var dlqSub = await bus.SubscribeAsync(MessageBus.DeadLetterAddress(topic), m =>
        {
            landed.TrySetResult(attempts);
            return m.AckAsync();
        }, new SubscribeOptions { Kind = TopologyKind.Fanout, SubscriberId = $"dlq-{Guid.NewGuid():N}" });

        await bus.PublishAsync(topic, "poison-99");
        var at = await Race(landed.Task, TimeSpan.FromSeconds(8));
        Outcome(at > 0
            ? $"dead-lettered after {at} attempts — an operator can now inspect it"
            : "FAILED: never dead-lettered");
        await mainSub.DisposeAsync();
        await dlqSub.DisposeAsync();
    }

    private static string Describe(ResilienceEvent e) => e switch
    {
        ResilienceEvent.Retry r => $"retry #{r.Attempt} ({r.Error})",
        ResilienceEvent.BreakerOpen => "circuit breaker OPENED — fast-failing further calls",
        ResilienceEvent.BreakerHalfOpen => "circuit breaker half-open — probing recovery",
        ResilienceEvent.BreakerClose => "circuit breaker CLOSED — healthy again",
        _ => e.ToString() ?? "event",
    };

    private static async Task<T?> Race<T>(Task<T> task, TimeSpan timeout)
    {
        var done = await Task.WhenAny(task, Task.Delay(timeout));
        return done == task ? await task : default;
    }

    // --- presentation -------------------------------------------------------

    private static void Title(string s) =>
        Console.WriteLine($"\n{Ansi.Bold}{Ansi.Cyan}{s}{Ansi.Reset}\n{Ansi.Hr}");

    private static void Act(string s) =>
        Console.WriteLine($"\n{Ansi.Bold}{Ansi.Magenta}▸ {s}{Ansi.Reset}");

    private static void Step(string s) =>
        Console.WriteLine($"  {Ansi.Dim}·{Ansi.Reset} {s}");

    private static void Outcome(string s) =>
        Console.WriteLine($"  {Ansi.Green}✓{Ansi.Reset} {s}");
}

/// <summary>
/// Adapts a (scheduler, bus) pair into one <see cref="IMessageScheduler"/> whose
/// bus calls route through the fault-injecting decorator, so the resilience
/// decorator can wrap them. Scheduler-port calls forward to the inner adapter.
/// </summary>
internal sealed class BusOnlyScheduler(IMessageScheduler scheduler, IMessageBus bus)
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
    public Task PublishAsync(string t, string p, string? rk = null, CancellationToken ct = default) =>
        bus.PublishAsync(t, p, rk, ct);
    public Task<ISubscription> SubscribeAsync(string t, AckHandler h, SubscribeOptions? o = null, CancellationToken ct = default) =>
        bus.SubscribeAsync(t, h, o, ct);

    public ValueTask DisposeAsync() => bus.DisposeAsync();
}
