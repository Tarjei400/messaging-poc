using Messaging.Abstractions;

namespace Messaging.Scenarios;

/// <summary>
/// The canonical scenario suite. Adding a scenario here automatically runs it
/// against every adapter — no other file changes (Open/Closed for the scenario
/// dimension too).
/// </summary>
public static class ScenarioRegistry
{
    /// <summary>The scheduling suite (S1–S4) — exercises the <see cref="IMessageScheduler"/> port.</summary>
    public static IReadOnlyList<IScenario> All { get; } = new IScenario[]
    {
        new ImmediateDelivery(),
        new ScheduledDelivery(),
        new CancelScheduled(),
        new ListPending(),
    };

    /// <summary>
    /// The pub/sub suite (S5–S9) — exercises the <see cref="IMessageBus"/> port.
    /// Disconnect/reconnect and broker-restart durability are demonstrated in the
    /// dedicated fault-tolerance mode (they need controlled outages, not pass/fail).
    /// </summary>
    public static IReadOnlyList<IBusScenario> AllBus { get; } = new IBusScenario[]
    {
        new PubSub(),
        new Fanout(),
        new ExplicitAck(),
        new PoisonDlq(),
        new CompetingConsumers(),
        new IdempotentConsumer(),
        new RetryQueue(),
        new MessageGroups(),
        new BrokerNativeDedup(),
        new Priority(),
        new RequestReply(),
        new TtlExpiry(),
        new DurableSubscription(),
        new SingleActiveConsumer(),
        new StreamReplay(),
    };
}

public sealed record RunReport(
    string Broker,
    Capabilities Capabilities,
    IReadOnlyList<ScenarioResult> Results);

/// <summary>
/// Runs the full suite (scheduling S1–S4 + pub/sub S5–S9) against a single,
/// already-constructed scheduler. The runner depends on
/// <see cref="IMessageScheduler"/> (+ optionally <see cref="IMessageBus"/>) and
/// nothing else — it has no idea which broker it is exercising. That is the
/// payoff of the abstraction: one runner, every broker. If an adapter does not
/// implement the bus port, every bus scenario is reported as ⊘ n/a.
/// </summary>
public static class SuiteRunner
{
    public static async Task<RunReport> RunAsync(IMessageScheduler scheduler)
    {
        var results = new List<ScenarioResult>();
        foreach (var scenario in ScenarioRegistry.All)
            results.Add(await RunOneAsync(scenario, scheduler));

        // The bus suite runs only when the adapter declares a bus surface. Adapters
        // that don't (a scheduler-only fake) report every bus scenario as ⊘ n/a.
        if (scheduler.Capabilities.Bus is not null && scheduler is IMessageBus bus)
        {
            Exception? connectError = null;
            try
            {
                await bus.ConnectBusAsync();
            }
            catch (Exception ex)
            {
                connectError = ex;
            }
            foreach (var scenario in ScenarioRegistry.AllBus)
            {
                results.Add(connectError is not null
                    ? FailResult(scenario.Name, connectError)
                    : await RunOneBusAsync(scenario, bus));
            }
        }
        else
        {
            foreach (var scenario in ScenarioRegistry.AllBus)
                results.Add(new ScenarioResult(scenario.Name, ScenarioStatus.Unsupported, "adapter has no bus port", 0));
        }

        return new RunReport(scheduler.Name, scheduler.Capabilities, results);
    }

    private static async Task<ScenarioResult> RunOneAsync(IScenario scenario, IMessageScheduler scheduler)
    {
        try { return await scenario.RunAsync(scheduler); }
        catch (Exception ex) { return FailResult(scenario.Name, ex); }
    }

    private static async Task<ScenarioResult> RunOneBusAsync(IBusScenario scenario, IMessageBus bus)
    {
        try { return await scenario.RunAsync(bus); }
        catch (Exception ex) { return FailResult(scenario.Name, ex); }
    }

    private static ScenarioResult FailResult(string name, Exception ex) =>
        new(name, ScenarioStatus.Fail, $"threw: {ex.Message}", 0);
}

/// <summary>Pretty-prints a <see cref="RunReport"/> with ANSI colour and glyphs.</summary>
public static class ReportPrinter
{
    public static void Print(RunReport report)
    {
        var cap = report.Capabilities;
        var capLine =
            $"protocol={cap.Protocol}  " +
            $"native-scheduling={Ansi.YesNo(cap.NativeScheduling)}  " +
            $"cancel={Ansi.YesNo(cap.SupportsCancel)}  " +
            $"list={Ansi.YesNo(cap.SupportsList)}";

        Console.WriteLine();
        Console.WriteLine($"{Ansi.Bold}{Ansi.Cyan}{report.Broker}{Ansi.Reset}");
        Console.WriteLine($"{Ansi.Dim}{capLine}{Ansi.Reset}");
        if (cap.Bus is { } b)
        {
            var busLine =
                $"pub/sub={Ansi.YesNo(b.SupportsTopic)}  " +
                $"fanout={Ansi.YesNo(b.SupportsFanout)}  " +
                $"manual-ack={Ansi.YesNo(b.SupportsManualAck)}  " +
                $"dead-letter={Ansi.YesNo(b.SupportsDeadLetter)}  " +
                $"delivery-count={Ansi.YesNo(b.ReportsDeliveryCount)}";
            Console.WriteLine($"{Ansi.Dim}{busLine}{Ansi.Reset}");
        }
        Console.WriteLine(Ansi.Hr);

        foreach (var r in report.Results)
        {
            var name = r.Name.PadRight(28);
            var time = $"{r.DurationMs,5}ms";
            Console.WriteLine($"  {Ansi.Glyph(r.Status)}  {name} {Ansi.Dim}{time}{Ansi.Reset}  {r.Detail}");
        }

        var passed = Count(report, ScenarioStatus.Pass);
        var na = Count(report, ScenarioStatus.Unsupported);
        var failed = Count(report, ScenarioStatus.Fail);

        Console.WriteLine(Ansi.Hr);
        Console.WriteLine(
            $"  {Ansi.Green}{passed} passed{Ansi.Reset}, " +
            $"{Ansi.Yellow}{na} unsupported{Ansi.Reset}, " +
            $"{(failed > 0 ? Ansi.Red : Ansi.Dim)}{failed} failed{Ansi.Reset}");
    }

    private static int Count(RunReport r, ScenarioStatus s) =>
        r.Results.Count(x => x.Status == s);
}
