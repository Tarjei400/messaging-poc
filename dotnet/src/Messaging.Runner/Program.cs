using Messaging.Abstractions;
using Messaging.Faults;
using Messaging.Runner;
using Messaging.Scenarios;

// Usage:
//   dotnet run --project src/Messaging.Runner -- <broker>        e.g. artemis | rabbitmq | in-memory
//   dotnet run --project src/Messaging.Runner -- all             run every broker that is reachable
//   dotnet run --project src/Messaging.Runner -- fault <broker>  narrate the fault-tolerance timeline
//
// Exit code is non-zero only if a scenario *failed* (unsupported is an expected,
// declared gap and does not count as a failure).

// Fault mode: drive a controlled outage and narrate the resilience timeline.
if (args.Length > 0 && args[0].Equals("fault", StringComparison.OrdinalIgnoreCase))
{
    var faultBroker = (args.Length > 1 ? args[1] : "in-memory").ToLowerInvariant();
    if (!BrokerFactory.Known.Contains(faultBroker))
    {
        Console.Error.WriteLine($"Unknown broker '{faultBroker}'. Known: {string.Join(", ", BrokerFactory.Known)}");
        return 2;
    }
    var narrator = new FaultNarrator(() => BrokerFactory.Create(faultBroker, resilient: false));
    await narrator.RunAsync();
    return 0;
}

var arg = (args.Length > 0 ? args[0] : "in-memory").ToLowerInvariant();

var brokers = arg == "all"
    ? BrokerFactory.Known.Where(b => b != "in-memory").ToList()
    : new List<string> { arg };

if (arg != "all" && !BrokerFactory.Known.Contains(arg))
{
    Console.Error.WriteLine($"Unknown broker '{arg}'. Known: {string.Join(", ", BrokerFactory.Known)}, all");
    return 2;
}

var reports = new List<RunReport>();
foreach (var id in brokers)
{
    var report = await RunAgainstAsync(id);
    if (report is not null)
    {
        reports.Add(report);
        ReportPrinter.Print(report);
    }
}

var anyFailed = reports.Any(r => r.Results.Any(x => x.Status == ScenarioStatus.Fail));
return anyFailed ? 1 : 0;

static async Task<RunReport?> RunAgainstAsync(string id)
{
    var scheduler = BrokerFactory.Create(id);
    try
    {
        await scheduler.ConnectAsync();
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(
            $"\n[{id}] could not connect: {ex.Message}\n" +
            $"  Is the broker running? Try: docker compose up -d {id}");
        await scheduler.DisposeAsync();
        return null;
    }

    try
    {
        return await SuiteRunner.RunAsync(scheduler);
    }
    finally
    {
        await scheduler.DisposeAsync();
    }
}
