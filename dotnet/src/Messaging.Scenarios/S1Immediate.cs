using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S1 — baseline: a message sent now is received now. Proves connectivity,
/// publish, and consume before we layer scheduling on top.
/// </summary>
public sealed class ImmediateDelivery : IScenario
{
    public string Name => "S1 immediate send/receive";
    public string Description => "A message published for immediate delivery is consumed.";

    public async Task<ScenarioResult> RunAsync(IMessageScheduler s)
    {
        var t0 = StartClock();
        var dest = $"mbc.s1.{Nonce()}";
        var token = $"now-{Nonce()}";
        await using var collector = await MessageCollector.StartAsync(s, dest);

        await s.SendNowAsync(dest, token);
        var got = await WaitUntilAsync(
            () => collector.Bodies().Contains(token), TimeSpan.FromSeconds(5));

        return got
            ? Pass(Name, "delivered immediately", t0)
            : Fail(Name, "message never arrived within 5s", t0);
    }
}
