using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S2 — the core requirement (R1): a scheduled message must NOT arrive early,
/// and MUST arrive at (or just after) its delivery time.
/// </summary>
public sealed class ScheduledDelivery : IScenario
{
    private static readonly TimeSpan Delay = TimeSpan.FromMilliseconds(3000);

    public string Name => "S2 scheduled delivery";
    public string Description => $"A message scheduled +{Delay.TotalMilliseconds}ms is withheld, then delivered.";

    public async Task<ScenarioResult> RunAsync(IMessageScheduler s)
    {
        var t0 = StartClock();
        var dest = $"mbc.s2.{Nonce()}";
        var token = $"sched-{Nonce()}";
        await using var collector = await MessageCollector.StartAsync(s, dest);

        var deliverAt = DateTimeOffset.UtcNow + Delay;
        await s.ScheduleAsync(dest, token, deliverAt);

        // Must still be withheld well before the deadline.
        await Task.Delay(Delay * 0.4);
        if (collector.Bodies().Contains(token))
            return Fail(Name, "message delivered early (not withheld)", t0);

        // Must arrive within a tolerance window after the deadline.
        var arrived = await WaitUntilAsync(
            () => collector.Bodies().Contains(token),
            Delay + TimeSpan.FromSeconds(4));

        return arrived
            ? Pass(Name, $"withheld then delivered after ~{Delay.TotalMilliseconds}ms", t0)
            : Fail(Name, "scheduled message never arrived", t0);
    }
}
