using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S4 — observability: schedule two messages far in the future and confirm the
/// broker can report them as pending (Artemis QueueControl.listScheduledMessages).
/// Brokers without an inspection API report ⊘.
/// </summary>
public sealed class ListPending : IScenario
{
    public string Name => "S4 list pending";
    public string Description => "Two far-future messages are scheduled and counted as pending.";

    public async Task<ScenarioResult> RunAsync(IMessageScheduler s)
    {
        var t0 = StartClock();
        var dest = $"mbc.s4.{Nonce()}";
        DateTimeOffset FarFuture() => DateTimeOffset.UtcNow + TimeSpan.FromSeconds(60);

        try
        {
            await s.ScheduleAsync(dest, $"a-{Nonce()}", FarFuture());
            await s.ScheduleAsync(dest, $"b-{Nonce()}", FarFuture());

            IReadOnlyList<ScheduledInfo> pending;
            try
            {
                pending = await s.ListScheduledAsync(dest);
            }
            catch (OperationNotSupportedException)
            {
                return Unsupported(Name, "no inspection API on this broker", t0);
            }

            return pending.Count >= 2
                ? Pass(Name, $"reported {pending.Count} pending", t0)
                : Fail(Name, $"expected >=2 pending, saw {pending.Count}", t0);
        }
        finally
        {
            // best-effort cleanup so far-future messages don't accumulate
            try
            {
                var left = await s.ListScheduledAsync(dest);
                foreach (var p in left)
                    await s.CancelAsync(new ScheduleHandle(p.Id, dest, DateTimeOffset.UtcNow));
            }
            catch
            {
                /* cleanup is best-effort; unsupported brokers simply skip it */
            }
        }
    }
}
