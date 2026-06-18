using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S3 — the key discriminator from the research: cancel a scheduled message
/// before it fires and prove it never arrives. Adapters that cannot do this
/// surface an <see cref="OperationNotSupportedException"/>, recorded as an honest
/// ⊘ rather than ✗.
/// </summary>
public sealed class CancelScheduled : IScenario
{
    private static readonly TimeSpan Delay = TimeSpan.FromMilliseconds(3000);

    public string Name => "S3 cancel scheduled";
    public string Description => "A scheduled message is cancelled and never delivered.";

    public async Task<ScenarioResult> RunAsync(IMessageScheduler s)
    {
        var t0 = StartClock();
        var dest = $"mbc.s3.{Nonce()}";
        var token = $"cancel-{Nonce()}";
        await using var collector = await MessageCollector.StartAsync(s, dest);

        var handle = await s.ScheduleAsync(dest, token, DateTimeOffset.UtcNow + Delay);

        try
        {
            await s.CancelAsync(handle);
        }
        catch (OperationNotSupportedException)
        {
            return Unsupported(Name, "no cancel API on this broker", t0);
        }

        // Wait past the original delivery time; the message must never appear.
        await Task.Delay(Delay + TimeSpan.FromSeconds(2));

        return collector.Bodies().Contains(token)
            ? Fail(Name, "cancel returned but message still delivered", t0)
            : Pass(Name, "scheduled message successfully cancelled", t0);
    }
}
