using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S17 — Durable subscription. A named subscriber attaches, then disconnects.
/// Messages published while it is gone must be retained on its (durable, non
/// auto-delete) queue and delivered when a consumer with the SAME
/// <c>SubscriberId</c> reattaches — nothing is lost across the gap.
///
/// This is the pub/sub-with-memory story: unlike a transient consumer (whose
/// queue vanishes on disconnect), a durable subscription keeps accumulating.
/// Needs no API change — all adapters declare durable, auto-delete=false queues;
/// the in-memory reference keeps the queue + its pending messages across
/// unsubscribe and replays them to the reattached consumer.
/// </summary>
public sealed class DurableSubscription : IBusScenario
{
    private const int GapCount = 5; // messages published while the subscriber is away

    public string Name => "S17 durable subscription";
    public string Description =>
        $"{GapCount} messages published while a durable subscriber is offline are delivered on reattach.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        var topic = $"mbc.s17.{Nonce()}";
        var subscriberId = $"durable-{Nonce()}";

        // 1. Attach the durable subscriber so its queue is declared and bound, then
        //    drop the consumer (the durable queue survives with no one attached).
        var warmup = await bus.SubscribeAsync(topic, m => m.AckAsync(),
            new SubscribeOptions { SubscriberId = subscriberId });
        await warmup.DisposeAsync();
        await Task.Delay(200); // let the unsubscribe settle on the broker

        // 2. Publish while the subscription has no live consumer.
        for (var i = 0; i < GapCount; i++)
            await bus.PublishAsync(topic, $"gap-{i}");
        await Task.Delay(200);

        // 3. Reattach with the SAME id — the retained messages must arrive.
        var received = new List<string>();
        var gate = new object();
        AckHandler handler = async m =>
        {
            lock (gate) received.Add(m.Body);
            await m.AckAsync();
        };
        var sub = await bus.SubscribeAsync(topic, handler,
            new SubscribeOptions { SubscriberId = subscriberId });
        try
        {
            int Count() { lock (gate) return received.Count; }
            var ok = await WaitUntilAsync(() => Count() >= GapCount, TimeSpan.FromSeconds(8));
            if (!ok)
                return Fail(Name, $"received {Count()}/{GapCount} after reattach", t0);

            HashSet<string> got;
            lock (gate) got = new HashSet<string>(received);
            for (var i = 0; i < GapCount; i++)
                if (!got.Contains($"gap-{i}"))
                    return Fail(Name, $"lost message gap-{i} across the gap", t0);

            return Pass(Name, $"all {GapCount} offline messages retained and delivered on reattach", t0);
        }
        finally
        {
            await sub.DisposeAsync();
        }
    }
}
