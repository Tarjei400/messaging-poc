using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S13 — Broker-native producer deduplication. The producer stamps the SAME
/// <c>DedupId</c> on two publishes; the broker drops the repeat within its dedup
/// window, so a single subscriber sees exactly one delivery. This is the
/// broker-side counterpart of the app-level idempotent consumer (S10): the same
/// "exactly-once effect" goal, but enforced by the infrastructure.
///
/// Artemis honours <c>_AMQ_DUPL_ID</c> (duplicate detection enabled on the AMQP
/// acceptor in broker.xml) → ✓. RabbitMQ has no native producer dedup, so it
/// declares <c>SupportsDedup=false</c> and this scenario reports ⊘ — the honest
/// gap that motivates S10.
/// </summary>
public sealed class BrokerNativeDedup : IBusScenario
{
    public string Name => "S13 broker-native dedup";
    public string Description => "Publishing the same dedupId twice is delivered exactly once.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsDedup)
            return Unsupported(Name, "no broker-native producer dedup", t0);

        var topic = $"mbc.s13.{Nonce()}";
        var dedupId = $"dup-{Nonce()}";

        var sub = await AckCollector.StartAsync(bus, topic, new SubscribeOptions
        {
            SubscriberId = $"dedup-{Nonce()}",
        });
        try
        {
            // Same dedupId, two publishes: the broker must collapse them to one.
            var opts = new PublishOptions { DedupId = dedupId };
            await bus.PublishAsync(topic, "order-42", options: opts);
            await bus.PublishAsync(topic, "order-42", options: opts);

            // Wait for the first to arrive, then give the (suppressed) second ample
            // time to show up if dedup were not working.
            var arrived = await WaitUntilAsync(() => sub.Count() >= 1, TimeSpan.FromSeconds(6));
            if (!arrived)
                return Fail(Name, "no delivery at all", t0);
            await Task.Delay(800); // window for a (wrongly) un-deduped second copy

            if (sub.Count() != 1)
                return Fail(Name, $"expected 1 delivery, got {sub.Count()}", t0);

            return Pass(Name, "duplicate dropped by the broker (1 delivery)", t0);
        }
        finally
        {
            await sub.DisposeAsync();
        }
    }
}
