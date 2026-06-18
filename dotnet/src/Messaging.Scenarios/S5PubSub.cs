using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S5 — Pub/Sub with topic routing. Two independent subscribers bind to the same
/// topic with different routing-key filters; a publish reaches exactly the
/// subscribers whose filter matches. Proves selective fan-out, not just delivery.
/// </summary>
public sealed class PubSub : IBusScenario
{
    public string Name => "S5 pub/sub (topic)";
    public string Description => "Two filtered subscribers each receive only their matching events.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsTopic)
            return Unsupported(Name, "no topic routing on this broker", t0);

        var topic = $"mbc.s5.{Nonce()}";
        var created = await AckCollector.StartAsync(bus, topic, new SubscribeOptions
        {
            Kind = TopologyKind.Topic,
            RoutingKey = "order.created",
            SubscriberId = $"created-{Nonce()}",
        });
        var all = await AckCollector.StartAsync(bus, topic, new SubscribeOptions
        {
            Kind = TopologyKind.Topic,
            RoutingKey = "order.#",
            SubscriberId = $"all-{Nonce()}",
        });
        try
        {
            await bus.PublishAsync(topic, "created-1", "order.created");
            await bus.PublishAsync(topic, "shipped-1", "order.shipped");

            var ok = await WaitUntilAsync(
                () => created.Count() >= 1 && all.Count() >= 2, TimeSpan.FromSeconds(5));
            if (!ok)
                return Fail(Name, $"timed out (created={created.Count()}, all={all.Count()})", t0);
            if (created.Count() != 1 || created.Bodies()[0] != "created-1")
                return Fail(Name, "filtered subscriber saw the wrong events", t0);

            return Pass(Name, $"order.created→1 sub, order.#→both ({all.Count()})", t0);
        }
        finally
        {
            await created.DisposeAsync();
            await all.DisposeAsync();
        }
    }
}
