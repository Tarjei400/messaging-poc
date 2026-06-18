using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S6 — Fanout multicast. One publish is delivered, in full, to N independent
/// subscriber queues. This is the cleanest cross-broker capability: Artemis
/// multicast addresses and RabbitMQ fanout exchanges both express it natively.
/// </summary>
public sealed class Fanout : IBusScenario
{
    private const int Subscribers = 3;

    public string Name => "S6 fanout multicast";
    public string Description => $"One publish reaches all {Subscribers} independent subscribers.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsFanout)
            return Unsupported(Name, "no fanout on this broker", t0);

        var topic = $"mbc.s6.{Nonce()}";
        var subs = new List<AckCollector>();
        for (var i = 0; i < Subscribers; i++)
        {
            subs.Add(await AckCollector.StartAsync(bus, topic, new SubscribeOptions
            {
                Kind = TopologyKind.Fanout,
                SubscriberId = $"s{i}-{Nonce()}",
            }));
        }
        try
        {
            await bus.PublishAsync(topic, "broadcast-1");
            var ok = await WaitUntilAsync(
                () => subs.All(s => s.Count() >= 1), TimeSpan.FromSeconds(5));
            var counts = subs.Select(s => s.Count()).ToList();
            if (!ok || counts.Any(n => n != 1))
                return Fail(Name, $"subscriber counts were [{string.Join(",", counts)}]", t0);

            return Pass(Name, $"1 publish → {Subscribers} subscribers each received it", t0);
        }
        finally
        {
            foreach (var s in subs) await s.DisposeAsync();
        }
    }
}
