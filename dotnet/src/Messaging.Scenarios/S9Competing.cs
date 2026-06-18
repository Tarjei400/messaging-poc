using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S9 — Competing consumers (work sharing, at-least-once). Two consumers attached
/// to the SAME queue share the load: each message is handled by exactly one of
/// them. This is the counterpoint to S6 fanout — same publish API, opposite
/// delivery semantics — and is the foundation of horizontal worker scaling.
/// </summary>
public sealed class CompetingConsumers : IBusScenario
{
    private const int MessageCount = 10;

    public string Name => "S9 competing consumers";
    public string Description => $"{MessageCount} messages are shared across 2 consumers, no duplicates.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsManualAck)
            return Unsupported(Name, "no manual ack on this broker", t0);

        var topic = $"mbc.s9.{Nonce()}";
        var queueId = $"workers-{Nonce()}"; // shared queue → competing consumers
        var byConsumer = new[] { new List<string>(), new List<string>() };
        var gate = new object();

        AckHandler Make(int i) => async m =>
        {
            lock (gate) byConsumer[i].Add(m.Body);
            await m.AckAsync();
        };

        var subA = await bus.SubscribeAsync(topic, Make(0), new SubscribeOptions { SubscriberId = queueId });
        var subB = await bus.SubscribeAsync(topic, Make(1), new SubscribeOptions { SubscriberId = queueId });
        try
        {
            for (var i = 0; i < MessageCount; i++)
                await bus.PublishAsync(topic, $"job-{i}");

            int Total() { lock (gate) return byConsumer[0].Count + byConsumer[1].Count; }
            var ok = await WaitUntilAsync(() => Total() >= MessageCount, TimeSpan.FromSeconds(6));

            List<string> all;
            int a, b;
            lock (gate)
            {
                all = byConsumer[0].Concat(byConsumer[1]).ToList();
                a = byConsumer[0].Count;
                b = byConsumer[1].Count;
            }
            var unique = new HashSet<string>(all);
            if (!ok || all.Count != MessageCount)
                return Fail(Name, $"received {all.Count}/{MessageCount}", t0);
            if (unique.Count != MessageCount)
                return Fail(Name, $"duplicate delivery ({unique.Count} unique)", t0);
            if (a == 0 || b == 0)
                return Fail(Name, "one consumer was starved (not balanced)", t0);

            return Pass(Name, $"split {a}/{b}, no dupes", t0);
        }
        finally
        {
            await subA.DisposeAsync();
            await subB.DisposeAsync();
        }
    }
}
