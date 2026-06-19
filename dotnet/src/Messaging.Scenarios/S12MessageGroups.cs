using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S12 — Ordered delivery / message groups. Messages of several groups are
/// published <i>interleaved</i> to ONE topic consumed by two competing consumers
/// (same <c>SubscriberId</c>) with <c>PartitionByGroup</c>. The broker must pin
/// each group to a single consumer so that, despite work-sharing, every group's
/// messages arrive in their published order and a group is never split.
///
/// Artemis: native message groups (AMQP <c>group-id</c>) pin a group to one
/// consumer on the shared multicast queue. RabbitMQ: a consistent-hash exchange
/// routes a groupId to a fixed per-consumer queue (bundled plugin) — gated by
/// <see cref="BusCapabilities.SupportsMessageGroups"/>. In-memory: group→consumer
/// affinity on first sight.
/// </summary>
public sealed class MessageGroups : IBusScenario
{
    private static readonly string[] Groups = { "a", "b", "c" };
    private const int PerGroup = 6; // ordered sequence 0..PerGroup-1 per group
    private static readonly int Total = Groups.Length * PerGroup;

    public string Name => "S12 message groups";
    public string Description =>
        $"{Total} messages across {Groups.Length} groups keep per-group order, each pinned to one of 2 consumers.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsManualAck)
            return Unsupported(Name, "no manual ack on this broker", t0);
        if (!bus.BusCapabilities.SupportsMessageGroups)
            return Unsupported(Name, "no broker-native message grouping", t0);

        var topic = $"mbc.s12.{Nonce()}";
        var queueId = $"groups-{Nonce()}"; // shared queue → competing consumers
        var gate = new object();
        // Per consumer, the (group, seq) of each message it handled, in arrival order.
        var byConsumer = new[] { new List<(string Group, int Seq)>(), new List<(string Group, int Seq)>() };

        AckHandler Make(int i) => async m =>
        {
            var parts = m.Body.Split(':');
            lock (gate) byConsumer[i].Add((parts[0], int.Parse(parts[1])));
            await m.AckAsync();
        };

        var subA = await bus.SubscribeAsync(topic, Make(0),
            new SubscribeOptions { SubscriberId = queueId, PartitionByGroup = true });
        var subB = await bus.SubscribeAsync(topic, Make(1),
            new SubscribeOptions { SubscriberId = queueId, PartitionByGroup = true });
        try
        {
            // Publish interleaved: a0,b0,c0,a1,b1,c1,… so ordering is only preserved
            // if the broker actually pins each group to one consumer.
            for (var seq = 0; seq < PerGroup; seq++)
                foreach (var g in Groups)
                    await bus.PublishAsync(topic, $"{g}:{seq}", options: new PublishOptions { GroupId = g });

            int Total2() { lock (gate) return byConsumer[0].Count + byConsumer[1].Count; }
            var ok = await WaitUntilAsync(() => Total2() >= Total, TimeSpan.FromSeconds(10));
            if (!ok)
                return Fail(Name, $"received {Total2()}/{Total}", t0);

            List<(string Group, int Seq)>[] snapshot;
            lock (gate) snapshot = new[] { byConsumer[0].ToList(), byConsumer[1].ToList() };

            // Which consumer(s) handled each group; per-group order per consumer.
            var ownersOf = new Dictionary<string, HashSet<int>>();
            for (var i = 0; i < snapshot.Length; i++)
            {
                var perGroupSeqs = new Dictionary<string, List<int>>();
                foreach (var (group, seq) in snapshot[i])
                {
                    if (!ownersOf.TryGetValue(group, out var owners)) ownersOf[group] = owners = new HashSet<int>();
                    owners.Add(i);
                    if (!perGroupSeqs.TryGetValue(group, out var seqs)) perGroupSeqs[group] = seqs = new List<int>();
                    seqs.Add(seq);
                }
                foreach (var (group, seqs) in perGroupSeqs)
                    for (var k = 1; k < seqs.Count; k++)
                        if (seqs[k] <= seqs[k - 1])
                            return Fail(Name, $"group {group} out of order on consumer {i}: {string.Join(",", seqs)}", t0);
            }

            // Every group handled by exactly one consumer, and it saw the full run.
            foreach (var g in Groups)
            {
                if (!ownersOf.TryGetValue(g, out var owners) || owners.Count == 0)
                    return Fail(Name, $"group {g} was never delivered", t0);
                if (owners.Count > 1)
                    return Fail(Name, $"group {g} split across consumers", t0);
                var owner = owners.First();
                var seen = snapshot[owner].Where(m => m.Group == g).Select(m => m.Seq).ToList();
                if (seen.Count != PerGroup)
                    return Fail(Name, $"group {g} got {seen.Count}/{PerGroup} messages", t0);
            }

            var split = string.Join("/", snapshot.Select(c => c.Count));
            return Pass(Name, $"per-group order preserved, each group pinned to one consumer (split {split})", t0);
        }
        finally
        {
            await subA.DisposeAsync();
            await subB.DisposeAsync();
        }
    }
}
