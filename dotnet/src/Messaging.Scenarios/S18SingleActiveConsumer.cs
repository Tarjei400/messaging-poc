using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S18 — Single active consumer / failover. Two consumers attach to the SAME
/// queue with <c>SingleActiveConsumer</c>. Only ONE may receive while it is up;
/// the other is a hot standby. We publish a first batch (only the active consumer
/// may get it), drop the active consumer, then publish a second batch — which
/// must be picked up by the standby that has now been promoted. Order is
/// preserved and nothing is lost. Leader/standby failover without an election.
///
/// RabbitMQ: <c>x-single-active-consumer</c> queue arg. Artemis: an exclusive
/// queue (<c>default-exclusive-queue</c> in broker.xml for <c>mbc.s18.#</c>).
/// In-memory: deliver to one active consumer and promote a standby on unsubscribe.
/// </summary>
public sealed class SingleActiveConsumer : IBusScenario
{
    private const int Batch = 5; // messages published before, then after, the failover
    private const int Total = Batch * 2;

    public string Name => "S18 single active consumer";
    public string Description =>
        $"{Total} messages: an active consumer takes the first {Batch}, then on failover a standby takes over the rest (order preserved).";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsManualAck)
            return Unsupported(Name, "no manual ack on this broker", t0);

        var topic = $"mbc.s18.{Nonce()}";
        var queueId = $"sac-{Nonce()}"; // shared queue → one active consumer
        var gate = new object();
        var byConsumer = new[] { new List<int>(), new List<int>() };

        AckHandler Make(int i) => async m =>
        {
            lock (gate) byConsumer[i].Add(int.Parse(m.Body.Split('-')[1]));
            try { await m.AckAsync(); }
            catch { /* channel may be closing on the way out */ }
        };

        var subA = await bus.SubscribeAsync(topic, Make(0),
            new SubscribeOptions { SubscriberId = queueId, SingleActiveConsumer = true });
        var subB = await bus.SubscribeAsync(topic, Make(1),
            new SubscribeOptions { SubscriberId = queueId, SingleActiveConsumer = true });
        try
        {
            // First batch — only the single active consumer may receive these.
            for (var i = 0; i < Batch; i++) await bus.PublishAsync(topic, $"m-{i}");

            int Count() { lock (gate) return byConsumer[0].Count + byConsumer[1].Count; }
            var firstBatchDone = await WaitUntilAsync(() => Count() >= Batch, TimeSpan.FromSeconds(8));
            if (!firstBatchDone)
                return Fail(Name, $"only {Count()}/{Batch} of the first batch delivered", t0);
            await Task.Delay(200); // let any stray second delivery surface

            int activeIdx, standbyIdx, standbyLen;
            lock (gate)
            {
                activeIdx = byConsumer[0].Count >= byConsumer[1].Count ? 0 : 1;
                standbyIdx = activeIdx == 0 ? 1 : 0;
                standbyLen = byConsumer[standbyIdx].Count;
            }
            if (standbyLen != 0)
                return Fail(Name,
                    $"both consumers were active before failover ({byConsumer[0].Count}/{byConsumer[1].Count})", t0);

            // Fail the active consumer over; the standby must be promoted.
            await (activeIdx == 0 ? subA : subB).DisposeAsync();
            await Task.Delay(300); // give the broker time to promote the standby

            // Second batch — these can only be served by the promoted standby.
            for (var i = Batch; i < Total; i++) await bus.PublishAsync(topic, $"m-{i}");

            int Distinct() { lock (gate) return byConsumer[0].Concat(byConsumer[1]).Distinct().Count(); }
            var ok = await WaitUntilAsync(() => Distinct() >= Total, TimeSpan.FromSeconds(8));
            if (!ok)
                return Fail(Name, $"only {Distinct()}/{Total} distinct messages delivered after failover", t0);
            await Task.Delay(200);

            List<int> active, standby;
            lock (gate)
            {
                active = byConsumer[activeIdx].ToList();
                standby = byConsumer[standbyIdx].ToList();
            }
            if (standby.Count == 0)
                return Fail(Name, "standby never took over after failover", t0);

            // Nothing lost: every sequence number present.
            var union = new HashSet<int>(active.Concat(standby));
            for (var i = 0; i < Total; i++)
                if (!union.Contains(i))
                    return Fail(Name, $"message m-{i} was lost across failover", t0);

            // Per-consumer order: each saw its messages in ascending order.
            foreach (var seq in new[] { active, standby })
                for (var k = 1; k < seq.Count; k++)
                    if (seq[k] <= seq[k - 1])
                        return Fail(Name, $"a consumer went out of order: {string.Join(",", seq)}", t0);

            // The standby must have served the post-failover batch.
            if (standby.Max() < Batch)
                return Fail(Name,
                    $"standby did not take over the post-failover batch (got {string.Join(",", standby)})", t0);

            return Pass(Name,
                $"active served {active.Count}, standby took over {standby.Count} after failover, order preserved", t0);
        }
        finally
        {
            try { await subA.DisposeAsync(); } catch { /* already closed */ }
            try { await subB.DisposeAsync(); } catch { /* already closed */ }
        }
    }
}
