using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S14 — Message priority. A high-priority message must overtake a backlog of
/// low-priority ones already waiting on the queue. A single slow consumer lets a
/// backlog build; we publish several <c>Priority:1</c> messages, then one
/// <c>Priority:9</c>, and assert the high-priority body is delivered near the
/// FRONT (within the first two received), not at the back.
///
/// Artemis honours priority natively (credit-window 1 on the consumer makes it
/// observable). RabbitMQ needs the queue declared with <c>x-max-priority</c> —
/// the <c>PriorityQueue</c> subscribe option. The in-memory reference selects the
/// highest-priority pending message (FIFO on a tie).
/// </summary>
public sealed class Priority : IBusScenario
{
    private const int LowCount = 8; // backlog of low-priority messages
    private const int HandlerDelayMs = 60; // slow ack so a backlog actually forms

    public string Name => "S14 priority";
    public string Description =>
        $"A Priority:9 message overtakes a backlog of {LowCount} Priority:1 messages.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        var topic = $"mbc.s14.{Nonce()}";
        var received = new List<string>();
        var gate = new object();
        var settled = 0;

        // A deliberately slow handler: each ack takes HandlerDelayMs, so the
        // publishes below pile up as a real backlog the broker must order. The ack
        // is guarded because the scenario may dispose (closing the channel) while a
        // late handler is still draining — settling a closed channel is harmless.
        AckHandler handler = async m =>
        {
            lock (gate) received.Add(m.Body);
            await Task.Delay(HandlerDelayMs);
            try { await m.AckAsync(); }
            catch { /* channel may already be closing on the way out */ }
            Interlocked.Increment(ref settled);
        };
        var sub = await bus.SubscribeAsync(topic, handler, new SubscribeOptions
        {
            SubscriberId = $"prio-{Nonce()}",
            PriorityQueue = true, // RabbitMQ: declare x-max-priority; native elsewhere
        });
        try
        {
            // Fill the backlog first, then drop in the high-priority message.
            for (var i = 0; i < LowCount; i++)
                await bus.PublishAsync(topic, $"low-{i}", options: new PublishOptions { Priority = 1 });
            await bus.PublishAsync(topic, "HIGH", options: new PublishOptions { Priority = 9 });

            int Count() { lock (gate) return received.Count; }
            var ok = await WaitUntilAsync(() => Count() >= LowCount + 1, TimeSpan.FromSeconds(8));
            if (!ok)
                return Fail(Name, $"received {Count()}/{LowCount + 1}", t0);

            // Let the in-flight handlers settle before we close the channel below.
            await WaitUntilAsync(() => Volatile.Read(ref settled) >= LowCount + 1, TimeSpan.FromSeconds(2));

            int highIndex;
            string order;
            lock (gate)
            {
                highIndex = received.IndexOf("HIGH");
                order = string.Join(",", received);
            }
            // Robust assertion: the high-priority message arrives near the front —
            // not an exact slot (the first low message is usually already in-flight
            // before HIGH is published, so index 0 or 1 are both correct).
            if (highIndex is < 0 or > 1)
                return Fail(Name, $"high-priority message arrived at index {highIndex} (order: {order})", t0);

            return Pass(Name, $"Priority:9 overtook the backlog (arrived at index {highIndex})", t0);
        }
        finally
        {
            await sub.DisposeAsync();
        }
    }
}
