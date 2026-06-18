using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S7 — Explicit acknowledgement. Three observable behaviours prove the consumer
/// (not the broker) controls settlement:
///   (a) ack removes the message — it is not redelivered;
///   (b) nack(requeue) redelivers it — and the delivery count climbs;
///   (c) a consumer that drops without acking causes redelivery to a fresh one.
/// (c) is also the "consumer crash → redelivery" fault-tolerance story.
/// </summary>
public sealed class ExplicitAck : IBusScenario
{
    public string Name => "S7 explicit ack";
    public string Description => "ack removes; nack requeues; a crashed consumer triggers redelivery.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsManualAck)
            return Unsupported(Name, "no manual ack on this broker", t0);

        var notes = new List<string>();

        // (a) ack removes — message delivered exactly once.
        {
            var topic = $"mbc.s7a.{Nonce()}";
            var c = await AckCollector.StartAsync(bus, topic, new SubscribeOptions
            {
                SubscriberId = $"ack-{Nonce()}",
            });
            try
            {
                await bus.PublishAsync(topic, "ack-1");
                await WaitUntilAsync(() => c.Count() >= 1, TimeSpan.FromSeconds(4));
                await Task.Delay(500); // give any erroneous redelivery a chance to show up
                if (c.Count() != 1)
                    return Fail(Name, $"(a) acked msg delivered {c.Count()}×", t0);
                notes.Add("ack→once");
            }
            finally
            {
                await c.DisposeAsync();
            }
        }

        // (b) nack(requeue) redelivers.
        {
            var topic = $"mbc.s7b.{Nonce()}";
            var attempts = 0;
            int? secondCount = null;
            var c = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = $"nack-{Nonce()}" },
                autoAck: false,
                onMessage: async m =>
                {
                    attempts += 1;
                    if (attempts == 1) await m.NackAsync(true); // requeue
                    else { secondCount = m.DeliveryCount; await m.AckAsync(); }
                });
            try
            {
                await bus.PublishAsync(topic, "nack-1");
                var ok = await WaitUntilAsync(() => attempts >= 2, TimeSpan.FromSeconds(5));
                if (!ok) return Fail(Name, "(b) nacked msg was not redelivered", t0);
                if (bus.BusCapabilities.ReportsDeliveryCount && secondCount != 2)
                    return Fail(Name, $"(b) expected deliveryCount 2, got {secondCount}", t0);
                notes.Add(bus.BusCapabilities.ReportsDeliveryCount
                    ? "nack→redelivered (count=2)"
                    : "nack→redelivered (count n/a)");
            }
            finally
            {
                await c.DisposeAsync();
            }
        }

        // (c) crashed consumer (drops without acking) → redelivery to a fresh one.
        {
            var topic = $"mbc.s7c.{Nonce()}";
            var queueId = $"crash-{Nonce()}"; // the fresh consumer reuses this queue
            var crashed = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = queueId },
                autoAck: false,
                onMessage: _ => Task.CompletedTask /* receive but never settle — simulate a crash */);
            await bus.PublishAsync(topic, "crash-1");
            await WaitUntilAsync(() => crashed.Count() >= 1, TimeSpan.FromSeconds(4));
            await crashed.DisposeAsync(); // drop the consumer with the message un-acked

            // A fresh consumer on the SAME queue must get the un-acked message back.
            var fresh = await AckCollector.StartAsync(bus, topic, new SubscribeOptions
            {
                SubscriberId = queueId,
            });
            try
            {
                var got = await WaitUntilAsync(() => fresh.Count() >= 1, TimeSpan.FromSeconds(5));
                if (!got) return Fail(Name, "(c) crashed consumer msg was lost", t0);
                notes.Add("crash→redelivered");
            }
            finally
            {
                await fresh.DisposeAsync();
            }
        }

        return Pass(Name, string.Join("; ", notes), t0);
    }
}
