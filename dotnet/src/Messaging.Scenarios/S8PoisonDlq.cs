using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S8 — Poison message → dead-letter after N attempts. A message that always
/// fails must not loop forever: after <c>maxDeliveries</c> attempts the broker
/// moves it to the dead-letter destination, where an operator (here, a DLQ
/// subscriber) can inspect it. Artemis does this with <c>max-delivery-attempts</c>;
/// RabbitMQ with a delivery-limit + dead-letter exchange.
/// </summary>
public sealed class PoisonDlq : IBusScenario
{
    private const int MaxDeliveries = 3;

    public string Name => "S8 poison → dead-letter";
    public string Description => $"A always-failing message is dead-lettered after {MaxDeliveries} attempts.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsDeadLetter)
            return Unsupported(Name, "no dead-letter support on this broker", t0);

        var topic = $"mbc.s8.{Nonce()}";
        var attempts = 0;
        int? lastCount = null;

        var main = await AckCollector.StartAsync(bus, topic,
            new SubscribeOptions
            {
                SubscriberId = $"poison-{Nonce()}",
                DeadLetter = true,
                MaxDeliveries = MaxDeliveries,
            },
            autoAck: false,
            onMessage: async m =>
            {
                attempts += 1;
                lastCount = m.DeliveryCount;
                await m.NackAsync(true); // always fail → forces redelivery then dead-letter
            });
        var dlq = await AckCollector.StartAsync(bus, MessageBus.DeadLetterAddress(topic), new SubscribeOptions
        {
            Kind = TopologyKind.Fanout,
            SubscriberId = $"dlq-{Nonce()}",
        });
        try
        {
            await bus.PublishAsync(topic, "poison-1");
            var landed = await WaitUntilAsync(() => dlq.Count() >= 1, TimeSpan.FromSeconds(8));
            if (!landed)
                return Fail(Name, $"never dead-lettered (attempts={attempts}, dlq={dlq.Count()})", t0);

            // The key property is "bounded, then dead-lettered" — not an exact count.
            // Artemis & the in-memory reference dead-letter at exactly maxDeliveries;
            // RabbitMQ quorum queues dead-letter when the count *exceeds* the limit
            // (maxDeliveries + 1). Both are correct; an infinite loop is not.
            if (attempts < 2 || attempts > MaxDeliveries + 1)
                return Fail(Name, $"delivery attempts out of range: {attempts} (limit {MaxDeliveries})", t0);

            var countNote = bus.BusCapabilities.ReportsDeliveryCount && lastCount is not null
                ? $" (final deliveryCount={lastCount})"
                : string.Empty;
            return Pass(Name, $"dead-lettered after {attempts} attempts{countNote}", t0);
        }
        finally
        {
            await main.DisposeAsync();
            await dlq.DisposeAsync();
        }
    }
}
