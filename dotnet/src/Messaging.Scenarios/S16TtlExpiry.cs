using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S16 — TTL → expiry. A message published with a short <c>TtlMs</c> to a queue
/// that has no active consumer must expire un-consumed and land on the expiry
/// address (distinct from the dead-letter address, which is for poison messages).
/// A subscriber on <see cref="MessageBus.ExpiryAddress"/> observes it.
///
/// Pattern: declare the main queue by briefly subscribing then dropping the
/// consumer (the durable queue persists and holds the message), subscribe to the
/// expiry address, then publish with a TTL and assert arrival on expiry.
///
/// Artemis: broker.xml routes <c>mbc.s16.#</c> to the multicast address
/// <c>mbc.EXPIRY</c>; the adapter maps the <c>.expiry</c> suffix onto it.
/// RabbitMQ: the per-subscriber queue carries a dead-letter-exchange wired to an
/// expiry fanout the <c>.expiry</c> subscriber binds to (per-message expiration
/// supplies the TTL). In-memory: a per-message timer drops the un-consumed
/// message to the expiry address.
/// </summary>
public sealed class TtlExpiry : IBusScenario
{
    private const int TtlMs = 600; // short enough to expire well within the wait window

    public string Name => "S16 TTL → expiry";
    public string Description => $"An unconsumed message with ttl={TtlMs}ms lands on the expiry address.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        var topic = $"mbc.s16.{Nonce()}";
        var subscriberId = $"expiring-{Nonce()}";

        // Declare the main per-subscriber queue (so it exists with its TTL/expiry
        // wiring), then drop the consumer — the durable queue stays and holds the
        // message with no one to consume it, so it can expire.
        var warmup = await bus.SubscribeAsync(topic, m => m.AckAsync(),
            new SubscribeOptions { SubscriberId = subscriberId, TtlMs = TtlMs });
        await warmup.DisposeAsync();

        // Watch the expiry address.
        var expiry = await AckCollector.StartAsync(bus, MessageBus.ExpiryAddress(topic), new SubscribeOptions
        {
            Kind = TopologyKind.Fanout,
            SubscriberId = $"expiry-watch-{Nonce()}",
        });
        try
        {
            // Publish with a short TTL; with no active consumer it must expire.
            await bus.PublishAsync(topic, "perishable", options: new PublishOptions { TtlMs = TtlMs });

            var landed = await WaitUntilAsync(() => expiry.Count() >= 1, TimeSpan.FromSeconds(8));
            if (!landed)
                return Fail(Name, "message never reached the expiry address", t0);
            if (expiry.Bodies()[0] != "perishable")
                return Fail(Name, $"unexpected expiry body \"{expiry.Bodies()[0]}\"", t0);

            return Pass(Name, "expired message landed on the expiry address", t0);
        }
        finally
        {
            await expiry.DisposeAsync();
        }
    }
}
