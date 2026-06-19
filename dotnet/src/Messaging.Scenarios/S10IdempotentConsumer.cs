using Messaging.Abstractions;
using Messaging.Idempotency;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S10 — At-least-once → idempotent consumer. Three observable sub-proofs show
/// the full problem/solution/resilience cycle:
///
/// <list type="bullet">
///   <item><b>(a) Without idempotency</b> — a message is delivered twice because the
///     first consumer crashes before acking. The business-logic side-effect (a
///     counter) runs twice — duplicates are real, not theoretical.</item>
///   <item><b>(b) With an idempotent consumer</b> — the same crash+redelivery happens,
///     but the <see cref="InMemoryIdempotencyStore"/> suppresses the duplicate.
///     The counter stays at 1 regardless of how many times the broker delivers
///     the message.</item>
///   <item><b>(c) Store-down resilience (Redis-outage simulation)</b> — the store always
///     throws. The consumer degrades gracefully — it processes the message
///     anyway (fail-open) rather than blocking. A Redis outage cannot halt
///     message processing; it merely removes the deduplication guarantee
///     temporarily.</item>
/// </list>
///
/// Idempotency key = message body. In production, use a publisher-assigned
/// correlation ID in a header so the key survives broker serialisation round-trips.
/// </summary>
public sealed class IdempotentConsumer : IBusScenario
{
    public string Name => "S10 idempotent consumer";
    public string Description =>
        "at-least-once causes duplicates; idempotency store deduplicates; store-down degrades gracefully.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsManualAck)
            return Unsupported(Name, "no manual ack on this broker", t0);

        var notes = new List<string>();

        // -------------------------------------------------------------------
        // (a) Without idempotency — duplicate side-effect is observable.
        // -------------------------------------------------------------------
        {
            var topic = $"mbc.s10a.{Nonce()}";
            var queueId = $"idm-a-{Nonce()}";
            var processCount = 0;
            var gate = new object();

            // First consumer: counts the side-effect but drops without acking.
            var first = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = queueId },
                autoAck: false,
                onMessage: _ =>
                {
                    lock (gate) processCount++;
                    return Task.CompletedTask; // never ack → simulates a crash
                });
            await bus.PublishAsync(topic, $"order-a-{Nonce()}");
            await WaitUntilAsync(() => first.Count() >= 1, TimeSpan.FromSeconds(4));
            await first.DisposeAsync(); // drop with message un-acked → triggers redelivery

            // Fresh consumer on the same queue picks up the redelivered message.
            var second = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = queueId },
                autoAck: false,
                onMessage: async m =>
                {
                    lock (gate) processCount++;
                    await m.AckAsync();
                });
            var redelivered = await WaitUntilAsync(() => second.Count() >= 1, TimeSpan.FromSeconds(5));
            await second.DisposeAsync();

            if (!redelivered) return Fail(Name, "(a) redelivery did not happen", t0);
            int count;
            lock (gate) count = processCount;
            if (count != 2) return Fail(Name, $"(a) expected 2 processings, got {count}", t0);
            notes.Add($"no-store\u2192processed\u00d7{count}");
        }

        // -------------------------------------------------------------------
        // (b) With idempotency — duplicate delivery, single processing.
        // -------------------------------------------------------------------
        {
            var topic = $"mbc.s10b.{Nonce()}";
            var queueId = $"idm-b-{Nonce()}";
            var msgBody = $"order-b-{Nonce()}";
            var processCount = 0;
            var gate = new object();
            var store = new InMemoryIdempotencyStore();

            // First consumer: idempotent handler processes once, then drops.
            var firstHandler = IdempotentHandler.Wrap(store, _ =>
            {
                lock (gate) processCount++;
                return Task.CompletedTask; // Don't ack — simulate crash so broker redelivers.
            });
            var first = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = queueId },
                autoAck: false,
                onMessage: m => firstHandler(m)); // bridge AckHandler → Func<IIncomingMessage,Task>
            await bus.PublishAsync(topic, msgBody);
            await WaitUntilAsync(() => first.Count() >= 1, TimeSpan.FromSeconds(4));
            await first.DisposeAsync();

            // Fresh consumer with the SAME store sees the key already recorded.
            var secondHandler = IdempotentHandler.Wrap(store, async m =>
            {
                lock (gate) processCount++;
                await m.AckAsync();
            });
            var second = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = queueId },
                autoAck: false,
                onMessage: m => secondHandler(m)); // bridge AckHandler → Func<IIncomingMessage,Task>
            var redelivered = await WaitUntilAsync(() => second.Count() >= 1, TimeSpan.FromSeconds(5));
            await second.DisposeAsync();

            if (!redelivered) return Fail(Name, "(b) redelivery did not happen", t0);
            int count;
            lock (gate) count = processCount;
            if (count != 1) return Fail(Name, $"(b) expected 1 processing, got {count}", t0);
            notes.Add("with-store\u2192processed\u00d71");
        }

        // -------------------------------------------------------------------
        // (c) Store throws (Redis-down simulation) → fail-open, still processes.
        // -------------------------------------------------------------------
        {
            var topic = $"mbc.s10c.{Nonce()}";
            var queueId = $"idm-c-{Nonce()}";
            var processCount = 0;
            Exception? leakedException = null;
            var gate = new object();

            var brokenStore = new ThrowingIdempotencyStore();
            var handler = IdempotentHandler.Wrap(brokenStore, async m =>
            {
                lock (gate) processCount++;
                await m.AckAsync();
            });

            var sub = await AckCollector.StartAsync(bus, topic,
                new SubscribeOptions { SubscriberId = queueId },
                autoAck: false,
                onMessage: async m =>
                {
                    try { await handler(m); }
                    catch (Exception ex) { lock (gate) leakedException = ex; await m.AckAsync(); }
                });
            try
            {
                await bus.PublishAsync(topic, $"order-c-{Nonce()}");
                var ok = await WaitUntilAsync(() => sub.Count() >= 1, TimeSpan.FromSeconds(4));
                if (!ok) return Fail(Name, "(c) message was not delivered", t0);
                await Task.Delay(200); // brief wait to confirm no second delivery

                Exception? leaked;
                int count;
                lock (gate) { leaked = leakedException; count = processCount; }

                if (leaked is not null)
                    return Fail(Name, $"(c) store error leaked to caller: {leaked.Message}", t0);
                if (count != 1)
                    return Fail(Name, $"(c) expected fail-open processing, got {count}", t0);
                notes.Add("store-down\u2192fail-open,processed\u00d71");
            }
            finally
            {
                await sub.DisposeAsync();
            }
        }

        return Pass(Name, string.Join("; ", notes), t0);
    }

    /// <summary>Simulates a Redis outage — always throws on every call.</summary>
    private sealed class ThrowingIdempotencyStore : IIdempotencyStore
    {
        public Task<bool> TryMarkSeenAsync(string key, int ttlSeconds = 300) =>
            throw new InvalidOperationException("Redis connection refused");
    }
}
