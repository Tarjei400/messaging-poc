using System.Diagnostics;
using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S11 — Non-blocking retry queue → dead-letter. Unlike S8 (which requeues in
/// place and can head-of-line-block), a failing message is parked in a dedicated
/// retry queue and redelivered after a short delay, up to 5 retries, then
/// dead-lettered. Meanwhile a batch of healthy messages must drain immediately —
/// proving the poison message does not block the main queue.
///
/// The same observable behaviour is wired natively per broker: RabbitMQ uses a
/// DLX + TTL retry queue that bounces back to the main queue; Artemis uses
/// <c>redelivery-delay</c> + <c>max-delivery-attempts</c> (configured for
/// <c>mbc.s11.#</c> in broker.xml); the in-memory reference parks on a timer.
/// </summary>
public sealed class RetryQueue : IBusScenario
{
    private const int MaxDeliveries = 6;   // initial attempt + 5 retries, then dead-letter
    private const int RetryDelayMs = 250;  // backoff parked in the retry queue between attempts
    private const int GoodCount = 8;       // healthy messages that must flow past the parked poison

    public string Name => "S11 retry queue";
    public string Description =>
        $"{GoodCount} good messages flow while a poison message is parked in a retry queue and dead-lettered after {MaxDeliveries - 1} retries.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsDeadLetter)
            return Unsupported(Name, "no dead-letter support on this broker", t0);

        var topic = $"mbc.s11.{Nonce()}"; // the prefix selects the Artemis retry policy
        var gate = new object();
        var good = new List<string>();
        var attempts = 0;
        int? lastCount = null;
        long? goodMark = null;
        long? dlqMark = null;

        var main = await AckCollector.StartAsync(bus, topic,
            new SubscribeOptions
            {
                SubscriberId = $"worker-{Nonce()}",
                DeadLetter = true,
                MaxDeliveries = MaxDeliveries,
                RetryDelayMs = RetryDelayMs,
            },
            autoAck: false,
            onMessage: async m =>
            {
                if (m.Body.StartsWith("poison", StringComparison.Ordinal))
                {
                    Interlocked.Increment(ref attempts);
                    lastCount = m.DeliveryCount;
                    await m.NackAsync(true); // always fail → park in retry queue, eventually DLQ
                }
                else
                {
                    lock (gate)
                    {
                        good.Add(m.Body);
                        if (good.Count == GoodCount) goodMark = Stopwatch.GetTimestamp();
                    }
                    await m.AckAsync();
                }
            });
        var dlq = await AckCollector.StartAsync(bus, MessageBus.DeadLetterAddress(topic),
            new SubscribeOptions { Kind = TopologyKind.Fanout, SubscriberId = $"dlq-{Nonce()}" },
            onMessage: async m =>
            {
                lock (gate) dlqMark ??= Stopwatch.GetTimestamp();
                await m.AckAsync();
            });
        try
        {
            // Interleave the poison among the healthy messages: if it blocked the
            // queue, the later good messages would be stuck behind its retries.
            for (var i = 0; i < 3; i++) await bus.PublishAsync(topic, $"job-{i}");
            await bus.PublishAsync(topic, "poison-1");
            for (var i = 3; i < GoodCount; i++) await bus.PublishAsync(topic, $"job-{i}");

            var timeout = TimeSpan.FromMilliseconds(MaxDeliveries * RetryDelayMs + 4000);
            int GoodCountNow() { lock (gate) return good.Count; }
            var done = await WaitUntilAsync(() => GoodCountNow() >= GoodCount && dlq.Count() >= 1, timeout);

            List<string> snapshot;
            long? goodAt, dlqAt;
            lock (gate)
            {
                snapshot = good.ToList();
                goodAt = goodMark;
                dlqAt = dlqMark;
            }
            if (!done)
                return Fail(Name,
                    $"pipeline did not drain (good={snapshot.Count}/{GoodCount}, dlq={dlq.Count()}, attempts={attempts})", t0);

            if (new HashSet<string>(snapshot).Count != GoodCount)
                return Fail(Name,
                    $"good messages not delivered exactly once ({new HashSet<string>(snapshot).Count} unique of {snapshot.Count})", t0);

            // Bounded retries, then dead-lettered — not an infinite loop. The exact
            // count can vary by ±1 across brokers; report the actual for comparison.
            if (attempts < 2 || attempts > MaxDeliveries + 1)
                return Fail(Name, $"retry attempts out of range: {attempts} (limit {MaxDeliveries})", t0);

            // Non-blocking proof: the healthy batch must finish before the poison
            // exhausts its retries and lands in the DLQ.
            if (goodAt is null || dlqAt is null || goodAt.Value >= dlqAt.Value)
                return Fail(Name,
                    $"main queue was blocked by the poison message (good done {Fmt(goodAt, t0)}, dlq {Fmt(dlqAt, t0)})", t0);

            var goodMs = (long)Stopwatch.GetElapsedTime(t0, goodAt.Value).TotalMilliseconds;
            var dlqMs = (long)Stopwatch.GetElapsedTime(t0, dlqAt.Value).TotalMilliseconds;
            var gap = (long)Stopwatch.GetElapsedTime(goodAt.Value, dlqAt.Value).TotalMilliseconds;
            var countNote = bus.BusCapabilities.ReportsDeliveryCount && lastCount is not null
                ? $" (final deliveryCount={lastCount})"
                : string.Empty;
            return Pass(Name,
                $"{GoodCount} ok in {goodMs}ms; poison→DLQ after {attempts} attempts{countNote} in {dlqMs}ms; main unblocked (ok done {gap}ms before DLQ)", t0);
        }
        finally
        {
            await main.DisposeAsync();
            await dlq.DisposeAsync();
        }
    }

    private static string Fmt(long? mark, long t0) =>
        mark is null ? "n/a" : $"{(long)Stopwatch.GetElapsedTime(t0, mark.Value).TotalMilliseconds}ms";
}
