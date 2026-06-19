using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S19 — Stream replay from the beginning. A brand-new subscriber replays the
/// FULL history of a topic — including messages published (and already consumed
/// by others) before it attached. This is the honest gap that separates RabbitMQ
/// (✓ streams) from Artemis (⊘): an append-only log can be re-read from offset 0,
/// a classic queue cannot.
///
/// RabbitMQ: a stream queue (<c>x-queue-type=stream</c>) bound to the topic
/// exchange; a fresh consumer with <c>x-stream-offset=first</c> re-reads the
/// whole log. The stream must be bound BEFORE the publishes to capture them, so
/// we establish it with an initial streamReplay subscription, drain it, publish
/// N, then attach a SECOND fresh streamReplay subscriber and prove it replays all
/// N from offset 0. In-memory: a per-topic append-only log seeded into each
/// streamReplay queue. Artemis: <c>SupportsStreamReplay=false</c> → ⊘.
/// </summary>
public sealed class StreamReplay : IBusScenario
{
    private const int N = 8; // messages published into the stream before the replay subscriber attaches

    public string Name => "S19 stream replay";
    public string Description =>
        $"A fresh subscriber replays all {N} messages from offset 0, including ones published & consumed before it attached.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        if (!bus.BusCapabilities.SupportsStreamReplay)
            return Unsupported(Name,
                "broker cannot replay consumed history (no stream/offset support)", t0);

        var topic = $"mbc.s19.{Nonce()}";
        var gate = new object();

        // 1. Establish the stream so it captures publishes. On RabbitMQ this
        //    declares + binds the stream queue; the log only captures from now on,
        //    so this MUST happen before the publishes. A first streamReplay
        //    subscriber that consumes everything it sees doubles as proof the early
        //    messages were really delivered and acked, not merely "still pending".
        var firstSeen = new List<string>();
        AckHandler firstHandler = async m =>
        {
            lock (gate) firstSeen.Add(m.Body);
            await m.AckAsync();
        };
        var first = await bus.SubscribeAsync(topic, firstHandler,
            new SubscribeOptions { StreamReplay = true });
        await Task.Delay(300); // let the stream queue/binding settle before publishing

        // 2. Publish N messages into the established stream.
        for (var i = 0; i < N; i++)
            await bus.PublishAsync(topic, $"evt-{i}");

        int FirstCount() { lock (gate) return firstSeen.Count; }
        var firstOk = await WaitUntilAsync(() => FirstCount() >= N, TimeSpan.FromSeconds(10));
        if (!firstOk)
        {
            await first.DisposeAsync();
            return Fail(Name, $"establishing subscriber saw {FirstCount()}/{N} live messages", t0);
        }
        // Drop the first subscriber — its messages are gone from a classic queue's
        // point of view; only an append-only log can hand them to a newcomer.
        await first.DisposeAsync();
        await Task.Delay(300);

        // 3. Attach a BRAND-NEW subscriber AFTER the publishes (and after they were
        //    consumed) and assert it replays the entire history from offset 0.
        var replayed = new List<string>();
        AckHandler replayHandler = async m =>
        {
            lock (gate) replayed.Add(m.Body);
            await m.AckAsync();
        };
        var replay = await bus.SubscribeAsync(topic, replayHandler,
            new SubscribeOptions { StreamReplay = true });
        try
        {
            int Count() { lock (gate) return replayed.Count; }
            var ok = await WaitUntilAsync(() => Count() >= N, TimeSpan.FromSeconds(12));
            if (!ok)
                return Fail(Name, $"fresh subscriber replayed {Count()}/{N} from offset 0", t0);

            HashSet<string> got;
            lock (gate) got = new HashSet<string>(replayed);
            for (var i = 0; i < N; i++)
                if (!got.Contains($"evt-{i}"))
                    return Fail(Name, $"replay missing evt-{i}", t0);

            return Pass(Name, $"fresh subscriber replayed all {N} messages from offset 0 (full history)", t0);
        }
        finally
        {
            await replay.DisposeAsync();
        }
    }
}
