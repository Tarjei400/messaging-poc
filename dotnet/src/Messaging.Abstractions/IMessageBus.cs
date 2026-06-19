namespace Messaging.Abstractions;

/// <summary>
/// The second seam of the project (a sibling of <see cref="IMessageScheduler"/>).
///
/// Scheduling is point-to-point and time-shifted; pub/sub, fanout and explicit
/// acknowledgement are about <i>fan-out</i> and <i>settlement</i>. Those are
/// different reasons to change, so they live on a different port (Interface
/// Segregation): an adapter that only schedules need not grow topic/ack methods,
/// and the scheduling scenarios (S1–S4) are untouched.
///
/// Every concrete adapter (in-memory, Artemis, RabbitMQ) implements BOTH ports;
/// the resilience decorator implements both and forwards through one pipeline.
/// </summary>
public interface IMessageBus : IAsyncDisposable
{
    /// <summary>Human-readable adapter name (shared with the scheduler port).</summary>
    string Name { get; }

    /// <summary>What this adapter's bus surface can and cannot do.</summary>
    BusCapabilities BusCapabilities { get; }

    /// <summary>Establish the connection and provision any required topic topology.</summary>
    Task ConnectBusAsync(CancellationToken ct = default);

    /// <summary>Publish to a topic/fanout address (NOT a single point-to-point queue).
    /// <paramref name="options"/> carries per-message metadata (priority, group,
    /// dedup, reply-to, ttl, headers); omit it for a plain publish.</summary>
    Task PublishAsync(
        string topic,
        string payload,
        string? routingKey = null,
        PublishOptions? options = null,
        CancellationToken ct = default);

    /// <summary>Subscribe to a topic. Each distinct <c>SubscriberId</c> is an independent queue.</summary>
    Task<ISubscription> SubscribeAsync(
        string topic,
        AckHandler handler,
        SubscribeOptions? options = null,
        CancellationToken ct = default);
}

/// <summary><c>Topic</c> = routing-key filtered; <c>Fanout</c> = every subscriber gets a copy.</summary>
public enum TopologyKind
{
    Topic,
    Fanout,
}

/// <summary>
/// A message delivered to a bus subscriber. Unlike <see cref="ReceivedMessage"/>
/// (auto-ack on the scheduler port), the consumer here controls settlement
/// explicitly — which is exactly what makes redelivery, poison-handling and
/// at-least-once delivery observable.
/// </summary>
public interface IIncomingMessage
{
    string Id { get; }
    string Destination { get; }
    string Body { get; }
    IReadOnlyDictionary<string, string> Headers { get; }

    /// <summary>
    /// 1-based broker-reported delivery attempt, when the broker exposes it.
    /// Absent on brokers that only report a redelivered boolean (RabbitMQ classic
    /// queues) — see <see cref="BusCapabilities.ReportsDeliveryCount"/>.
    /// </summary>
    int? DeliveryCount { get; }

    /// <summary>Address a reply should be sent to (request/reply — S15).</summary>
    string? ReplyTo => null;
    /// <summary>Correlates a reply with its request (request/reply — S15).</summary>
    string? CorrelationId => null;
    /// <summary>Ordering/affinity key — same group keeps order, pinned to one consumer (S12).</summary>
    string? GroupId => null;
    /// <summary>Broker-reported message priority, when exposed (S14).</summary>
    int? Priority => null;

    /// <summary>Settle positively: the broker removes the message.</summary>
    Task AckAsync();

    /// <summary>
    /// Settle negatively. <paramref name="requeue"/>=true → redeliver (subject to
    /// maxDeliveries); false → dead-letter (if configured) or drop.
    /// </summary>
    Task NackAsync(bool requeue);
}

/// <summary>Callback invoked for each received bus message; the consumer settles it.</summary>
public delegate Task AckHandler(IIncomingMessage message);

/// <summary>Options that shape a subscription's topology and dead-letter wiring.</summary>
public sealed record SubscribeOptions
{
    /// <summary><c>Topic</c> = routing-key filtered; <c>Fanout</c> = every subscriber gets a copy.</summary>
    public TopologyKind Kind { get; init; } = TopologyKind.Topic;

    /// <summary>Topic routing-key filter, e.g. <c>order.*</c>. Ignored for fanout.</summary>
    public string? RoutingKey { get; init; }

    /// <summary>
    /// Names the subscriber's queue. Distinct ids → independent copies (pub/sub,
    /// fanout). Re-using an id → consumers share one queue (competing consumers).
    /// </summary>
    public string? SubscriberId { get; init; }

    /// <summary>Provision dead-letter wiring so poison messages can be inspected.</summary>
    public bool DeadLetter { get; init; }

    /// <summary>Number of delivery attempts before a message is dead-lettered.</summary>
    public int? MaxDeliveries { get; init; }

    /// <summary>
    /// Delay (ms) before a nacked message is redelivered. When set (with
    /// <see cref="DeadLetter"/>), the adapter parks the failed message in a
    /// dedicated retry queue instead of requeuing it in place, so the main queue
    /// keeps flowing (non-blocking retry). Adapters that drive retry from broker
    /// config (Artemis <c>redelivery-delay</c>) treat this as advisory.
    /// </summary>
    public int? RetryDelayMs { get; init; }

    /// <summary>Preserve per-group order across competing consumers — each group
    /// is pinned to one consumer (S12).</summary>
    public bool PartitionByGroup { get; init; }

    /// <summary>Only one consumer on the shared queue is active at a time; a
    /// standby takes over if it drops, preserving order (S18).</summary>
    public bool SingleActiveConsumer { get; init; }

    /// <summary>Replay the whole retained log from the beginning rather than only
    /// new messages (S19). Requires <see cref="BusCapabilities.SupportsStreamReplay"/>.</summary>
    public bool StreamReplay { get; init; }

    /// <summary>Transient per-connection subscription (exclusive + auto-delete) —
    /// the queue vanishes when the subscriber disconnects (SSE cluster).</summary>
    public bool Transient { get; init; }

    /// <summary>Declare the queue priority-capable so <see cref="PublishOptions.Priority"/>
    /// is honoured (RabbitMQ <c>x-max-priority</c>; no-op where native).</summary>
    public bool PriorityQueue { get; init; }

    /// <summary>Declare the queue so an unconsumed message expires to the expiry
    /// address (S16). On RabbitMQ this wires <c>x-dead-letter-exchange</c> → the
    /// expiry fanout (per-message <c>expiration</c> then routes the expired
    /// message there). Artemis drives expiry from broker.xml, so this is a no-op
    /// there; the value mirrors the publish-side <c>TtlMs</c>.</summary>
    public int? TtlMs { get; init; }
}

/// <summary>
/// Per-message publish metadata. Each adapter maps these onto its native AMQP
/// properties. All optional — a bare <c>PublishAsync(topic, body)</c> is unchanged.
/// </summary>
public sealed record PublishOptions
{
    /// <summary>Broker priority (higher = sooner). RabbitMQ 0–9; Artemis 0–9.</summary>
    public int? Priority { get; init; }
    /// <summary>Ordering/affinity key — same group → ordered, pinned to one consumer (S12).</summary>
    public string? GroupId { get; init; }
    /// <summary>Producer dedup key — the broker drops a repeat within its window (S13).</summary>
    public string? DedupId { get; init; }
    /// <summary>Where a reply should be sent (request/reply — S15).</summary>
    public string? ReplyTo { get; init; }
    /// <summary>Correlates a reply with its request (request/reply — S15).</summary>
    public string? CorrelationId { get; init; }
    /// <summary>Time-to-live before the message expires to the expiry address (S16).</summary>
    public int? TtlMs { get; init; }
    /// <summary>Arbitrary application headers.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}

/// <summary>
/// Self-declared bus capabilities, scored by the runner the same way the
/// scheduling <see cref="Capabilities"/> are: an honest gap prints ⊘ n/a, a real
/// break ✗. The two trailing flags default to false so existing constructions
/// stay valid; adapters set them where the feature is genuinely supported.
/// </summary>
public sealed record BusCapabilities(
    bool SupportsTopic,
    bool SupportsFanout,
    bool SupportsManualAck,
    bool SupportsDeadLetter,
    bool ReportsDeliveryCount,
    bool SupportsDedup = false,
    bool SupportsStreamReplay = false,
    /// <summary>Broker-native ordered message groups — a groupId is pinned to one
    /// consumer so per-group order survives competing consumers (S12). Artemis
    /// message groups / RabbitMQ consistent-hash exchange / in-memory affinity.
    /// Defaults true (only an adapter that genuinely lacks it sets this false).</summary>
    bool SupportsMessageGroups = true);

/// <summary>Conventions shared by the bus port across every adapter.</summary>
public static class MessageBus
{
    /// <summary>Default number of delivery attempts before dead-lettering.</summary>
    public const int DefaultMaxDeliveries = 3;

    /// <summary>
    /// The conventional dead-letter destination for a topic. Every adapter maps
    /// this single logical name onto its native dead-letter concept (Artemis
    /// <c>dead-letter-address</c>, RabbitMQ <c>{topic}.dlq</c> via a DLX), so a
    /// scenario can subscribe here to prove a poison message was dead-lettered.
    /// </summary>
    public static string DeadLetterAddress(string topic) => $"{topic}.dlq";

    /// <summary>
    /// The conventional expiry destination for a topic — where a message that
    /// lives past its TTL lands (distinct from the dead-letter address, which is
    /// for poison messages). Artemis <c>expiry-address</c>; RabbitMQ per-queue
    /// <c>x-message-ttl</c> + an expiry exchange.
    /// </summary>
    public static string ExpiryAddress(string topic) => $"{topic}.expiry";
}
