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

    /// <summary>Publish to a topic/fanout address (NOT a single point-to-point queue).</summary>
    Task PublishAsync(string topic, string payload, string? routingKey = null, CancellationToken ct = default);

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
}

/// <summary>
/// Self-declared bus capabilities, scored by the runner the same way the
/// scheduling <see cref="Capabilities"/> are: an honest gap prints ⊘ n/a, a real
/// break ✗.
/// </summary>
public sealed record BusCapabilities(
    bool SupportsTopic,
    bool SupportsFanout,
    bool SupportsManualAck,
    bool SupportsDeadLetter,
    bool ReportsDeliveryCount);

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
}
