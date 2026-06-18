namespace Messaging.Abstractions;

/// <summary>
/// Self-declared capabilities of an adapter. The scenario runner reads these to
/// decide whether an unsupported operation is an expected gap (⊘) or a real
/// failure (✗), turning the comparison's claims into verifiable outcomes.
/// </summary>
/// <param name="Bus">
/// Pub/sub, fanout and explicit-ack capabilities — present when the adapter also
/// implements <see cref="IMessageBus"/>. Additive and trailing, so scheduler-only
/// constructions (e.g. test fakes) stay valid by leaving it null.
/// </param>
public sealed record Capabilities(
    string Protocol,
    bool NativeScheduling,
    bool SupportsCancel,
    bool SupportsList,
    BusCapabilities? Bus = null);

/// <summary>Opaque handle for a scheduled message. Only <see cref="Id"/> is contractual.</summary>
public sealed record ScheduleHandle(
    string Id,
    string Destination,
    DateTimeOffset DeliverAt,
    object? Native = null);

/// <summary>A message delivered to a consumer.</summary>
public sealed record ReceivedMessage(
    string Id,
    string Destination,
    string Body,
    IReadOnlyDictionary<string, string> Headers);

/// <summary>Summary of a still-pending scheduled message, for inspection.</summary>
public sealed record ScheduledInfo(
    string Id,
    string Destination,
    DateTimeOffset? DeliverAt = null);

/// <summary>An active subscription; dispose to stop consuming.</summary>
public interface ISubscription : IAsyncDisposable
{
}

/// <summary>Callback invoked for each received message.</summary>
public delegate Task MessageHandler(ReceivedMessage message);
