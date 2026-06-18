namespace Messaging.Abstractions;

/// <summary>
/// The single seam the whole project depends on (Dependency Inversion).
/// Application code, the scenario runner, and the tests all depend on this,
/// never on a concrete broker client. Adding a broker means adding an adapter,
/// with no change to existing code (Open/Closed). Each adapter has exactly one
/// reason to change: its broker (Single Responsibility).
/// </summary>
/// <remarks>
/// Contract: <see cref="CancelAsync"/> and <see cref="ListScheduledAsync"/> MUST
/// throw <see cref="OperationNotSupportedException"/> (never silently no-op) when
/// the corresponding capability is false.
/// </remarks>
public interface IMessageScheduler : IAsyncDisposable
{
    string Name { get; }

    Capabilities Capabilities { get; }

    Task ConnectAsync(CancellationToken ct = default);

    Task SendNowAsync(string destination, string payload, CancellationToken ct = default);

    Task<ScheduleHandle> ScheduleAsync(
        string destination,
        string payload,
        DateTimeOffset deliverAt,
        CancellationToken ct = default);

    /// <exception cref="OperationNotSupportedException">If cancel is unsupported.</exception>
    Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default);

    /// <exception cref="OperationNotSupportedException">If listing is unsupported.</exception>
    Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(
        string destination,
        CancellationToken ct = default);

    Task<ISubscription> ConsumeAsync(
        string destination,
        MessageHandler handler,
        CancellationToken ct = default);
}
