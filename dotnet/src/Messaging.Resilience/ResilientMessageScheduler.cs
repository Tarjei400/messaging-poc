using Messaging.Abstractions;
using Polly;
using Polly.CircuitBreaker;

namespace Messaging.Resilience;

/// <summary>
/// A resilience lifecycle event, surfaced so the fault-tolerance narrator can
/// print a real timeline (retries, breaker open/half-open/close) rather than a
/// faked one. The listener defaults to no-op, so normal runs are unaffected.
/// </summary>
public abstract record ResilienceEvent
{
    public sealed record Retry(int Attempt, string Error) : ResilienceEvent;
    public sealed record BreakerOpen : ResilienceEvent;
    public sealed record BreakerHalfOpen : ResilienceEvent;
    public sealed record BreakerClose : ResilienceEvent;
}

/// <summary>A sink for <see cref="ResilienceEvent"/>s (the .NET mirror of the TS onEvent hook).</summary>
public delegate void ResilienceListener(ResilienceEvent @event);

/// <summary>
/// A resilience <b>decorator</b> over any adapter. It adds retry-with-backoff and
/// a circuit breaker (Polly v8) around the broker calls, without any adapter
/// knowing it exists — the Decorator pattern on top of the port. It implements
/// BOTH ports: scheduling calls and bus publish/subscribe setup run through the
/// same pipeline.
///
/// Crucially, it treats <see cref="OperationNotSupportedException"/> as a
/// <i>contract outcome</i>, not a transient fault: that exception is never
/// retried and never trips the breaker, so an honestly-unsupported operation
/// (e.g. RabbitMQ cancel) still surfaces cleanly as "unsupported".
/// </summary>
public sealed class ResilientMessageScheduler : IMessageScheduler, IMessageBus
{
    private static readonly BusCapabilities AllBusFalse = new(false, false, false, false, false);

    private readonly IMessageScheduler _inner;
    private readonly ResiliencePipeline _pipeline;
    private int _retryCount;

    public ResilientMessageScheduler(
        IMessageScheduler inner,
        ResilienceOptions? options = null,
        ResilienceListener? onEvent = null)
    {
        _inner = inner;
        var o = options ?? ResilienceOptions.Default;

        // A transient fault is anything EXCEPT a declared "not supported" outcome.
        static bool IsTransient(Exception ex) => ex is not OperationNotSupportedException;

        _pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(IsTransient),
                MaxRetryAttempts = o.MaxRetryAttempts,
                Delay = o.BaseDelay,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                OnRetry = args =>
                {
                    var attempt = Interlocked.Increment(ref _retryCount);
                    onEvent?.Invoke(new ResilienceEvent.Retry(
                        attempt, args.Outcome.Exception?.Message ?? "transient"));
                    return default;
                },
            })
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(IsTransient),
                FailureRatio = o.FailureRatio,
                SamplingDuration = o.SamplingDuration,
                MinimumThroughput = o.MinimumThroughput,
                BreakDuration = o.BreakDuration,
                OnOpened = _ => { onEvent?.Invoke(new ResilienceEvent.BreakerOpen()); return default; },
                OnHalfOpened = _ => { onEvent?.Invoke(new ResilienceEvent.BreakerHalfOpen()); return default; },
                OnClosed = _ => { onEvent?.Invoke(new ResilienceEvent.BreakerClose()); return default; },
            })
            .Build();
    }

    public string Name => $"{_inner.Name} + Polly";

    public Capabilities Capabilities => _inner.Capabilities;

    // --- scheduler port -----------------------------------------------------

    public Task ConnectAsync(CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(async token => await _inner.ConnectAsync(token), ct).AsTask();

    public Task SendNowAsync(string destination, string payload, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(async token => await _inner.SendNowAsync(destination, payload, token), ct).AsTask();

    public Task<ScheduleHandle> ScheduleAsync(
        string destination, string payload, DateTimeOffset deliverAt, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(
            async token => await _inner.ScheduleAsync(destination, payload, deliverAt, token), ct).AsTask();

    public Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(async token => await _inner.CancelAsync(handle, token), ct).AsTask();

    public Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(
        string destination, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(
            async token => await _inner.ListScheduledAsync(destination, token), ct).AsTask();

    // The consume *setup* is protected; the long-lived message loop itself is not
    // wrapped (a circuit breaker belongs around discrete calls, not a stream).
    public Task<ISubscription> ConsumeAsync(
        string destination, MessageHandler handler, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(
            async token => await _inner.ConsumeAsync(destination, handler, token), ct).AsTask();

    // --- bus port (forwarded only when the inner adapter is a bus) ----------

    private IMessageBus InnerBus => (IMessageBus)_inner;

    public BusCapabilities BusCapabilities => _inner.Capabilities.Bus ?? AllBusFalse;

    public Task ConnectBusAsync(CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(async token => await InnerBus.ConnectBusAsync(token), ct).AsTask();

    public Task PublishAsync(
        string topic, string payload, string? routingKey = null, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(
            async token => await InnerBus.PublishAsync(topic, payload, routingKey, token), ct).AsTask();

    public Task<ISubscription> SubscribeAsync(
        string topic, AckHandler handler, SubscribeOptions? options = null, CancellationToken ct = default) =>
        _pipeline.ExecuteAsync(
            async token => await InnerBus.SubscribeAsync(topic, handler, options, token), ct).AsTask();

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}

/// <summary>Tunables for <see cref="ResilientMessageScheduler"/>.</summary>
public sealed record ResilienceOptions(
    int MaxRetryAttempts,
    TimeSpan BaseDelay,
    double FailureRatio,
    TimeSpan SamplingDuration,
    int MinimumThroughput,
    TimeSpan BreakDuration)
{
    public static ResilienceOptions Default { get; } = new(
        MaxRetryAttempts: 3,
        BaseDelay: TimeSpan.FromMilliseconds(200),
        FailureRatio: 0.5,
        SamplingDuration: TimeSpan.FromSeconds(10),
        MinimumThroughput: 4,
        BreakDuration: TimeSpan.FromSeconds(5));
}
