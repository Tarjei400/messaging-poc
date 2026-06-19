using System.Collections.Concurrent;
using System.Text;
using Messaging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Messaging.RabbitMq;

public sealed record RabbitConfig(string Host, int Port, string User, string Password, string Exchange)
{
    public static RabbitConfig FromEnv() => new(
        Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
        int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var p) ? p : 5673,
        Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
        Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest",
        Environment.GetEnvironmentVariable("RABBITMQ_EXCHANGE") ?? "mbc.delayed");
}

/// <summary>
/// RabbitMQ adapter built on the <c>rabbitmq_delayed_message_exchange</c> plugin.
///
/// Scheduling = publish to an <c>x-delayed-message</c> exchange with an
/// <c>x-delay</c> header (ms). This works, but the plugin stores pending
/// messages in a node-local Mnesia table and exposes NO API to enumerate or
/// remove an individual pending message. So <see cref="CancelAsync"/> and
/// <see cref="ListScheduledAsync"/> are honestly unsupported here — exactly the
/// limitation the comparison flags, now provable rather than asserted.
/// </summary>
public sealed class RabbitMqScheduler : IMessageScheduler, IMessageBus
{
    public string Name => "RabbitMQ (delayed-message plugin)";

    public BusCapabilities BusCapabilities { get; } = new(
        SupportsTopic: true,
        SupportsFanout: true,
        SupportsManualAck: true,
        SupportsDeadLetter: true,
        ReportsDeliveryCount: false, // classic queues expose only a redelivered flag
        SupportsDedup: false, // no native producer dedup (cf. app-level S10)
        SupportsStreamReplay: true); // stream queues replay from an offset

    public Capabilities Capabilities { get; }

    private readonly RabbitConfig _cfg;
    private readonly ConcurrentDictionary<string, bool> _topology = new();
    private readonly ConcurrentDictionary<string, bool> _busTopology = new();
    private readonly ConcurrentDictionary<string, bool> _hashTopology = new();
    private IConnection? _connection;
    private IModel? _channel;

    public RabbitMqScheduler(RabbitConfig cfg)
    {
        _cfg = cfg;
        Capabilities = new Capabilities(
            Protocol: "AMQP 0.9.1",
            NativeScheduling: false, // plugin, not core broker
            SupportsCancel: false,
            SupportsList: false,
            Bus: BusCapabilities);
    }

    public Task ConnectAsync(CancellationToken ct = default)
    {
        var factory = new ConnectionFactory
        {
            HostName = _cfg.Host,
            Port = _cfg.Port,
            UserName = _cfg.User,
            Password = _cfg.Password,
            DispatchConsumersAsync = true,
        };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(
            _cfg.Exchange,
            type: "x-delayed-message",
            durable: true,
            autoDelete: false,
            arguments: new Dictionary<string, object> { ["x-delayed-type"] = "direct" });
        return Task.CompletedTask;
    }

    private static string QueueName(string destination) => $"q.{destination}";

    private string EnsureTopology(string destination)
    {
        var queue = QueueName(destination);
        if (_topology.TryAdd(destination, true))
        {
            _channel!.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
            _channel!.QueueBind(queue, _cfg.Exchange, routingKey: destination);
        }
        return queue;
    }

    public Task SendNowAsync(string destination, string payload, CancellationToken ct = default)
    {
        EnsureTopology(destination);
        var props = _channel!.CreateBasicProperties();
        props.Persistent = true;
        _channel.BasicPublish(_cfg.Exchange, destination, props, Encoding.UTF8.GetBytes(payload));
        return Task.CompletedTask;
    }

    public Task<ScheduleHandle> ScheduleAsync(
        string destination, string payload, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        EnsureTopology(destination);
        var delayMs = (long)Math.Max(0, (deliverAt - DateTimeOffset.UtcNow).TotalMilliseconds);
        var id = $"rmq-{Guid.NewGuid():N}";

        var props = _channel!.CreateBasicProperties();
        props.Persistent = true;
        props.Headers = new Dictionary<string, object>
        {
            ["x-delay"] = (int)delayMs,
            ["scheduleId"] = id,
        };
        _channel.BasicPublish(_cfg.Exchange, destination, props, Encoding.UTF8.GetBytes(payload));

        // The handle is returned for API symmetry, but it cannot be acted upon:
        // the plugin offers no way to reach back in and cancel this message.
        return Task.FromResult(new ScheduleHandle(id, destination, deliverAt));
    }

    public Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default) =>
        throw new OperationNotSupportedException(
            "cancel", Name,
            "the delayed-message plugin has no API to remove a pending message");

    public Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(
        string destination, CancellationToken ct = default) =>
        throw new OperationNotSupportedException(
            "listScheduled", Name,
            "pending delayed messages live in a node-local Mnesia table, not a queue");

    public Task<ISubscription> ConsumeAsync(
        string destination, MessageHandler handler, CancellationToken ct = default)
    {
        var queue = EnsureTopology(destination);
        var consumer = new AsyncEventingBasicConsumer(_channel!);
        consumer.Received += async (_, ea) =>
        {
            var body = Encoding.UTF8.GetString(ea.Body.ToArray());
            var headers = new Dictionary<string, string>();
            if (ea.BasicProperties.Headers is { } h)
                foreach (var kv in h)
                    headers[kv.Key] = kv.Value is byte[] b ? Encoding.UTF8.GetString(b) : kv.Value?.ToString() ?? "";

            await handler(new ReceivedMessage(
                ea.BasicProperties.MessageId ?? string.Empty, destination, body, headers));
            _channel!.BasicAck(ea.DeliveryTag, multiple: false);
        };
        var tag = _channel!.BasicConsume(queue, autoAck: false, consumer);

        ISubscription sub = new RabbitSubscription(_channel!, tag);
        return Task.FromResult(sub);
    }

    // --- bus port -----------------------------------------------------------

    public Task ConnectBusAsync(CancellationToken ct = default)
    {
        EnsureChannel();
        return Task.CompletedTask;
    }

    private void EnsureChannel()
    {
        if (_connection is null)
        {
            var factory = new ConnectionFactory
            {
                HostName = _cfg.Host,
                Port = _cfg.Port,
                UserName = _cfg.User,
                Password = _cfg.Password,
                DispatchConsumersAsync = true,
            };
            _connection = factory.CreateConnection();
        }
        if (_channel is null)
        {
            _channel = _connection.CreateModel();
            _channel.BasicQos(0, prefetchCount: 1, global: false);
        }
    }

    private static string TopicExchange(string topic) => $"x.t.{topic}";
    private static string FanoutExchange(string topic) => $"x.f.{topic}";
    private static string HashExchange(string topic) => $"x.h.{topic}";

    /// <summary>Declare the topic + fanout exchanges for a logical topic (idempotent).</summary>
    private void EnsureBusExchanges(string topic)
    {
        if (!_busTopology.TryAdd(topic, true)) return;
        _channel!.ExchangeDeclare(TopicExchange(topic), type: "topic", durable: true, autoDelete: false);
        _channel!.ExchangeDeclare(FanoutExchange(topic), type: "fanout", durable: true, autoDelete: false);
    }

    /// <summary>
    /// S12: declare the per-topic consistent-hash exchange (idempotent). The
    /// <c>rabbitmq_consistent_hash_exchange</c> plugin hashes the routing key (we
    /// publish with routing key = groupId) and routes each message to ONE of the
    /// bound queues — so the same group always lands on the same queue, and thus
    /// the same competing consumer, preserving per-group order.
    /// </summary>
    private void EnsureHashExchange(string topic)
    {
        if (!_hashTopology.TryAdd(topic, true)) return;
        _channel!.ExchangeDeclare(HashExchange(topic), type: "x-consistent-hash", durable: true, autoDelete: false);
    }

    public Task PublishAsync(
        string topic, string payload, string? routingKey = null,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        EnsureBusExchanges(topic);
        var body = Encoding.UTF8.GetBytes(payload);
        var props = PublishProps(options);
        // Publish to both exchanges; a queue is bound to exactly one, so each
        // subscriber receives exactly one copy regardless of its topology kind.
        _channel.BasicPublish(TopicExchange(topic), routingKey ?? topic, props, body);
        _channel.BasicPublish(FanoutExchange(topic), "", props, body);
        // S12: when the message carries a groupId, also route it through the
        // consistent-hash exchange (routing key = groupId) so a partition-by-group
        // subscriber's per-consumer queues each own a stable subset of groups. The
        // hash exchange has no bound queues unless such a subscriber exists, so this
        // is a no-op for ordinary topics.
        if (options?.GroupId is { } g)
        {
            EnsureHashExchange(topic);
            _channel.BasicPublish(HashExchange(topic), g, props, body);
        }
        return Task.CompletedTask;
    }

    /// <summary>Map PublishOptions onto a RabbitMQ BasicProperties + headers.</summary>
    private IBasicProperties PublishProps(PublishOptions? options)
    {
        var props = _channel!.CreateBasicProperties();
        props.Persistent = true;
        var headers = new Dictionary<string, object>();
        if (options?.Headers is { } h)
            foreach (var kv in h) headers[kv.Key] = kv.Value;
        if (options?.GroupId is { } g) headers["x-group-id"] = g;
        if (options?.DedupId is { } d) headers["x-dedup-id"] = d;
        if (headers.Count > 0) props.Headers = headers;
        if (options?.Priority is { } p) props.Priority = (byte)p;
        if (options?.ReplyTo is { } rt) props.ReplyTo = rt;
        if (options?.CorrelationId is { } cid) props.CorrelationId = cid;
        if (options?.TtlMs is { } ttl) props.Expiration = ttl.ToString();
        return props;
    }

    private static string StreamQueueName(string topic) => $"stream.{topic}";

    /// <summary>
    /// S19: subscribe by replaying a topic's full history from the beginning.
    ///
    /// A RabbitMQ stream queue (<c>x-queue-type=stream</c>) is an append-only log:
    /// a fresh consumer with <c>x-stream-offset=first</c> re-reads every message
    /// ever published to it, even ones already consumed by other consumers. We
    /// declare one durable stream queue per topic and bind it to the topic
    /// exchange so it captures publishes — the queue must exist (and be bound)
    /// BEFORE the publish to capture it, which the scenario guarantees by
    /// establishing the stream with an initial streamReplay subscription first.
    ///
    /// Stream queues refuse a consumer without a QoS (prefetch), so we set one and
    /// use manual ack.
    /// </summary>
    private ISubscription SubscribeStreamReplay(string topic, AckHandler handler)
    {
        var queue = StreamQueueName(topic);
        var ch = _connection!.CreateModel();
        ch.BasicQos(0, prefetchCount: 10, global: false); // streams REQUIRE a QoS before consuming
        ch.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object> { ["x-queue-type"] = "stream" });
        ch.QueueBind(queue, TopicExchange(topic), "#");

        var consumer = new AsyncEventingBasicConsumer(ch);
        consumer.Received += async (_, ea) =>
        {
            await handler(new StreamIncomingMessage(ch, topic, ea));
        };
        // Each consumer reads from offset 0 (its own cursor) — full-history replay.
        ch.BasicConsume(queue, autoAck: false, consumerTag: string.Empty, noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object> { ["x-stream-offset"] = "first" },
            consumer);

        return new BusChannelSubscription(ch);
    }

    public Task<ISubscription> SubscribeAsync(
        string topic, AckHandler handler, SubscribeOptions? options = null, CancellationToken ct = default)
    {
        EnsureBusExchanges(topic);
        var opts = options ?? new SubscribeOptions();
        // S19: stream replay takes a dedicated append-only-log path.
        if (opts.StreamReplay)
            return Task.FromResult(SubscribeStreamReplay(topic, handler));
        var subscriberId = opts.SubscriberId ?? $"sub-{Guid.NewGuid():N}";
        // S12: each partition-by-group consumer needs its OWN queue bound to the
        // consistent-hash exchange (the hash routes a group to exactly one queue).
        // So even when they share a subscriberId, give each a unique queue name.
        var queue = opts.PartitionByGroup
            ? $"bus.{topic}.{subscriberId}.{Guid.NewGuid():N}"
            : $"bus.{topic}.{subscriberId}";

        // Each subscription gets its own channel. Closing it on unsubscribe requeues
        // any un-acked message — which is precisely how a crashed consumer (dropped
        // connection) surfaces as redelivery to a surviving consumer (S7c). A shared
        // channel would hold the message unacked and never redeliver it.
        var ch = _connection!.CreateModel();
        ch.BasicQos(0, prefetchCount: 1, global: false); // fair dispatch for competing consumers (S9)

        var maxDeliveries = opts.MaxDeliveries ?? MessageBus.DefaultMaxDeliveries;
        var retryEnabled = opts.DeadLetter && opts.RetryDelayMs is > 0;
        string? dlaExchange = null;

        var args = new Dictionary<string, object>();
        if (retryEnabled)
        {
            // Non-blocking retry: the main queue dead-letters a nacked message into a
            // dedicated retry (parking) queue, so the head of the main queue is free
            // immediately. The retry queue holds it for RetryDelayMs then dead-letters
            // it BACK to the main queue (default exchange routes by queue name), which
            // redelivers it. The adapter counts cycles via the x-death header and
            // routes to the real DLQ once MaxDeliveries is reached (see NackAsync).
            var dla = MessageBus.DeadLetterAddress(topic);
            EnsureBusExchanges(dla); // the DLQ destination is itself a topic
            dlaExchange = FanoutExchange(dla);
            var retryQueue = $"{queue}.retry";
            args["x-dead-letter-exchange"] = ""; // default exchange → route by queue name
            args["x-dead-letter-routing-key"] = retryQueue;
            ch.QueueDeclare(retryQueue, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    ["x-message-ttl"] = opts.RetryDelayMs!.Value,
                    ["x-dead-letter-exchange"] = "",
                    ["x-dead-letter-routing-key"] = queue, // bounce back to the main queue
                });
        }
        else if (opts.DeadLetter)
        {
            // Quorum queue gives a deterministic delivery-limit → dead-letter path.
            var dla = MessageBus.DeadLetterAddress(topic);
            EnsureBusExchanges(dla); // the DLQ destination is itself a topic
            args["x-queue-type"] = "quorum";
            args["x-delivery-limit"] = maxDeliveries;
            args["x-dead-letter-exchange"] = FanoutExchange(dla);
        }
        // S14: a priority-capable queue honours per-message `priority` (0..9).
        if (opts.PriorityQueue)
            args["x-max-priority"] = 10;
        // S18: a single-active-consumer queue dispatches to one consumer at a time
        // and promotes a standby when the active one drops (order preserved).
        if (opts.SingleActiveConsumer)
            args["x-single-active-consumer"] = true;
        // S16: an expiry-capable queue dead-letters an expired (TTL-elapsed) message
        // to the expiry fanout exchange the `{topic}.expiry` subscriber binds to.
        // (Per-message `expiration` on publish supplies the TTL; the DLX routes the
        // expired message — this mirrors the `.dlq` path but to a distinct address.)
        if (opts.TtlMs is not null)
        {
            var expiry = MessageBus.ExpiryAddress(topic);
            EnsureBusExchanges(expiry); // the expiry destination is itself a topic
            args["x-dead-letter-exchange"] = FanoutExchange(expiry);
        }
        ch.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false, arguments: args);

        if (opts.PartitionByGroup)
        {
            // S12: bind this consumer's queue to the consistent-hash exchange with an
            // equal weight ("1"). The plugin spreads groups across the bound queues
            // by hashing the routing key (groupId), so the same group always reaches
            // the same queue → the same consumer → per-group order.
            EnsureHashExchange(topic);
            ch.QueueBind(queue, HashExchange(topic), "1");
        }
        else if (opts.Kind == TopologyKind.Fanout)
            ch.QueueBind(queue, FanoutExchange(topic), "");
        else
            ch.QueueBind(queue, TopicExchange(topic), opts.RoutingKey ?? "#");

        var retry = retryEnabled ? new RetryContext(queue, maxDeliveries, dlaExchange!) : null;
        var consumer = new AsyncEventingBasicConsumer(ch);
        consumer.Received += async (_, ea) =>
        {
            await handler(new RabbitIncomingMessage(ch, topic, ea, retry));
        };
        ch.BasicConsume(queue, autoAck: false, consumer);

        ISubscription sub = new BusChannelSubscription(ch);
        return Task.FromResult(sub);
    }

    public ValueTask DisposeAsync()
    {
        _channel?.Close();
        _channel?.Dispose();
        _connection?.Close();
        _connection?.Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed class RabbitSubscription(IModel channel, string consumerTag) : ISubscription
    {
        public ValueTask DisposeAsync()
        {
            try { channel.BasicCancel(consumerTag); }
            catch { /* channel may already be closed */ }
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Closing the channel requeues any un-acked message → crash→redelivery.</summary>
    private sealed class BusChannelSubscription(IModel channel) : ISubscription
    {
        public ValueTask DisposeAsync()
        {
            try { channel.Close(); channel.Dispose(); }
            catch { /* already closed */ }
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Wiring for the non-blocking retry-queue path (null when disabled).</summary>
    private sealed record RetryContext(string MainQueue, int MaxDeliveries, string DlaExchange);

    /// <summary>
    /// S19: an <see cref="IIncomingMessage"/> over a single stream-queue delivery.
    /// Streams track a per-consumer offset cursor; ack just advances it (no
    /// requeue/redelivery semantics).
    /// </summary>
    private sealed class StreamIncomingMessage : IIncomingMessage
    {
        private readonly IModel _channel;
        private readonly ulong _deliveryTag;
        private int _settled;

        public StreamIncomingMessage(IModel channel, string topic, BasicDeliverEventArgs ea)
        {
            _channel = channel;
            _deliveryTag = ea.DeliveryTag;
            Destination = topic;
            Body = Encoding.UTF8.GetString(ea.Body.ToArray());
            Id = ea.BasicProperties.MessageId ?? string.Empty;
            var headers = new Dictionary<string, string>();
            if (ea.BasicProperties.Headers is { } h)
                foreach (var kv in h)
                    headers[kv.Key] = kv.Value is byte[] b
                        ? Encoding.UTF8.GetString(b)
                        : kv.Value?.ToString() ?? string.Empty;
            Headers = headers;
        }

        public string Id { get; }
        public string Destination { get; }
        public string Body { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
        public int? DeliveryCount => null;

        public Task AckAsync()
        {
            if (Interlocked.Exchange(ref _settled, 1) == 0)
                _channel.BasicAck(_deliveryTag, multiple: false);
            return Task.CompletedTask;
        }

        // Streams don't redeliver; a nack just advances the offset like an ack.
        public Task NackAsync(bool requeue)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 0)
                _channel.BasicAck(_deliveryTag, multiple: false);
            return Task.CompletedTask;
        }
    }

    /// <summary>An <see cref="IIncomingMessage"/> over a single RabbitMQ delivery.</summary>
    private sealed class RabbitIncomingMessage : IIncomingMessage
    {
        private readonly IModel _channel;
        private readonly ulong _deliveryTag;
        private readonly byte[] _bodyBytes;
        private readonly RetryContext? _retry;
        private int _settled;

        public RabbitIncomingMessage(IModel channel, string topic, BasicDeliverEventArgs ea, RetryContext? retry)
        {
            _channel = channel;
            _deliveryTag = ea.DeliveryTag;
            _bodyBytes = ea.Body.ToArray();
            _retry = retry;
            Destination = topic;
            Body = Encoding.UTF8.GetString(_bodyBytes);
            Id = ea.BasicProperties.MessageId ?? string.Empty;

            var headers = new Dictionary<string, string>();
            if (ea.BasicProperties.Headers is { } h)
                foreach (var kv in h)
                    headers[kv.Key] = kv.Value is byte[] b
                        ? Encoding.UTF8.GetString(b)
                        : kv.Value?.ToString() ?? string.Empty;
            Headers = headers;

            if (retry is not null)
                // Retry path: count main-queue rejections accumulated in x-death as the
                // message bounces main → retry → main.
                DeliveryCount = XDeathCount(ea.BasicProperties.Headers, retry.MainQueue) + 1;
            // Quorum queues expose x-delivery-count; classic queues do not, so the
            // count is best-effort and absent for the classic / first-delivery path.
            else if (headers.TryGetValue("x-delivery-count", out var dc) && long.TryParse(dc, out var n))
                DeliveryCount = (int)n + 1;
            else
                DeliveryCount = ea.Redelivered ? null : 1;

            ReplyTo = ea.BasicProperties.IsReplyToPresent() ? ea.BasicProperties.ReplyTo : null;
            CorrelationId = ea.BasicProperties.IsCorrelationIdPresent() ? ea.BasicProperties.CorrelationId : null;
            GroupId = headers.TryGetValue("x-group-id", out var g) ? g : null;
            Priority = ea.BasicProperties.IsPriorityPresent() ? ea.BasicProperties.Priority : null;
        }

        public string Id { get; }
        public string Destination { get; }
        public string Body { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
        public int? DeliveryCount { get; }
        public string? ReplyTo { get; }
        public string? CorrelationId { get; }
        public string? GroupId { get; }
        public int? Priority { get; }

        public Task AckAsync()
        {
            if (Interlocked.Exchange(ref _settled, 1) == 0)
                _channel.BasicAck(_deliveryTag, multiple: false);
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0) return Task.CompletedTask;
            if (_retry is not null)
            {
                // requeue=false (give up now) or the retry budget is spent → publish to
                // the DLQ fanout and ack the original. Otherwise nack(requeue=false)
                // dead-letters it into the parking/retry queue for a delayed retry.
                if (!requeue || (DeliveryCount ?? 1) >= _retry.MaxDeliveries)
                {
                    var props = _channel.CreateBasicProperties();
                    props.Persistent = true;
                    _channel.BasicPublish(_retry.DlaExchange, "", props, _bodyBytes);
                    _channel.BasicAck(_deliveryTag, multiple: false);
                }
                else
                {
                    _channel.BasicNack(_deliveryTag, multiple: false, requeue: false);
                }
                return Task.CompletedTask;
            }
            // requeue=false with a DLX configured → the message is dead-lettered.
            _channel.BasicNack(_deliveryTag, multiple: false, requeue: requeue);
            return Task.CompletedTask;
        }

        /// <summary>
        /// How many times the message was rejected from the main queue, read from the
        /// <c>x-death</c> header RabbitMQ accumulates as it bounces main → retry → main.
        /// Zero on the first delivery (no x-death yet).
        /// </summary>
        private static int XDeathCount(IDictionary<string, object>? headers, string mainQueue)
        {
            if (headers is null || !headers.TryGetValue("x-death", out var raw) ||
                raw is not System.Collections.IEnumerable list)
                return 0;
            foreach (var item in list)
            {
                if (item is not IDictionary<string, object> entry) continue;
                var q = entry.TryGetValue("queue", out var qv)
                    ? (qv is byte[] b ? Encoding.UTF8.GetString(b) : qv?.ToString())
                    : null;
                if (q != mainQueue) continue;
                if (entry.TryGetValue("count", out var cv))
                    return cv switch { long l => (int)l, int i => i, _ => 0 };
            }
            return 0;
        }
    }
}
