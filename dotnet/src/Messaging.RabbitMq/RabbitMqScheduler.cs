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
        ReportsDeliveryCount: false); // classic queues expose only a redelivered flag

    public Capabilities Capabilities { get; }

    private readonly RabbitConfig _cfg;
    private readonly ConcurrentDictionary<string, bool> _topology = new();
    private readonly ConcurrentDictionary<string, bool> _busTopology = new();
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

    /// <summary>Declare the topic + fanout exchanges for a logical topic (idempotent).</summary>
    private void EnsureBusExchanges(string topic)
    {
        if (!_busTopology.TryAdd(topic, true)) return;
        _channel!.ExchangeDeclare(TopicExchange(topic), type: "topic", durable: true, autoDelete: false);
        _channel!.ExchangeDeclare(FanoutExchange(topic), type: "fanout", durable: true, autoDelete: false);
    }

    public Task PublishAsync(
        string topic, string payload, string? routingKey = null, CancellationToken ct = default)
    {
        EnsureBusExchanges(topic);
        var body = Encoding.UTF8.GetBytes(payload);
        var props = _channel!.CreateBasicProperties();
        props.Persistent = true;
        // Publish to both exchanges; a queue is bound to exactly one, so each
        // subscriber receives exactly one copy regardless of its topology kind.
        _channel.BasicPublish(TopicExchange(topic), routingKey ?? topic, props, body);
        _channel.BasicPublish(FanoutExchange(topic), "", props, body);
        return Task.CompletedTask;
    }

    public Task<ISubscription> SubscribeAsync(
        string topic, AckHandler handler, SubscribeOptions? options = null, CancellationToken ct = default)
    {
        EnsureBusExchanges(topic);
        var opts = options ?? new SubscribeOptions();
        var subscriberId = opts.SubscriberId ?? $"sub-{Guid.NewGuid():N}";
        var queue = $"bus.{topic}.{subscriberId}";

        // Each subscription gets its own channel. Closing it on unsubscribe requeues
        // any un-acked message — which is precisely how a crashed consumer (dropped
        // connection) surfaces as redelivery to a surviving consumer (S7c). A shared
        // channel would hold the message unacked and never redeliver it.
        var ch = _connection!.CreateModel();
        ch.BasicQos(0, prefetchCount: 1, global: false); // fair dispatch for competing consumers (S9)

        var args = new Dictionary<string, object>();
        if (opts.DeadLetter)
        {
            // Quorum queue gives a deterministic delivery-limit → dead-letter path.
            var dla = MessageBus.DeadLetterAddress(topic);
            EnsureBusExchanges(dla); // the DLQ destination is itself a topic
            args["x-queue-type"] = "quorum";
            args["x-delivery-limit"] = opts.MaxDeliveries ?? MessageBus.DefaultMaxDeliveries;
            args["x-dead-letter-exchange"] = FanoutExchange(dla);
        }
        ch.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false, arguments: args);

        if (opts.Kind == TopologyKind.Fanout)
            ch.QueueBind(queue, FanoutExchange(topic), "");
        else
            ch.QueueBind(queue, TopicExchange(topic), opts.RoutingKey ?? "#");

        var consumer = new AsyncEventingBasicConsumer(ch);
        consumer.Received += async (_, ea) =>
        {
            await handler(new RabbitIncomingMessage(ch, topic, ea));
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

    /// <summary>An <see cref="IIncomingMessage"/> over a single RabbitMQ delivery.</summary>
    private sealed class RabbitIncomingMessage : IIncomingMessage
    {
        private readonly IModel _channel;
        private readonly ulong _deliveryTag;
        private int _settled;

        public RabbitIncomingMessage(IModel channel, string topic, BasicDeliverEventArgs ea)
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

            // Quorum queues expose x-delivery-count; classic queues do not, so the
            // count is best-effort and absent for the classic / first-delivery path.
            if (headers.TryGetValue("x-delivery-count", out var dc) && long.TryParse(dc, out var n))
                DeliveryCount = (int)n + 1;
            else
                DeliveryCount = ea.Redelivered ? null : 1;
        }

        public string Id { get; }
        public string Destination { get; }
        public string Body { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
        public int? DeliveryCount { get; }

        public Task AckAsync()
        {
            if (Interlocked.Exchange(ref _settled, 1) == 0)
                _channel.BasicAck(_deliveryTag, multiple: false);
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue)
        {
            // requeue=false with a DLX configured → the message is dead-lettered.
            if (Interlocked.Exchange(ref _settled, 1) == 0)
                _channel.BasicNack(_deliveryTag, multiple: false, requeue: requeue);
            return Task.CompletedTask;
        }
    }
}
