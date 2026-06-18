using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Messaging.Abstractions;

namespace Messaging.Artemis;

public sealed record ArtemisConfig(
    string Host,
    int Port,
    string Username,
    string Password,
    string JolokiaUrl)
{
    public static ArtemisConfig FromEnv()
    {
        var host = Environment.GetEnvironmentVariable("ARTEMIS_HOST") ?? "localhost";
        return new ArtemisConfig(
            host,
            int.TryParse(Environment.GetEnvironmentVariable("ARTEMIS_PORT"), out var p) ? p : 5672,
            Environment.GetEnvironmentVariable("ARTEMIS_USER") ?? "admin",
            Environment.GetEnvironmentVariable("ARTEMIS_PASSWORD") ?? "admin",
            Environment.GetEnvironmentVariable("ARTEMIS_JOLOKIA_URL")
                ?? $"http://{host}:8161/console/jolokia");
    }
}

/// <summary>
/// Apache ActiveMQ Artemis adapter.
///
///  - SendNow / Schedule / Consume go over AMQP 1.0 (AMQPNetLite).
///  - Scheduling uses the AMQP message annotation <c>x-opt-delivery-time</c>
///    (absolute epoch-ms), which Artemis honours natively — the same annotation
///    family Azure Service Bus uses, which is what makes this a low-friction
///    migration target.
///  - Cancel / ListScheduled go through broker management (Jolokia) because the
///    AMQP protocol itself has no cancel verb. Each scheduled message is tagged
///    with an application property <c>scheduleId</c> so we can cancel precisely.
///
/// Destinations are addressed via the fully-qualified queue name <c>dest::dest</c>
/// to force deterministic ANYCAST routing onto a queue whose name we know.
/// </summary>
public sealed class ArtemisScheduler : IMessageScheduler, IMessageBus
{
    private const string ScheduleIdKey = "scheduleId";
    private const string RoutingKeyProp = "routingKey";
    private static readonly Symbol DeliveryTimeAnnotation = new("x-opt-delivery-time");

    // Capability symbols telling Artemis how to auto-create the address:
    //  - 'queue' → ANYCAST (point-to-point); 'topic' → MULTICAST (fanout/pub-sub).
    private static readonly Symbol[] AnycastCaps = { new("queue") };
    private static readonly Symbol[] MulticastCaps = { new("topic") };

    // The JMS selector filter descriptor + symbol Artemis understands.
    private static readonly Symbol JmsSelectorKey = new("jms-selector");
    private static readonly Symbol SelectorFilterDescriptor = new("apache.org:selector-filter:string");

    /// <summary>
    /// The broker-configured multicast dead-letter address for <c>mbc.#</c> (see
    /// infra/artemis broker.xml). Our logical <c>{topic}.dlq</c> subscriptions
    /// read from here.
    /// </summary>
    private static readonly string ArtemisDla =
        Environment.GetEnvironmentVariable("ARTEMIS_DLA") ?? "mbc.DLQ";

    public string Name => "Apache ActiveMQ Artemis";

    public BusCapabilities BusCapabilities { get; } = new(
        SupportsTopic: true,
        SupportsFanout: true,
        SupportsManualAck: true,
        SupportsDeadLetter: true,
        ReportsDeliveryCount: true); // AMQP header delivery-count is precise

    public Capabilities Capabilities { get; }

    private readonly ArtemisConfig _cfg;
    private readonly JolokiaClient _jolokia;
    private readonly ConcurrentDictionary<string, SenderLink> _senders = new();
    private readonly ConcurrentDictionary<string, SenderLink> _busSenders = new();
    private Connection? _connection;
    private Session? _session;

    public ArtemisScheduler(ArtemisConfig cfg)
    {
        _cfg = cfg;
        _jolokia = new JolokiaClient(cfg.JolokiaUrl, cfg.Username, cfg.Password);
        Capabilities = new Capabilities(
            Protocol: "AMQP 1.0",
            NativeScheduling: true,
            SupportsCancel: true,
            SupportsList: true,
            Bus: BusCapabilities);
    }

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        var address = new Address(_cfg.Host, _cfg.Port, _cfg.Username, _cfg.Password, scheme: "AMQP");
        _connection = await Connection.Factory.CreateAsync(address);
        _session = new Session(_connection);
    }

    private static string Fqqn(string destination) => $"{destination}::{destination}";

    private SenderLink GetSender(string destination)
    {
        var address = Fqqn(destination);
        return _senders.GetOrAdd(address, addr =>
        {
            // The `queue` capability forces ANYCAST auto-create. Artemis defaults
            // auto-created addresses to MULTICAST, which would make the scheduling
            // queue a topic and break the Jolokia (anycast) cancel/list lookups.
            var target = new Target { Address = addr, Capabilities = AnycastCaps };
            return new SenderLink(_session!, $"snd-{Guid.NewGuid():N}", target, null);
        });
    }

    public Task SendNowAsync(string destination, string payload, CancellationToken ct = default)
    {
        var sender = GetSender(destination);
        return sender.SendAsync(new Message(payload));
    }

    public async Task<ScheduleHandle> ScheduleAsync(
        string destination, string payload, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        var id = $"art-{Guid.NewGuid():N}";
        var sender = GetSender(destination);
        var message = new Message(payload)
        {
            MessageAnnotations = new MessageAnnotations(),
            ApplicationProperties = new ApplicationProperties(),
        };
        message.MessageAnnotations[DeliveryTimeAnnotation] = deliverAt.ToUnixTimeMilliseconds();
        message.ApplicationProperties[ScheduleIdKey] = id;
        await sender.SendAsync(message);
        return new ScheduleHandle(id, destination, deliverAt);
    }

    public async Task CancelAsync(ScheduleHandle handle, CancellationToken ct = default)
    {
        var filter = $"{ScheduleIdKey} = '{handle.Id}'";
        await _jolokia.RemoveMessagesAsync(handle.Destination, filter, ct);
    }

    public async Task<IReadOnlyList<ScheduledInfo>> ListScheduledAsync(
        string destination, CancellationToken ct = default)
    {
        var raw = await _jolokia.ListScheduledMessagesAsync(destination, ct);
        var result = new List<ScheduledInfo>();
        foreach (var m in raw)
        {
            var id = m.TryGetProperty(ScheduleIdKey, out var sid) ? sid.ToString()
                : m.TryGetProperty("messageID", out var mid) ? mid.ToString()
                : string.Empty;
            DateTimeOffset? at = m.TryGetProperty("scheduledDeliveryTime", out var t)
                && t.TryGetInt64(out var ms)
                ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
                : null;
            result.Add(new ScheduledInfo(id, destination, at));
        }
        return result;
    }

    public Task<ISubscription> ConsumeAsync(
        string destination, MessageHandler handler, CancellationToken ct = default)
    {
        var source = new Source { Address = Fqqn(destination), Capabilities = AnycastCaps };
        var receiver = new ReceiverLink(_session!, $"rcv-{Guid.NewGuid():N}", source, null);
        receiver.Start(20, (link, message) =>
        {
            var body = message.Body?.ToString() ?? string.Empty;
            var headers = new Dictionary<string, string>();
            if (message.ApplicationProperties?.Map is { } map)
                foreach (var key in map.Keys)
                    headers[key.ToString()!] = map[key]?.ToString() ?? string.Empty;

            _ = handler(new ReceivedMessage(
                message.Properties?.MessageId ?? string.Empty, destination, body, headers));
            link.Accept(message);
        });

        ISubscription sub = new ArtemisSubscription(receiver);
        return Task.FromResult(sub);
    }

    // --- bus port -----------------------------------------------------------

    public Task ConnectBusAsync(CancellationToken ct = default) => ConnectAsync(ct);

    private SenderLink GetBusSender(string address)
    {
        return _busSenders.GetOrAdd(address, addr =>
        {
            // The `topic` capability makes Artemis treat the address as MULTICAST
            // on auto-create, which is what gives independent subscribers their
            // own copy.
            var target = new Target { Address = addr, Capabilities = MulticastCaps };
            return new SenderLink(_session!, $"bsnd-{Guid.NewGuid():N}", target, null);
        });
    }

    public Task PublishAsync(
        string topic, string payload, string? routingKey = null, CancellationToken ct = default)
    {
        var sender = GetBusSender(topic);
        var message = new Message(payload) { ApplicationProperties = new ApplicationProperties() };
        message.ApplicationProperties[RoutingKeyProp] = routingKey ?? topic;
        return sender.SendAsync(message);
    }

    public Task<ISubscription> SubscribeAsync(
        string topic, AckHandler handler, SubscribeOptions? options = null, CancellationToken ct = default)
    {
        var opts = options ?? new SubscribeOptions();
        var subscriberId = opts.SubscriberId ?? $"sub-{Guid.NewGuid():N}";

        // A `{topic}.dlq` subscription reads from the broker's multicast DLA, where
        // Artemis routes messages after `max-delivery-attempts` (set in broker.xml).
        var address = topic.EndsWith(".dlq", StringComparison.Ordinal) ? ArtemisDla : topic;
        var fqqn = $"{address}::{subscriberId}";

        var source = new Source { Address = fqqn, Capabilities = MulticastCaps };
        // Topic routing-key filtering uses an AMQP/JMS selector on a message
        // property — Artemis's mechanism differs from RabbitMQ's exchange bindings,
        // same observable outcome.
        if (opts.Kind == TopologyKind.Topic && !string.IsNullOrEmpty(opts.RoutingKey))
        {
            source.FilterSet = new Map
            {
                [JmsSelectorKey] = new DescribedValue(SelectorFilterDescriptor, ToSelector(opts.RoutingKey!)),
            };
        }

        var receiver = new ReceiverLink(_session!, $"brcv-{Guid.NewGuid():N}", source, null);
        // The consumer settles explicitly (ack/nack), so the Start callback never
        // auto-accepts — it hands the message to the handler with manual control.
        receiver.Start(20, (link, message) =>
        {
            _ = handler(new ArtemisIncomingMessage(receiver, message, address));
        });

        ISubscription sub = new ArtemisSubscription(receiver);
        return Task.FromResult(sub);
    }

    /// <summary>
    /// Translate a RabbitMQ-style topic pattern into an Artemis JMS selector on the
    /// routingKey property. <c>*</c>/<c>#</c> become SQL <c>LIKE</c> wildcards; an
    /// exact key uses equality.
    /// </summary>
    private static string ToSelector(string routingKey)
    {
        if (routingKey.Contains('*') || routingKey.Contains('#'))
        {
            var like = routingKey.Replace('*', '%').Replace('#', '%');
            return $"{RoutingKeyProp} LIKE '{like}'";
        }
        return $"{RoutingKeyProp} = '{routingKey}'";
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var s in _senders.Values) await SafeCloseAsync(s);
        foreach (var s in _busSenders.Values) await SafeCloseAsync(s);
        if (_session is not null) await SafeCloseAsync(_session);
        if (_connection is not null) await SafeCloseAsync(_connection);
        _jolokia.Dispose();
    }

    /// <summary>Close an AMQP object, tolerating a link/session already mid-detach.</summary>
    private static async Task SafeCloseAsync(AmqpObject obj)
    {
        try { await obj.CloseAsync().ConfigureAwait(false); }
        catch (AmqpException) { /* already closing/closed (e.g. consumer crash) */ }
        catch (ObjectDisposedException) { /* already disposed */ }
    }

    private sealed class ArtemisSubscription(ReceiverLink receiver) : ISubscription
    {
        public async ValueTask DisposeAsync() => await SafeCloseAsync(receiver).ConfigureAwait(false);
    }

    /// <summary>An <see cref="IIncomingMessage"/> over a single AMQP delivery.</summary>
    private sealed class ArtemisIncomingMessage : IIncomingMessage
    {
        private readonly ReceiverLink _link;
        private readonly Message _message;
        private int _settled;

        public ArtemisIncomingMessage(ReceiverLink link, Message message, string destination)
        {
            _link = link;
            _message = message;
            Destination = destination;
            Body = message.Body?.ToString() ?? string.Empty;
            Id = message.Properties?.MessageId ?? string.Empty;
            // AMQP `delivery-count` is the number of prior (failed) deliveries.
            DeliveryCount = (int)(message.Header?.DeliveryCount ?? 0) + 1;

            var headers = new Dictionary<string, string>();
            if (message.ApplicationProperties?.Map is { } map)
                foreach (var key in map.Keys)
                    headers[key.ToString()!] = map[key]?.ToString() ?? string.Empty;
            Headers = headers;
        }

        public string Id { get; }
        public string Destination { get; }
        public string Body { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
        public int? DeliveryCount { get; }

        public Task AckAsync()
        {
            if (Interlocked.Exchange(ref _settled, 1) == 0) _link.Accept(_message);
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0) return Task.CompletedTask;
            if (requeue)
                // Counts as a failed delivery; after max-delivery-attempts Artemis
                // routes the message to the dead-letter address.
                _link.Modify(_message, deliveryFailed: true, undeliverableHere: false, messageAnnotations: null);
            else
                _link.Reject(_message, error: null); // straight to the dead-letter address
            return Task.CompletedTask;
        }
    }
}
