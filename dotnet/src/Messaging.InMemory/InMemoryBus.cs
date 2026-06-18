using Messaging.Abstractions;

namespace Messaging.InMemory;

/// <summary>
/// The in-process pub/sub + fanout + explicit-ack engine. It is the executable
/// specification the real Artemis/RabbitMQ bus adapters are diffed against:
/// topic routing, independent fanout queues, manual ack/nack/requeue, delivery
/// counting, and dead-lettering after N attempts — all with zero infrastructure.
/// </summary>
public sealed class InMemoryBus
{
    private readonly Dictionary<string, BusQueue> _queues = new();
    private readonly Dictionary<string, HashSet<BusQueue>> _byTopic = new();
    private readonly object _gate = new();
    private int _seq;

    private static string Key(string topic, string subscriberId) => $"{topic} {subscriberId}";

    public void Publish(string topic, string body, string? routingKey = null)
    {
        var key = routingKey ?? topic;
        List<BusQueue> targets;
        lock (_gate)
        {
            if (!_byTopic.TryGetValue(topic, out var set)) return;
            targets = set.Where(q => q.Matches(key)).ToList();
            foreach (var queue in targets)
            {
                queue.Pending.Enqueue(new BusMessage(
                    $"mem-bus-{++_seq}", topic, body, key, deliveryCount: 0));
            }
        }
        foreach (var queue in targets) Pump(queue);
    }

    public ISubscription Subscribe(string topic, AckHandler handler, SubscribeOptions? options = null)
    {
        var opts = options ?? new SubscribeOptions();
        BusQueue queue;
        lock (_gate)
        {
            var subscriberId = opts.SubscriberId ?? $"sub-{++_seq}";
            var k = Key(topic, subscriberId);
            if (!_queues.TryGetValue(k, out queue!))
            {
                queue = new BusQueue(
                    topic,
                    opts.Kind,
                    opts.RoutingKey,
                    opts.MaxDeliveries ?? MessageBus.DefaultMaxDeliveries,
                    opts.DeadLetter);
                _queues[k] = queue;
                if (!_byTopic.TryGetValue(topic, out var set))
                    _byTopic[topic] = set = new HashSet<BusQueue>();
                set.Add(queue);
            }
            queue.Consumers.Add(handler);
        }
        Pump(queue);

        return new BusSubscription(() =>
        {
            lock (_gate)
            {
                queue.Consumers.Remove(handler);
                // Return this consumer's un-settled messages to the queue so a
                // surviving consumer can pick them up — this is the "consumer
                // crash → redelivery" behaviour real brokers exhibit when a
                // link/channel drops.
                foreach (var pair in queue.Inflight.Where(p => p.Value == handler).ToList())
                {
                    queue.Inflight.Remove(pair.Key);
                    queue.RequeueFront(pair.Key);
                }
            }
            Pump(queue);
            return ValueTask.CompletedTask;
        });
    }

    /// <summary>Deliver pending messages to available consumers (round-robin).</summary>
    private void Pump(BusQueue queue)
    {
        while (true)
        {
            BusMessage msg;
            AckHandler consumer;
            lock (_gate)
            {
                if (queue.Pending.Count == 0 || queue.Consumers.Count == 0) return;
                msg = queue.Pending.Dequeue();
                consumer = queue.NextConsumer()!;
                msg.DeliveryCount += 1;
                queue.Inflight[msg] = consumer;
            }

            var incoming = Wrap(queue, msg, consumer);
            // Deliver asynchronously, mirroring real broker push semantics.
            _ = Task.Run(async () =>
            {
                try
                {
                    await consumer(incoming);
                    // A handler that returns without settling leaves the message
                    // in-flight (it will be redelivered if the consumer later drops).
                }
                catch
                {
                    // A throwing handler is a crashed consumer: redeliver.
                    if (!incoming.Settled) await incoming.NackAsync(true);
                }
            });
        }
    }

    private IncomingBusMessage Wrap(BusQueue queue, BusMessage msg, AckHandler consumer)
    {
        void Settle(SettleKind resolved)
        {
            bool deadLetter = false;
            bool requeue = false;
            lock (_gate)
            {
                if (!queue.Inflight.TryGetValue(msg, out var owner) || owner != consumer)
                    return; // already settled
                queue.Inflight.Remove(msg);
                switch (resolved)
                {
                    case SettleKind.Ack:
                        return;
                    case SettleKind.Dead:
                        deadLetter = true;
                        break;
                    case SettleKind.Requeue:
                        if (msg.DeliveryCount >= queue.MaxDeliveries) deadLetter = true;
                        else { queue.Pending.Enqueue(msg); requeue = true; }
                        break;
                }
            }
            if (deadLetter) DeadLetter(queue, msg);
            else if (requeue) Pump(queue);
        }

        return new IncomingBusMessage(msg, Settle);
    }

    private void DeadLetter(BusQueue queue, BusMessage msg)
    {
        if (!queue.DeadLetter) return; // dropped
        Publish(MessageBus.DeadLetterAddress(queue.Topic), msg.Body, msg.RoutingKey);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _queues.Clear();
            _byTopic.Clear();
        }
    }

    /// <summary>
    /// RabbitMQ-style topic matching: <c>*</c> matches exactly one dot-delimited
    /// word, <c>#</c> matches zero or more words.
    /// </summary>
    public static bool TopicMatch(string pattern, string key)
    {
        var p = pattern.Split('.');
        var k = key.Split('.');
        return Match(p, 0, k, 0);
    }

    private static bool Match(string[] p, int pi, string[] k, int ki)
    {
        if (pi == p.Length) return ki == k.Length;
        if (p[pi] == "#")
        {
            for (var n = ki; n <= k.Length; n++)
                if (Match(p, pi + 1, k, n)) return true;
            return false;
        }
        if (ki == k.Length) return false;
        if (p[pi] == "*" || p[pi] == k[ki]) return Match(p, pi + 1, k, ki + 1);
        return false;
    }

    private enum SettleKind { Ack, Requeue, Dead }

    private sealed class BusMessage(string id, string topic, string body, string routingKey, int deliveryCount)
    {
        public string Id { get; } = id;
        public string Topic { get; } = topic;
        public string Body { get; } = body;
        public string RoutingKey { get; } = routingKey;
        public int DeliveryCount { get; set; } = deliveryCount;
    }

    /// <summary>
    /// One logical subscriber queue. Distinct ids are independent queues (pub/sub +
    /// fanout); multiple consumers on the SAME queue compete for its messages
    /// (competing consumers / work sharing).
    /// </summary>
    private sealed class BusQueue(
        string topic, TopologyKind kind, string? routingKey, int maxDeliveries, bool deadLetter)
    {
        public string Topic { get; } = topic;
        public int MaxDeliveries { get; } = maxDeliveries;
        public bool DeadLetter { get; } = deadLetter;

        public Queue<BusMessage> Pending { get; } = new();
        public Dictionary<BusMessage, AckHandler> Inflight { get; } = new();
        public List<AckHandler> Consumers { get; } = new();
        private int _rr;

        public bool Matches(string messageKey)
        {
            if (kind == TopologyKind.Fanout) return true;
            if (string.IsNullOrEmpty(routingKey)) return true; // unfiltered topic subscriber
            return TopicMatch(routingKey, messageKey);
        }

        public AckHandler? NextConsumer()
        {
            if (Consumers.Count == 0) return null;
            var c = Consumers[_rr % Consumers.Count];
            _rr += 1;
            return c;
        }

        /// <summary>Push a crashed consumer's message back to the front of the queue.</summary>
        public void RequeueFront(BusMessage msg)
        {
            var rest = Pending.ToArray();
            Pending.Clear();
            Pending.Enqueue(msg);
            foreach (var m in rest) Pending.Enqueue(m);
        }
    }

    private sealed class IncomingBusMessage : IIncomingMessage
    {
        private readonly Action<SettleKind> _settle;
        public bool Settled { get; private set; }

        public IncomingBusMessage(BusMessage msg, Action<SettleKind> settle)
        {
            Id = msg.Id;
            Destination = msg.Topic;
            Body = msg.Body;
            DeliveryCount = msg.DeliveryCount;
            _settle = settle;
        }

        public string Id { get; }
        public string Destination { get; }
        public string Body { get; }
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public int? DeliveryCount { get; }

        public Task AckAsync()
        {
            Settled = true;
            _settle(SettleKind.Ack);
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue)
        {
            Settled = true;
            _settle(requeue ? SettleKind.Requeue : SettleKind.Dead);
            return Task.CompletedTask;
        }
    }
}

/// <summary>An <see cref="ISubscription"/> backed by a dispose callback.</summary>
internal sealed class BusSubscription(Func<ValueTask> onDispose) : ISubscription
{
    public ValueTask DisposeAsync() => onDispose();
}
