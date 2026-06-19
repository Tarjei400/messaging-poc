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
    // Broker-native producer dedup (S13): a dedupId seen before is dropped before
    // it is ever enqueued — the in-memory analogue of Artemis duplicate detection.
    private readonly HashSet<string> _seenDedupIds = new();
    // S19: an append-only log of every message published to a topic, kept
    // independently of any subscriber. A `StreamReplay` subscription re-reads this
    // whole log from offset 0 — so a subscriber that attaches AFTER the publishes
    // (and after the messages were already consumed by others) still sees them
    // all. The in-memory analogue of a RabbitMQ stream queue's durable log.
    private readonly Dictionary<string, List<BusMessage>> _streamLog = new();
    private readonly object _gate = new();
    private int _seq;

    private static string Key(string topic, string subscriberId) => $"{topic} {subscriberId}";

    public void Publish(string topic, string body, string? routingKey = null, PublishOptions? options = null)
    {
        var key = routingKey ?? topic;
        List<(BusQueue Queue, BusMessage Message)> delivered;
        lock (_gate)
        {
            // S13: drop a repeat of a dedupId already seen, before any enqueue.
            if (options?.DedupId is { } dup && !_seenDedupIds.Add(dup)) return;
            // S19: record every publish in the topic's append-only log, even when
            // there is no subscriber yet — that is what lets a later StreamReplay
            // subscriber replay the full history from the beginning.
            if (!_streamLog.TryGetValue(topic, out var log))
                _streamLog[topic] = log = new List<BusMessage>();
            log.Add(new BusMessage($"mem-stream-{++_seq}", topic, body, key, deliveryCount: 0, options));
            if (!_byTopic.TryGetValue(topic, out var set)) return;
            delivered = new List<(BusQueue, BusMessage)>();
            foreach (var queue in set.Where(q => q.Matches(key)))
            {
                var msg = new BusMessage($"mem-bus-{++_seq}", topic, body, key, deliveryCount: 0, options);
                queue.Pending.Enqueue(msg);
                delivered.Add((queue, msg));
            }
        }
        foreach (var (queue, msg) in delivered)
        {
            // S16: if the message carries a TTL, arm a timer; should it still be
            // un-consumed (pending or in-flight) when it fires, remove it and route
            // it to the expiry address.
            if (options?.TtlMs is { } ttl && ttl >= 0)
                ArmExpiry(queue, msg, ttl);
            Pump(queue);
        }
    }

    /// <summary>Drop the message to the expiry address if it is still un-consumed when its TTL elapses (S16).</summary>
    private void ArmExpiry(BusQueue queue, BusMessage msg, int ttlMs)
    {
        _ = Task.Delay(ttlMs).ContinueWith(_ =>
        {
            bool removed;
            lock (_gate) removed = queue.RemoveUnconsumed(msg);
            if (removed) Publish(MessageBus.ExpiryAddress(queue.Topic), msg.Body, msg.RoutingKey);
        }, TaskScheduler.Default);
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
                    opts.DeadLetter,
                    opts.RetryDelayMs,
                    opts.PartitionByGroup,
                    opts.SingleActiveConsumer);
                _queues[k] = queue;
                if (!_byTopic.TryGetValue(topic, out var set))
                    _byTopic[topic] = set = new HashSet<BusQueue>();
                set.Add(queue);
                // S19: a brand-new StreamReplay queue is seeded with the ENTIRE
                // retained log from offset 0 (FIFO), then keeps receiving live
                // publishes. So a subscriber attaching after the publishes still
                // replays the full history. Only the first creation of the queue
                // replays; reusing the id does not re-seed (mirrors a stream
                // consumer's stable offset cursor).
                if (opts.StreamReplay && _streamLog.TryGetValue(topic, out var log))
                    foreach (var logged in log)
                        if (queue.Matches(logged.RoutingKey))
                            queue.Pending.Enqueue(
                                new BusMessage(logged.Id, topic, logged.Body, logged.RoutingKey, 0, logged.Options));
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
                if (queue.PartitionByGroup)
                {
                    // S12: delivery is driven by group affinity, not the plain
                    // free-consumer round-robin, so a group stays pinned to one
                    // consumer and keeps order.
                    var next = queue.NextGroupDelivery();
                    if (next is null) return;
                    (msg, consumer) = next.Value;
                }
                else
                {
                    var free = queue.NextConsumer();
                    if (free is null) return; // every consumer holds a message (prefetch 1)
                    consumer = free;
                    msg = queue.DequeueNext();
                }
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
            bool delayedRequeue = false;
            lock (_gate)
            {
                if (!queue.Inflight.TryGetValue(msg, out var owner) || owner != consumer)
                    return; // already settled
                queue.Inflight.Remove(msg);
                switch (resolved)
                {
                    case SettleKind.Ack:
                        break;
                    case SettleKind.Dead:
                        deadLetter = true;
                        break;
                    case SettleKind.Requeue:
                        if (msg.DeliveryCount >= queue.MaxDeliveries) deadLetter = true;
                        else if (queue.RetryDelayMs is > 0) delayedRequeue = true;
                        else queue.Pending.Enqueue(msg); // immediate requeue; final Pump delivers it
                        break;
                }
            }
            if (deadLetter) DeadLetter(queue, msg);
            else if (delayedRequeue)
            {
                // Park the message off the active queue for the delay, then
                // redeliver — the in-memory analogue of RabbitMQ's TTL retry queue
                // / Artemis's redelivery-delay. Other messages keep pumping while
                // it waits, so the main queue is never blocked.
                _ = Task.Delay(queue.RetryDelayMs!.Value).ContinueWith(_ =>
                {
                    lock (_gate) queue.Pending.Enqueue(msg);
                    Pump(queue);
                }, TaskScheduler.Default);
            }
            // Settling (ack / dead-letter / requeue) frees the consumer's prefetch
            // slot, so always re-pump so the next pending message is delivered.
            Pump(queue);
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
            _streamLog.Clear();
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

    private sealed class BusMessage(
        string id, string topic, string body, string routingKey, int deliveryCount,
        PublishOptions? options = null)
    {
        public string Id { get; } = id;
        public string Topic { get; } = topic;
        public string Body { get; } = body;
        public string RoutingKey { get; } = routingKey;
        public int DeliveryCount { get; set; } = deliveryCount;
        public PublishOptions? Options { get; } = options;
        public int Priority => Options?.Priority ?? 0;
    }

    /// <summary>
    /// One logical subscriber queue. Distinct ids are independent queues (pub/sub +
    /// fanout); multiple consumers on the SAME queue compete for its messages
    /// (competing consumers / work sharing).
    /// </summary>
    private sealed class BusQueue(
        string topic, TopologyKind kind, string? routingKey, int maxDeliveries, bool deadLetter,
        int? retryDelayMs, bool partitionByGroup = false, bool singleActiveConsumer = false)
    {
        public string Topic { get; } = topic;
        public int MaxDeliveries { get; } = maxDeliveries;
        public bool DeadLetter { get; } = deadLetter;
        public int? RetryDelayMs { get; } = retryDelayMs;
        public bool PartitionByGroup { get; } = partitionByGroup;
        public bool SingleActiveConsumer { get; } = singleActiveConsumer;

        public PendingQueue Pending { get; } = new();
        public Dictionary<BusMessage, AckHandler> Inflight { get; } = new();
        public List<AckHandler> Consumers { get; } = new();
        // S12: once a groupId has been handed to a consumer, every later message
        // of that group goes to the SAME consumer — that is what preserves
        // per-group order across competing consumers.
        private readonly Dictionary<string, AckHandler> _groupAffinity = new();
        private int _rr;

        public bool Matches(string messageKey)
        {
            if (kind == TopologyKind.Fanout) return true;
            if (string.IsNullOrEmpty(routingKey)) return true; // unfiltered topic subscriber
            return TopicMatch(routingKey, messageKey);
        }

        /// <summary>
        /// Pick the next consumer that has no message in flight. Modelling a
        /// prefetch of 1 (which both real adapters set) is what makes priority
        /// observable: a busy consumer holds exactly one message, so a backlog
        /// forms on the queue where ordering can take effect, instead of every
        /// message being pushed at once. Round-robins across free consumers for
        /// fair work sharing (S9).
        /// </summary>
        public AckHandler? NextConsumer()
        {
            if (Consumers.Count == 0) return null;
            // S18: only the single active consumer (the first one still attached)
            // is ever handed a message; the rest stand by until it drops.
            if (SingleActiveConsumer)
                return Inflight.Count == 0 ? Consumers[0] : null;
            var busy = new HashSet<AckHandler>(Inflight.Values);
            for (var i = 0; i < Consumers.Count; i++)
            {
                var c = Consumers[_rr % Consumers.Count];
                _rr += 1;
                if (!busy.Contains(c)) return c;
            }
            return null; // every consumer already holds a message
        }

        /// <summary>
        /// S12: pick the next deliverable (message, consumer) pair honouring group
        /// affinity. A message whose group is already pinned can only go to that
        /// group's consumer (and only if it is free); an un-pinned group's first
        /// message is assigned to any free consumer, round-robin, and that consumer
        /// becomes its owner. Returns null when nothing can be delivered right now,
        /// so a group never overtakes its own earlier messages.
        /// </summary>
        public (BusMessage Msg, AckHandler Consumer)? NextGroupDelivery()
        {
            if (Consumers.Count == 0 || Pending.Count == 0) return null;
            var busy = new HashSet<AckHandler>(Inflight.Values);
            for (var i = 0; i < Pending.Count; i++)
            {
                var msg = Pending.PeekAt(i);
                var group = msg.Options?.GroupId ?? string.Empty;
                _groupAffinity.TryGetValue(group, out var owner);
                if (owner is not null && !Consumers.Contains(owner))
                {
                    // The pinned consumer has gone away — re-pin on next sight.
                    _groupAffinity.Remove(group);
                    owner = null;
                }
                if (owner is not null)
                {
                    if (busy.Contains(owner)) continue; // group is in flight; wait its turn
                    Pending.RemoveAt(i);
                    return (msg, owner);
                }
                var free = PickFreeConsumer(busy);
                if (free is null) continue;
                _groupAffinity[group] = free;
                Pending.RemoveAt(i);
                return (msg, free);
            }
            return null;
        }

        private AckHandler? PickFreeConsumer(HashSet<AckHandler> busy)
        {
            for (var i = 0; i < Consumers.Count; i++)
            {
                var c = Consumers[_rr % Consumers.Count];
                _rr += 1;
                if (!busy.Contains(c)) return c;
            }
            return null;
        }

        /// <summary>
        /// Pull the next message to deliver. Higher <c>Priority</c> wins; ties (and
        /// the common no-priority case, where every message defaults to 0) fall
        /// back to FIFO — leaving un-prioritised behaviour identical to a plain
        /// queue while letting a high-priority message overtake a backlog (S14).
        /// </summary>
        public BusMessage DequeueNext() => Pending.DequeueHighestPriority();

        /// <summary>Remove a message that is still pending or in-flight; returns true if it was un-consumed (S16 expiry).</summary>
        public bool RemoveUnconsumed(BusMessage msg)
        {
            if (Pending.Remove(msg)) return true;
            return Inflight.Remove(msg);
        }

        /// <summary>Push a crashed consumer's message back to the front of the queue.</summary>
        public void RequeueFront(BusMessage msg) => Pending.EnqueueFront(msg);
    }

    /// <summary>
    /// A pending-message store that dequeues by priority (FIFO on a tie). Backed by
    /// a simple list — the queue depths here are small, so an O(n) scan is fine and
    /// keeps the priority + arbitrary-removal (TTL expiry) logic in one place.
    /// </summary>
    private sealed class PendingQueue
    {
        private readonly List<BusMessage> _items = new();

        public int Count => _items.Count;

        public void Enqueue(BusMessage msg) => _items.Add(msg);

        public void EnqueueFront(BusMessage msg) => _items.Insert(0, msg);

        public bool Remove(BusMessage msg) => _items.Remove(msg);

        /// <summary>Peek the message at an index in FIFO order (S12 group scan).</summary>
        public BusMessage PeekAt(int index) => _items[index];

        /// <summary>Remove the message at an index (S12 group delivery).</summary>
        public void RemoveAt(int index) => _items.RemoveAt(index);

        public BusMessage DequeueHighestPriority()
        {
            var best = 0;
            for (var i = 1; i < _items.Count; i++)
                if (_items[i].Priority > _items[best].Priority) best = i;
            var msg = _items[best];
            _items.RemoveAt(best);
            return msg;
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
            Headers = msg.Options?.Headers is { } h
                ? new Dictionary<string, string>(h)
                : new Dictionary<string, string>();
            ReplyTo = msg.Options?.ReplyTo;
            CorrelationId = msg.Options?.CorrelationId;
            GroupId = msg.Options?.GroupId;
            Priority = msg.Options?.Priority;
            _settle = settle;
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
