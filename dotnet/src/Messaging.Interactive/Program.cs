using Messaging.Abstractions;
using Messaging.Interactive;
using Messaging.Scenarios;

// A thin multi-terminal CLI over the adapters. Run several of these in separate
// terminals to watch pub/sub, fanout, competing consumers and dead-lettering
// happen across processes against a live broker.
//
// Usage:
//   mbc publish   <broker> <topic> [--key order.created] [--count N] [--message TEXT]
//   mbc subscribe <broker> <topic> [--id sub] [--kind topic|fanout] [--key order.#]
//                                   [--manual-ack] [--nack] [--dead-letter] [--crash-after N]
//   mbc worker    <broker> <topic> --id workers [--name w1]   (competing consumer; auto-acks)
//   mbc schedule  <broker> <dest>  [--in-ms 3000] [--message TEXT]

if (args.Length < 2)
{
    Console.Error.WriteLine(
        "Usage: mbc <publish|subscribe|worker|schedule> <broker> <topic|dest> [options]");
    return 2;
}

var verb = args[0].ToLowerInvariant();
var broker = args[1].ToLowerInvariant();
var target = args.Length > 2 && !args[2].StartsWith("--") ? args[2] : $"mbc.demo";
var opt = new Args(args);

if (!BrokerFactory.Known.Contains(broker))
{
    Console.Error.WriteLine($"Unknown broker '{broker}'. Known: {string.Join(", ", BrokerFactory.Known)}");
    return 2;
}

await using var adapter = BrokerFactory.Create(broker);

try
{
    switch (verb)
    {
        case "publish": await PublishAsync(); break;
        case "subscribe": await SubscribeAsync(); break;
        case "worker": await WorkerAsync(); break;
        case "schedule": await ScheduleAsync(); break;
        default:
            Console.Error.WriteLine($"Unknown verb '{verb}'. Use publish|subscribe|worker|schedule.");
            return 2;
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine($"{Ansi.Red}error:{Ansi.Reset} {ex.Message}");
    return 1;
}
return 0;

async Task PublishAsync()
{
    var bus = (IMessageBus)adapter;
    await bus.ConnectBusAsync();
    var count = opt.Int("count", 1);
    var key = opt.Str("key");
    var message = opt.Str("message") ?? "hello";
    for (var i = 0; i < count; i++)
    {
        var body = count > 1 ? $"{message}-{i}" : message;
        await bus.PublishAsync(target, body, key);
        Info($"published '{body}'" + (key is null ? "" : $" key={key}") + $" → {target}");
    }
}

async Task SubscribeAsync()
{
    var bus = (IMessageBus)adapter;
    await bus.ConnectBusAsync();
    var id = opt.Str("id") ?? $"sub-{Guid.NewGuid():N}"[..8];
    var kind = opt.Str("kind") == "fanout" ? TopologyKind.Fanout : TopologyKind.Topic;
    var manualAck = opt.Flag("manual-ack") || opt.Flag("nack");
    var nack = opt.Flag("nack");
    var crashAfter = opt.Int("crash-after", -1);
    var received = 0;

    var options = new SubscribeOptions
    {
        Kind = kind,
        RoutingKey = opt.Str("key"),
        SubscriberId = id,
        DeadLetter = opt.Flag("dead-letter"),
        MaxDeliveries = opt.Int("max-deliveries", MessageBus.DefaultMaxDeliveries),
    };

    Info($"subscribed id={id} kind={kind.ToString().ToLowerInvariant()} → {target}  (Ctrl-C to stop)");
    await bus.SubscribeAsync(target, async m =>
    {
        received += 1;
        Print(m);
        if (crashAfter >= 0 && received >= crashAfter)
        {
            Info($"{Ansi.Red}crashing{Ansi.Reset} after {received} (leaving msg un-acked)");
            Environment.Exit(0);
        }
        if (!manualAck) await m.AckAsync();
        else if (nack) await m.NackAsync(true);
        else await m.AckAsync();
    }, options);

    await Task.Delay(Timeout.Infinite);
}

async Task WorkerAsync()
{
    var bus = (IMessageBus)adapter;
    await bus.ConnectBusAsync();
    var id = opt.Str("id") ?? "workers"; // shared queue → competing consumers
    var name = opt.Str("name") ?? $"w-{Guid.NewGuid():N}"[..8];
    Info($"worker {name} on shared queue id={id} → {target}  (Ctrl-C to stop)");
    await bus.SubscribeAsync(target, async m =>
    {
        Console.WriteLine($"  {Ansi.Green}[{Stamp()}] {name}{Ansi.Reset} handled '{m.Body}'" +
                          DeliveryNote(m));
        await m.AckAsync();
    }, new SubscribeOptions { SubscriberId = id });
    await Task.Delay(Timeout.Infinite);
}

async Task ScheduleAsync()
{
    await adapter.ConnectAsync();
    var inMs = opt.Int("in-ms", 3000);
    var message = opt.Str("message") ?? "scheduled";
    var handle = await adapter.ScheduleAsync(target, message, DateTimeOffset.UtcNow.AddMilliseconds(inMs));
    Info($"scheduled '{message}' → {target} in {inMs}ms (id={handle.Id})");
}

static void Print(IIncomingMessage m) =>
    Console.WriteLine($"  {Ansi.Cyan}[{Stamp()}]{Ansi.Reset} {m.Destination} ← '{m.Body}'" + DeliveryNote(m));

static string DeliveryNote(IIncomingMessage m) =>
    m.DeliveryCount is { } dc ? $"  {Ansi.Dim}(delivery #{dc}){Ansi.Reset}" : "";

static void Info(string s) => Console.WriteLine($"{Ansi.Dim}· {s}{Ansi.Reset}");
static string Stamp() => DateTimeOffset.Now.ToString("HH:mm:ss.fff");
