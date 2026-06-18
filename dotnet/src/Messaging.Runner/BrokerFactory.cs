using Messaging.Abstractions;
using Messaging.Artemis;
using Messaging.InMemory;
using Messaging.RabbitMq;
using Messaging.Resilience;

namespace Messaging.Runner;

/// <summary>
/// The single registration point for brokers. Adding a broker is: write an
/// adapter, add one line here. Nothing else in the codebase changes — that is
/// the Open/Closed principle made concrete.
///
/// Every real broker is wrapped in <see cref="ResilientMessageScheduler"/> so the
/// comparison exercises the brokers through the same retry + circuit-breaker
/// layer you would run in production. Pass <c>resilient: false</c> for the raw
/// adapter.
/// </summary>
public static class BrokerFactory
{
    private static readonly Dictionary<string, Func<IMessageScheduler>> Registry = new()
    {
        ["artemis"] = () => new ArtemisScheduler(ArtemisConfig.FromEnv()),
        ["rabbitmq"] = () => new RabbitMqScheduler(RabbitConfig.FromEnv()),
        ["in-memory"] = () => new InMemoryScheduler(),
    };

    public static IReadOnlyList<string> Known => Registry.Keys.ToList();

    public static IMessageScheduler Create(string id, bool resilient = true)
    {
        if (!Registry.TryGetValue(id, out var factory))
            throw new ArgumentException($"Unknown broker '{id}'. Known: {string.Join(", ", Known)}");

        var scheduler = factory();
        return resilient ? new ResilientMessageScheduler(scheduler) : scheduler;
    }
}
