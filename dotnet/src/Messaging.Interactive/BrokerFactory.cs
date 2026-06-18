using Messaging.Abstractions;
using Messaging.Artemis;
using Messaging.InMemory;
using Messaging.RabbitMq;

namespace Messaging.Interactive;

/// <summary>
/// Builds the raw adapter for an interactive session. Unlike the comparison
/// runner, the interactive CLI uses the un-decorated adapter so the messages you
/// see are exactly what the broker delivered (no retry layer in the way).
/// </summary>
internal static class BrokerFactory
{
    private static readonly Dictionary<string, Func<IMessageScheduler>> Registry = new()
    {
        ["artemis"] = () => new ArtemisScheduler(ArtemisConfig.FromEnv()),
        ["rabbitmq"] = () => new RabbitMqScheduler(RabbitConfig.FromEnv()),
        ["in-memory"] = () => new InMemoryScheduler(),
    };

    public static IReadOnlyList<string> Known => Registry.Keys.ToList();

    public static IMessageScheduler Create(string id)
    {
        if (!Registry.TryGetValue(id, out var factory))
            throw new ArgumentException($"Unknown broker '{id}'. Known: {string.Join(", ", Known)}");
        return factory();
    }
}
