namespace Messaging.Abstractions;

/// <summary>
/// Thrown when a caller invokes an operation the broker cannot honor (e.g.
/// cancelling a scheduled message on RabbitMQ's delayed plugin). A dedicated
/// type lets the runner distinguish "honestly unsupported" from "tried and broke".
/// </summary>
public sealed class OperationNotSupportedException : NotSupportedException
{
    public string Operation { get; }
    public string Broker { get; }

    public OperationNotSupportedException(string operation, string broker, string? reason = null)
        : base($"Operation '{operation}' is not supported by {broker}" +
               (reason is null ? string.Empty : $": {reason}"))
    {
        Operation = operation;
        Broker = broker;
    }
}
