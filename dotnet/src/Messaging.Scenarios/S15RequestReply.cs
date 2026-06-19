using Messaging.Abstractions;
using static Messaging.Scenarios.ScenarioHelpers;

namespace Messaging.Scenarios;

/// <summary>
/// S15 — Request/reply (RPC over messaging). A responder subscribes to a request
/// topic; for each request it echoes a reply to the message's <c>ReplyTo</c>
/// address, stamping the same <c>CorrelationId</c>. A requester subscribes to its
/// own unique reply topic, publishes a request carrying <c>ReplyTo</c> +
/// <c>CorrelationId</c>, and asserts the correlated reply comes back. All brokers
/// support it because it only needs <c>ReplyTo</c>/<c>CorrelationId</c>, which
/// every adapter maps to native AMQP properties.
/// </summary>
public sealed class RequestReply : IBusScenario
{
    public string Name => "S15 request/reply";
    public string Description => "A request carrying ReplyTo + CorrelationId gets a correlated reply.";

    public async Task<ScenarioResult> RunAsync(IMessageBus bus)
    {
        var t0 = StartClock();
        var id = Nonce();
        var reqTopic = $"mbc.s15.req.{id}";
        var replyTopic = $"mbc.s15.reply.{id}";
        var correlationId = $"corr-{id}";

        // The responder: echo each request back to its ReplyTo, preserving the id.
        AckHandler responder = async m =>
        {
            if (!string.IsNullOrEmpty(m.ReplyTo))
                await bus.PublishAsync(m.ReplyTo!, $"echo:{m.Body}",
                    options: new PublishOptions { CorrelationId = m.CorrelationId });
            await m.AckAsync();
        };
        var respSub = await bus.SubscribeAsync(reqTopic, responder,
            new SubscribeOptions { SubscriberId = $"responder-{id}" });

        // The requester: collect correlated replies.
        var replies = new List<IIncomingMessage>();
        var gate = new object();
        AckHandler requester = async m =>
        {
            lock (gate) replies.Add(m);
            await m.AckAsync();
        };
        var reqSub = await bus.SubscribeAsync(replyTopic, requester,
            new SubscribeOptions { SubscriberId = $"requester-{id}" });
        try
        {
            await bus.PublishAsync(reqTopic, "ping", options: new PublishOptions
            {
                ReplyTo = replyTopic,
                CorrelationId = correlationId,
            });

            int Count() { lock (gate) return replies.Count; }
            var got = await WaitUntilAsync(() => Count() >= 1, TimeSpan.FromSeconds(6));
            if (!got)
                return Fail(Name, "no reply received", t0);

            IIncomingMessage reply;
            lock (gate) reply = replies[0];
            if (reply.Body != "echo:ping")
                return Fail(Name, $"unexpected reply body \"{reply.Body}\"", t0);
            if (reply.CorrelationId != correlationId)
                return Fail(Name, $"correlationId mismatch: \"{reply.CorrelationId}\" != \"{correlationId}\"", t0);

            return Pass(Name, $"correlated reply \"{reply.Body}\" (correlationId matched)", t0);
        }
        finally
        {
            await respSub.DisposeAsync();
            await reqSub.DisposeAsync();
        }
    }
}
