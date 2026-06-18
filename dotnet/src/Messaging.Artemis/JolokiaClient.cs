using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Messaging.Artemis;

/// <summary>
/// Minimal Jolokia (HTTP/JSON over JMX) client for the Artemis broker.
///
/// Artemis exposes scheduled-message management through its <c>QueueControl</c>
/// MBean (<c>listScheduledMessages</c>, <c>removeMessages</c>). The AMQP wire
/// protocol has no notion of "cancel a scheduled message", so the real
/// cancellation story goes through management — and Jolokia is the same JSON
/// endpoint from .NET and TypeScript, which keeps the two adapters symmetric.
/// </summary>
public sealed class JolokiaClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;

    public JolokiaClient(string baseUrl, string username, string password)
    {
        _baseUrl = baseUrl;
        _http = new HttpClient();
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", basic);
        // Jolokia requires a non-empty Origin/Referer when strict checking is on.
        _http.DefaultRequestHeaders.Add("Origin", "http://localhost");
    }

    private async Task<JsonElement> PostAsync(object body, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(body);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var res = await _http.PostAsync(_baseUrl, content, ct);
        var text = await res.Content.ReadAsStringAsync(ct);
        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"Jolokia HTTP {(int)res.StatusCode}: {text}");

        using var doc = JsonDocument.Parse(text);
        var root = doc.RootElement;
        var status = root.TryGetProperty("status", out var s) ? s.GetInt32() : 0;
        if (status != 200)
            throw new InvalidOperationException($"Jolokia error {status}");

        return root.TryGetProperty("value", out var v) ? v.Clone() : default;
    }

    /// <summary>Resolve the full ObjectName of the anycast queue backing <paramref name="queueName"/>.</summary>
    public async Task<string?> FindQueueMBeanAsync(string queueName, CancellationToken ct = default)
    {
        var pattern =
            "org.apache.activemq.artemis:broker=*,component=addresses," +
            $"address=\"{queueName}\",subcomponent=queues,routing-type=\"anycast\"," +
            $"queue=\"{queueName}\"";
        var value = await PostAsync(new { type = "search", mbean = pattern }, ct);
        if (value.ValueKind == JsonValueKind.Array && value.GetArrayLength() > 0)
            return value[0].GetString();
        return null;
    }

    /// <summary>Returns the raw list of scheduled messages currently held by the queue.</summary>
    public async Task<IReadOnlyList<JsonElement>> ListScheduledMessagesAsync(
        string queueName, CancellationToken ct = default)
    {
        var mbean = await FindQueueMBeanAsync(queueName, ct);
        if (mbean is null) return Array.Empty<JsonElement>();
        var value = await PostAsync(
            new { type = "exec", mbean, operation = "listScheduledMessages()" }, ct);
        return value.ValueKind == JsonValueKind.Array
            ? value.EnumerateArray().Select(e => e.Clone()).ToList()
            : Array.Empty<JsonElement>();
    }

    /// <summary>Removes messages matching an Artemis filter; returns the number removed.</summary>
    public async Task<int> RemoveMessagesAsync(
        string queueName, string filter, CancellationToken ct = default)
    {
        var mbean = await FindQueueMBeanAsync(queueName, ct);
        if (mbean is null) return 0;
        var value = await PostAsync(new
        {
            type = "exec",
            mbean,
            operation = "removeMessages(java.lang.String)",
            arguments = new[] { filter },
        }, ct);
        return value.ValueKind == JsonValueKind.Number ? value.GetInt32() : 0;
    }

    public void Dispose() => _http.Dispose();
}
