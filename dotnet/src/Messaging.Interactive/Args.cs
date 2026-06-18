namespace Messaging.Interactive;

/// <summary>
/// A minimal <c>--flag value</c> / <c>--flag</c> command-line parser, mirroring the
/// argument style of the TypeScript interactive CLIs. Kept tiny and dependency-free.
/// </summary>
internal sealed class Args
{
    private readonly Dictionary<string, string> _values = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _flags = new(StringComparer.OrdinalIgnoreCase);

    public Args(string[] argv)
    {
        for (var i = 0; i < argv.Length; i++)
        {
            if (!argv[i].StartsWith("--", StringComparison.Ordinal)) continue;
            var key = argv[i][2..];
            if (i + 1 < argv.Length && !argv[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                _values[key] = argv[i + 1];
                i++;
            }
            else
            {
                _flags.Add(key);
            }
        }
    }

    public string? Str(string key) => _values.TryGetValue(key, out var v) ? v : null;

    public int Int(string key, int fallback) =>
        _values.TryGetValue(key, out var v) && int.TryParse(v, out var n) ? n : fallback;

    public bool Flag(string key) => _flags.Contains(key) || _values.ContainsKey(key);
}
