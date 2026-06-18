namespace Messaging.Scenarios;

/// <summary>
/// The shared terminal vocabulary (colours + status glyphs) used by both the
/// scenario report and the fault-tolerance narration. Extracted here so the two
/// presenters stay visually consistent and we don't duplicate escape codes.
/// </summary>
public static class Ansi
{
    public const string Reset = "\x1b[0m";
    public const string Bold = "\x1b[1m";
    public const string Dim = "\x1b[2m";
    public const string Green = "\x1b[32m";
    public const string Red = "\x1b[31m";
    public const string Yellow = "\x1b[33m";
    public const string Cyan = "\x1b[36m";
    public const string Magenta = "\x1b[35m";
    public const string Blue = "\x1b[34m";

    public static readonly string Hr = $"{Dim}{new string('─', 76)}{Reset}";

    public static string Glyph(ScenarioStatus s) => s switch
    {
        ScenarioStatus.Pass => $"{Green}✓ pass{Reset}",
        ScenarioStatus.Fail => $"{Red}✗ fail{Reset}",
        ScenarioStatus.Unsupported => $"{Yellow}⊘ n/a {Reset}",
        _ => $"{Dim}- skip{Reset}",
    };

    public static string YesNo(bool b) => b ? "yes" : "no";
}
