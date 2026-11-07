using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;

namespace Worf;

/// <summary>
/// Reads and writes worf's JSON messages whose strings may hold the bytes of a SQL_ASCII name that aren't valid UTF-8,
/// which a string holds as unpaired surrogates, and which .NET's JSON neither reads nor writes. Such a message is
/// parsed with each of those surrogates written as U+FFFF and the byte's Latin-1 character, and U+FFFF as itself
/// twice, which every string read from it reads back from, so that the queue keeps the bytes as PostgreSQL's json does.
/// </summary>
internal static class WorfJson
{
    private const char Marker = '￿';
    private const char EscapedFirst = '\uDC80';
    private const char EscapedLast = '\uDCFF';

    /// <summary>
    /// Parses a message that worf stored, whose strings hold such surrogates as they are.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <returns>The parsed message.</returns>
    internal static JsonDocument Parse(string message) => JsonDocument.Parse(Encode(message));

    /// <summary>
    /// Writes a string for a message that worf parses with <see cref="JsonDocument.Parse(string, JsonDocumentOptions)"/>
    /// and reads with <see cref="GetWorfString"/>.
    /// </summary>
    /// <param name="value">The string, or null.</param>
    /// <returns>The string to write.</returns>
    [return: NotNullIfNotNull(nameof(value))]
    internal static string? Encode(string? value)
    {
        if (value is null || value.AsSpan().IndexOfAnyInRange(EscapedFirst, EscapedLast) < 0 && !value.Contains(Marker, StringComparison.Ordinal))
        {
            return value;
        }

        StringBuilder result = new(value.Length + 8);
        foreach (char c in value)
        {
            _ = c switch
            {
                Marker => result.Append(Marker).Append(Marker),
                >= EscapedFirst and <= EscapedLast => result.Append(Marker).Append((char)(c - EscapedFirst + 0x80)),
                _ => result.Append(c),
            };
        }

        return result.ToString();
    }

    /// <summary>
    /// Reads a string of a message written with <see cref="Encode"/>, or parsed with <see cref="Parse"/>.
    /// </summary>
    /// <param name="element">The string's element.</param>
    /// <returns>The string, or null.</returns>
    internal static string? GetWorfString(this JsonElement element) => Decode(element.GetString());

    /// <summary>
    /// Reads back a string that <see cref="Encode"/> wrote.
    /// </summary>
    private static string? Decode(string? value)
    {
        if (value is null || !value.Contains(Marker, StringComparison.Ordinal))
        {
            return value;
        }

        StringBuilder result = new(value.Length);
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] != Marker || i + 1 == value.Length)
            {
                _ = result.Append(value[i]);
                continue;
            }

            char next = value[++i];
            _ = result.Append(next == Marker ? Marker : (char)(next - 0x80 + EscapedFirst));
        }

        return result.ToString();
    }
}
