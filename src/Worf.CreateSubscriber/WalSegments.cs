using System.Buffers;
using System.Globalization;

namespace Worf.CreateSubscriber;

/// <summary>
/// A data directory's WAL segments, each read from the latest timeline up to a given one that the directory holds it on,
/// as recovery reads it, opened as it's first read and kept open until this is disposed.
/// </summary>
internal sealed class WalSegments : IDisposable
{
    private static readonly SearchValues<char> s_hex = SearchValues.Create("0123456789ABCDEF");
    private readonly Dictionary<ulong, FileStream> _files = [];
    private readonly Dictionary<ulong, long> _timelines = [];
    private readonly string _directory;
    private readonly ulong _segmentSize;

    /// <summary>
    /// Initializes a new instance of the <see cref="WalSegments"/> class.
    /// </summary>
    /// <param name="directory">The directory the segments are in.</param>
    /// <param name="names">The names of the files in the directory.</param>
    /// <param name="timeline">The latest timeline to read a segment from.</param>
    /// <param name="segmentSize">The size of a segment.</param>
    internal WalSegments(string directory, IEnumerable<string> names, long timeline, long segmentSize)
    {
        _directory = directory;
        _segmentSize = (ulong)segmentSize;
        foreach ((ulong segment, long found) in names
            .Select(name => TryParse(name, segmentSize, out long found, out ulong segment) && name.Length == 24 ? (Segment: segment, Found: found) : (Segment: 0UL, Found: -1L))
            .Where(parsed => parsed.Found >= 0 && parsed.Found <= timeline))
        {
            _timelines[segment] = Math.Max(_timelines.GetValueOrDefault(segment, found), found);
        }
    }

    /// <summary>
    /// Reads a WAL file's name: a segment's, with its timeline and number, as a timeline's last segment, which a
    /// promotion renames with ".partial" after it, also has.
    /// </summary>
    /// <param name="name">The file's name.</param>
    /// <param name="segmentSize">The size of a segment.</param>
    /// <param name="timeline">The segment's timeline.</param>
    /// <param name="segment">The segment's number.</param>
    /// <returns>Whether the name is a segment's.</returns>
    internal static bool TryParse(string name, long segmentSize, out long timeline, out ulong segment)
    {
        timeline = 0;
        segment = 0;
        if (name.Length is not (24 or 32) || (name.Length == 32 && !name.EndsWith(".partial", StringComparison.Ordinal)) ||
            !ulong.TryParse(name.AsSpan(0, 8), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out ulong tli) ||
            !ulong.TryParse(name.AsSpan(8, 8), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out ulong log) ||
            !ulong.TryParse(name.AsSpan(16, 8), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out ulong seg) ||
            name.AsSpan(0, 24).ContainsAnyExcept(s_hex))
        {
            return false;
        }

        timeline = (long)tli;
        segment = (log * (0x100000000UL / (ulong)segmentSize)) + seg;
        return true;
    }

    /// <summary>
    /// Reads a timeline history file's name.
    /// </summary>
    /// <param name="name">The file's name.</param>
    /// <param name="timeline">The timeline it gives the history of.</param>
    /// <returns>Whether the name is a history file's.</returns>
    internal static bool TryParseHistory(string name, out long timeline)
    {
        timeline = 0;
        if (name.Length != 16 || !name.EndsWith(".history", StringComparison.Ordinal) || name.AsSpan(0, 8).ContainsAnyExcept(s_hex) ||
            !long.TryParse(name.AsSpan(0, 8), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out long tli))
        {
            return false;
        }

        timeline = tli;
        return true;
    }

    /// <summary>
    /// Gets the name of the file this reads a position's segment from, or null when the directory doesn't hold it.
    /// </summary>
    /// <param name="at">The position.</param>
    /// <returns>The segment's file name, or null.</returns>
    internal string? Name(ulong at)
    {
        ulong segment = at / _segmentSize;
        if (!_timelines.TryGetValue(segment, out long timeline))
        {
            return null;
        }

        ulong perId = 0x100000000UL / _segmentSize;
        return string.Create(CultureInfo.InvariantCulture, $"{timeline:X8}{segment / perId:X8}{segment % perId:X8}");
    }

    /// <summary>
    /// Reads a number of bytes from a position, in the segment it's in.
    /// </summary>
    /// <param name="at">The position.</param>
    /// <param name="length">The number of bytes.</param>
    /// <returns>The bytes, or null when the segment is missing or ends before them, or can't be read.</returns>
    internal byte[]? Read(ulong at, int length)
    {
        ulong segment = at / _segmentSize;
        try
        {
            if (!_files.TryGetValue(segment, out FileStream? file))
            {
                if (Name(at) is not string name)
                {
                    return null;
                }

                file = File.OpenRead(Path.Join(_directory, name));
                _files[segment] = file;
            }

            byte[] bytes = new byte[length];
            _ = file.Seek((long)(at % _segmentSize), SeekOrigin.Begin);
            file.ReadExactly(bytes);
            return bytes;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        foreach (FileStream file in _files.Values)
        {
            file.Dispose();
        }

        _files.Clear();
    }
}
