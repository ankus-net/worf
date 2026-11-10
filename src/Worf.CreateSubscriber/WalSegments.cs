using System.Globalization;

namespace Worf.CreateSubscriber;

/// <summary>
/// A data directory's WAL segments on one timeline, each opened as it's first read and kept open until this is disposed.
/// </summary>
internal sealed class WalSegments : IDisposable
{
    private readonly Dictionary<ulong, FileStream> _files = [];
    private readonly string _directory;
    private readonly long _timeline;
    private readonly ulong _segmentSize;

    /// <summary>
    /// Initializes a new instance of the <see cref="WalSegments"/> class.
    /// </summary>
    /// <param name="directory">The directory the segments are in.</param>
    /// <param name="timeline">The timeline.</param>
    /// <param name="segmentSize">The size of a segment.</param>
    internal WalSegments(string directory, long timeline, long segmentSize)
    {
        _directory = directory;
        _timeline = timeline;
        _segmentSize = (ulong)segmentSize;
    }

    /// <summary>
    /// Gets the name of the segment a position is in.
    /// </summary>
    /// <param name="at">The position.</param>
    /// <returns>The segment's file name.</returns>
    internal string Name(ulong at)
    {
        ulong perId = 0x100000000UL / _segmentSize;
        ulong segment = at / _segmentSize;
        return string.Create(CultureInfo.InvariantCulture, $"{_timeline:X8}{segment / perId:X8}{segment % perId:X8}");
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
                file = File.OpenRead(Path.Join(_directory, Name(at)));
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
