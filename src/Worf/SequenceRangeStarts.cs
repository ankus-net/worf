using System.Globalization;
using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Keeps this server's starts, each with the WAL position it started from, in a file in the data directory, which a base
/// backup, or a snapshot of the server's storage, copies with the rest. A node's messages report them, so that a neighbor
/// that heard of a WAL position under one start knows the node went back at a later one, as a restore from a backup does,
/// though the node has started again since, from past that position. The file keeps the latest starts; the oldest it
/// keeps stands for those it let go, with the lowest position of any of them.
/// </summary>
internal static class SequenceRangeStarts
{
    private const string FileName = "worf_starts";
    private const string WrittenName = "worf_starts.tmp";
    private const int MaxStarts = 32;

    private static (long Since, ulong Lsn)[]? s_starts;

    /// <summary>
    /// Gets the starts the file kept, oldest first, read once by each process.
    /// </summary>
    internal static (long Since, ulong Lsn)[] All => s_starts ??= Read();

    /// <summary>
    /// Adds a start to the file, as worf's supervisor starts, writing it anew and renaming it in place durably, so that a
    /// crash leaves the file before or after the start, never half written.
    /// </summary>
    /// <param name="since">When the server started, as its messages tell.</param>
    /// <param name="lsn">The WAL position it started from.</param>
    internal static unsafe void Note(long since, ulong lsn)
    {
        List<(long Since, ulong Lsn)> starts = [.. Read().Where(start => start.Since != since), (since, lsn)];
        if (starts.Count > MaxStarts)
        {
            int dropped = starts.Count - MaxStarts;
            ulong lowest = starts.Take(dropped + 1).Min(start => start.Lsn);
            starts = [(starts[dropped].Since, lowest), .. starts.Skip(dropped + 1)];
        }

        s_starts = [.. starts];
        try
        {
            File.WriteAllText(WrittenName, string.Concat(starts.Select(start =>
                string.Create(CultureInfo.InvariantCulture, $"{start.Since} {Worf.FormatLsn(start.Lsn)}\n"))));
        }
        catch (IOException error)
        {
            WorfLog.Warning($"could not write file \"{WrittenName}\": {error.Message}");
            return;
        }

        fixed (byte* from = Encoding.UTF8.GetBytes(WrittenName + "\0"), to = Encoding.UTF8.GetBytes(FileName + "\0"))
        {
            _ = NativeMethods.durable_rename((sbyte*)from, (sbyte*)to, (int)PgLogLevel.Warning);
        }
    }

    // The starts the file holds, in the data directory, every process's working directory.
    private static (long Since, ulong Lsn)[] Read()
    {
        string[] lines;
        try
        {
            lines = File.ReadAllLines(FileName);
        }
        catch (FileNotFoundException)
        {
            return [];
        }
        catch (IOException error)
        {
            WorfLog.Warning($"could not read file \"{FileName}\": {error.Message}");
            return [];
        }

        List<(long Since, ulong Lsn)> starts = [];
        foreach (string line in lines)
        {
            string[] parts = line.Split(' ');
            if (parts.Length == 2 && long.TryParse(parts[0], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long since) &&
                Worf.TryParseLsn(parts[1], out ulong lsn))
            {
                starts.Add((since, lsn));
            }
        }

        return [.. starts];
    }
}
