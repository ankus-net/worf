using System.Globalization;
using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Keeps this server's starts, each with the WAL position it started from, in a file in the data directory, which a base
/// backup, or a snapshot of the server's storage, copies with the rest. A node's messages report them, so that a neighbor
/// that heard of a WAL position under one start knows the node went back at a later one, as a restore from a backup does,
/// though the node has started again since, from past that position. The file keeps the latest starts; the oldest it
/// keeps stands for those it let go, with the lowest position of any of them. A start whose file is gone, can't be read,
/// or wasn't written to its end, as when it was deleted or a crash cut a write short, lost the starts before it, which
/// may have hidden a restore: it keeps a start just before its own at position 0, so that every neighbor that heard of a
/// position from the node before it takes the node as gone back.
/// </summary>
internal static class SequenceRangeStarts
{
    private const string FileName = "worf_starts";
    private const string WrittenName = "worf_starts.tmp";
    private const string End = "end";
    private const int MaxStarts = 32;

    private static (long Since, ulong Lsn)[]? s_starts;

    /// <summary>
    /// Gets the starts the file kept, oldest first, read once by each process.
    /// </summary>
    internal static (long Since, ulong Lsn)[] All => s_starts ??= Read().Starts;

    /// <summary>
    /// Adds a start to the file, as worf's supervisor starts, or a single-user server's first statement runs, writing it
    /// anew and renaming it in place durably, so that a crash leaves the file before or after the start, never half
    /// written, or, if that fails, as when worf_starts.tmp can't be written, writing the file in place, ending it with a
    /// line that shows it whole.
    /// </summary>
    /// <param name="since">When the server started, as its messages tell.</param>
    /// <param name="lsn">The WAL position it started from.</param>
    /// <returns>Whether the file keeps the start.</returns>
    internal static bool Note(long since, ulong lsn)
    {
        ((long Since, ulong Lsn)[] read, bool lost) = Read();
        List<(long Since, ulong Lsn)> starts = [.. read.Where(start => start.Since != since)];
        if (lost && !starts.Exists(start => start.Lsn == 0 && start.Since == since - 1))
        {
            WorfLog.Warning($"file \"{FileName}\" lost the starts of this server before this one, so its neighbors take every WAL position they heard of from it before this start as gone back");
            starts.Add((since - 1, 0));
        }

        starts.Add((since, lsn));
        if (starts.Count > MaxStarts)
        {
            int dropped = starts.Count - MaxStarts;
            ulong lowest = starts.Take(dropped + 1).Min(start => start.Lsn);
            starts = [(starts[dropped].Since, lowest), .. starts.Skip(dropped + 1)];
        }

        s_starts = [.. starts];
        string text = string.Concat(starts.Select(start => string.Create(CultureInfo.InvariantCulture, $"{start.Since} {Worf.FormatLsn(start.Lsn)}\n"))) + End + "\n";
        string? failure = WriteRenamed(text);
        if (failure is null)
        {
            return true;
        }

        try
        {
            WriteSynced(FileName, text);
            return true;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or NotSupportedException or System.Security.SecurityException)
        {
            WorfLog.Warning($"could not keep this server's start in file \"{FileName}\": {failure}; {error.Message}");
            return false;
        }
    }

    // Writes the file anew under its temporary name and renames it in place durably, returning why that failed, or null.
    private static unsafe string? WriteRenamed(string text)
    {
        try
        {
            WriteSynced(WrittenName, text);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or NotSupportedException or System.Security.SecurityException)
        {
            return error.Message;
        }

        // durable_rename syncs the file it replaces first, which fails when that can't be opened, as at mode 000; such a file
        // goes first, after which a crash leaves none, which counts as lost.
        for (int attempt = 0; attempt < 2; attempt++)
        {
            int renamed;
            fixed (byte* from = Encoding.UTF8.GetBytes(WrittenName + "\0"), to = Encoding.UTF8.GetBytes(FileName + "\0"))
            {
                renamed = NativeMethods.durable_rename((sbyte*)from, (sbyte*)to, (int)PgLogLevel.Warning);
            }

            if (renamed == 0)
            {
                return null;
            }

            try
            {
                File.Delete(FileName);
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException or NotSupportedException or System.Security.SecurityException)
            {
                return error.Message;
            }
        }

        return $"could not rename file \"{WrittenName}\" to \"{FileName}\"";
    }

    // Writes a file and flushes it to disk.
    private static void WriteSynced(string name, string text)
    {
        using FileStream file = new(name, FileMode.Create, FileAccess.Write);
        file.Write(Encoding.UTF8.GetBytes(text));
        file.Flush(true);
    }

    // The starts the file holds, in the data directory, every process's working directory, and whether it lost those
    // before them: it is gone, can't be read, or doesn't end with the line that shows it whole.
    private static ((long Since, ulong Lsn)[] Starts, bool Lost) Read()
    {
        string[] lines;
        try
        {
            lines = File.ReadAllLines(FileName);
        }
        catch (Exception error) when (error is FileNotFoundException or DirectoryNotFoundException)
        {
            return ([], true);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or NotSupportedException or System.Security.SecurityException)
        {
            WorfLog.Warning($"could not read file \"{FileName}\": {error.Message}");
            return ([], true);
        }

        bool whole = lines.Length > 0 && lines[^1] == End;
        return ([.. lines.Select(line => line.Split(' ')).Select(Parse).OfType<(long Since, ulong Lsn)>()], !whole);
    }

    // A start of the file's, from a line's two fields, if well formed.
    private static (long Since, ulong Lsn)? Parse(string[] parts)
        => parts.Length == 2 && long.TryParse(parts[0], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long since) &&
            Worf.TryParseLsn(parts[1], out ulong lsn) ? (since, lsn) : null;
}
