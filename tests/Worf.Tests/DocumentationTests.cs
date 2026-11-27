using System.Text.RegularExpressions;

namespace Worf.Tests;

/// <summary>
/// Checks that the documentation is plain and says what worf does where reviews found it wrong, reading it with its
/// line breaks and indentation collapsed, so that a wrong statement can't come back unseen. The behaviour each statement
/// describes has its own test.
/// </summary>
[TestClass]
public sealed partial class DocumentationTests
{
    private static readonly string s_configuration = Read("docs", "src", "content", "docs", "configuration.md");
    private static readonly string s_sequenceRanges = Read("docs", "src", "content", "docs", "sequence-ranges.md");
    private static readonly string s_nodes = Read("docs", "src", "content", "docs", "nodes.md");

    /// <summary>
    /// Every sentence of the README and the documentation site, outside code, tables and markup, has at most 45 words,
    /// so that none chains clause after clause.
    /// </summary>
    [TestMethod]
    public void DocsUseShortSentences()
    {
        // A sentence of clauses joined by colons and semicolons is one sentence.
        string clauses = string.Join("; ", Enumerable.Repeat(string.Join(' ', Enumerable.Repeat("word", 33)), 3)) + ".";
        Assert.HasCount(1, [.. Sentences(clauses)], "a sentence of three clauses");

        string docs = Path.Join(RepositoryPaths.Root, "docs", "src", "content", "docs");
        foreach (string file in Directory.EnumerateFiles(docs, "*.md", SearchOption.AllDirectories).Append(Path.Join(RepositoryPaths.Root, "README.md")))
        {
            foreach (string sentence in Sentences(File.ReadAllText(file)))
            {
                int words = sentence.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
                Assert.IsLessThanOrEqualTo(45, words, $"{Path.GetRelativePath(RepositoryPaths.Root, file)}: {sentence}");
            }
        }
    }

    /// <summary>
    /// The sequence range docs say what the allocator is without calling it the owner, that a node that can't reach the
    /// others takes its blocks, a load's too, from its reserve and draws after a restart unless it waits until the others
    /// answer, and what the reviews found them wrong about: a load's block granted whole, made-up requests, the switch at
    /// a block's end, rows whose triggers draw unseen, upserts, and a sequence's bounds after worf is dropped.
    /// </summary>
    [TestMethod]
    public void SequenceRangeDocsSayWhatWorfDoes()
    {
        Assert.DoesNotContain("holds the root", s_sequenceRanges, "the allocator");
        Assert.DoesNotContain("owner", s_sequenceRanges.Replace("`OWNER TO`", "", StringComparison.Ordinal), "the allocator");
        Assert.Contains("One node, the **allocator**, keeps track of which numbers are still free and hands out the blocks. You don't need to remember which node it is: `worf.sequence_ranges` shows it, and a function that has to run there tells you which node to run it on.",
            s_sequenceRanges, "the allocator");
        Assert.Contains("It waits for them for up to `worf.sequence_range_offline_wait`, 10 seconds by default, and then goes on without them",
            s_sequenceRanges, "going on without the other nodes");
        Assert.Contains("A node takes its next block from its reserve when the node it asks hasn't answered.", s_sequenceRanges, "offline blocks");
        Assert.Contains("That includes blocks for `reserve_sequence_range`, so bulk loads work offline too", s_sequenceRanges, "offline loads");
        Assert.Contains("A node that restarted is different: it might have been restored from a backup, so it draws nothing, and takes nothing from its reserve, until the other nodes it exchanges with have answered it.",
            s_sequenceRanges, "restarts (2110–2114)");
        Assert.Contains("With `-1`, a restarted node waits until they answer", s_configuration, "restarts (2110–2114)");
        Assert.Contains("A node tells its other neighbors about each new neighbor, such as a node that starts subscribing to it, and draws only once one of them has heard of it.",
            s_sequenceRanges, "new neighbors (1605)");
        Assert.Contains("With `-1`, a node that can't reach the others keeps drawing from the blocks it holds, and then waits for them.", s_configuration,
            "what -1 does (2160)");
        Assert.Contains("A call that runs out of time, or a failed probe, is retried after a delay that starts at no more than 10 seconds and doubles each time, up to a minute or this interval, whichever is longer, whether or not anything waits.",
            s_configuration, "a timed-out call's retries (2161)");
        Assert.Contains("make a node give up its blocks: once in ten minutes for each neighbor, and, for the node's grantor, once in ten seconds at first",
            s_sequenceRanges, "made-up resets (2162)");
        Assert.Contains("Rows written with `session_replication_role` set to `replica`, as worf's apply and sync workers write the rows they bring from other nodes, aren't checked.",
            s_sequenceRanges, "unchecked rows (2163)");
        Assert.Contains("or \"reached minimum value\" for a descending sequence", s_sequenceRanges, "a descending sequence's error (2165)");
        Assert.Contains("statements at the end of its last block fail at once with SQLSTATE `2200H` instead of waiting", s_sequenceRanges,
            "a used-up reserve (2154)");
        Assert.Contains("How long a running node waits for the other nodes before it goes on without them", s_configuration, "offline blocks");
        Assert.Contains("The block is handed out whole, up to a sixteenth of the values the allocator has left.", s_sequenceRanges,
            "a participant's reservation (2000, 2101)");
        Assert.DoesNotContain("Repeated requests can take all of a sequence's numbers.", s_sequenceRanges, "made-up pool and reserved requests (1900)");
        Assert.Contains("Every ten minutes, that's up to two pools of sixteen times `max_range_size` for the node and for each node behind it, and loads of up to a sixteenth of the values the allocator has left.",
            s_sequenceRanges, "made-up pool and reserved requests (1900, 2100)");
        Assert.Contains("From a block's last twentieth, statements wait for the switch before they draw more.", s_sequenceRanges, "the switch (130)");
        Assert.Contains("it allows half a block per row until it has seen such a row", s_sequenceRanges, "a row's unseen draws (1663)");
        Assert.Contains("Inserts, `COPY FROM`, `INSERT … ON CONFLICT` and `MERGE` check between rows.", s_sequenceRanges, "upserts and merges (2040)");
        Assert.Contains("up to twice this long, plus `worf.sequence_range_offline_wait`, if the next block hasn't arrived or a switch failed, and otherwise until the switch is done", s_configuration,
            "how long inserts wait for a switch (130)");
        Assert.Contains("goes on to the end of its range if no other node was ever given any of its values. Otherwise it stays limited to the block of numbers this node was using, so it can't hand out a number another node may have, and worf logs a warning for it.",
            s_nodes, "sequences' bounds after worf's drop (2021, 2164)");
        Assert.Contains("The drop does wait for a transaction that already holds one of worf's tables, such as one that read them or reserved a block. That transaction's inserts go on, and the drop runs once it ends.",
            s_sequenceRanges, "inserts beside worf's drop (2205)");
        Assert.DoesNotContain("While the drop runs, new inserts that use a managed sequence fail", s_sequenceRanges, "inserts beside worf's drop (2205)");
        Assert.Contains("A cursor between fetches, a client that reads slowly and a transaction left idle hold nothing, so they don't delay a switch.",
            s_sequenceRanges, "statements that hold no gate while they idle (2200)");
        Assert.Contains("Where the next row draws straight away, they keep the gate and let it go when a switch waits for it. They look for a waiting switch at each row, or every tenth of a millisecond when rows take less.",
            s_sequenceRanges, "statements that keep the gate from row to row (2300)");
        Assert.DoesNotContain("hold the gate only while they work out a row", s_sequenceRanges, "statements that keep the gate from row to row (2300)");
        Assert.Contains("Other draws, such as those of `SELECT nextval(...)` or of a PL/pgSQL assignment in a loop, take the gate only when they have to check the block, as a statement's first draw does, or the first by a PL/pgSQL expression in a transaction. The draws between hold nothing, and a switch waits only for the ones under way.",
            s_sequenceRanges, "draws that hold no gate (2301, 2500, 2501, the thirty-sixth review's rows)");
        Assert.Contains("worf can't always reach a statement's calls one by one, for example one in a join's condition, unless PostgreSQL compiles the statement with JIT. Such statements check before each row instead and hold the gate while they work out a row.",
            s_sequenceRanges, "statements that check before each row");
        Assert.DoesNotContain("or the next turn of a loop draws straight away", s_sequenceRanges, "PL/pgSQL loops that keep no gate (2501)");

        string ranges = Read("src", "Worf", "SequenceRanges.cs");
        Assert.DoesNotContain("which switches beside those transactions, without waiting for them, while the range worker checks", ranges,
            "the switch worker's wait (146)");
        Assert.DoesNotContain("isn't waited for: the sequence's range says so, with a warning, and inserts run the block out", ranges, "prepared holders (146)");
        Assert.Contains("A prepared transaction that holds the sequence, which may hold it across restarts, is switched beside as any other.", ranges,
            "prepared holders (146)");
        Assert.DoesNotContain("A transaction that runs the block out cancelled the wait to switch the sequence itself",
            Read("src", "Worf", "SequenceRangeSwitchWorker.cs"), "the switch worker's cancellation (146)");
    }

    /// <summary>
    /// The configuration says which rows of a copy in batches have the batch's earlier rows copied again, which rows a
    /// merge skips at once and how it records them, that foreign keys aren't checked, and that apply's rows that swap
    /// values still apply.
    /// </summary>
    [TestMethod]
    public void CopyDocsSayWhichRowsCopyAgain()
    {
        Assert.Contains("If `COPY` rejects a row for another reason, such as a concurrent write to the same key, a trigger, or a deferrable constraint checked at the end of the batch, the batch's rows before it are copied again without it.",
            s_configuration, "the rows copied again (179, 257)");
        Assert.Contains("or an immediate unique index or exclusion constraint rejects, unless a `BEFORE ROW` trigger could change them first", s_configuration,
            "the rows a merge skips at once (40, 173, 251)");
        Assert.Contains("Skipped rows are recorded as a `MERGE`, with their column values as text and their row numbers in the copy", s_configuration,
            "how a merge's skipped rows are recorded (256)");
        Assert.Contains("Foreign keys aren't checked, just as apply doesn't check them.", s_configuration, "foreign keys (257)");
        Assert.Contains("so rows that swap values still apply", s_configuration, "apply's deferrable rechecks (253)");
    }

    // The sentences of a page's prose: without its front matter, code, tables and markup, split after each full stop,
    // question mark or exclamation mark that ends one, and before each list item.
    private static IEnumerable<string> Sentences(string page)
    {
        string text = Markup().Replace(Tables().Replace(Code().Replace(FrontMatter().Replace(page, ""), ""), ""), " ");
        return Blocks().Split(text)
            .SelectMany(block => SentenceEnd().Split(Spaces().Replace(block, " ").Trim()))
            .Where(sentence => sentence.Length > 0);
    }

    [GeneratedRegex(@"\A---.*?\n---\n", RegexOptions.Singleline)]
    private static partial Regex FrontMatter();

    [GeneratedRegex("```.*?```", RegexOptions.Singleline)]
    private static partial Regex Code();

    [GeneratedRegex(@"^\s*\|.*$", RegexOptions.Multiline)]
    private static partial Regex Tables();

    [GeneratedRegex("<[^>]+>")]
    private static partial Regex Markup();

    // A blank line, or a line break before a list item.
    [GeneratedRegex(@"\n\s*\n|\n(?=\s*(?:[-*]|\d+\.) )")]
    private static partial Regex Blocks();

    [GeneratedRegex(@"(?<=[.?!])\s+")]
    private static partial Regex SentenceEnd();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();

    // A file's words, one space apart, without the slashes that start a comment's lines.
    private static string Read(params string[] path)
        => string.Join(' ', File.ReadAllText(Path.Join([RepositoryPaths.Root, .. path])).Split([' ', '\n', '\r', '\t'], StringSplitOptions.RemoveEmptyEntries)
            .Where(word => word is not ("///" or "//")));
}
