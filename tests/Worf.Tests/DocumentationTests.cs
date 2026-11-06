namespace Worf.Tests;

/// <summary>
/// Checks that the documentation and the comments say what worf does where reviews found them wrong, reading them with
/// their line breaks and indentation collapsed, so that a wrong statement can't come back unseen. The behaviour each
/// statement describes has its own test.
/// </summary>
[TestClass]
public sealed class DocumentationTests
{
    private static readonly string s_readme = Read("README.md");
    private static readonly string s_configuration = Read("docs", "src", "content", "docs", "configuration.md");
    private static readonly string s_sequenceRanges = Read("docs", "src", "content", "docs", "sequence-ranges.md");

    /// <summary>
    /// The sequence range docs say what a neighbor that asks a node to reset does meanwhile, how often made-up requests
    /// reset a node, that a COPY's switch may retire values, what a rolled-back switch leaves, and that an open holder
    /// is switched beside at a block's end; the configuration says how long inserts wait for a switch; and the comments
    /// on prepared holders and the switch worker's cancellation say what the code does.
    /// </summary>
    [TestMethod]
    public void SequenceRangeDocsSayWhatWorfDoes()
    {
        Assert.DoesNotContain("neither takes the node's grants nor answers its requests", s_sequenceRanges, "a neighbor asking to reset (130)");
        Assert.Contains("takes no grant from the node and answers none of its requests while it sees the node's WAL position behind where it was",
            s_sequenceRanges, "a neighbor asking to reset (130)");
        Assert.Contains("and twice as long after each reset that comes within a day of the one before, up to a day", s_sequenceRanges,
            "how often made-up requests reset a node (6)");
        Assert.Contains("and so are both holders of grants that overlap, on every forced lift", s_sequenceRanges, "a forced lift's resets (130, 47)");
        Assert.DoesNotContain("leaves no block's values unused", s_sequenceRanges, "a COPY's switch (130)");
        Assert.Contains("A switch retires what its block has left", s_sequenceRanges, "a COPY's switch (130)");
        Assert.Contains("a statement that would draw from it waits for the switch as at the block's end, or makes it in its transaction",
            s_sequenceRanges, "a switch that rolled back (130, 128)");
        Assert.DoesNotContain("and stays open holds off the switch to the next block until it ends.", s_sequenceRanges, "an open holder (146)");
        Assert.Contains("holds off the range worker's own switch to the next block until it ends; from the block's last twentieth, a switch worker switches beside it instead",
            s_sequenceRanges, "an open holder (146)");

        foreach ((string name, string text) in new[] { ("README.md", s_readme), ("configuration.md", s_configuration) })
        {
            Assert.DoesNotContain("wait up to twice this in all for the switch", text, $"how long inserts wait for a switch, in {name} (130)");
            Assert.Contains("up to twice this for each block in all while its next block is still to come, or once a worker has tried and couldn't switch it, and otherwise until it comes, as for a lock",
                text, $"how long inserts wait for a switch, in {name} (130)");
        }

        string ranges = Read("src", "Worf", "SequenceRanges.cs");
        Assert.DoesNotContain("isn't waited for: the sequence's range says so, with a warning, and inserts run the block out", ranges, "prepared holders (146)");
        Assert.Contains("A prepared transaction that holds the sequence, which may hold it across restarts, is switched beside as any other.", ranges,
            "prepared holders (146)");
        Assert.DoesNotContain("A transaction that runs the block out cancelled the wait to switch the sequence itself",
            Read("src", "Worf", "SequenceRangeSwitchWorker.cs"), "the switch worker's cancellation (146)");
    }

    /// <summary>
    /// The configuration says which rows of a copy in batches have the batch's earlier rows copied again: only those
    /// COPY itself refuses, while rows that don't convert, that a column's input, a NOT NULL column or a CHECK constraint
    /// refuses, or whose keys the table holds, are left out.
    /// </summary>
    [TestMethod]
    public void CopyDocsSayWhichRowsCopyAgain()
    {
        foreach ((string name, string text) in new[] { ("README.md", s_readme), ("configuration.md", s_configuration) })
        {
            Assert.DoesNotContain("a row that the column's input or a constraint refuses has the rows before it copied again", text,
                $"the rows copied again, in {name} (179)");
            Assert.Contains("A row that `COPY` refuses otherwise, as one whose key a concurrent transaction writes, or that a foreign key or a trigger refuses, has the batch's rows before it copied again.",
                text, $"the rows copied again, in {name} (179)");
            Assert.Contains("and those whose values in an immediate unique index without expressions or a predicate a row the table keeps holds, are skipped at once",
                text, $"the rows a merge skips at once, in {name} (173)");
        }
    }

    // A file's words, one space apart, without the slashes that start a comment's lines.
    private static string Read(params string[] path)
        => string.Join(' ', File.ReadAllText(Path.Join([RepositoryPaths.Root, .. path])).Split([' ', '\n', '\r', '\t'], StringSplitOptions.RemoveEmptyEntries)
            .Where(word => word is not ("///" or "//")));
}
