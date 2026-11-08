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
        Assert.DoesNotContain("and twice as long after each reset that comes within a day of the one before, up to a day", s_sequenceRanges,
            "how often made-up requests reset a node (130/146)");
        Assert.Contains("and twice as long after each reset that comes within a day of the end of the last such window, up to a day, and half as long, down to ten minutes, for each two days and a window more in which the neighbor could have and didn't",
            s_sequenceRanges, "how often made-up requests reset a node (6, 130/146, 290)");
        Assert.DoesNotContain("both holders", s_sequenceRanges, "a forced lift's resets (130, 47)");
        Assert.Contains("whether or not it has drawn since it last reset, and so is every holder of grants that overlap, on every forced lift.", s_sequenceRanges,
            "a forced lift's resets (130, 47, 294)");
        Assert.Contains("every holder of a grant that overlaps another resets, the others as they hear from this node", s_sequenceRanges,
            "a forced lift's resets (130)");
        Assert.DoesNotContain("while one that missed a call is reached again within a minute of answering", s_sequenceRanges, "a slow neighbor (130, 125)");
        Assert.Contains("and is made again in the next round without giving way", s_sequenceRanges, "a slow neighbor (125)");
        Assert.DoesNotContain("no more than once every ten minutes for each neighbor", s_sequenceRanges, "made-up resets (130, 211)");
        Assert.DoesNotContain("so a grantor resets a node no more often than it grants it blocks", s_sequenceRanges, "a grantor's lifts (211)");
        Assert.Contains("has a limit of its own, so that another request's reset doesn't hold it up, which starts at ten seconds rather than ten minutes",
            s_sequenceRanges, "a grantor's lifts (211)");
        Assert.DoesNotContain("without waiting for them, and commits at once", s_sequenceRanges, "a switch worker's wait (130/146)");
        Assert.DoesNotContain("three seconds a minute at most", s_sequenceRanges, "an unreachable neighbor's cost (294)");
        Assert.Contains("holds up the others' calls for its first call's `worf.sequence_range_timeout`, and then about three seconds ten, twenty and forty seconds after the call before ends", s_sequenceRanges,
            "an unreachable neighbor's cost (294)");
        Assert.Contains("A forced lift's request to reset from the node's grantor", s_sequenceRanges, "a grantor's lifts (352)");
        Assert.Contains("the grantor's lifts may reset the node 9 times in their first hour and 14 in their first day", s_sequenceRanges, "a grantor's lifts (353)");
        Assert.Contains("A switch worker waits for the gate before it locks the sequence's range row", s_sequenceRanges, "a paused gate holder (291)");
        Assert.Contains("a call whose sequence none of those name, as one computed as it runs, a PL/pgSQL expression's", s_sequenceRanges,
            "a returning node's draws (292, 293)");
        Assert.Contains("Nothing else waits: `nextval` of a sequence no range manages, or of a temporary one, that its statement or `COPY`'s defaults name", s_sequenceRanges,
            "what doesn't wait for a grantor (351, 422)");
        Assert.Contains("an `UPDATE` or a `MERGE` that sets a column to a value rather than its default", s_sequenceRanges, "updates that don't draw (460)");
        Assert.Contains("a statement a worf worker applies, as DDL `replicate_ddl_command` queued", s_sequenceRanges, "applied statements (420)");
        Assert.Contains("up to `worf.sequence_range_check_interval` more before the watcher cancels it", s_sequenceRanges, "a probe's time (292)");
        Assert.Contains("an `UPDATE` or a `MERGE` that sets such a column to its default", s_sequenceRanges, "an identity column's draw (421)");
        Assert.Contains("and again half `worf.sequence_range_timeout` after each that fails", s_sequenceRanges, "a waiting statement's exchanges (423)");
        Assert.Contains("a pool holder grants its children nothing from a pool block until the node that granted it has answered so", s_sequenceRanges,
            "a restarted pool holder's grants (424)");
        Assert.Contains("a node whose last call ran out of time, as one whose host drops connection attempts, after the others", s_sequenceRanges,
            "an unreachable neighbor (212)");
        Assert.DoesNotContain("leaves no block's values unused", s_sequenceRanges, "a COPY's switch (130)");
        Assert.Contains("A switch retires what its block has left", s_sequenceRanges, "a COPY's switch (130)");
        Assert.Contains("a statement that would draw from it waits for the switch as at the block's end, or makes it in its transaction",
            s_sequenceRanges, "a switch that rolled back (130, 128)");
        Assert.DoesNotContain("and stays open holds off the switch to the next block until it ends.", s_sequenceRanges, "an open holder (146)");
        Assert.Contains("holds off the range worker's own switch to the next block until it ends; from the block's last twentieth, a switch worker switches beside it instead",
            s_sequenceRanges, "an open holder (146)");

        foreach ((string name, string text) in new[] { ("README.md", s_readme), ("configuration.md", s_configuration) })
        {
            Assert.DoesNotContain("ran out of time waits a second at most, until one answers", text, $"a probe's time, in {name} (130)");
            Assert.Contains("ran out of time waits a second at most, three for the connection, until one answers", text, $"a probe's time, in {name} (130)");
            Assert.DoesNotContain("wait up to twice this in all for the switch", text, $"how long inserts wait for a switch, in {name} (130)");
            Assert.Contains("up to twice this for each block in all while its next block is still to come, or once a worker has tried and couldn't switch it, and otherwise until it comes, as for a lock",
                text, $"how long inserts wait for a switch, in {name} (130)");
        }

        Assert.Contains("The switch waits for the gate as for a lock, for as long as it takes in a switch worker", s_sequenceRanges,
            "a switch's wait for the gate (142, 210)");
        Assert.Contains("is a deadlock that PostgreSQL detects, ending one of them with SQLSTATE `40P01`", s_sequenceRanges, "a cycle through the gate (142)");

        Assert.Contains("or that changed the type of an identity column a managed sequence numbers, unless a savepoint rolled it back", s_sequenceRanges,
            "prepared column type changes (144)");
        Assert.Contains("holds up only that sequence's exchanges, which leave it out until the transaction ends", s_sequenceRanges, "prepared holders (144)");
        Assert.Contains("after its server starts, a node draws from a block another node granted it, its parent or one that was, only once a message of that node's", s_sequenceRanges,
            "a returning node (46, 74, 424)");
        Assert.Contains("is asked to reset once it's heard from again after the fence lifted, whatever it reports", s_sequenceRanges, "an unchanged report");

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
    /// The configuration says which rows of a copy in batches have the batch's earlier rows copied again: only those
    /// COPY itself refuses, while rows that don't convert, that a column's input, a NOT NULL column or a CHECK constraint
    /// refuses, or that an immediate unique index or exclusion constraint refuses, are left out; which rows a merge skips
    /// at once and how it records them; that foreign keys aren't checked; and when the copy and apply recheck deferrable
    /// constraints.
    /// </summary>
    [TestMethod]
    public void CopyDocsSayWhichRowsCopyAgain()
    {
        foreach ((string name, string text) in new[] { ("README.md", s_readme), ("configuration.md", s_configuration) })
        {
            Assert.DoesNotContain("a row that the column's input or a constraint refuses has the rows before it copied again", text,
                $"the rows copied again, in {name} (179)");
            Assert.Contains("A row that `COPY` refuses otherwise, as one whose key a concurrent transaction writes, one that a trigger refuses,",
                text, $"the rows copied again, in {name} (179)");
            Assert.DoesNotContain("without expressions or a predicate", text, $"the rows skipped at once, in {name} (40, 173)");
            Assert.Contains("and those that an immediate unique index or exclusion constraint refuses, as a row the table keeps or an earlier row of the copy holds their values, whatever the index's expressions and predicate, are skipped at once, unless a `BEFORE ROW` trigger could change them first",
                text, $"the rows a merge skips at once, in {name} (40, 173, 251)");
            Assert.Contains("skipped rows are recorded as a `MERGE` with their columns' text and their numbers in the copy", text,
                $"how a merge's skipped rows are recorded, in {name} (256)");
            Assert.DoesNotContain("or that a foreign key or a trigger refuses", text, $"foreign keys, in {name} (257)");
            Assert.Contains("Foreign keys aren't checked, as for apply.", text, $"foreign keys, in {name} (257)");
            Assert.DoesNotContain("whose recheck doesn't fire as the copy writes", text, $"the copy's deferrable rechecks, in {name} (257)");
            Assert.Contains("or one that a deferrable constraint that is initially immediate refuses as the batch's `COPY` ends, has the batch's rows before it copied again",
                text, $"the copy's deferrable rechecks, in {name} (257)");
            Assert.DoesNotContain("and those a merge skips, have no number in the copy", text, $"the rows recorded with 0, in {name} (256, 257)");
        }

        Assert.Contains("so it checks those at commit too, and rows that swap values, as one statement may, apply", s_readme,
            "apply's deferrable rechecks (253)");
    }

    // A file's words, one space apart, without the slashes that start a comment's lines.
    private static string Read(params string[] path)
        => string.Join(' ', File.ReadAllText(Path.Join([RepositoryPaths.Root, .. path])).Split([' ', '\n', '\r', '\t'], StringSplitOptions.RemoveEmptyEntries)
            .Where(word => word is not ("///" or "//")));
}
