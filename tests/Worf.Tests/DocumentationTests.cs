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
        Assert.DoesNotContain("and is made again in the next round without giving way", s_sequenceRanges, "a slow neighbor (125, 763)");
        Assert.Contains("once a probe's connection is made, the node is called over it at once", s_sequenceRanges, "a slow neighbor (125, 763)");
        Assert.DoesNotContain("no more than once every ten minutes for each neighbor", s_sequenceRanges, "made-up resets (130, 211)");
        Assert.DoesNotContain("so a grantor resets a node no more often than it grants it blocks", s_sequenceRanges, "a grantor's lifts (211)");
        Assert.Contains("has a limit of its own, so that another request's reset doesn't hold it up, which starts at ten seconds rather than ten minutes",
            s_sequenceRanges, "a grantor's lifts (211)");
        Assert.DoesNotContain("without waiting for them, and commits at once", s_sequenceRanges, "a switch worker's wait (130/146)");
        Assert.DoesNotContain("three seconds a minute at most", s_sequenceRanges, "an unreachable neighbor's cost (294)");
        Assert.DoesNotContain("and then for up to that long ten, twenty and forty seconds after the call before ends", s_sequenceRanges,
            "an unreachable neighbor's cost (294, 660, 763)");
        Assert.DoesNotContain("for its first call's `worf.sequence_range_timeout` alone", s_sequenceRanges, "an unreachable neighbor's cost (763, 804)");
        Assert.Contains("So a node that never answers, or answers slowly, holds up no other node's calls", s_sequenceRanges,
            "an unreachable or slow neighbor's cost (763)");
        Assert.Contains("and takes them on as their sockets let it, between its other work and as it waits between rounds, one call at a time with each node",
            s_sequenceRanges, "calls made without waiting (763)");
        Assert.Contains("and the delay holds until a call is answered", s_sequenceRanges, "a probe that answers and a call that doesn't (804)");
        Assert.DoesNotContain("which a neighbor whose own route to the owner goes through the node can't be, as a node reports no distance to its parent", s_sequenceRanges,
            "a ring's parents (705)");
        Assert.Contains("and reports its route, the nodes from its parent to the owner, to its other neighbors, which take no route that goes through themselves", s_sequenceRanges,
            "a ring's parents (604, 705)");
        Assert.Contains("the CHECK constraints and trigger conditions of the tables a statement, a `COPY FROM` or a worf worker's row writes", s_sequenceRanges,
            "the expressions a statement starts as it runs (422, 700)");
        Assert.Contains("A forced lift's request to reset from the node's grantor", s_sequenceRanges, "a grantor's lifts (352)");
        Assert.Contains("the grantor's lifts may reset the node 9 times in their first hour and 14 in their first day", s_sequenceRanges, "a grantor's lifts (353)");
        Assert.Contains("A switch worker waits for the gate before it locks the sequence's range row", s_sequenceRanges, "a paused gate holder (291)");
        Assert.Contains("A call whose sequence those don't name, as one computed as it runs, one an `ALTER TABLE` that changes a column's type starts, or one a function in a procedural language other than SQL and PL/pgSQL starts", s_sequenceRanges,
            "a returning node's draws (292, 293, 422)");
        Assert.Contains("or, within it, a PL/pgSQL expression, which PL/pgSQL starts outside a plan, as a trigger's or a function's that partition pruning calls as the plan starts", s_sequenceRanges,
            "a returning node's PL/pgSQL draws (422, 601)");
        Assert.Contains("Nothing else waits: `nextval` of a sequence no range manages, or of a temporary one, that those name", s_sequenceRanges,
            "what doesn't wait for a grantor (351, 422)");
        Assert.Contains("whose failure the worker applies again later under any `worf.exception_behaviour`", s_sequenceRanges, "an applied statement's wait (420)");
        Assert.Contains("an `UPDATE` or a `MERGE` that sets a column to a value rather than its default", s_sequenceRanges, "updates that don't draw (460)");
        Assert.Contains("a statement a worf worker applies, as DDL `replicate_ddl_command` queued", s_sequenceRanges, "applied statements (420)");
        Assert.DoesNotContain("before the watcher cancels it", s_sequenceRanges, "a call's time (292, 763)");
        Assert.Contains("an `UPDATE` or a `MERGE` that sets such a column to its default", s_sequenceRanges, "an identity column's draw (421)");
        Assert.Contains("and again half `worf.sequence_range_timeout` after each that fails", s_sequenceRanges, "a waiting statement's exchanges (423)");
        Assert.Contains("a pool holder grants its children nothing from a pool block until the node that granted it has answered so", s_sequenceRanges,
            "a restarted pool holder's grants (424)");
        Assert.Contains("A node whose last call ran out of time, as one whose host drops connection attempts, is probed after a delay", s_sequenceRanges,
            "an unreachable neighbor (212, 763)");
        Assert.DoesNotContain("leaves no block's values unused", s_sequenceRanges, "a COPY's switch (130)");
        Assert.Contains("A switch retires what its block has left", s_sequenceRanges, "a COPY's switch (130)");
        Assert.Contains("a statement that would draw from it waits for the switch as at the block's end, or makes it in its transaction",
            s_sequenceRanges, "a switch that rolled back (130, 128)");
        Assert.DoesNotContain("and stays open holds off the switch to the next block until it ends.", s_sequenceRanges, "an open holder (146)");
        Assert.Contains("holds off the range worker's own switch to the next block until it ends; from the block's last twentieth, a switch worker switches beside it instead",
            s_sequenceRanges, "an open holder (146)");

        foreach ((string name, string text) in new[] { ("README.md", s_readme), ("configuration.md", s_configuration) })
        {
            Assert.DoesNotContain("ran out of time waits a second at", text, $"a probe's time, in {name} (130, 660, 763)");
            Assert.Contains("which the exchange worker makes without waiting on either", text, $"a call's time, in {name} (763)");
            Assert.Contains("ran out of time is probed, by a connection that has this long, until one answers", text, $"a probe's time, in {name} (763)");
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
        Assert.Contains("after its server starts, a node draws from a block of a sequence another node granted it, its parent or one that was, whether it subscribes to that node or only answers its calls, only once a message of that node's", s_sequenceRanges,
            "a returning node (46, 74, 424, 1404)");
        Assert.Contains("saying that it has checked the node's report of the sequence since then", s_sequenceRanges, "a returning node's wait for each sequence (1100, 1211)");
        Assert.DoesNotContain("a restarted node waits for its grantor until a call has taken every sequence in", s_sequenceRanges, "a returning node's wait (1211)");
        Assert.DoesNotContain("with time left for its answer", s_sequenceRanges, "a call's answer (1213)");
        Assert.Contains("once a grant has committed, and reached the synchronous standbys that commits wait for, the range worker asks the nodes that subscribe to its database to call it", s_sequenceRanges,
            "an owner's own blocks (1101, 1160, 1500, 1702)");
        Assert.Contains("as a notification to the connection each one's exchanges keep, and they do at once, though their replication streams are behind a large transaction or a delay",
            s_sequenceRanges, "a word behind a large transaction (1700)");
        Assert.Contains("so the provider doesn't end the stream at `wal_sender_timeout`", s_sequenceRanges, "a delay longer than wal_sender_timeout (1703)");
        Assert.Contains("which the other nodes take as they next hear from the owner", s_sequenceRanges, "altered block sizes (1723)");
        Assert.Contains("and every other node it subscribes to, or that calls it, that reports the sequence, and so may have heard of its draws", s_sequenceRanges,
            "the neighbors a start waits for (1601, 1605)");
        Assert.Contains("a single-user server keeps its start as its first statement runs", s_sequenceRanges, "a single-user start (1602)");
        Assert.DoesNotContain("not at all once the node has written past where they saw its WAL position", s_sequenceRanges, "a restore's notice (1100, 1211)");
        Assert.Contains("Each server keeps its last 32 starts", s_sequenceRanges, "a restore across restarts (1400)");
        Assert.DoesNotContain("as contrib's `autoinc` trigger does, isn't seen", s_sequenceRanges, "a trigger's arguments (800)");
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
