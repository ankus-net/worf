using System.Globalization;

namespace Worf.Tests;

/// <summary>
/// Covers how often a neighbor may have a node reset its sequences' blocks, by working out the windows of a run of
/// requests: a reset request can't be told from one made up, and a made-up one may fence an owner's root until every
/// participant reports.
/// </summary>
[TestClass]
public sealed class ResetWindowTests
{
    private const long Minute = 60_000_000;

    /// <summary>
    /// A neighbor's first request resets at once, with the requests of the minute after it, which one rollback brings
    /// for every sequence, and the next comes ten minutes on, then twice as long after each reset within a day of the
    /// one before, up to a day, so that requests made up to reset a node come ever more seldom, once a day at most while
    /// they keep coming; after a day in which one could have reset the node and none came, a request again resets at
    /// once and the next again comes ten minutes on.
    /// </summary>
    [TestMethod]
    public void ResetRequestsBackOffUpToADay()
    {
        long resetAt = 0;
        long resetEvery = 0;
        long start = 1_000 * Minute;
        List<string> allowed = [];

        // A request every minute for a day, none for two days, and then a request every minute again.
        foreach (long minute in Enumerable.Range(0, (24 * 60) + 1).Concat(Enumerable.Range(3 * 24 * 60, 20)))
        {
            long now = start + (minute * Minute);
            (long? window, resetAt, resetEvery) = SequenceRangeHeard.ResetWindow(resetAt, resetEvery, now);
            if (window == now)
            {
                allowed.Add(minute.ToString(CultureInfo.InvariantCulture));
            }
        }

        // 10, 20, 40, 80, 160, 320 and 640 minutes apart, the next to come 1,280 minutes on, past the day; then, after the
        // quiet, at once and ten minutes on.
        Assert.AreEqual("0,10,30,70,150,310,630,1270,4320,4330", string.Join(",", allowed), "the minutes at which a request reset the node");

        // A request every minute for ten days resets the node at most once a day after the first day, as the window
        // stays a day while the requests keep coming.
        (resetAt, resetEvery) = (0, 0);
        int[] perDay = new int[10];
        for (long minute = 0; minute < 10 * 24 * 60; minute++)
        {
            long now = start + (minute * Minute);
            (long? window, resetAt, resetEvery) = SequenceRangeHeard.ResetWindow(resetAt, resetEvery, now);
            if (window == now)
            {
                perDay[minute / (24 * 60)]++;
            }
        }

        Assert.AreEqual("8,1,1,1,1,1,1,1,1,1", string.Join(",", perDay), "the resets of each of ten days of a request a minute");

        // A request within the minute after a reset counts in its window, and one after it waits.
        (long? sameMinute, long at, long every) = SequenceRangeHeard.ResetWindow(start, 600_000_000, start + (Minute / 2));
        Assert.AreEqual(start, sameMinute, "a request in the minute after a reset");
        Assert.IsNull(SequenceRangeHeard.ResetWindow(at, every, start + (2 * Minute)).Window, "a request two minutes after a reset");
    }
}
