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

    // The parts and multiples of two days and a window that a pausing neighbor waits, and the minutes it waits more.
    private static readonly double[] s_times = [0.5, 1, 2, 3];
    private static readonly int[] s_extras = [0, 1, 30];

    /// <summary>
    /// A neighbor's first request resets at once, with the requests of the minute after it, which one rollback brings
    /// for every sequence, and the next comes ten minutes on, then twice as long after each reset within a day of the
    /// last window's end, up to a day, so that requests made up to reset a node come ever more seldom, once a day at most
    /// while they keep coming; after a pause, a request again resets at once, and the window halves for each two days
    /// and a window more of the pause.
    /// </summary>
    [TestMethod]
    public void ResetRequestsBackOffUpToADay()
    {
        long resetAt = 0;
        long resetEvery = 0;
        long start = 1_000 * Minute;
        List<string> allowed = [];

        // A request every minute for a day; none for two days, then a request every minute for two hours; and none for
        // four times two days and a window more, then a request every minute for two hours again.
        int[] after = [3 * 24 * 60, (3 * 24 * 60) + 1280 + (4 * ((2 * 24 * 60) + 1280))];
        foreach (long minute in Enumerable.Range(0, (24 * 60) + 1).Concat(after.SelectMany(at => Enumerable.Range(at, 120))))
        {
            long now = start + (minute * Minute);
            (long? window, resetAt, resetEvery) = SequenceRangeHeard.ResetWindow(resetAt, resetEvery, now);
            if (window == now)
            {
                allowed.Add(minute.ToString(CultureInfo.InvariantCulture));
            }
        }

        // 10, 20, 40, 80, 160, 320 and 640 minutes apart, the next to come 1,280 minutes on, past the day; after the
        // shorter pause, at once, the window staying 1,280 minutes; after the longer, at once, and 80 minutes on.
        Assert.AreEqual("0,10,30,70,150,310,630,1270,4320,22240,22320", string.Join(",", allowed), "the minutes at which a request reset the node");

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

        // A neighbor that pauses its requests after each run of resets, for a day, or for some part or multiple of two
        // days and its window, which halves the window, resets the node no more often in ten days than one that keeps
        // asking.
        int steady = perDay.Sum();
        List<Func<long, long>> pauses = [_ => 24 * 60, _ => (24 * 60) + 1, _ => 2 * 24 * 60];
        foreach (double times in s_times)
        {
            foreach (int extra in s_extras)
            {
                pauses.Add(window => (long)(times * ((2 * 24 * 60) + window)) + extra);
            }
        }

        foreach (int run in Enumerable.Range(1, 9))
        {
            foreach (Func<long, long> pause in pauses)
            {
                (resetAt, resetEvery) = (0, 0);
                int resets = 0;
                long minute = 0;
                while (minute < 10 * 24 * 60)
                {
                    long now = start + (minute * Minute);
                    (long? window, resetAt, resetEvery) = SequenceRangeHeard.ResetWindow(resetAt, resetEvery, now);
                    minute += window == now && ++resets % run == 0 ? (resetEvery / Minute) + pause(resetEvery / Minute) : 1;
                }

                Assert.IsLessThanOrEqualTo(steady, resets, $"the resets in ten days of a neighbor that pauses after each {run} resets");
            }
        }

        // A request within the minute after a reset counts in its window, and one after it waits.
        (long? sameMinute, long at, long every) = SequenceRangeHeard.ResetWindow(start, 600_000_000, start + (Minute / 2));
        Assert.AreEqual(start, sameMinute, "a request in the minute after a reset");
        Assert.IsNull(SequenceRangeHeard.ResetWindow(at, every, start + (2 * Minute)).Window, "a request two minutes after a reset");
    }
}
