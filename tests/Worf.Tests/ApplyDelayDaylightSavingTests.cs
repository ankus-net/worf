namespace Worf.Tests;

/// <summary>
/// Covers what pglogical's disabled <c>t/030_pglogical_daylight_times_switching.pl</c> checks, apply delay across a
/// daylight saving change, by calculating the delay for commit times on either side of a transition.
/// </summary>
/// <remarks>
/// pglogical disables the Perl test because it changes the system clock. Like it, these use America/Los_Angeles and a
/// one-second apply delay.
/// </remarks>
[TestClass]
public sealed class ApplyDelayDaylightSavingTests
{
    private const long ApplyDelayMilliseconds = 1000;

    private static readonly DateTimeOffset s_postgresEpoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly TimeZoneInfo s_losAngeles = TimeZoneInfo.FindSystemTimeZoneById("America/Los_Angeles");

    /// <summary>
    /// A transaction that commits just before clocks fall back waits for the rest of its delay, not an hour more or
    /// less.
    /// </summary>
    [TestMethod]
    public void DelayAcrossFallBack()
    {
        DateTimeOffset commit = new(2016, 11, 6, 1, 59, 59, 500, TimeSpan.FromHours(-7));
        DateTimeOffset current = new(2016, 11, 6, 1, 0, 0, 200, TimeSpan.FromHours(-8));
        AssertCrossesTransition(commit, current);

        Assert.AreEqual(300_000, Apply.ApplyDelaySleep(ToTimestamp(commit), ToTimestamp(current), ApplyDelayMilliseconds));
    }

    /// <summary>
    /// A transaction that commits just before clocks spring forward waits for the rest of its delay, not an hour less
    /// or more.
    /// </summary>
    [TestMethod]
    public void DelayAcrossSpringForward()
    {
        DateTimeOffset commit = new(2017, 3, 12, 1, 59, 59, 500, TimeSpan.FromHours(-8));
        DateTimeOffset current = new(2017, 3, 12, 3, 0, 0, 200, TimeSpan.FromHours(-7));
        AssertCrossesTransition(commit, current);

        Assert.AreEqual(300_000, Apply.ApplyDelaySleep(ToTimestamp(commit), ToTimestamp(current), ApplyDelayMilliseconds));
    }

    /// <summary>
    /// A transaction whose delay has passed by the time clocks fall back applies at once, although the wall clock reads
    /// an hour earlier than its commit.
    /// </summary>
    [TestMethod]
    public void ElapsedDelayAcrossFallBack()
    {
        DateTimeOffset commit = new(2016, 11, 6, 1, 59, 59, 0, TimeSpan.FromHours(-7));
        DateTimeOffset current = new(2016, 11, 6, 1, 0, 1, 0, TimeSpan.FromHours(-8));
        AssertCrossesTransition(commit, current);

        Assert.AreEqual(0, Apply.ApplyDelaySleep(ToTimestamp(commit), ToTimestamp(current), ApplyDelayMilliseconds));
    }

    /// <summary>
    /// A transaction whose delay has passed by the time clocks spring forward applies at once.
    /// </summary>
    [TestMethod]
    public void ElapsedDelayAcrossSpringForward()
    {
        DateTimeOffset commit = new(2017, 3, 12, 1, 59, 59, 0, TimeSpan.FromHours(-8));
        DateTimeOffset current = new(2017, 3, 12, 3, 0, 1, 0, TimeSpan.FromHours(-7));
        AssertCrossesTransition(commit, current);

        Assert.AreEqual(0, Apply.ApplyDelaySleep(ToTimestamp(commit), ToTimestamp(current), ApplyDelayMilliseconds));
    }

    /// <summary>
    /// A commit time ahead of the local clock, from clock drift between the servers, doesn't make the apply worker wait.
    /// </summary>
    [TestMethod]
    public void CommitAheadOfLocalClock()
    {
        DateTimeOffset commit = new(2016, 11, 6, 1, 0, 0, 500, TimeSpan.FromHours(-8));
        DateTimeOffset current = new(2016, 11, 6, 1, 59, 59, 900, TimeSpan.FromHours(-7));

        Assert.AreEqual(0, Apply.ApplyDelaySleep(ToTimestamp(commit), ToTimestamp(current), ApplyDelayMilliseconds));
    }

    private static void AssertCrossesTransition(DateTimeOffset commit, DateTimeOffset current)
    {
        Assert.AreEqual(commit.Offset, s_losAngeles.GetUtcOffset(commit), "commit offset");
        Assert.AreEqual(current.Offset, s_losAngeles.GetUtcOffset(current), "current offset");
        Assert.AreNotEqual(commit.Offset, current.Offset, "the times are on either side of a transition");
    }

    private static long ToTimestamp(DateTimeOffset time) => (time - s_postgresEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
}
