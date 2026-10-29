using System.Globalization;
using Ankus.Postgres;

[assembly: PgSql("snowflake", """
    CREATE SEQUENCE worf.snowflake_node AS smallint MINVALUE 0 MAXVALUE 1023;
    CREATE SEQUENCE worf.snowflake_clock MINVALUE 0;
    """, Requires = ["sequence-range-tables"])]

namespace Worf;

/// <summary>
/// Snowflake IDs: 64-bit keys made of the time in milliseconds since 2025, this node's number and a counter, so keys
/// from different nodes never collide and each node's keys only grow. Node numbers are single values of
/// <c>worf.snowflake_node</c> under range management, which never grants a value twice. The range worker reserves the
/// clock ahead in <c>worf.snowflake_clock</c>, so a node never issues a time it may have issued before a restart, even
/// when its clock went back.
/// </summary>
internal static class SnowflakeIds
{
    private const int MaxDatabases = 64;
    private const int CounterBits = 12;
    private const int NodeBits = 10;
    private const int TimeBits = 41;
    private const long ReserveAheadMs = 10000;
    private const long WaitMs = 10000;

    // Microseconds from PostgreSQL's epoch, 2000-01-01, to Snowflake's, 2025-01-01.
    private const long EpochOffset = 9132L * 86400 * 1000000;

    private static long s_nodeVersion = -1;
    private static int? s_node;

    /// <summary>
    /// Gets the shared table of each database's clock.
    /// </summary>
    internal static PgLwLock<WorfSnowflakeTable, WorfSnowflakeClock> Clocks { get; } = new("worf_snowflake_clocks");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit() => PgSharedMemory.Initialize(Clocks, MaxDatabases, static () => default, static _ => default);

    /// <summary>
    /// Starts handing out Snowflake node numbers, with this node as their owner.
    /// </summary>
    /// <param name="participants">The other nodes that issue Snowflake IDs, or null to wait for activation.</param>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.enable_snowflake(participants text[] DEFAULT NULL)
        RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void EnableSnowflake(string?[]? participants)
        => SequenceRanges.Enable(Spi.ExecuteScalar<uint>("SELECT 'worf.snowflake_node'::regclass::oid"), participants, 1, 1, null, false);

    /// <summary>
    /// Returns this node's Snowflake node number.
    /// </summary>
    /// <returns>The number, or null until the node has one.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.snowflake_node()
        RETURNS integer VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static int? SnowflakeNode() => NodeNumber();

    /// <summary>
    /// Returns a new Snowflake ID.
    /// </summary>
    /// <returns>The ID.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.snowflake_id()
        RETURNS bigint VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static long SnowflakeId()
    {
        int node = NodeNumber() ?? throw new PgException("55000", "this node has no Snowflake node number yet",
            hint: "Run worf.enable_snowflake on one node; the others get their numbers over their subscriptions.");
        uint dboid = DatabaseId();
        long deadline = Worker.GetCurrentTimestamp() + (WaitMs * 1000);
        while (true)
        {
            long now = NowMs();
            using (PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> guard = Clocks.Exclusive())
            {
                int slot = Slot(guard, dboid);
                WorfSnowflakeClock clock = guard[slot];
                if (clock.Floored)
                {
                    // A new node number starts past the last time the old one used, so the keys still only grow.
                    long last = clock.LastNode == node ? clock.LastState : ((clock.LastState >> CounterBits) + 1) << CounterBits;
                    long state = Math.Max(now << CounterBits, clock.LastNode == node ? last + 1 : last);
                    long ms = state >> CounterBits;
                    if (ms < clock.ReservedMs)
                    {
                        if (ms >= 1L << TimeBits)
                        {
                            throw new PgException("2200H", "Snowflake IDs ran out of time bits");
                        }

                        clock.LastState = state;
                        clock.LastNode = node;
                        guard[slot] = clock;
                        return (ms << (NodeBits + CounterBits)) | ((long)node << CounterBits) | (state & ((1L << CounterBits) - 1));
                    }
                }
            }

            // The range worker hasn't reserved this far yet.
            if (Worker.GetCurrentTimestamp() >= deadline)
            {
                throw new PgException("55000", "the Snowflake clock isn't reserved this far yet",
                    hint: "The database's sequence range worker reserves it; check that it runs.");
            }

            PgInterrupts.Check();
            Worker.WaitLatch(10);
        }
    }

    /// <summary>
    /// Keeps this database's clock reserved ahead, from the range worker. After a restart, it first raises the clock to
    /// the time reserved before, and IDs wait for that.
    /// </summary>
    internal static void Reserve()
    {
        if (NodeNumber() is null)
        {
            return;
        }

        uint dboid = DatabaseId();
        long now = NowMs();
        bool floored;
        long reserved;
        using (PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> guard = Clocks.Exclusive())
        {
            WorfSnowflakeClock clock = guard[Slot(guard, dboid)];
            floored = clock.Floored;
            reserved = clock.ReservedMs;
        }

        long durable = Spi.ExecuteScalar<long>("SELECT last_value FROM worf.snowflake_clock");
        long target = Math.Max(durable, Math.Max(reserved, now + ReserveAheadMs));
        if (floored && reserved - now >= ReserveAheadMs / 2)
        {
            return;
        }

        if (target > durable)
        {
            _ = Spi.Execute("SELECT pg_catalog.setval('worf.snowflake_clock', $1)", SpiParameter.Create(target));
        }

        using (PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> guard = Clocks.Exclusive())
        {
            int slot = Slot(guard, dboid);
            WorfSnowflakeClock clock = guard[slot];
            if (!clock.Floored)
            {
                clock.LastState = Math.Max(clock.LastState, durable << CounterBits);
                clock.Floored = true;
            }

            clock.ReservedMs = Math.Max(clock.ReservedMs, target);
            guard[slot] = clock;
        }
    }

    private static long NowMs() => (Worker.GetCurrentTimestamp() - EpochOffset) / 1000;

    /// <summary>
    /// Finds this database's clock, or claims an entry for it.
    /// </summary>
    private static int Slot(PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> guard, uint dboid)
    {
        int free = -1;
        for (int i = 0; i < MaxDatabases; i++)
        {
            if (guard[i].Dboid == dboid)
            {
                return i;
            }

            if (free < 0 && guard[i].Dboid == 0)
            {
                free = i;
            }
        }

        if (free < 0)
        {
            throw new PgException("53000", "too many databases issue Snowflake IDs");
        }

        guard[free] = new WorfSnowflakeClock { Dboid = dboid };
        WorfSnowflakeTable table = guard.Header;
        table.Used++;
        guard.Header = table;
        return free;
    }

    /// <summary>
    /// Reads this node's number, the value of its active block of <c>worf.snowflake_node</c> received under its current
    /// identity, again whenever its blocks change.
    /// </summary>
    private static int? NodeNumber()
    {
        long version = SequenceRanges.Version.Value;
        if (version == s_nodeVersion)
        {
            return s_node;
        }

        ulong sysid;
        uint timeline;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
            timeline = NativeMethods.GetWALInsertionTimeLine();
        }

        long? lo = Spi.ExecuteScalar<long?>("""
            SELECT b.lo FROM worf.sequence_range_block b JOIN worf.local_node l ON l.node_id = b.node_id
            WHERE b.seqoid = 'worf.snowflake_node'::regclass AND b.kind = 'n' AND b.state = 'active'
                AND b.sysid = $1::numeric AND b.timeline = $2
            """, SpiParameter.Create(sysid.ToString(CultureInfo.InvariantCulture)), SpiParameter.Create((long)timeline));
        s_node = lo is long value ? (int)value : null;
        s_nodeVersion = version;
        return s_node;
    }

    private static uint DatabaseId()
    {
        unsafe
        {
            return NativeGlobals.MyDatabaseId;
        }
    }
}
