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
    private static long s_nodeReadAt;
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
        => SequenceRanges.Enable(Worf.ReadScalar<uint>("SELECT 'worf.snowflake_node'::regclass::oid"), participants, 1, 1, null, false);

    /// <summary>
    /// Returns this node's Snowflake node number. A standby has none of its own: the number its blocks show is its
    /// primary's.
    /// </summary>
    /// <returns>The number, or null until the node has one.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.snowflake_node()
        RETURNS integer VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static int? SnowflakeNode() => SequenceRanges.Recovering() ? null : NodeNumber();

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
        // A standby would issue its primary's IDs.
        if (SequenceRanges.Recovering())
        {
            throw new PgException("25006", "cannot issue Snowflake IDs during recovery");
        }

        int node = NodeNumber() ?? throw new PgException("55000", "this node has no Snowflake node number yet",
            hint: "Run worf.enable_snowflake on one node; the others get their numbers over their subscriptions.");
        uint dboid = DatabaseId();
        long deadline = Worker.GetCurrentTimestamp() + (WaitMs * 1000);
        while (true)
        {
            long now = NowMs();
            bool wake;
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

                // The range worker hasn't reserved this far yet, which it does ahead only while sessions issue IDs. The
                // first session to wait asks it to.
                wake = !clock.Wanted;
                clock.Wanted = true;
                guard[slot] = clock;
            }

            if (wake)
            {
                Worker.WakeRangeWorker();
            }

            // The range worker hasn't reserved this far yet, as just after a restart, before its first pass, or while it
            // lags. This session reserves ahead itself, unless another process is reserving, or this transaction can't
            // write, and then waits for the reservation.
            if (ReserveAhead())
            {
                continue;
            }

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
    /// Keeps this database's clock reserved ahead, from the range worker, while sessions issue IDs, once less than half the
    /// time reserved ahead is left. A node that issues none reserves nothing, so that it writes nothing: its first ID
    /// reserves the clock itself.
    /// </summary>
    internal static void Reserve()
    {
        if (NodeNumber() is null)
        {
            return;
        }

        uint dboid = DatabaseId();
        long now = NowMs();
        using (PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> guard = Clocks.Exclusive())
        {
            WorfSnowflakeClock clock = guard[Slot(guard, dboid)];
            if (!clock.Floored || clock.ReservedMs - now >= ReserveAheadMs / 2 || (clock.LastState >> CounterBits) < now - ReserveAheadMs)
            {
                return;
            }
        }

        _ = ReserveAhead();
    }

    /// <summary>
    /// Reserves this database's clock ahead in <c>worf.snowflake_clock</c>, and gives backends the time reserved once the
    /// reservation's WAL is flushed, so that no ID passes a time a crash could forget. One process reserves at a time,
    /// so the clock only moves forward. The first reservation after a restart starts the clock past the time reserved
    /// before.
    /// </summary>
    /// <returns>Whether it reserved: not when another process is reserving, or this transaction can't write.</returns>
    private static bool ReserveAhead()
    {
        bool locked = false;
        try
        {
            bool reserved = false;
            PgTransaction.RunInSubtransaction(() => reserved = Elevated.Run(() =>
            {
                if (!Worf.ReadScalar<bool>("SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtext('worf.snowflake_clock'))"))
                {
                    return false;
                }

                locked = true;
                uint dboid = DatabaseId();
                long already;
                using (PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> guard = Clocks.Exclusive())
                {
                    already = guard[Slot(guard, dboid)].ReservedMs;
                }

                long durable = Worf.ReadScalar<long>("SELECT last_value FROM worf.snowflake_clock");
                long target = Math.Max(durable, Math.Max(already, NowMs() + ReserveAheadMs));
                if (target > durable)
                {
                    _ = Spi.Execute("SELECT pg_catalog.setval('worf.snowflake_clock', $1)", SpiParameter.Create(target));

                    // While this node issues IDs, the tables keep within an hour of when it last heard from the owner of the
                    // numbers, which shared memory records, so that it can go on issuing after a restart.
                    _ = Spi.Execute("""
                        UPDATE worf.sequence_range_node n SET reported_at = worf.sequence_range_heard(n.node_id, false, n.reported_at, false)
                        FROM worf.sequence_range r
                        WHERE r.seqoid = 'worf.snowflake_node'::regclass AND n.seqoid = r.seqoid AND n.node_id = r.owner_node
                            AND worf.sequence_range_heard(n.node_id, false, n.reported_at, false) > n.reported_at + interval '1 hour'
                        """);
                    unsafe
                    {
                        NativeMethods.XLogFlush(NativeMethods.GetXLogInsertRecPtr());
                    }
                }

                using PgLwLockExclusiveGuard<WorfSnowflakeTable, WorfSnowflakeClock> published = Clocks.Exclusive();
                int slot = Slot(published, dboid);
                WorfSnowflakeClock clock = published[slot];
                if (!clock.Floored)
                {
                    clock.LastState = Math.Max(clock.LastState, durable << CounterBits);
                    clock.Floored = true;
                }

                clock.ReservedMs = Math.Max(clock.ReservedMs, target);
                published[slot] = clock;
                return true;
            }));
            return reserved;
        }
        catch (PgException)
        {
            return false;
        }
        finally
        {
            if (locked)
            {
                Elevated.Run(() => _ = Worf.Read("SELECT pg_catalog.pg_advisory_unlock(pg_catalog.hashtext('worf.snowflake_clock'))", 0));
            }
        }
    }

    /// <summary>
    /// Gets the sequence that hands out Snowflake node numbers.
    /// </summary>
    /// <returns>Its oid, or zero before worf is installed.</returns>
    internal static uint NodeSequence() => Worf.ReadScalar<uint?>("SELECT to_regclass('worf.snowflake_node')::oid") ?? 0;

    /// <summary>
    /// Gets the time Snowflake IDs carry: milliseconds since 2025.
    /// </summary>
    /// <returns>The time.</returns>
    internal static long NowMs() => (Worker.GetCurrentTimestamp() - EpochOffset) / 1000;

    /// <summary>
    /// Gets how far this database's Snowflake clock is reserved, which no ID it issued has passed.
    /// </summary>
    /// <returns>The time, in milliseconds since 2025.</returns>
    internal static long ReservedClock() => Worf.ReadScalar<long>("SELECT last_value FROM worf.snowflake_clock");

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
        // The number is read again when the blocks change, and each minute, since the lease on it can run out.
        long version = SequenceRanges.Version.Value;
        long now = Worker.GetCurrentTimestamp();
        if (version == s_nodeVersion && now - s_nodeReadAt < 60_000_000)
        {
            return s_node;
        }

        ulong sysid;
        uint timeline;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
            timeline = SequenceRanges.Timeline();
        }

        // A node that hasn't heard from the owner of the numbers, directly or through its neighbors, for half the time
        // after which the owner gives the number of a node that stopped reporting to another, stops using it, and its
        // range worker gives it up.
        long? lo = Elevated.Run(() => Worf.ReadScalar<long?>("""
            SELECT b.lo FROM worf.sequence_range_block b JOIN worf.local_node l ON l.node_id = b.node_id
                JOIN worf.sequence_range r ON r.seqoid = b.seqoid
            WHERE b.seqoid = 'worf.snowflake_node'::regclass AND b.kind = 'n' AND b.state = 'active'
                AND b.sysid = $1 AND b.timeline = $2
                AND (r.owner_node = l.node_id
                    OR EXISTS (SELECT 1 FROM worf.sequence_range_node n
                        WHERE n.seqoid = b.seqoid AND n.node_id = r.owner_node
                            AND worf.sequence_range_heard(n.node_id, false, n.reported_at, false) > now() - $3::interval / 2))
            """, SpiParameter.Create((decimal)sysid), SpiParameter.Create((long)timeline),
            SpiParameter.Create(SequenceRanges.SnowflakeReuseAfter())));
        s_node = lo is long value ? (int)value : null;
        s_nodeVersion = version;
        s_nodeReadAt = now;
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
