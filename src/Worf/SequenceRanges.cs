using System.Globalization;
using Ankus.Postgres;

[assembly: PgSql("sequence-range-tables", """
    CREATE TABLE worf.sequence_range (
        seqoid regclass PRIMARY KEY,
        owner_node oid NOT NULL,
        epoch integer NOT NULL,
        range_size bigint NOT NULL,
        max_range_size bigint NOT NULL,
        state text NOT NULL,
        parent_node oid,
        distance integer,
        request_size bigint NOT NULL,
        high_water bigint,
        watermark bigint,
        participants name[],
        pool_start bigint,
        root_start bigint,
        root_end bigint NOT NULL,
        peak_rate double precision NOT NULL DEFAULT 0,
        switched_at timestamptz,
        exchanged_at timestamptz,
        fenced_at timestamptz,
        detail text
    );

    CREATE TABLE worf.sequence_range_block (
        block_id uuid PRIMARY KEY,
        seqoid regclass NOT NULL,
        epoch integer NOT NULL,
        kind "char" NOT NULL,
        lo bigint NOT NULL,
        hi bigint NOT NULL,
        state text NOT NULL,
        from_node oid,
        node_id oid NOT NULL,
        sysid numeric(20,0) NOT NULL,
        timeline bigint NOT NULL,
        received_at timestamptz NOT NULL,
        activated_at timestamptz,
        retired_at timestamptz,
        reserved boolean NOT NULL DEFAULT false,
        not_before bigint
    );

    CREATE TABLE worf.sequence_range_restore (
        name text PRIMARY KEY,
        owner_node oid NOT NULL,
        epoch integer NOT NULL,
        range_size bigint NOT NULL,
        max_range_size bigint NOT NULL,
        root_start bigint,
        root_end bigint
    );

    CREATE TABLE worf.sequence_range_free (
        block_id uuid NOT NULL REFERENCES worf.sequence_range_block,
        lo bigint NOT NULL,
        hi bigint NOT NULL,
        not_before bigint,
        PRIMARY KEY (block_id, lo)
    );

    CREATE TABLE worf.sequence_range_grant (
        grant_id uuid PRIMARY KEY,
        request_id uuid NOT NULL UNIQUE,
        seqoid regclass NOT NULL,
        from_block uuid REFERENCES worf.sequence_range_block,
        grantee_id oid NOT NULL,
        grantee_name name NOT NULL,
        kind "char" NOT NULL,
        lo bigint NOT NULL,
        hi bigint NOT NULL,
        state text NOT NULL,
        granted_at timestamptz NOT NULL,
        abandoned_at timestamptz,
        not_before bigint
    );

    CREATE TABLE worf.sequence_range_request (
        request_id uuid PRIMARY KEY,
        seqoid regclass NOT NULL,
        parent_node oid NOT NULL,
        kind "char" NOT NULL,
        size bigint NOT NULL,
        lo bigint,
        hi bigint,
        state text NOT NULL DEFAULT 'open',
        created_at timestamptz NOT NULL,
        reserved boolean NOT NULL DEFAULT false
    );

    CREATE TABLE worf.sequence_range_node (
        seqoid regclass NOT NULL,
        node_id oid NOT NULL,
        node_name name NOT NULL,
        via_node oid NOT NULL,
        sysid numeric(20,0),
        timeline bigint,
        wal_lsn pg_lsn,
        reply_lsn pg_lsn,
        epoch integer,
        state text,
        distance integer,
        high_water bigint,
        watermark bigint,
        reset_lsn pg_lsn,
        reset_done pg_lsn,
        held_at timestamptz,
        holds bigint[],
        clock bigint,
        reported_at timestamptz NOT NULL,
        PRIMARY KEY (seqoid, node_id)
    );

    CREATE TABLE worf.sequence_range_link (
        slot_name name PRIMARY KEY,
        node_id oid NOT NULL
    );

    CREATE VIEW worf.sequence_ranges AS
        SELECT r.seqoid AS sequence, coalesce(o.node_name, ro.node_name) AS owner, r.epoch, coalesce(p.node_name, rp.node_name) AS parent,
            CASE WHEN r.fenced_at IS NOT NULL THEN 'fenced' ELSE r.state END AS state,
            a.lo AS active_lo, a.hi AS active_hi,
            CASE
                WHEN a.block_id IS NULL THEN NULL
                WHEN s.seqincrement > 0 THEN
                    (a.hi - CASE WHEN l.last_value BETWEEN a.lo AND a.hi THEN l.last_value ELSE a.lo::numeric - s.seqincrement END) / s.seqincrement
                ELSE (CASE WHEN l.last_value BETWEEN a.lo AND a.hi THEN l.last_value ELSE a.hi::numeric - s.seqincrement END - a.lo) / -s.seqincrement
            END::bigint AS values_left,
            n.lo AS next_lo, n.hi AS next_hi, r.peak_rate AS insert_rate, r.switched_at AS last_switch,
            r.exchanged_at AS last_exchange, r.detail
        FROM worf.sequence_range r
            JOIN pg_catalog.pg_sequence s ON s.seqrelid = r.seqoid
            LEFT JOIN worf.node o ON o.node_id = r.owner_node
            LEFT JOIN worf.node p ON p.node_id = r.parent_node
            LEFT JOIN worf.sequence_range_node ro ON ro.seqoid = r.seqoid AND ro.node_id = r.owner_node
            LEFT JOIN worf.sequence_range_node rp ON rp.seqoid = r.seqoid AND rp.node_id = r.parent_node
            LEFT JOIN worf.sequence_range_block a
                ON a.seqoid = r.seqoid AND a.epoch = r.epoch AND a.kind = 'n' AND a.state = 'active'
            -- The next block is the one the sequence switches to next: one reserved for a load, or else the first received.
            LEFT JOIN LATERAL (
                SELECT b.lo, b.hi FROM worf.sequence_range_block b
                WHERE b.seqoid = r.seqoid AND b.epoch = r.epoch AND b.kind = 'n' AND b.state = 'next'
                ORDER BY b.reserved DESC, b.received_at
                LIMIT 1) n ON true
            -- A sequence that hasn't switched to its active block yet, as while it resets, has all of the block left.
            CROSS JOIN LATERAL (SELECT worf.sequence_range_position(r.seqoid)::numeric AS last_value) l;

    CREATE VIEW worf.sequence_range_grants AS
        SELECT g.seqoid AS sequence, g.grantee_name AS grantee, g.kind, g.lo, g.hi, g.state, g.granted_at
        FROM worf.sequence_range_grant g;
    """, Requires = ["node-tables"])]

namespace Worf;

/// <summary>
/// Automatic sequence range management: each node draws a managed sequence's values from blocks that worf assigns to
/// that node alone, so rows inserted on different nodes never share a key.
/// </summary>
internal static unsafe partial class SequenceRanges
{
    /// <summary>
    /// The name of the trigger that checks the values new rows hold.
    /// </summary>
    internal const string GuardName = "worf_sequence_range_guard";

    /// <summary>
    /// The name of the trigger that checks the values updates write.
    /// </summary>
    internal const string UpdateGuardName = "worf_sequence_range_guard_update";

    private const string SequenceLimitExceeded = "2200H";
    private const string FeatureNotSupported = "0A000";

    // Types whose values the guard reads: smallint, integer, bigint and numeric.
    private const uint Int2Oid = 21;
    private const uint Int4Oid = 23;
    private const uint Int8Oid = 20;
    private const uint NumericOid = 1700;

    private static readonly string[] s_guardNames = [GuardName, UpdateGuardName];

    private static bool s_changedOnCommit;

    // The furthest position each sequence's active block has reached, which the range worker tracks to notice its
    // last value moving backwards.
    private static readonly Dictionary<uint, (Guid Block, long Position)> s_positions = [];

    // An allocator gives a Snowflake node number that a node gave up back for use once a day has passed, so the old
    // holder's IDs, which carry their times, can't meet the new holder's.
    private static readonly TimeSpan s_snowflakeReuseAfter = TimeSpan.FromDays(1);

    // When a block that lasted less than an hour is followed by a bigger request, and one that lasted more than a day by
    // a smaller one.
    private static readonly TimeSpan s_growBelow = TimeSpan.FromHours(1);
    private static readonly TimeSpan s_shrinkAbove = TimeSpan.FromDays(1);

    /// <summary>
    /// Gets how often the range worker checks each managed sequence.
    /// </summary>
    [PgGucInt("worf.sequence_range_check_interval", 1000, "Sets how often each managed sequence is checked.",
        Minimum = 10, Maximum = int.MaxValue, Unit = PgGucUnit.Milliseconds, Context = PgGucContext.Sighup)]
    internal static partial int CheckInterval { get; }

    /// <summary>
    /// Gets the connection and statement timeout for an exchange with another node.
    /// </summary>
    [PgGucInt("worf.sequence_range_timeout", 10000, "Sets the timeout for a sequence range exchange.",
        Minimum = 100, Maximum = int.MaxValue, Unit = PgGucUnit.Milliseconds, Context = PgGucContext.Sighup)]
    internal static partial int ExchangeTimeout { get; }

    /// <summary>
    /// Gets how often each node exchanges with its neighbors when nothing is waiting.
    /// </summary>
    [PgGucInt("worf.sequence_range_exchange_interval", 60000, "Sets how often each node exchanges sequence ranges with its neighbors.",
        Minimum = 100, Maximum = int.MaxValue, Unit = PgGucUnit.Milliseconds, Context = PgGucContext.Sighup)]
    internal static partial int ExchangeInterval { get; }

    /// <summary>
    /// Gets or sets whether worf itself runs the DDL that managed sequences otherwise reject.
    /// </summary>
    internal static bool InternalDdl { get; set; }

    /// <summary>
    /// Gets the counter that tells backends to reload the blocks they cache for the guard.
    /// </summary>
    internal static PgAtomic<long> Version { get; } = new("worf_sequence_range_version");

    /// <summary>
    /// Gets the counter that tells this backend to reload the blocks it caches for the guard, as when its own transaction
    /// changed them, or rolled back such a change, and no other backend may see either.
    /// </summary>
    internal static long LocalVersion { get; private set; }

    /// <summary>
    /// Gets the counter that tells the range worker to exchange with every neighbor now, as when a backend that
    /// answered an exchange learned of a new node or sequence.
    /// </summary>
    internal static PgAtomic<long> ExchangeRequested { get; } = new("worf_sequence_range_exchange");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit()
    {
        PgSharedMemory.Initialize(Version, static () => 0L);
        PgSharedMemory.Initialize(ExchangeRequested, static () => 0L);
    }

    /// <summary>
    /// Puts a sequence under range management, with this node as its owner.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.enable_sequence_range(sequence regclass, participants text[] DEFAULT NULL,
            range_size bigint DEFAULT NULL, max_range_size bigint DEFAULT NULL, pool_start bigint DEFAULT NULL,
            force boolean DEFAULT false)
        RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void EnableSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum? sequence,
        string?[]? participants, long? rangeSize, long? maxRangeSize, long? poolStart, bool? force)
    {
        uint seqoid = RequireSequence(sequence);
        CheckSizes(seqoid, rangeSize, maxRangeSize);
        Enable(seqoid, participants, rangeSize, maxRangeSize, poolStart, force == true);
    }

    /// <summary>
    /// Checks block sizes a caller asks for. A sequence's bounds need two values at least, and a block of a single value
    /// is a Snowflake node number, which is never used up, so a node with one never asks for another.
    /// </summary>
    private static void CheckSizes(uint seqoid, long? rangeSize, long? maxRangeSize)
    {
        if (seqoid == SnowflakeIds.NodeSequence())
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "worf.snowflake_node hands out single node numbers",
                hint: "Use worf.enable_snowflake.");
        }

        if (rangeSize is < 2)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "range_size must be at least 2");
        }

        if (maxRangeSize is < 2)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "max_range_size must be at least 2");
        }
    }

    /// <summary>
    /// Puts a sequence under range management, with this node as its owner.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="participants">The other nodes that issue its values, or null to wait for activation.</param>
    /// <param name="rangeSize">The size of a node's first block, or null for its type's default.</param>
    /// <param name="maxRangeSize">The largest block a node requests, or null for its type's default.</param>
    /// <param name="poolStart">Where the root starts at the lowest.</param>
    /// <param name="force">Whether to take over from an owner that seems live.</param>
    internal static void Enable(uint seqoid, string?[]? participants, long? rangeSize, long? maxRangeSize, long? poolStart, bool force)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;

        SequenceInfo info = GetInfo(seqoid);
        if (info.Persistence != 'p')
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"sequence {info.Name} is temporary or unlogged");
        }

        if (info.Cycle)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"sequence {info.Name} cycles",
                hint: "Range management needs NO CYCLE, since cycling would wrap a block back to its start.");
        }

        if (Worf.SelectCatalog("SELECT 1 FROM worf.replication_set_seq WHERE set_seqoid = $1", SpiParameter.Create(seqoid)).Count > 0)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"sequence {info.Name} is in a replication set",
                hint: "Remove it from its replication sets first; sequence state replication would set it into another node's block.");
        }

        long quarantine = info.QuarantineEdge;
        if (poolStart is long start && (info.Ascending ? start <= quarantine : start >= quarantine))
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"pool_start {start} is at or beyond the value {quarantine} reserved for quarantine");
        }

        (long defaultSize, long defaultMax) = DefaultSizes(info.TypeOid);
        long size = rangeSize ?? defaultSize;
        long maxSize = Math.Max(maxRangeSize ?? defaultMax, size);
        if (size <= 0)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "range_size must be positive");
        }

        // Moving the owner starts the next epoch. Two live roots could overlap, so it waits until no neighbor has
        // reported the old owner's epoch as live lately, unless forced because the old owner is gone.
        SpiResult existing = Worf.SelectCatalog(
            "SELECT epoch, state, owner_node, watermark, root_start, root_end FROM worf.sequence_range WHERE seqoid = $1",
            SpiParameter.Create(seqoid));
        int epoch = 1;
        long? watermark = null;
        long? rootStart = info.Ascending ? info.Min : info.Max;
        long end = RootEnd(info);
        if (existing.Count > 0)
        {
            int current = existing[0].Get<int>(0);
            bool live = existing[0].Get<string>(1) != "disabled";
            if (live && existing[0].Get<uint>(2) == node.Node.Id)
            {
                throw new PgException(PgSqlStates.ObjectInUse, $"sequence {info.Name} is already under range management",
                    hint: "Disable it with worf.disable_sequence_range first.");
            }

            // The old owner itself starts the next epoch at once: its root, the epoch's only one, stopped when it disabled.
            if (!force && existing[0].Get<uint>(2) != node.Node.Id && Worf.SelectCatalog("""
                SELECT 1 FROM worf.sequence_range_node
                WHERE seqoid = $1 AND node_id <> $2 AND epoch = $3 AND state <> 'disabled' AND reported_at > now() - $4::interval
                """, SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(current),
                SpiParameter.Create(LiveReportAge)).Count > 0)
            {
                throw new PgException(PgSqlStates.ObjectInUse, $"sequence {info.Name} is still under range management by its owner",
                    hint: "Disable it with worf.disable_sequence_range on its owner first, or pass force := true if the owner is gone for good.");
            }

            epoch = Math.Max(current, Worf.ReadScalar<int?>("SELECT max(epoch) FROM worf.sequence_range_node WHERE seqoid = $1",
                SpiParameter.Create(seqoid)) ?? 0) + 1;
            watermark = existing[0].Get<long?>(3);

            // The sequence's bounds are a block's now, so the root spans what the last one did.
            rootStart = existing[0].Get<long?>(4);
            end = existing[0].Get<long>(5);
            Disable(seqoid);
            _ = Spi.Execute("DELETE FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
        }

        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range (seqoid, owner_node, epoch, range_size, max_range_size, state, distance,
                request_size, participants, pool_start, root_start, root_end, watermark)
            VALUES ($1, $2, $3, $4, $5, 'collecting', 0, $4, $6::name[], $7, $8, $9, $10)
            """,
            SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(epoch),
            SpiParameter.Create(size), SpiParameter.Create(maxSize), SpiParameter.Create(participants),
            SpiParameter.Create(poolStart), SpiParameter.Create(rootStart), SpiParameter.Create(end), SpiParameter.Create(watermark));

        Join(seqoid, node);

        // With every participant reported, here none but the owner, the root starts at once.
        if (participants is { Length: 0 })
        {
            Activate(seqoid, node);
        }

        ChangedOnCommit();

        // The manager starts the database's range worker.
        Worker.SubscriptionChanged(0, false);
    }

    /// <summary>
    /// Starts granting a sequence's values with the nodes that have reported so far.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.activate_sequence_range(sequence regclass)
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void ActivateSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence)
    {
        uint seqoid = RequireSequence(sequence);
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
        RequireOwner(seqoid, node);
        if (!LiftFence(seqoid, node, true))
        {
            Activate(seqoid, node);
        }

        ChangedOnCommit();
    }

    /// <summary>
    /// Changes the sizes of the blocks a managed sequence's nodes request.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_sequence_range(sequence regclass, range_size bigint DEFAULT NULL,
            max_range_size bigint DEFAULT NULL)
        RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void AlterSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum? sequence, long? rangeSize,
        long? maxRangeSize)
    {
        uint seqoid = RequireSequence(sequence);
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
        RequireOwner(seqoid, node);
        CheckSizes(seqoid, rangeSize, maxRangeSize);

        _ = Spi.Execute("""
            UPDATE worf.sequence_range
            SET range_size = coalesce($2, range_size),
                max_range_size = greatest(coalesce($3, max_range_size), coalesce($2, range_size)),
                request_size = least(greatest(request_size, coalesce($2, range_size)), greatest(coalesce($3, max_range_size), coalesce($2, range_size)))
            WHERE seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(rangeSize), SpiParameter.Create(maxRangeSize));
    }

    /// <summary>
    /// Stops granting a managed sequence's values. Each node keeps its bounds, so values stay unique until its blocks
    /// run out.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.disable_sequence_range(sequence regclass)
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void DisableSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence)
    {
        uint seqoid = RequireSequence(sequence);
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
        RequireOwner(seqoid, node);
        Disable(seqoid);
        ChangedOnCommit();
    }

    /// <summary>
    /// Abandons this node's blocks of one or every managed sequence, quarantines them and requests new blocks. Its
    /// root and pool blocks grant nothing until the nodes it granted to report what they hold. Without a sequence named,
    /// the node keeps its Snowflake node number, whose IDs carry their times and so never repeat after a rollback.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.reset_sequence_range(sequence regclass DEFAULT NULL)
        RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void ResetSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum? sequence)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
        bool all = sequence is null || sequence.IsNull;
        SpiResult managed = all
            ? Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range ORDER BY seqoid")
            : Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(RequireSequence(sequence)));
        for (int i = 0; i < managed.Count; i++)
        {
            uint seqoid = managed[i].Get<uint>(0);
            LockLedger(seqoid);
            Reset(seqoid, node, !all || seqoid != SnowflakeIds.NodeSequence());
        }

        ChangedOnCommit();
    }

    /// <summary>
    /// Removes a dropped sequence's range state.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void Forget(uint seqoid)
    {
        if (!Worf.ReadScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL"))
        {
            return;
        }

        SpiParameter seq = SpiParameter.Create(seqoid);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_free WHERE block_id IN (SELECT block_id FROM worf.sequence_range_block WHERE seqoid = $1)", seq);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_grant WHERE seqoid = $1", seq);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_block WHERE seqoid = $1", seq);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1", seq);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_node WHERE seqoid = $1", seq);
        if (Spi.Execute("DELETE FROM worf.sequence_range WHERE seqoid = $1", seq) > 0)
        {
            ChangedOnCommit();
        }
    }

    /// <summary>
    /// Checks whether a sequence is under range management.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>Whether it is.</returns>
    internal static bool IsManaged(uint seqoid)
        => Elevated.Run(() => Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid)).Count > 0);

    /// <summary>
    /// Rejects replicating the state of a sequence under range management, whose setval would put it into another
    /// node's block.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void RejectReplicatedState(uint seqoid)
    {
        if (IsManaged(seqoid))
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "a sequence under range management can't replicate its state",
                hint: "Range management keeps the nodes' values apart; disable it first.");
        }
    }

    /// <summary>
    /// Checks a managed sequence: switches to its next block once the active one runs low, and gets a new next block.
    /// The range worker calls this in a transaction for each managed sequence.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="node">This node.</param>
    /// <param name="rate">The sequence's peak rate in values a second, which the caller tracks.</param>
    /// <returns>Whether anything changed, for the guard's caches.</returns>
    internal static bool Check(uint seqoid, WorfLocalNode node, double rate)
    {
        // The range row is locked first, as every change to a sequence's ranges locks it, so that a check and an exchange
        // that both change the sequence take their locks in the same order.
        SpiResult rows = Spi.Query("SELECT state, epoch, owner_node FROM worf.sequence_range WHERE seqoid = $1 FOR NO KEY UPDATE", false, 1,
            SpiParameter.Create(seqoid));
        if (rows.Count == 0)
        {
            return false;
        }

        string state = rows[0].Get<string>(0);
        int epoch = rows[0].Get<int>(1);
        uint owner = rows[0].Get<uint>(2);
        // A node keeps using the blocks it holds while disabled or without a route to the owner.
        if (state is "collecting" or "refused")
        {
            return false;
        }

        SequenceInfo info = GetInfo(seqoid);
        bool snowflake = seqoid == SnowflakeIds.NodeSequence();
        (Guid Id, long Lo, long Hi)? active = NodeBlock(seqoid, epoch, "active");
        (Guid Id, long Lo, long Hi, bool Reserved)? next = NextBlock(seqoid, epoch);
        bool changed = false;

        if (active is null)
        {
            if (next is not null)
            {
                changed = Switch(seqoid, info, (next.Value.Id, next.Value.Lo, next.Value.Hi));
                next = null;
            }
        }
        else if (!snowflake)
        {
            long left = StepsLeft(seqoid, info, active.Value);
            long size = Steps(info, active.Value.Lo, active.Value.Hi);
            // A minute of inserts, at least a twentieth of the block, but no more than half of it, so that a block
            // smaller than a minute's use still serves most of its values while the next ones grow. A block reserved
            // for a load is switched to at once, and so is any once the sequence moved backwards in its block.
            long margin = Math.Min(Math.Max(size / 20, (long)(rate * 60)), size / 2);
            bool backwards = MovedBackwards(seqoid, info, active.Value);
            if (next is not null && (left <= margin || backwards || next.Value.Reserved))
            {
                if (Switch(seqoid, info, (next.Value.Id, next.Value.Lo, next.Value.Hi)))
                {
                    changed = true;
                    next = null;
                }
            }
            else if (backwards)
            {
                // With no next block, the sequence stops until one arrives, rather than issue its values again.
                Quarantine(info);
                _ = Spi.Execute("UPDATE worf.sequence_range_block SET state = 'retired', retired_at = now() WHERE block_id = $1",
                    SpiParameter.Create(active.Value.Id));
                _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'quarantined' WHERE seqoid = $1", SpiParameter.Create(seqoid));
                changed = true;
            }
            else if (left <= 0 && next is null && state != "exhausted")
            {
                _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'exhausted' WHERE seqoid = $1", SpiParameter.Create(seqoid));
                PgLog.Warning($"sequence {info.Name} has used up its block and has no next block yet");
            }
        }

        // The owner grants its own next block from the root, and its neighbors hear of the new watermark at once, so
        // that a rollback of the owner can't lose it. Another node requests one from its parent, committed before the
        // exchange sends it. A Snowflake node number is never used up, so a node that has one needs no next.
        if (next is null && !(snowflake && active is not null) && state != "disabled" && NextBlock(seqoid, epoch) is null)
        {
            if (owner == node.Node.Id)
            {
                if (GrantOwnBlock(seqoid, info, node, epoch))
                {
                    changed = true;
                    _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () => _ = PgAtomic.Add(ExchangeRequested, 1L));
                }
            }
            else if (!Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = $1 AND kind = 'n')",
                SpiParameter.Create(seqoid)))
            {
                _ = Spi.Execute("""
                    INSERT INTO worf.sequence_range_request (request_id, seqoid, parent_node, kind, size, created_at)
                    SELECT gen_random_uuid(), r.seqoid, r.parent_node, 'n', r.request_size, now()
                    FROM worf.sequence_range r
                    WHERE r.seqoid = $1 AND r.parent_node IS NOT NULL
                        AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request q WHERE q.seqoid = r.seqoid AND q.kind = 'n')
                    """, SpiParameter.Create(seqoid));
            }
        }

        if (snowflake && owner == node.Node.Id)
        {
            ReuseSnowflakeNumbers(seqoid, epoch);
        }

        return changed;
    }

    /// <summary>
    /// Checks whether a sequence's last value moved backwards in its active block since the last check, as a
    /// <c>setval</c> by a superuser or a member of <c>pg_write_all_data</c> moves it, so that it would issue its values
    /// again.
    /// </summary>
    private static bool MovedBackwards(uint seqoid, SequenceInfo info, (Guid Id, long Lo, long Hi) active)
    {
        if (LastValue(seqoid) is not long position)
        {
            return false;
        }

        if (s_positions.TryGetValue(seqoid, out (Guid Block, long Position) seen) && seen.Block == active.Id &&
            (info.Ascending ? position < seen.Position : position > seen.Position))
        {
            PgLog.Warning($"sequence {info.Name}'s last value moved back from {seen.Position} to {position} in its block [{active.Lo}, {active.Hi}], so it leaves the block");
            return true;
        }

        s_positions[seqoid] = (active.Id, position);
        return false;
    }

    /// <summary>
    /// Gives the root back, once it has no number left to grant, the Snowflake node numbers no node holds: numbers
    /// below the start of a root that moved above them, and numbers given up in this epoch. A number is free once every
    /// node has reported holding numbers in this epoch without it, or has reported nothing for a day; a node reports a
    /// number it gave up as held for a day, against a copy of it that may still run, and stops using its own number
    /// after half a day without word from a neighbor. The numbers come back to be used no earlier than the furthest any
    /// node has reserved its clock, so that no ID of a number's earlier holder can repeat, whatever the clocks.
    /// </summary>
    private static void ReuseSnowflakeNumbers(uint seqoid, int epoch)
    {
        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter current = SpiParameter.Create(epoch);
        if (Worf.ReadScalar<bool>("""
            SELECT EXISTS (
                SELECT 1 FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
                WHERE b.seqoid = $1 AND b.epoch = $2 AND b.kind = 'r' AND b.state = 'open')
            """, seq, current))
        {
            return;
        }

        long reused = Spi.Execute("""
            WITH root AS (
                SELECT block_id, lo, hi, received_at FROM worf.sequence_range_block
                WHERE seqoid = $1 AND epoch = $2 AND kind = 'r' AND state = 'open'),
            floor AS (
                SELECT greatest((SELECT max(clock) FROM worf.sequence_range_node WHERE seqoid = $1), $4) AS not_before)
            INSERT INTO worf.sequence_range_free
            SELECT root.block_id, v, v, floor.not_before
            FROM root CROSS JOIN floor CROSS JOIN LATERAL generate_series(root.lo, root.hi) v
            WHERE NOT EXISTS (
                    SELECT 1 FROM worf.sequence_range_grant g JOIN worf.sequence_range_block b ON b.block_id = g.from_block
                    WHERE g.seqoid = $1 AND b.epoch = $2 AND g.kind <> 'x' AND g.lo <= v AND v <= g.hi
                        AND (g.state <> 'abandoned' OR g.abandoned_at > now() - $3::interval))
                AND NOT EXISTS (
                    SELECT 1 FROM worf.sequence_range_block o
                    WHERE o.seqoid = $1 AND o.kind = 'n' AND o.lo = v
                        AND (o.state IN ('next', 'active') OR o.state = 'abandoned' AND o.retired_at > now() - $3::interval))
                AND NOT EXISTS (
                    SELECT 1 FROM worf.sequence_range_node n CROSS JOIN worf.local_node l
                    WHERE n.seqoid = $1 AND n.node_id <> l.node_id AND n.reported_at > now() - $3::interval
                        AND (n.epoch IS DISTINCT FROM $2 OR n.reported_at <= root.received_at OR v = ANY (coalesce(n.holds, '{}'))))
            """, seq, current, SpiParameter.Create(SnowflakeReuseAfter()), SpiParameter.Create(SnowflakeIds.ReservedClock()));
        if (reused > 0)
        {
            PgLog.Log($"Snowflake node numbers: {reused} that no node holds are free again");
        }
    }

    /// <summary>
    /// Gets how long a Snowflake node number a node gave up, or a node that stopped reporting, keeps its number from use.
    /// </summary>
    /// <returns>The time, as an interval.</returns>
    internal static string SnowflakeReuseAfter()
        => $"{s_snowflakeReuseAfter.TotalSeconds.ToString(CultureInfo.InvariantCulture)} seconds";

    /// <summary>
    /// Rejects DDL that would let values escape range management, except when worf itself runs it: changing, renaming,
    /// moving or unlogging a managed sequence, granting UPDATE on one, changing the default or identity of a column that
    /// uses one, creating, dropping, renaming or disabling the guard or making it depend on an extension, and restarting
    /// a managed sequence through <c>TRUNCATE</c>. A table that becomes a partition gives up its own guard for its
    /// tree's.
    /// </summary>
    /// <param name="parsetree">The statement, before it runs.</param>
    internal static void CheckDdl(Node* parsetree)
    {
        if (InternalDdl)
        {
            return;
        }

        if (parsetree->type == NodeTag.T_AlterTableStmt)
        {
            DropAttachedGuards((AlterTableStmt*)parsetree);
        }

        // The restore of a subscription's structure is worf's own.
        if (parsetree->type is not (NodeTag.T_AlterSeqStmt or NodeTag.T_GrantStmt or NodeTag.T_AlterTableStmt or
            NodeTag.T_DropStmt or NodeTag.T_RenameStmt or NodeTag.T_TruncateStmt or NodeTag.T_CreateTrigStmt or
            NodeTag.T_AlterObjectSchemaStmt or NodeTag.T_AlterObjectDependsStmt) || !AnyManaged() || DdlReplication.Restoring())
        {
            return;
        }

        switch (parsetree->type)
        {
            case NodeTag.T_AlterSeqStmt:
                if (!OnlyOwnedBy(((AlterSeqStmt*)parsetree)->options) && IsManaged(Resolve(((AlterSeqStmt*)parsetree)->sequence)))
                {
                    throw Rejected("ALTER SEQUENCE on a managed sequence", "Use worf.alter_sequence_range, or disable range management first.");
                }

                break;

            case NodeTag.T_GrantStmt:
                CheckGrant((GrantStmt*)parsetree);
                break;

            case NodeTag.T_AlterTableStmt:
                CheckAlterTable((AlterTableStmt*)parsetree);
                break;

            case NodeTag.T_DropStmt when ((DropStmt*)parsetree)->removeType == ObjectType.OBJECT_TRIGGER:
                List* objects = ((DropStmt*)parsetree)->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    List* name = (List*)objects->elements[i].ptr_value;
                    if (IsGuardName(NodeName(name->elements[name->length - 1].ptr_value)))
                    {
                        throw Rejected("dropping the guard of a table that uses a managed sequence", "Disable range management first.");
                    }
                }

                break;

            case NodeTag.T_RenameStmt:
                CheckRename((RenameStmt*)parsetree);
                break;

            case NodeTag.T_AlterObjectSchemaStmt:
                AlterObjectSchemaStmt* move = (AlterObjectSchemaStmt*)parsetree;
                if (move->relation is not null && MovesManaged(Resolve(move->relation)))
                {
                    throw Rejected("moving a managed sequence to another schema", "Other nodes find it by its name; disable range management first.");
                }

                break;

            // A dumped guard is skipped, since worf creates the guard itself.
            case NodeTag.T_CreateTrigStmt when IsGuardName(Worf.FromServer(((CreateTrigStmt*)parsetree)->trigname)) &&
                !IsGuardFunction(((CreateTrigStmt*)parsetree)->funcname):
                throw Rejected("creating a trigger with the guard's name", "worf creates the guard itself.");

            // An extension's drop would drop the guard with it.
            case NodeTag.T_AlterObjectDependsStmt when ((AlterObjectDependsStmt*)parsetree)->objectType == ObjectType.OBJECT_TRIGGER &&
                !((AlterObjectDependsStmt*)parsetree)->remove:
                List* trigger = (List*)((AlterObjectDependsStmt*)parsetree)->@object;
                if (trigger is not null && IsGuardName(NodeName(trigger->elements[trigger->length - 1].ptr_value)))
                {
                    throw Rejected("making the guard of a table that uses a managed sequence depend on an extension", "Disable range management first.");
                }

                break;

            case NodeTag.T_TruncateStmt when ((TruncateStmt*)parsetree)->restart_seqs:
                CheckTruncate((TruncateStmt*)parsetree);
                break;

            default:
                break;
        }
    }

    private static void CheckAlterTable(AlterTableStmt* alter)
    {
        uint relid = Resolve(alter->relation);
        if (relid == 0)
        {
            return;
        }

        // ALTER SEQUENCE ... SET LOGGED or UNLOGGED comes as ALTER TABLE; an unlogged sequence restarts after a crash
        // and issues its values again. Its owner may change, which leaves its values alone.
        bool sequence = IsManaged(relid);
        List* cmds = alter->cmds;
        for (int i = 0; cmds is not null && i < cmds->length; i++)
        {
            AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
            string? name = cmd->name is null ? null : Worf.FromServer(cmd->name);
            bool rejected = cmd->subtype switch
            {
                _ when sequence => cmd->subtype != AlterTableType.AT_ChangeOwner,
                AlterTableType.AT_ColumnDefault or AlterTableType.AT_AddIdentity or AlterTableType.AT_SetIdentity or
                    AlterTableType.AT_DropIdentity => name is not null && ColumnUsesManaged(relid, name),
                AlterTableType.AT_DisableTrig or AlterTableType.AT_EnableReplicaTrig => name is not null && IsGuardName(name),
                AlterTableType.AT_DisableTrigAll or AlterTableType.AT_DisableTrigUser => IsGuarded(relid),

                // A table's sequences follow it to unlogged.
                AlterTableType.AT_SetUnLogged => OwnsManaged(relid),
                _ => false,
            };
            if (rejected)
            {
                throw Rejected("changing a managed sequence, a column that uses one, or its table's guard", "Disable range management first.");
            }
        }
    }

    /// <summary>
    /// Drops the guard of a table that becomes a partition. PostgreSQL gives it its tree's guard, which can't stand beside
    /// one of its own, and which checks its columns too.
    /// </summary>
    private static void DropAttachedGuards(AlterTableStmt* alter)
    {
        for (int i = 0; alter->cmds is not null && i < alter->cmds->length; i++)
        {
            AlterTableCmd* cmd = (AlterTableCmd*)alter->cmds->elements[i].ptr_value;
            if (cmd->subtype == AlterTableType.AT_AttachPartition)
            {
                uint partition = Resolve(((PartitionCmd*)cmd->def)->name);
                foreach (string guard in partition == 0 ? [] : OwnGuards(partition))
                {
                    RunInternalDdl($"DROP TRIGGER {Spi.QuoteIdentifier(guard)} ON {QualifiedName(partition)}");
                }
            }
        }
    }

    private static void CheckRename(RenameStmt* rename)
    {
        switch (rename->renameType)
        {
            case ObjectType.OBJECT_TRIGGER when IsGuardName(Worf.FromServer(rename->subname)) || IsGuardName(Worf.FromServer(rename->newname)):
                throw Rejected("renaming the guard of a table that uses a managed sequence", "Disable range management first.");

            // Other nodes find a managed sequence by its schema and name.
            case ObjectType.OBJECT_SEQUENCE or ObjectType.OBJECT_TABLE or ObjectType.OBJECT_VIEW or ObjectType.OBJECT_MATVIEW or
                ObjectType.OBJECT_FOREIGN_TABLE or ObjectType.OBJECT_INDEX when rename->relation is not null && IsManaged(Resolve(rename->relation)):
                throw Rejected("renaming a managed sequence", "Other nodes find it by its name; disable range management first.");

            case ObjectType.OBJECT_SCHEMA when Elevated.Run(() => Worf.ReadScalar<bool>("""
                SELECT EXISTS (
                    SELECT 1 FROM worf.sequence_range r
                        JOIN pg_catalog.pg_class c ON c.oid = r.seqoid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = $1)
                """, SpiParameter.Create(Worf.FromServer(rename->subname)))):
                throw Rejected("renaming the schema of a managed sequence", "Other nodes find it by its name; disable range management first.");

            default:
                break;
        }
    }

    /// <summary>
    /// Rejects <c>TRUNCATE ... RESTART IDENTITY</c> that would restart a managed sequence: one owned by a table it
    /// names, by one of their inheritors or partitions unless it names the table with <c>ONLY</c>, or, with
    /// <c>CASCADE</c>, by a table whose foreign keys reach them.
    /// </summary>
    private static void CheckTruncate(TruncateStmt* truncate)
    {
        List<long> only = [];
        List<long> inherited = [];
        List* relations = truncate->relations;
        for (int i = 0; relations is not null && i < relations->length; i++)
        {
            RangeVar* relation = (RangeVar*)relations->elements[i].ptr_value;
            uint relid = Resolve(relation);
            if (relid != 0)
            {
                (relation->inh ? inherited : only).Add(relid);
            }
        }

        if (Elevated.Run(() => Worf.ReadScalar<bool>("""
            WITH RECURSIVE named (relid) AS (
                SELECT unnest($2::int8[])::oid
                UNION
                SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN named n ON i.inhparent = n.relid),
            truncated (relid) AS (
                SELECT unnest($1::int8[])::oid
                UNION
                SELECT relid FROM named
                UNION
                SELECT c.conrelid FROM pg_catalog.pg_constraint c JOIN truncated t ON c.confrelid = t.relid
                WHERE c.contype = 'f' AND $3)
            SELECT EXISTS (
                SELECT 1 FROM truncated t
                    JOIN pg_catalog.pg_depend d ON d.refclassid = 'pg_catalog.pg_class'::regclass AND d.refobjid = t.relid
                        AND d.classid = 'pg_catalog.pg_class'::regclass AND d.deptype IN ('a', 'i')
                    JOIN worf.sequence_range r ON r.seqoid = d.objid)
            """, SpiParameter.Create(only.ToArray()), SpiParameter.Create(inherited.ToArray()),
            SpiParameter.Create(truncate->behavior == DropBehavior.DROP_CASCADE))))
        {
            throw Rejected("TRUNCATE ... RESTART IDENTITY of a managed sequence", "Truncate without RESTART IDENTITY.");
        }
    }

    /// <summary>
    /// Checks whether ALTER SEQUENCE only changes the column that owns the sequence, which leaves its values alone.
    /// </summary>
    private static bool OnlyOwnedBy(List* options)
    {
        for (int i = 0; options is not null && i < options->length; i++)
        {
            if (Worf.FromServer(((DefElem*)options->elements[i].ptr_value)->defname) != "owned_by")
            {
                return false;
            }
        }

        return options is not null && options->length > 0;
    }

    /// <summary>
    /// Skips the guard that a dump creates, as a provider's dumped structure or a restored table's does, since worf
    /// installs the guards itself, on the tables that use a managed sequence here.
    /// </summary>
    /// <param name="parsetree">The statement, before it runs.</param>
    /// <returns>Whether to skip it.</returns>
    internal static bool SkipsDumpedGuard(Node* parsetree)
        => !InternalDdl && parsetree->type == NodeTag.T_CreateTrigStmt && IsGuardName(Worf.FromServer(((CreateTrigStmt*)parsetree)->trigname)) &&
            IsGuardFunction(((CreateTrigStmt*)parsetree)->funcname);

    /// <summary>
    /// Checks whether a trigger's function is the guard's.
    /// </summary>
    private static bool IsGuardFunction(List* funcname)
        => funcname is not null && NodeName(funcname->elements[funcname->length - 1].ptr_value) == "sequence_range_guard" &&
            (funcname->length == 1 || NodeName(funcname->elements[funcname->length - 2].ptr_value) == Worf.ExtensionName);

    /// <summary>
    /// Puts managed sequences in place after DDL: the restore of a subscription's structure quarantines and joins the
    /// provider's managed sequences as it creates them, and a table or partition tree that comes to use a managed
    /// sequence, or a partition detached from a guarded tree, gets the guard in the same transaction. The rows that DDL
    /// brings under the guard, by setting a column's default, attaching a partition or giving a managed sequence an
    /// owning column, must hold values the guard passes, or values from before the sequence's root.
    /// </summary>
    /// <param name="parsetree">The statement, which has run.</param>
    internal static void AfterDdl(Node* parsetree)
    {
        if (InternalDdl || parsetree->type is not (NodeTag.T_CreateSeqStmt or NodeTag.T_AlterSeqStmt or NodeTag.T_AlterTableStmt or NodeTag.T_CreateStmt))
        {
            return;
        }

        if (parsetree->type is NodeTag.T_CreateSeqStmt or NodeTag.T_AlterTableStmt && DdlReplication.Restoring())
        {
            Elevated.Run(SequenceRangeSync.JoinRestored);
        }

        if (!AnyManaged())
        {
            return;
        }

        switch (parsetree->type)
        {
            case NodeTag.T_AlterSeqStmt:
                uint seqoid = Resolve(((AlterSeqStmt*)parsetree)->sequence);
                if (seqoid != 0 && IsManaged(seqoid))
                {
                    // Only OWNED BY changes a managed sequence outside worf's own DDL, and its column brings its rows.
                    InstallGuards(seqoid);
                    if (Worf.Read("""
                        SELECT d.refobjid::oid, a.attname::text FROM pg_catalog.pg_depend d
                            JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
                        WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = $1 AND d.refclassid = 'pg_catalog.pg_class'::regclass
                            AND d.deptype = 'a' AND d.refobjsubid > 0
                        """, 1, SpiParameter.Create(seqoid)) is { Count: 1 } owner)
                    {
                        CheckColumnRows(owner[0].Get<uint>(0), owner[0].Get<string>(1), false);
                    }
                }

                break;

            case NodeTag.T_AlterTableStmt:
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                uint relid = Resolve(alter->relation);
                if (relid == 0)
                {
                    break;
                }

                // A partition tree's guard is its root's; an inheritance parent's default reaches its children.
                if (NativeMethods.get_rel_relkind(relid) == (sbyte)'p')
                {
                    InstallGuard(relid);
                }
                else
                {
                    List* inheritors = NativeMethods.find_all_inheritors(relid, 0, null);
                    for (int i = 0; inheritors is not null && i < inheritors->length; i++)
                    {
                        InstallGuard(inheritors->elements[i].oid_value);
                    }
                }

                for (int i = 0; alter->cmds is not null && i < alter->cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)alter->cmds->elements[i].ptr_value;
                    switch (cmd->subtype)
                    {
                        // PostgreSQL drops the guard a detached partition had from its tree.
                        case AlterTableType.AT_DetachPartition or AlterTableType.AT_DetachPartitionFinalize:
                            InstallGuard(Resolve(((PartitionCmd*)cmd->def)->name));
                            break;

                        case AlterTableType.AT_AttachPartition:
                            CheckAttachedRows(Resolve(((PartitionCmd*)cmd->def)->name));
                            break;

                        // A column that uses a managed sequence can't change its default, so one that does now didn't.
                        case AlterTableType.AT_ColumnDefault when cmd->name is not null:
                            CheckColumnRows(relid, Worf.FromServer(cmd->name), alter->relation->inh);
                            break;

                        default:
                            break;
                    }
                }

                break;

            case NodeTag.T_CreateStmt:
                InstallGuard(Resolve(((CreateStmt*)parsetree)->relation));
                break;

            default:
                break;
        }
    }

    /// <summary>
    /// Checks the values a table's rows hold in a column that has come under the guard, with its inheritors' and
    /// partitions' rows when <paramref name="inherited"/> is set or it's partitioned.
    /// </summary>
    private static void CheckColumnRows(uint relid, string column, bool inherited)
    {
        if (GuardedColumns(TreeRoot(relid)).TryGetValue(column, out (uint TypeOid, List<uint> Sequences) guarded))
        {
            SequenceRangeGuard.CheckRows(QualifiedName(relid), inherited || NativeMethods.get_rel_relkind(relid) == (sbyte)'p', column,
                guarded.TypeOid == NumericOid, guarded.Sequences);
        }
    }

    /// <summary>
    /// Checks the rows of a partition that joined a guarded tree, in each column its tree's guard checks against
    /// sequences beyond those the partition's own guard did.
    /// </summary>
    private static void CheckAttachedRows(uint partition)
    {
        if (partition == 0)
        {
            return;
        }

        Dictionary<string, (uint TypeOid, List<uint> Sequences)> own = GuardedColumns(partition);
        foreach ((string column, (uint typeOid, List<uint> sequences)) in GuardedColumns(TreeRoot(partition)))
        {
            if (!own.TryGetValue(column, out (uint TypeOid, List<uint> Sequences) before) || !before.Sequences.TrueForAll(sequences.Contains))
            {
                SequenceRangeGuard.CheckRows(QualifiedName(partition), true, column, typeOid == NumericOid, sequences);
            }
        }
    }

    private static void CheckGrant(GrantStmt* grant)
    {
        if (!grant->is_grant || grant->objtype is not (ObjectType.OBJECT_SEQUENCE or ObjectType.OBJECT_TABLE))
        {
            return;
        }

        // No privileges named means ALL.
        bool update = grant->privileges is null;
        for (int i = 0; !update && i < grant->privileges->length; i++)
        {
            AccessPriv* privilege = (AccessPriv*)grant->privileges->elements[i].ptr_value;
            update = privilege->priv_name is not null && Worf.FromServer(privilege->priv_name) == "update";
        }

        if (!update)
        {
            return;
        }

        List* objects = grant->objects;
        bool managed = false;
        if (grant->targtype == GrantTargetType.ACL_TARGET_OBJECT)
        {
            for (int i = 0; !managed && objects is not null && i < objects->length; i++)
            {
                managed = IsManaged(Resolve((RangeVar*)objects->elements[i].ptr_value));
            }
        }
        else if (grant->targtype == GrantTargetType.ACL_TARGET_ALL_IN_SCHEMA && grant->objtype == ObjectType.OBJECT_SEQUENCE && objects is not null)
        {
            // ALL TABLES leaves sequences out. Each schema is a name of its own.
            string[] schemas = new string[objects->length];
            for (int i = 0; i < objects->length; i++)
            {
                schemas[i] = NodeName(objects->elements[i].ptr_value);
            }

            managed = Elevated.Run(() => Worf.ReadScalar<bool>("""
                SELECT EXISTS (
                    SELECT 1 FROM worf.sequence_range r
                        JOIN pg_catalog.pg_class c ON c.oid = r.seqoid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = ANY ($1))
                """, SpiParameter.Create(schemas)));
        }

        if (managed)
        {
            throw Rejected("granting UPDATE on a managed sequence", "Only worf sets a managed sequence's position.");
        }
    }

    private static bool AnyManaged()
        => Worf.CatalogOid("sequence_range") != 0 && Elevated.Run(() => Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)"));

    /// <summary>
    /// Checks whether a trigger name is one of the guard's.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <returns>Whether it is.</returns>
    internal static bool IsGuardName(string name) => name is GuardName or UpdateGuardName;

    private static bool IsGuarded(uint relid)
        => relid != 0 && Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgname = ANY ($2))",
            SpiParameter.Create(relid), SpiParameter.Create(s_guardNames));

    /// <summary>
    /// Lists the guards a table has of its own, rather than from a parent it is a partition of.
    /// </summary>
    private static List<string> OwnGuards(uint relid)
    {
        SpiResult result = Worf.Read("""
            SELECT tgname::text FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgname = ANY ($2) AND tgparentid = 0
            """, 0, SpiParameter.Create(relid), SpiParameter.Create(s_guardNames));
        List<string> guards = [];
        for (int i = 0; i < result.Count; i++)
        {
            guards.Add(result[i].Get<string>(0));
        }

        return guards;
    }

    /// <summary>
    /// Checks whether moving a relation to another schema moves a managed sequence: the sequence itself, or one its
    /// table owns, which follows the table.
    /// </summary>
    private static bool MovesManaged(uint relid) => relid != 0 && (IsManaged(relid) || OwnsManaged(relid));

    /// <summary>
    /// Checks whether a table owns a managed sequence, as a serial or identity column's.
    /// </summary>
    private static bool OwnsManaged(uint relid)
        => Elevated.Run(() => Worf.ReadScalar<bool>("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_depend d JOIN worf.sequence_range r ON r.seqoid = d.objid
                WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.refclassid = 'pg_catalog.pg_class'::regclass
                    AND d.refobjid = $1 AND d.deptype IN ('a', 'i'))
            """, SpiParameter.Create(relid)));

    private static bool ColumnUsesManaged(uint relid, string column)
    {
        return Elevated.Run(() =>
        {
            SpiResult managed = Spi.Select("SELECT seqoid::oid FROM worf.sequence_range");
            return Enumerable.Range(0, managed.Count)
                .Any(i => UsingColumns(managed[i].Get<uint>(0)).Exists(c => c.Relid == relid && c.Column == column));
        });
    }

    /// <summary>
    /// Reads the name a parse tree's string node holds.
    /// </summary>
    private static string NodeName(void* value) => Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, value)));

    private static uint Resolve(RangeVar* relation)
        => relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, 0, 1, default, null);

    private static PgException Rejected(string what, string hint)
        => new("55000", $"{what} is not allowed under worf's sequence range management", hint: hint);

    /// <summary>
    /// Lists the columns, with their tables and the types beneath any domains, whose default or identity uses a
    /// sequence.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The columns.</returns>
    internal static List<(uint Relid, string Column, uint TypeOid)> UsingColumns(uint seqoid)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT DISTINCT a.attrelid, a.attname::text, a.atttypid
            FROM (
                SELECT d.refobjid AS relid, d.refobjsubid AS attnum
                FROM pg_catalog.pg_depend d
                WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = $1
                    AND d.refclassid = 'pg_catalog.pg_class'::regclass AND d.deptype IN ('a', 'i') AND d.refobjsubid > 0
                UNION
                SELECT ad.adrelid, ad.adnum
                FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_attrdef ad ON ad.oid = d.objid
                WHERE d.classid = 'pg_catalog.pg_attrdef'::regclass AND d.refclassid = 'pg_catalog.pg_class'::regclass
                    AND d.refobjid = $1
            ) u
            JOIN pg_catalog.pg_attribute a ON a.attrelid = u.relid AND a.attnum = u.attnum
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            WHERE c.relkind IN ('r', 'p') AND NOT a.attisdropped
            """, SpiParameter.Create(seqoid));
        List<(uint Relid, string Column, uint TypeOid)> columns = [];
        for (int i = 0; i < result.Count; i++)
        {
            columns.Add((result[i].Get<uint>(0), result[i].Get<string>(1), NativeMethods.getBaseType(result[i].Get<uint>(2))));
        }

        return columns;
    }

    /// <summary>
    /// Checks that the guard can read a column's values: an integer or numeric column. A column whose default only
    /// uses the sequence, as in <c>'INV-' || nextval(...)</c>, holds values the guard can't check.
    /// </summary>
    private static void RequireCheckable(uint relid, string column, uint typeOid, uint seqoid)
    {
        if (!Checkable(typeOid))
        {
            throw new PgException(FeatureNotSupported,
                $"column \"{column}\" of table {QualifiedName(relid)} uses sequence {GetInfo(seqoid).Name} but its type {Worf.FromServer(NativeMethods.format_type_be(typeOid))} can't hold its values",
                hint: "Range management checks the values of smallint, integer, bigint and numeric columns only.");
        }
    }

    private static bool Checkable(uint typeOid) => typeOid is Int2Oid or Int4Oid or Int8Oid or NumericOid;

    /// <summary>
    /// Runs DDL that managed sequences otherwise reject, and that isn't replicated.
    /// </summary>
    /// <param name="sql">The DDL.</param>
    internal static void RunInternalDdl(string sql)
    {
        bool internalDdl = InternalDdl;
        bool suppressed = DdlReplication.Suppressed;
        InternalDdl = true;
        DdlReplication.Suppressed = true;
        try
        {
            // worf runs it for whoever's statement needs it, as when a table comes to use a managed sequence.
            Elevated.Run(() => _ = Spi.Execute(sql));
        }
        finally
        {
            InternalDdl = internalDdl;
            DdlReplication.Suppressed = suppressed;
        }
    }

    /// <summary>
    /// Tells backends to reload the blocks they cache for the guard.
    /// </summary>
    internal static void Changed() => _ = PgAtomic.Add(Version, 1L);

    /// <summary>
    /// Tells backends to reload their blocks once this transaction commits, so they don't cache what it hasn't, and this
    /// backend at once, and again if the transaction rolls back.
    /// </summary>
    internal static void ChangedOnCommit()
    {
        LocalVersion++;
        if (s_changedOnCommit)
        {
            return;
        }

        s_changedOnCommit = true;
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () =>
        {
            s_changedOnCommit = false;
            Changed();
        });
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () =>
        {
            s_changedOnCommit = false;
            LocalVersion++;
        });
    }

    /// <summary>
    /// Gets a sequence's definition.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The definition.</returns>
    internal static SequenceInfo GetInfo(uint seqoid)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT s.seqtypid, s.seqincrement, s.seqmin, s.seqmax, s.seqcycle, c.relpersistence::text,
                pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)
            FROM pg_catalog.pg_sequence s
                JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE s.seqrelid = $1
            """, SpiParameter.Create(seqoid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.WrongObjectType, $"relation {seqoid} is not a sequence");
        }

        SpiRow row = result[0];
        return new SequenceInfo(seqoid, row.Get<string>(6), row.Get<uint>(0), row.Get<long>(1), row.Get<long>(2), row.Get<long>(3),
            row.Get<bool>(4), row.Get<string>(5)[0]);
    }

    /// <summary>
    /// Gets the range of values a type holds.
    /// </summary>
    /// <param name="typeOid">The type: <c>smallint</c>, <c>integer</c> or <c>bigint</c>.</param>
    /// <returns>The smallest and largest values.</returns>
    internal static (long Min, long Max) TypeRange(uint typeOid) => typeOid switch
    {
        21 => (short.MinValue, short.MaxValue),
        23 => (int.MinValue, int.MaxValue),
        _ => (long.MinValue, long.MaxValue),
    };

    private static (long Size, long Max) DefaultSizes(uint typeOid) => typeOid switch
    {
        21 => (100, 1000),
        23 => (10000, 1000000),
        _ => (1000000, 1000000000000),
    };

    /// <summary>
    /// Computes where a root of a sequence that isn't managed yet ends: at the sequence's own limit, its type's, and
    /// those of the columns it fills.
    /// </summary>
    private static long RootEnd(SequenceInfo info)
    {
        long end = info.Ascending ? info.Max : info.Min;
        foreach ((_, _, uint typeOid) in UsingColumns(info.Seqoid))
        {
            (long min, long max) = TypeRange(typeOid);
            end = info.Ascending ? Math.Min(end, max) : Math.Max(end, min);
        }

        return end;
    }

    internal static uint RequireSequence(PgDatum? sequence)
    {
        if (sequence is null || sequence.IsNull)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "sequence cannot be NULL");
        }

        uint seqoid = (uint)sequence.DangerousGetBits();
        if (NativeMethods.get_rel_relkind(seqoid) != (sbyte)'S')
        {
            throw new PgException(PgSqlStates.WrongObjectType, $"\"{Worf.FromServer(NativeMethods.get_rel_name(seqoid))}\" is not a sequence");
        }

        return seqoid;
    }

    private static void RequireOwner(uint seqoid, WorfLocalNode node)
    {
        SpiResult result = Worf.SelectCatalog("SELECT owner_node FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.UndefinedObject, $"sequence {GetInfo(seqoid).Name} is not under range management");
        }

        if (result[0].Get<uint>(0) != node.Node.Id)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"sequence {GetInfo(seqoid).Name} is owned by another node",
                hint: "Run this on its owner.");
        }
    }

    /// <summary>
    /// Joins this node to a managed sequence: guards the tables that use it, quarantines it, and records the highest
    /// value it may have issued or its tables hold.
    /// </summary>
    private static void Join(uint seqoid, WorfLocalNode node)
    {
        // The guard's and the quarantine's locks hold off writers and nextval, so the high-water mark sees every value. A
        // node that draws from a block of an earlier epoch keeps drawing from it, since the mark covers the block.
        InstallGuards(seqoid);
        SequenceInfo info = GetInfo(seqoid);
        if (Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_block WHERE seqoid = $1 AND kind = 'n' AND state = 'active'",
            SpiParameter.Create(seqoid)).Count == 0)
        {
            Quarantine(info);
        }

        Report(seqoid, info, node);
    }

    /// <summary>
    /// Records this node's high-water mark, its own report, under the sequence's epoch.
    /// </summary>
    private static void Report(uint seqoid, SequenceInfo info, WorfLocalNode node)
    {
        long? high = HighWater(info);
        _ = Spi.Execute("UPDATE worf.sequence_range SET high_water = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid), SpiParameter.Create(high));
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, distance, high_water, epoch, state, reported_at)
            SELECT $1, $2, $3, $2, 0, $4, r.epoch, r.state, now() FROM worf.sequence_range r WHERE r.seqoid = $1
            ON CONFLICT (seqoid, node_id) DO UPDATE SET high_water = excluded.high_water, epoch = excluded.epoch, state = excluded.state,
                reported_at = excluded.reported_at
            """, SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(node.Node.Name),
            SpiParameter.Create(high));
    }

    /// <summary>
    /// Takes up a newer epoch of a sequence that a neighbor reports: its owner, a high-water mark that covers this
    /// node's blocks of earlier epochs, which it keeps drawing from, and a new route. Its root and pool blocks stop
    /// granting.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="owner">The epoch's owner.</param>
    /// <param name="epoch">The epoch.</param>
    /// <param name="node">This node.</param>
    internal static void TakeEpoch(uint seqoid, uint owner, int epoch, WorfLocalNode node)
    {
        SpiParameter seq = SpiParameter.Create(seqoid);
        _ = Spi.Execute("UPDATE worf.sequence_range_block SET state = 'spent' WHERE seqoid = $1 AND kind IN ('r', 'p') AND state IN ('open', 'fenced')", seq);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1", seq);
        _ = Spi.Execute("""
            UPDATE worf.sequence_range r SET owner_node = $2, epoch = $3, parent_node = NULL, distance = NULL, fenced_at = NULL, detail = NULL,
                state = CASE WHEN EXISTS (SELECT 1 FROM worf.sequence_range_block b WHERE b.seqoid = r.seqoid AND b.kind = 'n' AND b.state = 'active')
                    THEN 'active' ELSE 'quarantined' END
            WHERE seqoid = $1
            """, seq, SpiParameter.Create(owner), SpiParameter.Create(epoch));
        Report(seqoid, GetInfo(seqoid), node);
        PgLog.Log($"sequence {GetInfo(seqoid).Name} moved to epoch {epoch}");
    }

    /// <summary>
    /// Stops granting a sequence's values on this node, as its owner or a neighbor reported. The node keeps drawing from
    /// the blocks it holds.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void Disable(uint seqoid)
    {
        SpiParameter seq = SpiParameter.Create(seqoid);
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'disabled', fenced_at = NULL WHERE seqoid = $1", seq);
        _ = Spi.Execute("UPDATE worf.sequence_range_block SET state = 'spent' WHERE seqoid = $1 AND kind IN ('r', 'p') AND state IN ('open', 'fenced')", seq);
        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1", seq);
        _ = Spi.Execute("UPDATE worf.sequence_range_node n SET state = 'disabled' FROM worf.local_node l WHERE n.seqoid = $1 AND n.node_id = l.node_id", seq);
    }

    /// <summary>
    /// Checks the identity recorded on this node's blocks: a node whose node id, system identifier or timeline changed
    /// abandons its node blocks and requests new ones. An allocator whose system identifier or timeline changed fences its
    /// root and pool blocks, since a grant committed on the old timeline might be lost, and a clone abandons the ones
    /// that belong to another node id.
    /// </summary>
    /// <param name="node">This node.</param>
    /// <returns>Whether anything changed.</returns>
    internal static bool CheckIdentity(WorfLocalNode node)
    {
        ulong sysid = NativeMethods.GetSystemIdentifier();
        uint timeline = NativeMethods.GetWALInsertionTimeLine();
        SpiParameter id = SpiParameter.Create(node.Node.Id);
        SpiParameter system = SpiParameter.Create((decimal)sysid);
        SpiParameter line = SpiParameter.Create((long)timeline);
        bool changed = false;

        // Allocator blocks first, so that a node that resets doesn't grant itself from a root it is about to fence. This
        // runs on every tick, so it reads first: a write takes a transaction ID even when it changes nothing.
        if (Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE kind IN ('r', 'p') AND state IN ('open', 'fenced') AND node_id <> $1)", id))
        {
            _ = Spi.Execute("UPDATE worf.sequence_range_block SET state = 'abandoned' WHERE kind IN ('r', 'p') AND state IN ('open', 'fenced') AND node_id <> $1", id);
        }

        if (Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE kind IN ('r', 'p') AND state = 'open' AND (sysid <> $1 OR timeline <> $2))",
            system, line))
        {
            SpiResult fenced = Spi.Query(Spi.Sql($"""
                UPDATE worf.sequence_range_block SET state = 'fenced'
                WHERE kind IN ('r', 'p') AND state = 'open' AND (sysid <> {(decimal)sysid} OR timeline <> {(long)timeline})
                RETURNING seqoid::oid
                """), false, 0);
            for (int i = 0; i < fenced.Count; i++)
            {
                uint seqoid = fenced[i].Get<uint>(0);
                if (Spi.Execute("UPDATE worf.sequence_range SET fenced_at = now() WHERE seqoid = $1 AND fenced_at IS NULL", SpiParameter.Create(seqoid)) > 0)
                {
                    PgLog.Warning($"sequence {GetInfo(seqoid).Name}: this allocator's timeline or system identifier changed, so it grants nothing until every node it granted to reports");
                }
            }
        }

        SpiResult moved = Worf.SelectCatalog("""
            SELECT DISTINCT seqoid::oid FROM worf.sequence_range_block
            WHERE kind IN ('n', 'i') AND state IN ('next', 'active', 'imported')
                AND (node_id <> $1 OR sysid <> $2 OR timeline <> $3)
            """, id, system, line);
        for (int i = 0; i < moved.Count; i++)
        {
            uint seqoid = moved[i].Get<uint>(0);
            PgLog.Warning($"sequence {GetInfo(seqoid).Name}: this node's identity changed, so it abandons its blocks and requests new ones");
            LockLedger(seqoid);
            Reset(seqoid, node, true);
            changed = true;
        }

        return changed;
    }

    /// <summary>
    /// Lifts an allocator's fence once every node with a grant from its fenced blocks, every neighbor still in touch, and
    /// on the owner every listed participant, has exchanged since the fence began, unless two grants overlap. The owner's
    /// root then gives up its free values up to the highest any node has seen granted, which covers the blocks it granted
    /// itself and lost.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="node">This node.</param>
    /// <param name="force">Whether to lift it without waiting for reports, as <c>activate_sequence_range</c> does.</param>
    /// <returns>Whether it lifted.</returns>
    internal static bool LiftFence(uint seqoid, WorfLocalNode node, bool force)
    {
        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiResult range = Worf.SelectCatalog("SELECT fenced_at IS NOT NULL, owner_node, participants FROM worf.sequence_range WHERE seqoid = $1", seq);
        if (range.Count == 0 || !range[0].Get<bool>(0))
        {
            return false;
        }

        SpiResult overlap = Worf.SelectCatalog("""
            SELECT a.grantee_name::text, a.lo, a.hi, b.grantee_name::text, b.lo, b.hi
            FROM worf.sequence_range_grant a JOIN worf.sequence_range_grant b
                ON b.seqoid = a.seqoid AND a.grant_id < b.grant_id AND a.lo <= b.hi AND b.lo <= a.hi
            WHERE a.seqoid = $1 AND a.kind <> 'x' AND b.kind <> 'x' AND a.state <> 'unclaimed' AND b.state <> 'unclaimed'
            LIMIT 1
            """, seq);
        if (overlap.Count > 0)
        {
            string detail = $"grants to {overlap[0].Get<string>(0)} [{overlap[0].Get<long>(1)}, {overlap[0].Get<long>(2)}] and to " +
                $"{overlap[0].Get<string>(3)} [{overlap[0].Get<long>(4)}, {overlap[0].Get<long>(5)}] overlap";
            _ = Spi.Execute("UPDATE worf.sequence_range SET detail = $2 WHERE seqoid = $1 AND detail IS DISTINCT FROM $2", seq, SpiParameter.Create(detail));
            return false;
        }

        if (!force && Worf.ReadScalar<bool>("""
            SELECT EXISTS (
                SELECT 1 FROM worf.sequence_range_grant g JOIN worf.sequence_range_block b ON b.block_id = g.from_block
                WHERE g.seqoid = $1 AND b.state = 'fenced' AND g.grantee_id <> $2 AND g.state NOT IN ('abandoned', 'unclaimed')
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_node n
                        WHERE n.seqoid = g.seqoid AND n.node_id = g.grantee_id AND n.held_at > r.fenced_at))
                OR (r.owner_node = $2 AND EXISTS (
                SELECT 1 FROM unnest(r.participants) p(name)
                WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_node n
                    WHERE n.seqoid = r.seqoid AND n.node_name = p.name
                        AND (n.held_at > r.fenced_at OR n.via_node <> n.node_id AND n.reported_at > r.fenced_at))))
                OR EXISTS (
                SELECT 1 FROM worf.sequence_range_node n
                WHERE n.seqoid = r.seqoid AND n.via_node = n.node_id AND n.node_id <> $2 AND n.reported_at > now() - $3::interval
                    AND (n.held_at IS NULL OR n.held_at <= r.fenced_at))
            FROM worf.sequence_range r WHERE r.seqoid = $1
            """, seq, SpiParameter.Create(node.Node.Id), SpiParameter.Create(StaleAfter())))
        {
            return false;
        }

        _ = Spi.Execute("""
            UPDATE worf.sequence_range_block SET state = 'open', sysid = $2, timeline = $3
            WHERE seqoid = $1 AND state = 'fenced'
            """, seq, SpiParameter.Create((decimal)NativeMethods.GetSystemIdentifier()),
            SpiParameter.Create((long)NativeMethods.GetWALInsertionTimeLine()));
        _ = Spi.Execute("UPDATE worf.sequence_range SET fenced_at = NULL, detail = NULL WHERE seqoid = $1", seq);
        SequenceInfo info = GetInfo(seqoid);
        if (range[0].Get<uint>(1) == node.Node.Id)
        {
            LockLedger(seqoid);
            _ = Spi.Execute(info.Ascending
                ? """
                    DELETE FROM worf.sequence_range_free f USING worf.sequence_range_block b, worf.sequence_range r
                    WHERE f.block_id = b.block_id AND b.seqoid = $1 AND b.kind = 'r' AND r.seqoid = $1 AND f.hi <= r.watermark
                    """
                : """
                    DELETE FROM worf.sequence_range_free f USING worf.sequence_range_block b, worf.sequence_range r
                    WHERE f.block_id = b.block_id AND b.seqoid = $1 AND b.kind = 'r' AND r.seqoid = $1 AND f.lo >= r.watermark
                    """, seq);
            _ = Spi.Execute(info.Ascending
                ? """
                    UPDATE worf.sequence_range_free f SET lo = r.watermark + 1 FROM worf.sequence_range_block b, worf.sequence_range r
                    WHERE f.block_id = b.block_id AND b.seqoid = $1 AND b.kind = 'r' AND r.seqoid = $1 AND f.lo <= r.watermark
                    """
                : """
                    UPDATE worf.sequence_range_free f SET hi = r.watermark - 1 FROM worf.sequence_range_block b, worf.sequence_range r
                    WHERE f.block_id = b.block_id AND b.seqoid = $1 AND b.kind = 'r' AND r.seqoid = $1 AND f.hi >= r.watermark
                    """, seq);
        }

        PgLog.Log($"sequence {info.Name}: the allocator's fence lifted");
        return true;
    }

    /// <summary>
    /// Gets how long a neighbor's report counts as current: a link exchanges at least once an exchange interval, so one
    /// that hasn't in three, and a timeout, is down or gone.
    /// </summary>
    /// <returns>The time, as an interval.</returns>
    internal static string StaleAfter()
        => $"{((3L * ExchangeInterval) + ExchangeTimeout).ToString(CultureInfo.InvariantCulture)} milliseconds";

    /// <summary>
    /// Records a block a neighbor reports holding from this allocator: one the allocator has no record of, as when the
    /// grant was lost with an old timeline, goes back into the ledger and leaves the free spans, and an abandoned one is
    /// marked so.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="info">The sequence's details.</param>
    /// <param name="id">The block, which is its grant.</param>
    /// <param name="kind">Its kind.</param>
    /// <param name="lo">Its start.</param>
    /// <param name="hi">Its end.</param>
    /// <param name="abandoned">Whether the holder abandoned it.</param>
    /// <param name="fenced">Whether this allocator is fenced, which alone takes unknown blocks back into its ledger.</param>
    /// <param name="holderId">The holder.</param>
    /// <param name="holderName">The holder's name.</param>
    internal static void RecordHeld(uint seqoid, SequenceInfo info, Guid id, char kind, long lo, long hi, bool abandoned, bool fenced,
        uint holderId, string holderName)
    {
        SpiResult grant = Worf.SelectCatalog("SELECT state FROM worf.sequence_range_grant WHERE grant_id = $1", SpiParameter.Create(id));
        if (grant.Count > 0)
        {
            // The holder reports an abandoned block at each exchange for a while, which changes the grant once.
            if (abandoned && grant[0].Get<string>(0) != "abandoned")
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'abandoned', abandoned_at = now() WHERE grant_id = $1 AND state <> 'abandoned'",
                    SpiParameter.Create(id));
            }

            return;
        }

        if (!fenced)
        {
            return;
        }

        SpiResult from = Worf.SelectCatalog("""
            SELECT block_id FROM worf.sequence_range_block
            WHERE seqoid = $1 AND kind IN ('r', 'p') AND state IN ('open', 'fenced') AND lo <= $2 AND $3 <= hi
            LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(lo), SpiParameter.Create(hi));
        if (from.Count == 0)
        {
            return;
        }

        Guid block = from[0].Get<Guid>(0);
        LockLedger(seqoid);
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_grant
            VALUES ($1, gen_random_uuid(), $2, $3, $4, $5, $6::"char", $7, $8, $9, now(), CASE WHEN $9 = 'abandoned' THEN now() END)
            """, SpiParameter.Create(id), SpiParameter.Create(seqoid), SpiParameter.Create(block), SpiParameter.Create(holderId),
            SpiParameter.Create(holderName), SpiParameter.Create(kind.ToString()), SpiParameter.Create(lo), SpiParameter.Create(hi),
            SpiParameter.Create(abandoned ? "abandoned" : "acknowledged"));
        TakeFromFree(block, lo, hi);
        _ = Spi.Execute($"UPDATE worf.sequence_range SET watermark = {(info.Ascending ? "greatest" : "least")}(coalesce(watermark, $2), $2) WHERE seqoid = $1",
            SpiParameter.Create(seqoid), SpiParameter.Create(info.Ascending ? hi : lo));
        PgLog.Warning($"sequence {info.Name}: node {holderName} holds block [{lo}, {hi}], which this allocator had no record of; it is back in the ledger");
    }

    /// <summary>
    /// Locks a sequence's range row before its free spans, as an exchange that updates the row and then grants does, so
    /// that two allocations don't wait on each other.
    /// </summary>
    private static void LockLedger(uint seqoid)
        => _ = Spi.Execute("SELECT 1 FROM worf.sequence_range WHERE seqoid = $1 FOR NO KEY UPDATE", SpiParameter.Create(seqoid));

    /// <summary>
    /// Removes values from a block's free spans. Each span shrinks in place, or is deleted once empty, so that a
    /// concurrent allocation waiting on its lock rechecks what is left of it.
    /// </summary>
    private static void TakeFromFree(Guid block, long lo, long hi)
    {
        SpiResult spans = Spi.Query(Spi.Sql($"SELECT lo, hi FROM worf.sequence_range_free WHERE block_id = {block} AND lo <= {hi} AND {lo} <= hi FOR UPDATE"),
            false, 0);
        for (int i = 0; i < spans.Count; i++)
        {
            long spanLo = spans[i].Get<long>(0);
            long spanHi = spans[i].Get<long>(1);
            SpiParameter id = SpiParameter.Create(block);
            SpiParameter start = SpiParameter.Create(spanLo);
            if (spanLo < lo)
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_free SET hi = $3 WHERE block_id = $1 AND lo = $2", id, start, SpiParameter.Create(lo - 1));
                if (spanHi > hi)
                {
                    _ = Spi.Execute("INSERT INTO worf.sequence_range_free SELECT $1, $3, $4, not_before FROM worf.sequence_range_free WHERE block_id = $1 AND lo = $2",
                        id, start, SpiParameter.Create(hi + 1), SpiParameter.Create(spanHi));
                }
            }
            else if (spanHi > hi)
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_free SET lo = $3 WHERE block_id = $1 AND lo = $2", id, start, SpiParameter.Create(hi + 1));
            }
            else
            {
                _ = Spi.Execute("DELETE FROM worf.sequence_range_free WHERE block_id = $1 AND lo = $2", id, start);
            }
        }
    }

    /// <summary>
    /// Joins this node to a sequence that a neighbor reports under range management, which this node has too.
    /// </summary>
    /// <param name="seqoid">The sequence here.</param>
    /// <param name="owner">Its owner.</param>
    /// <param name="epoch">Its epoch.</param>
    /// <param name="rangeSize">The size of a node's first block.</param>
    /// <param name="maxRangeSize">The largest block a node requests.</param>
    /// <param name="rootStart">Where the root may start, or null to take the sequence's first value here.</param>
    /// <param name="rootEnd">Where the root ends, or null to take the sequence's limits here.</param>
    /// <param name="node">This node.</param>
    internal static void JoinFromNeighbor(uint seqoid, uint owner, int epoch, long rangeSize, long maxRangeSize, long? rootStart, long? rootEnd,
        WorfLocalNode node)
    {
        SequenceInfo info = GetInfo(seqoid);
        string? refused = info.Persistence != 'p' ? "the sequence is temporary or unlogged here"
            : info.Cycle ? "the sequence cycles here"
            : Worf.SelectCatalog("SELECT 1 FROM worf.replication_set_seq WHERE set_seqoid = $1", SpiParameter.Create(seqoid)).Count > 0
                ? "the sequence is in a replication set here"
                : UsingColumns(seqoid).Find(column => !Checkable(column.TypeOid)) is (uint relid, string column, uint typeOid) && relid != 0
                    ? $"column \"{column}\" of table {QualifiedName(relid)} has type {Worf.FromServer(NativeMethods.format_type_be(typeOid))} here, whose values the guard can't check"
                    : null;
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range (seqoid, owner_node, epoch, range_size, max_range_size, state, request_size, root_start, root_end, detail)
            VALUES ($1, $2, $3, $4, $5, $6, $4, $9, $7, $8)
            """, SpiParameter.Create(seqoid), SpiParameter.Create(owner), SpiParameter.Create(epoch), SpiParameter.Create(rangeSize),
            SpiParameter.Create(maxRangeSize), SpiParameter.Create(refused is null ? "quarantined" : "refused"),
            SpiParameter.Create(rootEnd ?? RootEnd(info)), SpiParameter.Create(refused),
            SpiParameter.Create(rootStart ?? (info.Ascending ? info.Min : info.Max)));
        if (refused is null)
        {
            Join(seqoid, node);
            PgLog.Log($"sequence {info.Name} joined range management");
        }

        // The manager starts this database's range worker, if it hasn't one.
        Worker.SubscriptionChanged(0, false);
    }

    /// <summary>
    /// Starts the root once every listed participant has reported.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="node">This node, its owner.</param>
    internal static void ActivateWhenReported(uint seqoid, WorfLocalNode node)
    {
        bool ready = Worf.ReadScalar<bool>("""
            SELECT r.state = 'collecting' AND r.participants IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM unnest(r.participants) p(name)
                WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_node n WHERE n.seqoid = r.seqoid AND n.node_name = p.name AND n.epoch = r.epoch))
            FROM worf.sequence_range r WHERE r.seqoid = $1
            """, SpiParameter.Create(seqoid));
        if (ready)
        {
            Activate(seqoid, node);
        }
    }

    /// <summary>
    /// Starts the root, above every value any reported node may have used, and gives the owner its first block.
    /// </summary>
    internal static void Activate(uint seqoid, WorfLocalNode node)
    {
        SequenceInfo info = GetInfo(seqoid);
        // The root starts no lower than the sequence's own first value, or pool_start if higher.
        SpiResult range = Worf.SelectCatalog("""
            SELECT epoch, CASE WHEN $2 THEN greatest(pool_start, root_start) ELSE least(pool_start, root_start) END, root_end, state
            FROM worf.sequence_range WHERE seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(info.Ascending));
        int epoch = range[0].Get<int>(0);
        long? poolStart = range[0].Get<long?>(1);
        long end = range[0].Get<long>(2);
        if (range[0].Get<string>(3) != "collecting")
        {
            return;
        }

        long? high = Worf.SelectCatalog(info.Ascending
                ? "SELECT max(high_water) FROM worf.sequence_range_node WHERE seqoid = $1 AND epoch = $2"
                : "SELECT min(high_water) FROM worf.sequence_range_node WHERE seqoid = $1 AND epoch = $2",
            SpiParameter.Create(seqoid), SpiParameter.Create(epoch))[0].Get<long?>(0);

        long start;
        bool snowflake = seqoid == SnowflakeIds.NodeSequence();
        if (info.Ascending)
        {
            start = Math.Max(Math.Max(poolStart ?? info.QuarantineEdge + 1, high is long h ? h + 1 : info.QuarantineEdge + 1),
                info.QuarantineEdge + 1);
            if (start > end && !snowflake)
            {
                throw new PgException(SequenceLimitExceeded, $"sequence {info.Name} has no values left above {start - 1} for a root");
            }
        }
        else
        {
            start = Math.Min(Math.Min(poolStart ?? info.QuarantineEdge - 1, high is long h ? h - 1 : info.QuarantineEdge - 1),
                info.QuarantineEdge - 1);
            if (start < end)
            {
                throw new PgException(SequenceLimitExceeded, $"sequence {info.Name} has no values left below {start + 1} for a root");
            }
        }

        // A Snowflake root spans every node number, since the numbers below its start come back once no node holds
        // them.
        (long lo, long hi) = info.Ascending ? (start, end) : (end, start);
        Guid root = InsertBlock(seqoid, epoch, 'r', snowflake ? info.Min : lo, hi, "open", null, node);
        if (lo <= hi)
        {
            _ = Spi.Execute("INSERT INTO worf.sequence_range_free VALUES ($1, $2, $3)",
                SpiParameter.Create(root), SpiParameter.Create(lo), SpiParameter.Create(hi));
        }
        // The root's start goes to the other nodes, so that each allocator refuses a node that joins later with values
        // that reach into it.
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'quarantined', root_start = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid),
            SpiParameter.Create(start));

        // The owner needs no exchange for its own blocks, so it leaves quarantine at once. An owner that keeps the
        // Snowflake number it held, as when every number is in use, keeps drawing from it.
        _ = GrantOwnBlock(seqoid, info, node, epoch);
        (Guid Id, long Lo, long Hi)? next = NodeBlock(seqoid, epoch, "next");
        if (next is not null)
        {
            _ = Switch(seqoid, info, next.Value);
            _ = GrantOwnBlock(seqoid, info, node, epoch);
        }

        _ = Spi.Execute("""
            UPDATE worf.sequence_range r SET state = 'active'
            WHERE r.seqoid = $1 AND r.state = 'quarantined'
                AND EXISTS (SELECT 1 FROM worf.sequence_range_block b WHERE b.seqoid = r.seqoid AND b.kind = 'n' AND b.state = 'active')
            """, SpiParameter.Create(seqoid));
    }

    /// <summary>
    /// Resets this node's part of a sequence after a rollback or a change of identity. The node abandons its node
    /// blocks, quarantines the sequence and requests new blocks, with new requests, since a request from before a
    /// rollback may have been answered with a block the node already used. Its root and pool blocks grant nothing until
    /// the nodes it granted to report what they hold, since grants made after the point it went back to are lost. A
    /// Snowflake node number, whose IDs carry their times, is kept unless <paramref name="renew"/> says otherwise.
    /// </summary>
    /// <param name="seqoid">The sequence, whose range row the caller has locked.</param>
    /// <param name="node">This node.</param>
    /// <param name="renew">Whether to give up a Snowflake node number too, as when the node's identity changed.</param>
    internal static void Reset(uint seqoid, WorfLocalNode node, bool renew)
    {
        SequenceInfo info = GetInfo(seqoid);
        SpiParameter seq = SpiParameter.Create(seqoid);
        if (Spi.Execute("UPDATE worf.sequence_range_block SET state = 'fenced' WHERE seqoid = $1 AND kind IN ('r', 'p') AND state = 'open'", seq) > 0 &&
            Spi.Execute("UPDATE worf.sequence_range SET fenced_at = now() WHERE seqoid = $1 AND fenced_at IS NULL", seq) > 0)
        {
            PgLog.Warning($"sequence {info.Name}: this allocator reset, so it grants nothing until every node it granted to reports");
        }

        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1", seq);
        if (seqoid == SnowflakeIds.NodeSequence() && !renew)
        {
            return;
        }

        // The node's own grants of the blocks it abandons are marked so, as their holders' reports mark others'.
        _ = Spi.Execute("""
            WITH abandoned AS (
                UPDATE worf.sequence_range_block SET state = 'abandoned', retired_at = now()
                WHERE seqoid = $1 AND kind IN ('n', 'i') AND state IN ('next', 'active', 'imported')
                RETURNING block_id)
            UPDATE worf.sequence_range_grant SET state = 'abandoned', abandoned_at = now()
            WHERE grant_id IN (SELECT block_id FROM abandoned) AND state <> 'abandoned'
            """, seq);
        Quarantine(info);
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'quarantined' WHERE seqoid = $1 AND state NOT IN ('collecting', 'disabled', 'refused')",
            seq);
        SpiResult range = Worf.SelectCatalog("SELECT epoch, owner_node, state FROM worf.sequence_range WHERE seqoid = $1", seq);
        if (range[0].Get<uint>(1) == node.Node.Id && range[0].Get<string>(2) == "quarantined")
        {
            int epoch = range[0].Get<int>(0);
            _ = GrantOwnBlock(seqoid, info, node, epoch);
            if (NodeBlock(seqoid, epoch, "next") is (Guid, long, long) next)
            {
                _ = Switch(seqoid, info, next);
                _ = GrantOwnBlock(seqoid, info, node, epoch);
            }
        }
    }

    /// <summary>
    /// Sets a sequence's bounds so that nextval fails, at a value no root contains, keeps it at CACHE 1, and revokes
    /// UPDATE on it from every role, so that only superusers and pg_write_all_data can setval it.
    /// </summary>
    private static void Quarantine(SequenceInfo info)
    {
        ChangedOnCommit();
        (long lo, long hi) = info.Quarantine;
        string q = info.QuarantineEdge.ToString(CultureInfo.InvariantCulture);
        RunInternalDdl($"ALTER SEQUENCE {info.Name} NO CYCLE CACHE 1 MINVALUE {lo.ToString(CultureInfo.InvariantCulture)} " +
            $"MAXVALUE {hi.ToString(CultureInfo.InvariantCulture)} START WITH {q} RESTART WITH {q}");
        _ = Spi.Execute("SELECT pg_catalog.setval($1::regclass, $2, true)", SpiParameter.Create(info.Seqoid),
            SpiParameter.Create(info.QuarantineEdge));

        SpiResult grantees = Worf.SelectCatalog("""
            SELECT DISTINCT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_catalog.quote_ident(r.rolname) END
            FROM pg_catalog.pg_class c, pg_catalog.aclexplode(coalesce(c.relacl, pg_catalog.acldefault('s', c.relowner))) a
                LEFT JOIN pg_catalog.pg_roles r ON r.oid = a.grantee
            WHERE c.oid = $1 AND a.privilege_type = 'UPDATE'
            """, SpiParameter.Create(info.Seqoid));
        for (int i = 0; i < grantees.Count; i++)
        {
            RunInternalDdl($"REVOKE UPDATE ON SEQUENCE {info.Name} FROM {grantees[i].Get<string>(0)}");
        }
    }

    /// <summary>
    /// Switches a sequence to a block: the block becomes its bounds and its position the block's start.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="info">The sequence's details.</param>
    /// <param name="block">The block.</param>
    /// <returns>Whether it switched; it doesn't when a transaction that called nextval holds the sequence for 200 ms.</returns>
    internal static bool Switch(uint seqoid, SequenceInfo info, (Guid Id, long Lo, long Hi) block)
    {
        LockLedger(seqoid);
        if (Worf.ReadScalar<long?>("SELECT not_before FROM worf.sequence_range_block WHERE block_id = $1", SpiParameter.Create(block.Id)) is long notBefore &&
            SnowflakeIds.NowMs() <= notBefore)
        {
            return false;
        }

        string lo = block.Lo.ToString(CultureInfo.InvariantCulture);
        string hi = block.Hi.ToString(CultureInfo.InvariantCulture);
        string start = info.Ascending ? lo : hi;

        // A block of one value, as a Snowflake node number, can't be a sequence's bounds, so the sequence stays
        // quarantined and the block only marks the value as this node's.
        if (block.Lo != block.Hi)
        {
            try
            {
                PgTransaction.RunInSubtransaction(() =>
                {
                    _ = Spi.Execute("SET LOCAL lock_timeout = 200");
                    RunInternalDdl($"ALTER SEQUENCE {info.Name} NO CYCLE CACHE 1 MINVALUE {lo} MAXVALUE {hi} START WITH {start} RESTART WITH {start}");
                    _ = Spi.Execute("SET LOCAL lock_timeout = 0");
                });
            }
            catch (PgException error) when (error.SqlState == "55P03")
            {
                return false;
            }
        }

        // A block that lasted less than an hour makes the next request bigger, and one that lasted more than a day
        // smaller.
        double? lasted = Spi.ExecuteScalar<double?>("""
            UPDATE worf.sequence_range_block SET state = 'retired', retired_at = now()
            WHERE seqoid = $1 AND kind = 'n' AND state = 'active'
            RETURNING extract(epoch FROM now() - activated_at)::float8
            """, SpiParameter.Create(seqoid));
        if (lasted is double seconds)
        {
            string resize = seconds < s_growBelow.TotalSeconds ? "least(request_size * 2, max_range_size)"
                : seconds > s_shrinkAbove.TotalSeconds ? "greatest(request_size / 2, range_size)"
                : "request_size";
            _ = Spi.Execute($"UPDATE worf.sequence_range SET request_size = {resize} WHERE seqoid = $1", SpiParameter.Create(seqoid));
        }

        _ = Spi.Execute("UPDATE worf.sequence_range_block SET state = 'active', activated_at = now() WHERE block_id = $1",
            SpiParameter.Create(block.Id));
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'active', switched_at = now() WHERE seqoid = $1",
            SpiParameter.Create(seqoid));
        // Sessions' guards reload before any nextval reaches the block, which waits for the switch's lock until then.
        ChangedOnCommit();
        PgLog.Log($"sequence {info.Name} switched to its block [{lo}, {hi}]");
        return true;
    }

    /// <summary>
    /// Grants the owner a next block from the start of the root's lowest free span that holds it, under a row lock,
    /// committed with synchronous_commit on.
    /// </summary>
    private static bool GrantOwnBlock(uint seqoid, SequenceInfo info, WorfLocalNode node, int epoch)
    {
        long size = Worf.SelectCatalog("SELECT request_size FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid))[0].Get<long>(0);
        if (Allocate(seqoid, info, epoch, size, node.Node.Id, node.Node.Name, Guid.NewGuid()) is not (Guid grant, long lo, long hi))
        {
            return false;
        }

        _ = InsertBlock(seqoid, epoch, 'n', lo, hi, "next", node.Node.Id, node, grant);
        _ = Spi.Execute("""
            UPDATE worf.sequence_range_block b SET not_before = g.not_before FROM worf.sequence_range_grant g
            WHERE g.grant_id = $1 AND b.block_id = g.grant_id
            """, SpiParameter.Create(grant));
        _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = $1", SpiParameter.Create(grant));
        return true;
    }

    /// <summary>
    /// Grants a block of a size, in steps of the increment, from the start of the lowest free span of this node's root
    /// or pool blocks that holds it, under a row lock, committed with synchronous_commit on. A request granted before
    /// gets the same grant again.
    /// </summary>
    /// <returns>The grant and its bounds, or null when no free span holds the size.</returns>
    internal static (Guid Grant, long Lo, long Hi)? Allocate(uint seqoid, SequenceInfo info, int epoch, long size, uint granteeId,
        string granteeName, Guid requestId, char kind = 'n')
    {
        SpiResult granted = Worf.SelectCatalog("SELECT grant_id, lo, hi FROM worf.sequence_range_grant WHERE request_id = $1 AND kind <> 'x'",
            SpiParameter.Create(requestId));
        if (granted.Count > 0)
        {
            return (granted[0].Get<Guid>(0), granted[0].Get<long>(1), granted[0].Get<long>(2));
        }

        // A block of size steps spans size times the increment, less one step. An allocator whose system identifier or
        // timeline changed grants nothing, even before its worker fences it, since a grant on the old timeline may be lost.
        LockLedger(seqoid);
        decimal span = ((decimal)size - 1) * Math.Abs(info.Increment);
        bool ascending = info.Ascending;
        SpiResult free = Spi.Query(Spi.Sql($"""
            SELECT f.block_id, f.lo, f.hi, f.not_before
            FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
            WHERE b.seqoid = {seqoid}::oid AND b.epoch = {epoch} AND b.kind IN ('r', 'p') AND b.state = 'open'
                AND b.sysid = {(decimal)NativeMethods.GetSystemIdentifier()} AND b.timeline = {(long)NativeMethods.GetWALInsertionTimeLine()}
                AND f.hi::numeric - f.lo >= {span}
            ORDER BY CASE WHEN {ascending} THEN f.lo END, CASE WHEN NOT {ascending} THEN f.hi END DESC
            LIMIT 1
            FOR UPDATE OF f
            """), false, 1);
        if (free.Count == 0)
        {
            return null;
        }

        Guid from = free[0].Get<Guid>(0);
        long freeLo = free[0].Get<long>(1);
        long freeHi = free[0].Get<long>(2);
        long lo = info.Ascending ? freeLo : (long)(freeHi - span);
        long hi = info.Ascending ? (long)(freeLo + span) : freeHi;

        // The span shrinks in place, so that a concurrent allocation waiting on its lock sees what is left of it.
        TakeFromFree(from, lo, hi);

        Guid grant = Guid.NewGuid();
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_grant
            VALUES ($1, $2, $3, $4, $5, $6, $7::"char", $8, $9, 'sent', now(), NULL, $10)
            """, SpiParameter.Create(grant), SpiParameter.Create(requestId), SpiParameter.Create(seqoid), SpiParameter.Create(from),
            SpiParameter.Create(granteeId), SpiParameter.Create(granteeName), SpiParameter.Create(kind.ToString()), SpiParameter.Create(lo),
            SpiParameter.Create(hi), SpiParameter.Create(free[0].Get<long?>(3)));
        _ = Spi.Execute($"UPDATE worf.sequence_range SET watermark = {(info.Ascending ? "greatest" : "least")}(coalesce(watermark, $2), $2) WHERE seqoid = $1",
            SpiParameter.Create(seqoid), SpiParameter.Create(info.Ascending ? hi : lo));

        // A grant that a crash lost could be granted again.
        _ = Spi.Execute("SET LOCAL synchronous_commit = on");
        PgLog.Log($"sequence {info.Name} granted block [{lo}, {hi}] to node {granteeName}");
        WarnWhenRootRunsLow(info, from, (decimal)hi - lo + 1);
        return (grant, lo, hi);
    }

    /// <summary>
    /// Warns when a grant takes the root past nine tenths of its values granted.
    /// </summary>
    private static void WarnWhenRootRunsLow(SequenceInfo info, Guid block, decimal taken)
    {
        decimal? left = Worf.ReadScalar<decimal?>("""
            SELECT l.left
            FROM worf.sequence_range_block b
                CROSS JOIN LATERAL (SELECT coalesce(sum(f.hi::numeric - f.lo + 1), 0) AS left FROM worf.sequence_range_free f WHERE f.block_id = b.block_id) l
            WHERE b.block_id = $1 AND b.kind = 'r' AND l.left * 10 < b.hi::numeric - b.lo + 1 AND (l.left + $2) * 10 >= b.hi::numeric - b.lo + 1
            """, SpiParameter.Create(block), SpiParameter.Create(taken));
        if (left is not null)
        {
            PgLog.Warning($"sequence {info.Name}'s root has granted nine tenths of its values and has {left} left; widen the sequence's type, or re-enable it with a later pool_start, before they run out");
        }
    }

    /// <summary>
    /// Grants exactly [lo, hi] from a free span of this node's root or pool blocks that holds all of it, as for rows that
    /// already carry those keys. A request granted or refused before gets the same answer again.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="info">The sequence's details.</param>
    /// <param name="epoch">The sequence's epoch.</param>
    /// <param name="lo">The span's start.</param>
    /// <param name="hi">The span's end.</param>
    /// <param name="granteeId">The node it goes to.</param>
    /// <param name="granteeName">That node's name.</param>
    /// <param name="requestId">The request.</param>
    /// <param name="kind">'i' for the node that loads the rows, 'f' for a node that grants it on.</param>
    /// <param name="refuse">Whether to record a refusal when no free span holds it, as the owner does, since then some of
    /// the values might be in use.</param>
    /// <returns>The grant, or null when there is none; then <paramref name="refuse"/> says whether it was refused.</returns>
    internal static Guid? AllocateExact(uint seqoid, SequenceInfo info, int epoch, long lo, long hi, uint granteeId, string granteeName,
        Guid requestId, char kind, bool refuse)
    {
        SpiResult answered = Worf.SelectCatalog("SELECT grant_id, kind::text FROM worf.sequence_range_grant WHERE request_id = $1",
            SpiParameter.Create(requestId));
        if (answered.Count > 0)
        {
            // A refusal whose answer was lost goes out again.
            if (answered[0].Get<string>(1) == "x")
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'sent' WHERE request_id = $1", SpiParameter.Create(requestId));
                return null;
            }

            return answered[0].Get<Guid>(0);
        }

        LockLedger(seqoid);
        SpiResult free = Spi.Query(Spi.Sql($"""
            SELECT f.block_id, f.lo, f.hi
            FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
            WHERE b.seqoid = {seqoid}::oid AND b.epoch = {epoch} AND b.kind IN ('r', 'p') AND b.state = 'open'
                AND b.sysid = {(decimal)NativeMethods.GetSystemIdentifier()} AND b.timeline = {(long)NativeMethods.GetWALInsertionTimeLine()}
                AND f.lo <= {lo} AND {hi} <= f.hi
            LIMIT 1
            FOR UPDATE OF f
            """), false, 1);
        Guid grant = Guid.NewGuid();
        if (free.Count == 0)
        {
            if (refuse)
            {
                _ = Spi.Execute("""
                    INSERT INTO worf.sequence_range_grant VALUES ($1, $2, $3, NULL, $4, $5, 'x', $6, $7, 'sent', now())
                    """, SpiParameter.Create(grant), SpiParameter.Create(requestId), SpiParameter.Create(seqoid), SpiParameter.Create(granteeId),
                    SpiParameter.Create(granteeName), SpiParameter.Create(lo), SpiParameter.Create(hi));
                PgLog.Log($"sequence {info.Name} refused [{lo}, {hi}] to node {granteeName}: some of those values may be in use");
            }

            return null;
        }

        Guid from = free[0].Get<Guid>(0);
        TakeFromFree(from, lo, hi);
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_grant VALUES ($1, $2, $3, $4, $5, $6, $7::"char", $8, $9, 'sent', now())
            """, SpiParameter.Create(grant), SpiParameter.Create(requestId), SpiParameter.Create(seqoid), SpiParameter.Create(from),
            SpiParameter.Create(granteeId), SpiParameter.Create(granteeName), SpiParameter.Create(kind.ToString()), SpiParameter.Create(lo),
            SpiParameter.Create(hi));
        _ = Spi.Execute($"UPDATE worf.sequence_range SET watermark = {(info.Ascending ? "greatest" : "least")}(coalesce(watermark, $2), $2) WHERE seqoid = $1",
            SpiParameter.Create(seqoid), SpiParameter.Create(info.Ascending ? hi : lo));
        _ = Spi.Execute("SET LOCAL synchronous_commit = on");
        PgLog.Log($"sequence {info.Name} granted [{lo}, {hi}] exactly to node {granteeName}");
        WarnWhenRootRunsLow(info, from, (decimal)hi - lo + 1);
        return grant;
    }

    /// <summary>
    /// Records a block this node holds, under its identity now.
    /// </summary>
    internal static Guid InsertBlock(uint seqoid, int epoch, char kind, long lo, long hi, string state, uint? fromNode, WorfLocalNode node,
        Guid? id = null, bool reserved = false, long? notBefore = null)
    {
        ulong sysid = NativeMethods.GetSystemIdentifier();
        uint timeline = NativeMethods.GetWALInsertionTimeLine();
        Guid block = id ?? Guid.NewGuid();
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_block
                (block_id, seqoid, epoch, kind, lo, hi, state, from_node, node_id, sysid, timeline, received_at, retired_at, reserved, not_before)
            VALUES ($1, $2, $3, $4::"char", $5, $6, $7, $8, $9, $10, $11, now(), CASE WHEN $7 = 'abandoned' THEN now() END, $12, $13)
            """, SpiParameter.Create(block), SpiParameter.Create(seqoid), SpiParameter.Create(epoch),
            SpiParameter.Create(kind.ToString()), SpiParameter.Create(lo), SpiParameter.Create(hi), SpiParameter.Create(state),
            SpiParameter.Create(fromNode), SpiParameter.Create(node.Node.Id), SpiParameter.Create((decimal)sysid),
            SpiParameter.Create((long)timeline), SpiParameter.Create(reserved), SpiParameter.Create(notBefore));

        // The guard admits an imported block's values.
        if (kind == 'i')
        {
            ChangedOnCommit();
        }

        return block;
    }

    internal static (Guid Id, long Lo, long Hi)? NodeBlock(uint seqoid, int epoch, string state)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT block_id, lo, hi FROM worf.sequence_range_block
            WHERE seqoid = $1 AND epoch = $2 AND kind = 'n' AND state = $3
            ORDER BY received_at LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(epoch), SpiParameter.Create(state));
        return result.Count == 0 ? null : (result[0].Get<Guid>(0), result[0].Get<long>(1), result[0].Get<long>(2));
    }

    /// <summary>
    /// Finds the next block a sequence switches to: one reserved for a load first, and otherwise the one received first.
    /// </summary>
    internal static (Guid Id, long Lo, long Hi, bool Reserved)? NextBlock(uint seqoid, int epoch)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT block_id, lo, hi, reserved FROM worf.sequence_range_block
            WHERE seqoid = $1 AND epoch = $2 AND kind = 'n' AND state = 'next'
            ORDER BY reserved DESC, received_at LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(epoch));
        return result.Count == 0 ? null : (result[0].Get<Guid>(0), result[0].Get<long>(1), result[0].Get<long>(2), result[0].Get<bool>(3));
    }

    /// <summary>
    /// Counts the values, in steps of the increment, a block holds.
    /// </summary>
    internal static long Steps(SequenceInfo info, long lo, long hi)
        => (long)Math.Min(((decimal)hi - lo) / Math.Abs(info.Increment) + 1, long.MaxValue);

    /// <summary>
    /// Returns the last value a managed sequence issued, as <c>worf.sequence_ranges</c> shows it, without the lock that
    /// <c>pg_sequence_last_value</c> takes, which would hold off every switch until the reader's transaction ends.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <returns>The last value, or null before its first.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_position(sequence regclass)
        RETURNS bigint STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static long? SequenceRangePosition([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence)
    {
        uint seqoid = RequireSequence(sequence);

        // As pg_sequence_last_value, it needs SELECT or USAGE on the sequence.
        return Worf.ReadScalar<bool>("SELECT pg_catalog.has_sequence_privilege($1, 'SELECT, USAGE')", SpiParameter.Create(seqoid))
            ? LastValue(seqoid)
            : throw new PgException("42501", $"permission denied for sequence {GetInfo(seqoid).Name}");
    }

    /// <summary>
    /// Reads the last value a sequence issued, or null before its first, from its row. pg_sequence_last_value locks
    /// the sequence as nextval does, until the transaction ends, and ALTER SEQUENCE waits on that lock, so two
    /// transactions that each read a sequence that way and then switch it to a block deadlock. This read holds that
    /// lock only while it reads.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The last value, or null.</returns>
    internal static long? LastValue(uint seqoid) => Elevated.Run(() =>
    {
        SpiResult name = Worf.Read("""
            SELECT n.nspname::text, c.relname::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = $1
            """, 1, SpiParameter.Create(seqoid));

        // A switch's ALTER SEQUENCE writes the sequence to a new file. Its block is visible once it commits, a moment
        // before other sessions learn of the new file, so a read that takes no lock ALTER SEQUENCE waits on can pair the
        // new block with the old position, the quarantine's at the start. The read takes the lock nextval takes, which
        // waits for a switch to finish committing, and lets it go once done, so that it holds off no switch for the rest
        // of the transaction.
        NativeMethods.LockRelationOid(seqoid, (int)PgLockMode.RowExclusive);
        try
        {
            return Worf.ReadScalar<long?>("SELECT CASE WHEN is_called THEN last_value END FROM " +
                Spi.QuoteQualifiedIdentifier(name[0].Get<string>(0), name[0].Get<string>(1)));
        }
        finally
        {
            NativeMethods.UnlockRelationOid(seqoid, (int)PgLockMode.RowExclusive);
        }
    });

    /// <summary>
    /// Counts the values the sequence has left in its active block.
    /// </summary>
    internal static long StepsLeft(uint seqoid, SequenceInfo info, (Guid Id, long Lo, long Hi) block)
    {
        long? last = LastValue(seqoid);
        if (last is not long position)
        {
            return Steps(info, block.Lo, block.Hi);
        }

        decimal left = info.Ascending ? ((decimal)block.Hi - position) / info.Increment : ((decimal)position - block.Lo) / -info.Increment;
        return (long)Math.Max(Math.Min(left, long.MaxValue), 0);
    }

    /// <summary>
    /// Computes the highest value, or lowest for a descending sequence, that the sequence may have issued or its
    /// tables hold. A numeric column's values are rounded away from the root's start and kept within bigint.
    /// </summary>
    private static long? HighWater(SequenceInfo info)
    {
        long? high = null;
        void Take(long? value)
        {
            if (value is long v)
            {
                high = high is not long h ? v : info.Ascending ? Math.Max(h, v) : Math.Min(h, v);
            }
        }

        SpiParameter seq = SpiParameter.Create(info.Seqoid);
        string pick = info.Ascending ? "max" : "min";
        string round = info.Ascending ? "ceil" : "floor";
        // The value a quarantine reserves is never issued.
        long? last = LastValue(info.Seqoid);
        Take(last == info.QuarantineEdge ? null : last);
        Take(Worf.ReadScalar<long?>("SELECT last_value FROM worf.sequence_state WHERE seqoid = $1", seq));
        Take(Worf.ReadScalar<long?>("SELECT watermark FROM worf.sequence_range WHERE seqoid = $1", seq));
        Take(Worf.ReadScalar<long?>("SELECT " + pick + "(" + (info.Ascending ? "hi" : "lo") +
            ") FROM worf.sequence_range_block WHERE seqoid = $1 AND kind IN ('n', 'i')", seq));
        foreach ((uint relid, string column, uint typeOid) in UsingColumns(info.Seqoid))
        {
            string nspname = Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(relid)));
            string relname = Worf.FromServer(NativeMethods.get_rel_name(relid));
            Take(Worf.ReadScalar<long?>(typeOid == NumericOid
                ? "SELECT least(greatest(" + round + "(" + pick + "(" + Spi.QuoteIdentifier(column) +
                    ")), -9223372036854775808), 9223372036854775807)::bigint FROM " + Spi.QuoteQualifiedIdentifier(nspname, relname)
                : "SELECT " + pick + "(" + Spi.QuoteIdentifier(column) + ")::bigint FROM " + Spi.QuoteQualifiedIdentifier(nspname, relname)));
        }

        return high;
    }

    private static string QualifiedName(uint relid)
        => Spi.QuoteQualifiedIdentifier(Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(relid))),
            Worf.FromServer(NativeMethods.get_rel_name(relid)));

    /// <summary>
    /// Puts the guard on every table with a column that uses a managed sequence, checking each such column.
    /// </summary>
    private static void InstallGuards(uint seqoid)
    {
        foreach (uint root in UsingColumns(seqoid).Select(column => TreeRoot(column.Relid)).Distinct())
        {
            InstallGuard(root);
        }
    }

    /// <summary>
    /// Puts the guard on a table's partition tree, or on the table itself when it's no partition, checking each column
    /// that uses a managed sequence anywhere in the tree. The guard is a pair of triggers on the tree's root, which
    /// PostgreSQL gives every partition, that run after each row is written, so that they see the row every BEFORE
    /// trigger left, and whose conditions check the values: those of a new row, and those an update changes. A value
    /// passes under any of the managed sequences its column uses. Columns are referenced by number, so a renamed column
    /// stays checked, and the triggers' arguments list what they check, so that a guard that checks the same is left
    /// alone and its tree's writers aren't held up.
    /// </summary>
    private static void InstallGuard(uint relid)
    {
        if (relid == 0)
        {
            return;
        }

        uint root = TreeRoot(relid);
        Dictionary<string, (uint TypeOid, List<uint> Sequences)> columns = GuardedColumns(root);
        SpiResult attributes = Worf.Read("SELECT attname::text, attnum FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped",
            0, SpiParameter.Create(root));
        Dictionary<string, short> attnums = [];
        for (int i = 0; i < attributes.Count; i++)
        {
            attnums[attributes[i].Get<string>(0)] = attributes[i].Get<short>(1);
        }

        List<(short Attnum, string Name, string Type, List<uint> Sequences)> guarded = [.. columns
            .Select(column => (attnums[column.Key], Spi.QuoteIdentifier(column.Key), column.Value.TypeOid == NumericOid ? "numeric" : "bigint",
                column.Value.Sequences))
            .OrderBy(column => column.Item1)];
        List<string> arguments = [.. guarded.Select(column => $"{column.Attnum}:{string.Join(",", column.Sequences)}")];
        string table = QualifiedName(root);
        SpiResult existing = Worf.Read("""
            SELECT tgname::text, encode(tgargs, 'escape') FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgname = ANY ($2) AND tgparentid = 0
            """, 0, SpiParameter.Create(root), SpiParameter.Create(s_guardNames));
        string encoded = string.Concat(arguments.Select(argument => argument + "\\000"));
        if (guarded.Count == 0 || (existing.Count == s_guardNames.Length && Enumerable.Range(0, existing.Count).All(i => existing[i].Get<string>(1) == encoded)))
        {
            // A tree with no column that uses a managed sequence keeps no guard.
            for (int i = 0; guarded.Count == 0 && i < existing.Count; i++)
            {
                RunInternalDdl($"DROP TRIGGER {Spi.QuoteIdentifier(existing[i].Get<string>(0))} ON {table}");
            }

            return;
        }

        // An update's condition checks only a value it changes, as when it moves a row to another partition, which
        // PostgreSQL checks against the partitioned table's update guard after the row's insert.
        string Check(string check, string name, string type, List<uint> sequences)
        {
            string first = $"{check}({Sequence(sequences[0])}, NEW.{name}::{type})";
            return sequences.Count == 1
                ? first
                : $"(NOT ({string.Join(" OR ", sequences.Select(seq => $"worf.sequence_range_admits({Sequence(seq)}, NEW.{name}::{type})"))}) AND {first})";
        }

        static string Sequence(uint seqoid) => Spi.QuoteLiteral(GetInfo(seqoid).Name) + "::regclass";

        string function = $"worf.sequence_range_guard({string.Join(", ", arguments.Select(Spi.QuoteLiteral))})";
        RunInternalDdl($"""
            CREATE OR REPLACE TRIGGER {GuardName} AFTER INSERT ON {table}
            FOR EACH ROW WHEN ({string.Join(" OR ", guarded.Select(c => Check("worf.sequence_range_check", c.Name, c.Type, c.Sequences)))})
            EXECUTE FUNCTION {function}
            """);
        RunInternalDdl($"""
            CREATE OR REPLACE TRIGGER {UpdateGuardName} AFTER UPDATE ON {table}
            FOR EACH ROW WHEN ({string.Join(" OR ", guarded.Select(c =>
                $"(OLD.{c.Name} IS DISTINCT FROM NEW.{c.Name} AND {Check("worf.sequence_range_check_update", c.Name, c.Type, c.Sequences)})"))})
            EXECUTE FUNCTION {function}
            """);
    }

    /// <summary>
    /// Finds the root of a table's partition tree: the table itself when it's no partition.
    /// </summary>
    private static uint TreeRoot(uint relid)
        => relid == 0 ? 0 : Worf.ReadScalar<uint?>("SELECT pg_catalog.pg_partition_root($1::regclass)::oid", SpiParameter.Create(relid)) ?? relid;

    /// <summary>
    /// Lists, by name, the columns of a table and its partitions that use a managed sequence, with their types and the
    /// sequences they use.
    /// </summary>
    private static Dictionary<string, (uint TypeOid, List<uint> Sequences)> GuardedColumns(uint top)
    {
        HashSet<uint> tree = [top];
        SpiResult partitions = Worf.Read("SELECT relid::oid FROM pg_catalog.pg_partition_tree($1::regclass)", 0, SpiParameter.Create(top));
        for (int i = 0; i < partitions.Count; i++)
        {
            _ = tree.Add(partitions[i].Get<uint>(0));
        }

        Dictionary<string, (uint TypeOid, List<uint> Sequences)> columns = [];
        SpiResult managed = Elevated.Run(() => Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range ORDER BY seqoid"));
        for (int i = 0; i < managed.Count; i++)
        {
            uint managedSeq = managed[i].Get<uint>(0);
            foreach ((uint relid, string column, uint typeOid) in UsingColumns(managedSeq).Where(entry => tree.Contains(entry.Relid)))
            {
                RequireCheckable(relid, column, typeOid, managedSeq);
                if (!columns.TryGetValue(column, out (uint TypeOid, List<uint> Sequences) entry))
                {
                    entry = (typeOid, []);
                    columns.Add(column, entry);
                }

                if (!entry.Sequences.Contains(managedSeq))
                {
                    entry.Sequences.Add(managedSeq);
                }
            }
        }

        return columns;
    }
}
