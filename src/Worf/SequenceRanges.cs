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
        root_end bigint NOT NULL,
        peak_rate double precision NOT NULL DEFAULT 0,
        switched_at timestamptz,
        exchanged_at timestamptz,
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
        retired_at timestamptz
    );

    CREATE TABLE worf.sequence_range_free (
        block_id uuid NOT NULL REFERENCES worf.sequence_range_block,
        lo bigint NOT NULL,
        hi bigint NOT NULL,
        PRIMARY KEY (block_id, lo)
    );

    CREATE TABLE worf.sequence_range_grant (
        grant_id uuid PRIMARY KEY,
        request_id uuid NOT NULL UNIQUE,
        seqoid regclass NOT NULL,
        from_block uuid NOT NULL REFERENCES worf.sequence_range_block,
        grantee_id oid NOT NULL,
        grantee_name name NOT NULL,
        kind "char" NOT NULL,
        lo bigint NOT NULL,
        hi bigint NOT NULL,
        state text NOT NULL,
        granted_at timestamptz NOT NULL
    );

    CREATE TABLE worf.sequence_range_request (
        request_id uuid PRIMARY KEY,
        seqoid regclass NOT NULL,
        parent_node oid NOT NULL,
        kind "char" NOT NULL,
        size bigint NOT NULL,
        lo bigint,
        hi bigint,
        created_at timestamptz NOT NULL
    );

    CREATE TABLE worf.sequence_range_node (
        seqoid regclass NOT NULL,
        node_id oid NOT NULL,
        node_name name NOT NULL,
        via_node oid NOT NULL,
        sysid numeric(20,0),
        timeline bigint,
        wal_lsn pg_lsn,
        distance integer,
        high_water bigint,
        watermark bigint,
        reported_at timestamptz NOT NULL,
        PRIMARY KEY (seqoid, node_id)
    );

    CREATE VIEW worf.sequence_ranges AS
        SELECT r.seqoid AS sequence, o.node_name AS owner, r.epoch, p.node_name AS parent, r.state,
            a.lo AS active_lo, a.hi AS active_hi,
            CASE
                WHEN a.block_id IS NULL THEN NULL
                WHEN s.seqincrement > 0 THEN
                    (a.hi::numeric - coalesce(pg_catalog.pg_sequence_last_value(r.seqoid), a.lo - s.seqincrement)) / s.seqincrement
                ELSE (coalesce(pg_catalog.pg_sequence_last_value(r.seqoid), a.hi - s.seqincrement)::numeric - a.lo) / -s.seqincrement
            END::bigint AS values_left,
            n.lo AS next_lo, n.hi AS next_hi, r.peak_rate AS insert_rate, r.switched_at AS last_switch,
            r.exchanged_at AS last_exchange, r.detail
        FROM worf.sequence_range r
            JOIN pg_catalog.pg_sequence s ON s.seqrelid = r.seqoid
            LEFT JOIN worf.node o ON o.node_id = r.owner_node
            LEFT JOIN worf.node p ON p.node_id = r.parent_node
            LEFT JOIN worf.sequence_range_block a
                ON a.seqoid = r.seqoid AND a.epoch = r.epoch AND a.kind = 'n' AND a.state = 'active'
            LEFT JOIN worf.sequence_range_block n
                ON n.seqoid = r.seqoid AND n.epoch = r.epoch AND n.kind = 'n' AND n.state = 'next';

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
    /// The name of the trigger that checks values written by hand.
    /// </summary>
    internal const string GuardName = "worf_sequence_range_guard";

    private const string CheckViolation = "23514";
    private const string SequenceLimitExceeded = "2200H";

    private static bool s_changedOnCommit;

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
    /// Gets or sets whether worf itself runs the DDL that managed sequences otherwise reject.
    /// </summary>
    internal static bool InternalDdl { get; set; }

    /// <summary>
    /// Gets the counter that tells backends to reload the blocks they cache for the guard.
    /// </summary>
    internal static PgAtomic<long> Version { get; } = new("worf_sequence_range_version");

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
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
        _ = force;

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

        // The root ends at the sequence's own limit, its type's, and those of the columns it fills.
        long end = info.Ascending ? info.Max : info.Min;
        foreach ((_, _, uint typeOid) in UsingColumns(seqoid))
        {
            (long min, long max) = TypeRange(typeOid);
            end = info.Ascending ? Math.Min(end, max) : Math.Max(end, min);
        }

        (long defaultSize, long defaultMax) = DefaultSizes(info.TypeOid);
        long size = rangeSize ?? defaultSize;
        long maxSize = Math.Max(maxRangeSize ?? defaultMax, size);
        if (size <= 0)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "range_size must be positive");
        }

        SpiResult existing = Worf.SelectCatalog("SELECT epoch, state FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
        int epoch = 1;
        if (existing.Count > 0)
        {
            if (existing[0].Get<string>(1) != "disabled")
            {
                throw new PgException(PgSqlStates.ObjectInUse, $"sequence {info.Name} is already under range management",
                    hint: "Disable it with worf.disable_sequence_range first.");
            }

            epoch = existing[0].Get<int>(0) + 1;
            _ = Spi.Execute("DELETE FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
        }

        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range (seqoid, owner_node, epoch, range_size, max_range_size, state, distance,
                request_size, participants, pool_start, root_end)
            VALUES ($1, $2, $3, $4, $5, 'collecting', 0, $4, $6::name[], $7, $8)
            """,
            SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(epoch),
            SpiParameter.Create(size), SpiParameter.Create(maxSize), SpiParameter.Create(participants),
            SpiParameter.Create(poolStart), SpiParameter.Create(end));

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
        Activate(seqoid, node);
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
        if (rangeSize is <= 0)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "range_size must be positive");
        }

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
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'disabled' WHERE seqoid = $1", SpiParameter.Create(seqoid));
        _ = Spi.Execute("""
            UPDATE worf.sequence_range_block SET state = 'spent'
            WHERE seqoid = $1 AND kind IN ('r', 'p') AND state = 'open'
            """, SpiParameter.Create(seqoid));
        ChangedOnCommit();
    }

    /// <summary>
    /// Abandons this node's blocks of one or every managed sequence, quarantines them and requests new blocks.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.reset_sequence_range(sequence regclass DEFAULT NULL)
        RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void ResetSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum? sequence)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
        SpiResult managed = sequence is null || sequence.IsNull
            ? Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range")
            : Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(RequireSequence(sequence)));
        for (int i = 0; i < managed.Count; i++)
        {
            Reset(managed[i].Get<uint>(0), node);
        }

        ChangedOnCommit();
    }

    /// <summary>
    /// Removes a dropped sequence's range state.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void Forget(uint seqoid)
    {
        if (!Spi.ExecuteScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL"))
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
        => Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid)).Count > 0;

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
        SpiResult rows = Worf.SelectCatalog(
            "SELECT state, epoch, owner_node, request_size FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
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
        (Guid Id, long Lo, long Hi)? active = NodeBlock(seqoid, epoch, "active");
        (Guid Id, long Lo, long Hi)? next = NodeBlock(seqoid, epoch, "next");
        bool changed = false;

        if (active is null)
        {
            if (next is not null)
            {
                changed = Switch(seqoid, info, next.Value);
                next = null;
            }
        }
        else
        {
            long left = StepsLeft(seqoid, info, active.Value);
            long size = Steps(info, active.Value.Lo, active.Value.Hi);
            // A minute of inserts, at least a twentieth of the block, but no more than half of it, so that a block
            // smaller than a minute's use still serves most of its values while the next ones grow.
            long margin = Math.Min(Math.Max(size / 20, (long)(rate * 60)), size / 2);
            if (left <= margin && next is not null)
            {
                changed = Switch(seqoid, info, next.Value);
                next = null;
            }
            else if (left <= 0 && next is null && state != "exhausted")
            {
                _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'exhausted' WHERE seqoid = $1", SpiParameter.Create(seqoid));
                PgLog.Warning($"sequence {info.Name} has used up its block and has no next block yet");
            }
        }

        // The owner grants its own next block from the root. Another node requests one from its parent, committed
        // before the exchange sends it.
        if (next is null && NodeBlock(seqoid, epoch, "next") is null)
        {
            if (owner == node.Node.Id)
            {
                changed |= GrantOwnBlock(seqoid, info, node, epoch);
            }
            else
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

        return changed;
    }

    /// <summary>
    /// Rejects DDL that would let values escape range management, except when worf itself runs it: changing a managed
    /// sequence, granting UPDATE on one, changing the default or identity of a column that uses one, dropping,
    /// renaming or disabling the guard, and restarting a guarded table's sequences.
    /// </summary>
    /// <param name="parsetree">The statement, before it runs.</param>
    internal static void CheckDdl(Node* parsetree)
    {
        if (InternalDdl || parsetree->type is not (NodeTag.T_AlterSeqStmt or NodeTag.T_GrantStmt or NodeTag.T_AlterTableStmt or
            NodeTag.T_DropStmt or NodeTag.T_RenameStmt or NodeTag.T_TruncateStmt) || !AnyManaged())
        {
            return;
        }

        switch (parsetree->type)
        {
            case NodeTag.T_AlterSeqStmt:
                if (IsManaged(Resolve(((AlterSeqStmt*)parsetree)->sequence)))
                {
                    throw Rejected("ALTER SEQUENCE on a managed sequence", "Use worf.alter_sequence_range, or disable range management first.");
                }

                break;

            case NodeTag.T_GrantStmt:
                CheckGrant((GrantStmt*)parsetree);
                break;

            case NodeTag.T_AlterTableStmt:
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                uint relid = Resolve(alter->relation);
                List* cmds = alter->cmds;
                for (int i = 0; relid != 0 && cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    string? name = cmd->name is null ? null : Worf.FromServer(cmd->name);
                    bool rejected = cmd->subtype switch
                    {
                        AlterTableType.AT_ColumnDefault or AlterTableType.AT_AddIdentity or AlterTableType.AT_SetIdentity or
                            AlterTableType.AT_DropIdentity => name is not null && ColumnUsesManaged(relid, name),
                        AlterTableType.AT_DisableTrig or AlterTableType.AT_EnableReplicaTrig or AlterTableType.AT_EnableAlwaysTrig =>
                            name == GuardName,
                        AlterTableType.AT_DisableTrigAll or AlterTableType.AT_DisableTrigUser => IsGuarded(relid),
                        _ => false,
                    };
                    if (rejected)
                    {
                        throw Rejected("changing a column that uses a managed sequence, or its table's guard",
                            "Disable range management first.");
                    }
                }

                break;

            case NodeTag.T_DropStmt when ((DropStmt*)parsetree)->removeType == ObjectType.OBJECT_TRIGGER:
                List* objects = ((DropStmt*)parsetree)->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    if (Worf.FromServer(NativeMethods.NameListToString((List*)objects->elements[i].ptr_value)).EndsWith("." + GuardName, StringComparison.Ordinal))
                    {
                        throw Rejected("dropping the guard of a table that uses a managed sequence", "Disable range management first.");
                    }
                }

                break;

            case NodeTag.T_RenameStmt when ((RenameStmt*)parsetree)->renameType == ObjectType.OBJECT_TRIGGER &&
                Worf.FromServer(((RenameStmt*)parsetree)->subname) == GuardName:
                throw Rejected("renaming the guard of a table that uses a managed sequence", "Disable range management first.");

            case NodeTag.T_TruncateStmt when ((TruncateStmt*)parsetree)->restart_seqs:
                List* relations = ((TruncateStmt*)parsetree)->relations;
                for (int i = 0; relations is not null && i < relations->length; i++)
                {
                    if (IsGuarded(Resolve((RangeVar*)relations->elements[i].ptr_value)))
                    {
                        throw Rejected("TRUNCATE ... RESTART IDENTITY on a table that uses a managed sequence", "Truncate it without RESTART IDENTITY.");
                    }
                }

                break;

            default:
                break;
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
        else if (grant->targtype == GrantTargetType.ACL_TARGET_ALL_IN_SCHEMA && objects is not null)
        {
            // A schema name is a list of one name, which NameListToString joins with dots.
            string[] schemas = Worf.FromServer(NativeMethods.NameListToString(objects)).Split('.');
            managed = Spi.ExecuteScalar<bool>("""
                SELECT EXISTS (
                    SELECT 1 FROM worf.sequence_range r
                        JOIN pg_catalog.pg_class c ON c.oid = r.seqoid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = ANY ($1))
                """, SpiParameter.Create(schemas));
        }

        if (managed)
        {
            throw Rejected("granting UPDATE on a managed sequence", "Only worf sets a managed sequence's position.");
        }
    }

    private static bool AnyManaged()
        => Spi.ExecuteScalar<bool>("SELECT to_regclass('worf.sequence_range') IS NOT NULL") &&
            Spi.ExecuteScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range)");

    private static bool IsGuarded(uint relid)
        => relid != 0 && Spi.ExecuteScalar<bool>("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgname = $2)",
            SpiParameter.Create(relid), SpiParameter.Create(GuardName));

    private static bool ColumnUsesManaged(uint relid, string column)
    {
        SpiResult managed = Spi.Select("SELECT seqoid::oid FROM worf.sequence_range");
        for (int i = 0; i < managed.Count; i++)
        {
            if (UsingColumns(managed[i].Get<uint>(0)).Exists(c => c.Relid == relid && c.Column == column))
            {
                return true;
            }
        }

        return false;
    }

    private static uint Resolve(RangeVar* relation)
        => relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, 0, 1, default, null);

    private static PgException Rejected(string what, string hint)
        => new("55000", $"{what} is not allowed under worf's sequence range management", hint: hint);

    /// <summary>
    /// Lists the columns, with their tables and types, whose default or identity uses a sequence.
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
            columns.Add((result[i].Get<uint>(0), result[i].Get<string>(1), result[i].Get<uint>(2)));
        }

        return columns;
    }

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
            _ = Spi.Execute(sql);
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
    /// Tells backends to reload their blocks once this transaction commits, so they don't cache what it hasn't.
    /// </summary>
    internal static void ChangedOnCommit()
    {
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
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () => s_changedOnCommit = false);
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

    private static uint RequireSequence(PgDatum? sequence)
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
        // The guard's and the quarantine's locks hold off writers and nextval, so the high-water mark sees every value.
        InstallGuards(seqoid);
        SequenceInfo info = GetInfo(seqoid);
        Quarantine(info);
        long? high = HighWater(info);
        _ = Spi.Execute("UPDATE worf.sequence_range SET high_water = $2 WHERE seqoid = $1", SpiParameter.Create(seqoid), SpiParameter.Create(high));
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, distance, high_water, reported_at)
            VALUES ($1, $2, $3, $2, 0, $4, now())
            ON CONFLICT (seqoid, node_id) DO UPDATE SET high_water = excluded.high_water, reported_at = excluded.reported_at
            """, SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(node.Node.Name),
            SpiParameter.Create(high));
    }

    /// <summary>
    /// Joins this node to a sequence that a neighbor reports under range management, which this node has too.
    /// </summary>
    /// <param name="seqoid">The sequence here.</param>
    /// <param name="owner">Its owner.</param>
    /// <param name="epoch">Its epoch.</param>
    /// <param name="rangeSize">The size of a node's first block.</param>
    /// <param name="maxRangeSize">The largest block a node requests.</param>
    /// <param name="node">This node.</param>
    internal static void JoinFromNeighbor(uint seqoid, uint owner, int epoch, long rangeSize, long maxRangeSize, WorfLocalNode node)
    {
        SequenceInfo info = GetInfo(seqoid);
        string? refused = info.Persistence != 'p' ? "the sequence is temporary or unlogged here"
            : info.Cycle ? "the sequence cycles here"
            : Worf.SelectCatalog("SELECT 1 FROM worf.replication_set_seq WHERE set_seqoid = $1", SpiParameter.Create(seqoid)).Count > 0
                ? "the sequence is in a replication set here"
                : null;
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range (seqoid, owner_node, epoch, range_size, max_range_size, state, request_size, root_end, detail)
            VALUES ($1, $2, $3, $4, $5, $6, $4, $7, $8)
            """, SpiParameter.Create(seqoid), SpiParameter.Create(owner), SpiParameter.Create(epoch), SpiParameter.Create(rangeSize),
            SpiParameter.Create(maxRangeSize), SpiParameter.Create(refused is null ? "quarantined" : "refused"),
            SpiParameter.Create(info.Ascending ? info.Max : info.Min), SpiParameter.Create(refused));
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
        bool ready = Spi.ExecuteScalar<bool>("""
            SELECT r.state = 'collecting' AND r.participants IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM unnest(r.participants) p(name)
                WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_node n WHERE n.seqoid = r.seqoid AND n.node_name = p.name))
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
        SpiResult range = Worf.SelectCatalog("SELECT epoch, pool_start, root_end, state FROM worf.sequence_range WHERE seqoid = $1",
            SpiParameter.Create(seqoid));
        int epoch = range[0].Get<int>(0);
        long? poolStart = range[0].Get<long?>(1);
        long end = range[0].Get<long>(2);
        if (range[0].Get<string>(3) != "collecting")
        {
            return;
        }

        long? high = Worf.SelectCatalog(info.Ascending
                ? "SELECT max(high_water) FROM worf.sequence_range_node WHERE seqoid = $1"
                : "SELECT min(high_water) FROM worf.sequence_range_node WHERE seqoid = $1",
            SpiParameter.Create(seqoid))[0].Get<long?>(0);

        long start;
        if (info.Ascending)
        {
            start = Math.Max(Math.Max(poolStart ?? info.QuarantineEdge + 1, high is long h ? h + 1 : info.QuarantineEdge + 1),
                info.QuarantineEdge + 1);
            if (start > end)
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

        (long lo, long hi) = info.Ascending ? (start, end) : (end, start);
        Guid root = InsertBlock(seqoid, epoch, 'r', lo, hi, "open", null, node);
        _ = Spi.Execute("INSERT INTO worf.sequence_range_free VALUES ($1, $2, $3)",
            SpiParameter.Create(root), SpiParameter.Create(lo), SpiParameter.Create(hi));
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'quarantined' WHERE seqoid = $1", SpiParameter.Create(seqoid));

        // The owner needs no exchange for its own blocks, so it leaves quarantine at once.
        _ = GrantOwnBlock(seqoid, info, node, epoch);
        (Guid Id, long Lo, long Hi)? next = NodeBlock(seqoid, epoch, "next");
        if (next is not null)
        {
            _ = Switch(seqoid, info, next.Value);
            _ = GrantOwnBlock(seqoid, info, node, epoch);
        }
    }

    /// <summary>
    /// Abandons this node's node blocks of a sequence, quarantines it, and on the owner grants and switches to a new
    /// block.
    /// </summary>
    private static void Reset(uint seqoid, WorfLocalNode node)
    {
        SequenceInfo info = GetInfo(seqoid);
        _ = Spi.Execute("""
            UPDATE worf.sequence_range_block SET state = 'abandoned'
            WHERE seqoid = $1 AND kind IN ('n', 'i') AND state IN ('next', 'active', 'imported')
            """, SpiParameter.Create(seqoid));
        Quarantine(info);
        _ = Spi.Execute("UPDATE worf.sequence_range SET state = 'quarantined' WHERE seqoid = $1 AND state NOT IN ('collecting', 'disabled')",
            SpiParameter.Create(seqoid));
        SpiResult range = Worf.SelectCatalog("SELECT epoch, owner_node, state FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
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
    /// <returns>Whether it switched; it doesn't when a transaction that called nextval holds the sequence.</returns>
    private static bool Switch(uint seqoid, SequenceInfo info, (Guid Id, long Lo, long Hi) block)
    {
        string lo = block.Lo.ToString(CultureInfo.InvariantCulture);
        string hi = block.Hi.ToString(CultureInfo.InvariantCulture);
        string start = info.Ascending ? lo : hi;
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                _ = Spi.Execute("SET LOCAL lock_timeout = '200ms'");
                RunInternalDdl($"ALTER SEQUENCE {info.Name} NO CYCLE CACHE 1 MINVALUE {lo} MAXVALUE {hi} START WITH {start} RESTART WITH {start}");
                _ = Spi.Execute("SET LOCAL lock_timeout = 0");
            });
        }
        catch (PgException error) when (error.SqlState == "55P03")
        {
            return false;
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
        SpiResult granted = Worf.SelectCatalog("SELECT grant_id, lo, hi FROM worf.sequence_range_grant WHERE request_id = $1",
            SpiParameter.Create(requestId));
        if (granted.Count > 0)
        {
            return (granted[0].Get<Guid>(0), granted[0].Get<long>(1), granted[0].Get<long>(2));
        }

        // A block of size steps spans size times the increment, less one step.
        decimal span = ((decimal)size - 1) * Math.Abs(info.Increment);
        bool ascending = info.Ascending;
        SpiResult free = Spi.Query(Spi.Sql($"""
            SELECT f.block_id, f.lo, f.hi
            FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
            WHERE b.seqoid = {seqoid}::oid AND b.epoch = {epoch} AND b.kind IN ('r', 'p') AND b.state = 'open'
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

        _ = Spi.Execute("DELETE FROM worf.sequence_range_free WHERE block_id = $1 AND lo = $2", SpiParameter.Create(from), SpiParameter.Create(freeLo));
        if (info.Ascending ? hi < freeHi : lo > freeLo)
        {
            _ = Spi.Execute("INSERT INTO worf.sequence_range_free VALUES ($1, $2, $3)", SpiParameter.Create(from),
                SpiParameter.Create(info.Ascending ? hi + 1 : freeLo), SpiParameter.Create(info.Ascending ? freeHi : lo - 1));
        }

        Guid grant = Guid.NewGuid();
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_grant
            VALUES ($1, $2, $3, $4, $5, $6, $7::"char", $8, $9, 'sent', now())
            """, SpiParameter.Create(grant), SpiParameter.Create(requestId), SpiParameter.Create(seqoid), SpiParameter.Create(from),
            SpiParameter.Create(granteeId), SpiParameter.Create(granteeName), SpiParameter.Create(kind.ToString()), SpiParameter.Create(lo),
            SpiParameter.Create(hi));
        _ = Spi.Execute($"UPDATE worf.sequence_range SET watermark = {(info.Ascending ? "greatest" : "least")}(coalesce(watermark, $2), $2) WHERE seqoid = $1",
            SpiParameter.Create(seqoid), SpiParameter.Create(info.Ascending ? hi : lo));

        // A grant that a crash lost could be granted again.
        _ = Spi.Execute("SET LOCAL synchronous_commit = on");
        PgLog.Log($"sequence {info.Name} granted block [{lo}, {hi}] to node {granteeName}");
        return (grant, lo, hi);
    }

    /// <summary>
    /// Records a block this node holds, under its identity now.
    /// </summary>
    internal static Guid InsertBlock(uint seqoid, int epoch, char kind, long lo, long hi, string state, uint? fromNode, WorfLocalNode node,
        Guid? id = null)
    {
        ulong sysid = NativeMethods.GetSystemIdentifier();
        uint timeline = NativeMethods.GetWALInsertionTimeLine();
        Guid block = id ?? Guid.NewGuid();
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_block
                (block_id, seqoid, epoch, kind, lo, hi, state, from_node, node_id, sysid, timeline, received_at)
            VALUES ($1, $2, $3, $4::"char", $5, $6, $7, $8, $9, $10::numeric, $11, now())
            """, SpiParameter.Create(block), SpiParameter.Create(seqoid), SpiParameter.Create(epoch),
            SpiParameter.Create(kind.ToString()), SpiParameter.Create(lo), SpiParameter.Create(hi), SpiParameter.Create(state),
            SpiParameter.Create(fromNode), SpiParameter.Create(node.Node.Id), SpiParameter.Create(sysid.ToString(CultureInfo.InvariantCulture)),
            SpiParameter.Create((long)timeline));
        return block;
    }

    private static (Guid Id, long Lo, long Hi)? NodeBlock(uint seqoid, int epoch, string state)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT block_id, lo, hi FROM worf.sequence_range_block
            WHERE seqoid = $1 AND epoch = $2 AND kind = 'n' AND state = $3
            ORDER BY received_at LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(epoch), SpiParameter.Create(state));
        return result.Count == 0 ? null : (result[0].Get<Guid>(0), result[0].Get<long>(1), result[0].Get<long>(2));
    }

    /// <summary>
    /// Counts the values, in steps of the increment, a block holds.
    /// </summary>
    private static long Steps(SequenceInfo info, long lo, long hi)
        => (long)Math.Min(((decimal)hi - lo) / Math.Abs(info.Increment) + 1, long.MaxValue);

    /// <summary>
    /// Counts the values the sequence has left in its active block.
    /// </summary>
    private static long StepsLeft(uint seqoid, SequenceInfo info, (Guid Id, long Lo, long Hi) block)
    {
        long? last = Worf.SelectCatalog("SELECT pg_catalog.pg_sequence_last_value($1::regclass)", SpiParameter.Create(seqoid))[0].Get<long?>(0);
        if (last is not long position)
        {
            return Steps(info, block.Lo, block.Hi);
        }

        decimal left = info.Ascending ? ((decimal)block.Hi - position) / info.Increment : ((decimal)position - block.Lo) / -info.Increment;
        return (long)Math.Max(Math.Min(left, long.MaxValue), 0);
    }

    /// <summary>
    /// Computes the highest value, or lowest for a descending sequence, that the sequence may have issued or its
    /// tables hold.
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
        Take(Spi.ExecuteScalar<long?>("SELECT pg_catalog.pg_sequence_last_value($1::regclass)", seq));
        Take(Spi.ExecuteScalar<long?>("SELECT last_value FROM worf.sequence_state WHERE seqoid = $1", seq));
        Take(Spi.ExecuteScalar<long?>("SELECT watermark FROM worf.sequence_range WHERE seqoid = $1", seq));
        Take(Spi.ExecuteScalar<long?>("SELECT " + pick + "(" + (info.Ascending ? "hi" : "lo") +
            ") FROM worf.sequence_range_block WHERE seqoid = $1 AND kind IN ('n', 'i')", seq));
        foreach ((uint relid, string column, _) in UsingColumns(info.Seqoid))
        {
            Take(Spi.ExecuteScalar<long?>("SELECT " + pick + "(" + Spi.QuoteIdentifier(column) + ")::bigint FROM " +
                Spi.QuoteQualifiedIdentifier(Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(relid))),
                    Worf.FromServer(NativeMethods.get_rel_name(relid)))));
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
        HashSet<uint> tables = [.. UsingColumns(seqoid).Select(column => column.Relid)];
        foreach (uint relid in tables)
        {
            List<string> arguments = [];
            List<string> columns = [];
            SpiResult managed = Worf.SelectCatalog("SELECT seqoid::oid FROM worf.sequence_range");
            for (int i = 0; i < managed.Count; i++)
            {
                uint managedSeq = managed[i].Get<uint>(0);
                foreach ((uint table, string column, uint typeOid) in UsingColumns(managedSeq))
                {
                    if (table == relid)
                    {
                        columns.Add(Spi.QuoteIdentifier(column));
                        arguments.Add(Spi.QuoteLiteral(column));
                        arguments.Add(Spi.QuoteLiteral(managedSeq.ToString(CultureInfo.InvariantCulture)));
                        arguments.Add(Spi.QuoteLiteral(typeOid.ToString(CultureInfo.InvariantCulture)));
                    }
                }
            }

            RunInternalDdl($"""
                CREATE OR REPLACE TRIGGER {GuardName} BEFORE INSERT OR UPDATE OF {string.Join(", ", columns)} ON {QualifiedName(relid)}
                FOR EACH ROW EXECUTE FUNCTION worf.sequence_range_guard({string.Join(", ", arguments)})
                """);
        }
    }
}
