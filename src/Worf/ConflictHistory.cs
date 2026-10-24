using System.Globalization;
using System.Text;
using Ankus.Postgres;

[assembly: PgSql("conflict-history", """
    -- Conflict history persistence

    -- ============================================================================
    -- Conflict History Table (Partitioned)
    -- ============================================================================

    CREATE TABLE worf.conflict_history (
        -- Identity
        id                  BIGSERIAL,
        recorded_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

        -- Subscription context
        sub_id              OID NOT NULL,
        sub_name            NAME,

        -- Conflict classification
        conflict_type       TEXT NOT NULL CHECK (conflict_type IN (
                                'insert_insert', 'update_update',
                                'update_delete', 'delete_delete')),
        resolution          TEXT NOT NULL CHECK (resolution IN (
                                'apply_remote', 'keep_local', 'skip')),

        -- Affected relation
        schema_name         NAME NOT NULL,
        table_name          NAME NOT NULL,
        index_name          NAME,

        -- Local tuple information
        local_tuple         JSONB,
        local_xid           XID,
        local_origin        INTEGER,
        local_commit_ts     TIMESTAMPTZ,

        -- Remote tuple information
        remote_tuple        JSONB,
        remote_origin       INTEGER NOT NULL,
        remote_commit_ts    TIMESTAMPTZ NOT NULL,
        remote_commit_lsn   PG_LSN NOT NULL,

        -- Flags
        has_before_triggers BOOLEAN NOT NULL DEFAULT FALSE,

        PRIMARY KEY (recorded_at, id)
    ) PARTITION BY RANGE (recorded_at);

    COMMENT ON TABLE worf.conflict_history IS
        'Records replication conflicts detected by the apply worker for analysis and monitoring';

    COMMENT ON COLUMN worf.conflict_history.id IS
        'Unique identifier within partition';
    COMMENT ON COLUMN worf.conflict_history.recorded_at IS
        'When conflict was recorded (partition key)';
    COMMENT ON COLUMN worf.conflict_history.sub_id IS
        'Subscription OID from worf.subscription';
    COMMENT ON COLUMN worf.conflict_history.sub_name IS
        'Subscription name (denormalized for queries)';
    COMMENT ON COLUMN worf.conflict_history.conflict_type IS
        'Type of conflict: insert_insert, update_update, update_delete, delete_delete';
    COMMENT ON COLUMN worf.conflict_history.resolution IS
        'How conflict was resolved: apply_remote, keep_local, skip';
    COMMENT ON COLUMN worf.conflict_history.schema_name IS
        'Schema containing the affected table';
    COMMENT ON COLUMN worf.conflict_history.table_name IS
        'Name of the affected table';
    COMMENT ON COLUMN worf.conflict_history.index_name IS
        'Index where conflict was detected';
    COMMENT ON COLUMN worf.conflict_history.local_tuple IS
        'Local row data as JSONB (if tuple storage enabled)';
    COMMENT ON COLUMN worf.conflict_history.local_xid IS
        'Transaction ID that created local tuple';
    COMMENT ON COLUMN worf.conflict_history.local_origin IS
        'Replication origin of local tuple';
    COMMENT ON COLUMN worf.conflict_history.local_commit_ts IS
        'Commit timestamp of local tuple';
    COMMENT ON COLUMN worf.conflict_history.remote_tuple IS
        'Remote row data as JSONB (if tuple storage enabled)';
    COMMENT ON COLUMN worf.conflict_history.remote_origin IS
        'Replication origin of remote change';
    COMMENT ON COLUMN worf.conflict_history.remote_commit_ts IS
        'Commit timestamp on remote origin';
    COMMENT ON COLUMN worf.conflict_history.remote_commit_lsn IS
        'LSN of remote commit';
    COMMENT ON COLUMN worf.conflict_history.has_before_triggers IS
        'Whether BEFORE triggers modified remote tuple';

    -- ============================================================================
    -- Indexes
    -- ============================================================================

    -- Query by subscription
    CREATE INDEX conflict_history_sub_id_idx
        ON worf.conflict_history (sub_id);

    -- Query by affected table
    CREATE INDEX conflict_history_relation_idx
        ON worf.conflict_history (schema_name, table_name);

    -- Query by conflict type
    CREATE INDEX conflict_history_type_idx
        ON worf.conflict_history (conflict_type);

    -- Query by resolution
    CREATE INDEX conflict_history_resolution_idx
        ON worf.conflict_history (resolution);

    -- ============================================================================
    -- Partition Management Functions
    -- ============================================================================

    CREATE OR REPLACE FUNCTION worf.conflict_history_ensure_partition(
        target_date DATE DEFAULT CURRENT_DATE
    ) RETURNS VOID AS $$
    DECLARE
        partition_name TEXT;
        start_date DATE;
        end_date DATE;
    BEGIN
        -- Calculate month boundaries
        start_date := date_trunc('month', target_date)::DATE;
        end_date := (date_trunc('month', target_date) + interval '1 month')::DATE;

        -- Generate partition name
        partition_name := 'conflict_history_' || to_char(target_date, 'YYYY_MM');

        -- Check if partition already exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'worf'
            AND c.relname = partition_name
        ) THEN
            -- Create the partition
            EXECUTE format(
                'CREATE TABLE worf.%I PARTITION OF worf.conflict_history
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
        END IF;
    END;
    $$ LANGUAGE plpgsql;

    COMMENT ON FUNCTION worf.conflict_history_ensure_partition(DATE) IS
        'Ensures a partition exists for the specified date. Creates the partition if it does not exist.';

    CREATE OR REPLACE FUNCTION worf.conflict_history_cleanup(
        retention_days INTEGER DEFAULT 30
    ) RETURNS INTEGER AS $$
    DECLARE
        partition_rec RECORD;
        dropped_count INTEGER := 0;
        cutoff_date DATE;
        partition_end_date DATE;
    BEGIN
        -- Calculate cutoff date
        cutoff_date := CURRENT_DATE - retention_days;

        -- Find and drop old partitions
        FOR partition_rec IN
            SELECT c.relname, c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_inherits i ON i.inhrelid = c.oid
            JOIN pg_class parent ON parent.oid = i.inhparent
            WHERE n.nspname = 'worf'
            AND parent.relname = 'conflict_history'
            AND c.relname ~ '^conflict_history_[0-9]{4}_[0-9]{2}$'
        LOOP
            -- Parse the date from partition name (format: conflict_history_YYYY_MM)
            partition_end_date := to_date(
                substring(partition_rec.relname from 'conflict_history_([0-9]{4}_[0-9]{2})'),
                'YYYY_MM'
            ) + interval '1 month';

            -- Drop if partition's month end is before cutoff
            IF partition_end_date <= cutoff_date THEN
                EXECUTE format('DROP TABLE worf.%I', partition_rec.relname);
                dropped_count := dropped_count + 1;
            END IF;
        END LOOP;

        RETURN dropped_count;
    END;
    $$ LANGUAGE plpgsql;

    COMMENT ON FUNCTION worf.conflict_history_cleanup(INTEGER) IS
        'Drops conflict_history partitions older than the specified retention period. Returns number of partitions dropped.';

    -- ============================================================================
    -- Create Initial Partition for Current Month
    -- ============================================================================

    SELECT worf.conflict_history_ensure_partition(CURRENT_DATE);

    -- ============================================================================
    -- Views
    -- ============================================================================

    CREATE VIEW worf.recent_conflicts AS
    SELECT *
    FROM worf.conflict_history
    WHERE recorded_at > now() - interval '24 hours'
    ORDER BY recorded_at DESC;

    COMMENT ON VIEW worf.recent_conflicts IS
        'Shows conflicts recorded in the last 24 hours';

    CREATE VIEW worf.conflict_summary AS
    SELECT
        schema_name,
        table_name,
        conflict_type,
        resolution,
        count(*) as conflict_count,
        min(recorded_at) as first_seen,
        max(recorded_at) as last_seen
    FROM worf.conflict_history
    WHERE recorded_at > now() - interval '7 days'
    GROUP BY schema_name, table_name, conflict_type, resolution
    ORDER BY conflict_count DESC;

    COMMENT ON VIEW worf.conflict_summary IS
        'Aggregated view of conflicts by table and type over last 7 days';

    -- ============================================================================
    -- Statistics Functions
    -- ============================================================================

    CREATE OR REPLACE FUNCTION worf.conflict_stats(
        OUT total_conflicts BIGINT,
        OUT last_24h BIGINT,
        OUT last_hour BIGINT,
        OUT tables_affected BIGINT
    ) RETURNS RECORD AS $$
    BEGIN
        SELECT count(*) INTO total_conflicts
        FROM worf.conflict_history;

        SELECT count(*) INTO last_24h
        FROM worf.conflict_history
        WHERE recorded_at > now() - interval '24 hours';

        SELECT count(*) INTO last_hour
        FROM worf.conflict_history
        WHERE recorded_at > now() - interval '1 hour';

        SELECT count(DISTINCT (schema_name, table_name)) INTO tables_affected
        FROM worf.conflict_history;
    END;
    $$ LANGUAGE plpgsql STABLE;

    COMMENT ON FUNCTION worf.conflict_stats() IS
        'Returns aggregate statistics about recorded conflicts';

    CREATE OR REPLACE FUNCTION worf.show_subscription_conflicts(
        subscription_name NAME,
        since TIMESTAMPTZ DEFAULT now() - interval '24 hours',
        max_rows INTEGER DEFAULT 100
    ) RETURNS SETOF worf.conflict_history AS $$
    BEGIN
        RETURN QUERY
        SELECT ch.*
        FROM worf.conflict_history ch
        WHERE ch.sub_name = subscription_name
        AND ch.recorded_at >= since
        ORDER BY ch.recorded_at DESC
        LIMIT max_rows;
    END;
    $$ LANGUAGE plpgsql STABLE;

    COMMENT ON FUNCTION worf.show_subscription_conflicts(NAME, TIMESTAMPTZ, INTEGER) IS
        'Returns conflicts for a specific subscription within the given time range';
    """)]

namespace Worf;

/// <summary>
/// worf conflict history persistence.
/// </summary>
internal static partial class ConflictHistory
{
    /// <summary>
    /// Gets whether to record conflicts to the <c>worf.conflict_history</c> table.
    /// </summary>
    [PgGucBool("worf.conflict_history_enabled", false, "Record conflicts to worf.conflict_history table",
        Context = PgGucContext.SuperuserSet)]
    internal static partial bool Enabled { get; }

    /// <summary>
    /// Gets whether to store tuple data in conflict history.
    /// </summary>
    [PgGucBool("worf.conflict_history_store_tuples", true, "Store tuple data in conflict history",
        LongDescription = "When enabled, stores local and remote tuple data as JSONB", Context = PgGucContext.SuperuserSet)]
    internal static partial bool StoreTuples { get; }

    /// <summary>
    /// Gets the maximum bytes per tuple field in conflict history.
    /// </summary>
    [PgGucInt("worf.conflict_history_max_tuple_size", 1024, "Maximum bytes per tuple field in conflict history",
        LongDescription = "Tuple field values exceeding this size will be truncated", Minimum = 64, Maximum = 65536,
        Context = PgGucContext.SuperuserSet)]
    internal static partial int MaxTupleSize { get; }

    private const string InsertSql =
        "INSERT INTO worf.conflict_history (" +
        "sub_id, sub_name, conflict_type, resolution, " +
        "schema_name, table_name, index_name, " +
        "local_tuple, local_xid, local_origin, local_commit_ts, " +
        "remote_tuple, remote_origin, remote_commit_ts, remote_commit_lsn, " +
        "has_before_triggers" +
        ") VALUES (" +
        "$1, $2::name, $3, $4, " +
        "$5::name, $6::name, $7::name, " +
        "$8::jsonb, $9::text::xid, $10, $11, " +
        "$12::jsonb, $13, $14, $15, " +
        "$16" +
        ")";

    /// <summary>
    /// Record a conflict to the worf.conflict_history table.
    /// </summary>
    /// <remarks>
    /// This function is called from <see cref="Conflict.ReportConflict"/> after the conflict has been resolved. It
    /// stores the conflict information for later querying and analysis. Any failures in recording do not abort the
    /// apply worker transaction, because recording is purely observational and should never interfere with
    /// replication.
    /// </remarks>
    internal static unsafe void RecordConflict(WorfConflictType conflictType, WorfRelation rel, HeapTupleData* localtuple,
        HeapTupleData* remotetuple, WorfConflictResolution resolution, uint localTupleXid, bool foundLocalOrigin,
        ushort localTupleOrigin, long localTupleCommitTs, uint conflictIdxOid, bool hasBeforeTriggers)
    {
        // Early exit if feature is disabled
        if (!Enabled)
        {
            return;
        }

        // Early exit if no subscription context
        WorfSubscription? sub = Apply.MySubscription;
        if (sub is null)
        {
            return;
        }

        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                // Ensure partition exists for current date
                EnsurePartitionExists();

                // Get tuple descriptor for JSONB conversion
                RelationData* relation = rel.Rel;
                TupleDescData* tupdesc = relation->rd_att;

                // Get index name if available
                string? idxname = null;
                if (conflictIdxOid != 0)
                {
                    sbyte* name = NativeMethods.get_rel_name(conflictIdxOid);
                    idxname = name is null ? null : Worf.FromServer(name);
                }

                _ = Spi.Execute(InsertSql,
                    SpiParameter.Create(sub.Id),
                    SpiParameter.Create(sub.Name),
                    SpiParameter.Create(ConflictTypeToString(conflictType)),
                    SpiParameter.Create(ResolutionToString(resolution)),
                    SpiParameter.Create(Worf.FromServer(NativeMethods.get_namespace_name(relation->rd_rel->relnamespace))),
                    SpiParameter.Create(Worf.FromServer((sbyte*)&relation->rd_rel->relname)),
                    SpiParameter.Create<string?>(idxname),
                    SpiParameter.Create<string?>(localtuple is not null && StoreTuples ? TupleToJson(tupdesc, localtuple, MaxTupleSize) : null),
                    SpiParameter.Create<string?>(localTupleXid != 0 ? localTupleXid.ToString(CultureInfo.InvariantCulture) : null),
                    SpiParameter.Create<int?>(foundLocalOrigin ? localTupleOrigin : null),
                    SpiParameter.Create<PgTimestampTz?>(foundLocalOrigin ? new PgTimestampTz(localTupleCommitTs) : null),
                    SpiParameter.Create<string?>(remotetuple is not null && StoreTuples ? TupleToJson(tupdesc, remotetuple, MaxTupleSize) : null),
                    SpiParameter.Create((int)Worf.ReploriginSessionOrigin),
                    SpiParameter.Create(new PgTimestampTz(Worf.ReploriginSessionOriginTimestamp)),
                    Worf.LsnParameter(Worf.ReploriginSessionOriginLsn),
                    SpiParameter.Create(hasBeforeTriggers));
            });
        }
        catch (PgException)
        {
            // Log the error and continue - don't abort the apply transaction
            PgLog.Warning("conflict_history: recording failed, continuing without recording");
        }
    }

    /// <summary>
    /// Convert a conflict type to its string representation for storage.
    /// </summary>
    private static string ConflictTypeToString(WorfConflictType type) => type switch
    {
        WorfConflictType.InsertInsert => "insert_insert",
        WorfConflictType.UpdateUpdate => "update_update",
        WorfConflictType.UpdateDelete => "update_delete",
        WorfConflictType.DeleteDelete => "delete_delete",
        _ => "unknown",
    };

    /// <summary>
    /// Convert a resolution to its string representation for storage.
    /// </summary>
    private static string ResolutionToString(WorfConflictResolution res) => res switch
    {
        WorfConflictResolution.ApplyRemote => "apply_remote",
        WorfConflictResolution.KeepLocal => "keep_local",
        WorfConflictResolution.Skip => "skip",
        _ => "unknown",
    };

    /// <summary>
    /// Convert a tuple to a JSON object of its columns' text for storage.
    /// </summary>
    private static unsafe string TupleToJson(TupleDescData* tupdesc, HeapTupleData* tuple, int maxSize)
    {
        StringBuilder json = new("{");
        bool first = true;

        for (int natt = 0; natt < tupdesc->natts; natt++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(tupdesc, natt);

            // Skip dropped columns
            if (attr->attisdropped)
            {
                continue;
            }

            // Skip system columns
            if (attr->attnum < 0)
            {
                continue;
            }

            if (!first)
            {
                json.Append(", ");
            }

            first = false;

            // Add column name as key
            Executor.EscapeJson(json, Worf.FromServer((sbyte*)&attr->attname));
            json.Append(": ");

            // Get the value
            string? outputstr = Conflict.ColumnOutput(tupdesc, tuple, natt, out bool isnull);
            if (isnull)
            {
                // NULL SQL value becomes JSON null
                json.Append("null");
            }
            else
            {
                // Truncate if necessary
                byte[] bytes = Worf.ToServer(outputstr!);
                if (bytes.Length > maxSize && outputstr != "(unchanged-toast-datum)")
                {
                    outputstr = Worf.FromServer(bytes.AsSpan(0, maxSize));
                }

                Executor.EscapeJson(json, outputstr!);
            }
        }

        json.Append('}');
        return json.ToString();
    }

    /// <summary>
    /// Ensure that a partition exists for the current date.
    /// </summary>
    private static void EnsurePartitionExists() => _ = Spi.Execute("SELECT worf.conflict_history_ensure_partition(CURRENT_DATE)");
}
