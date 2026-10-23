using System.Text;
using Ankus.Postgres;

[assembly: PgSql("replication-set-tables", """
    CREATE TABLE worf.replication_set (
        set_id oid NOT NULL PRIMARY KEY,
        set_nodeid oid NOT NULL,
        set_name name NOT NULL,
        replicate_insert boolean NOT NULL DEFAULT true,
        replicate_update boolean NOT NULL DEFAULT true,
        replicate_delete boolean NOT NULL DEFAULT true,
        replicate_truncate boolean NOT NULL DEFAULT true,
        UNIQUE (set_nodeid, set_name)
    ) WITH (user_catalog_table=true);

    CREATE TABLE worf.replication_set_table (
        set_id oid NOT NULL,
        set_reloid regclass NOT NULL,
        set_att_list text[],
        set_row_filter pg_node_tree,
        PRIMARY KEY(set_id, set_reloid)
    ) WITH (user_catalog_table=true);

    CREATE TABLE worf.replication_set_seq (
        set_id oid NOT NULL,
        set_seqoid regclass NOT NULL,
        PRIMARY KEY(set_id, set_seqoid)
    ) WITH (user_catalog_table=true);
    """, Requires = ["node-tables"])]

[assembly: PgSql("tables-view", """
    CREATE VIEW worf.TABLES AS
        WITH set_relations AS (
            SELECT s.set_name, r.set_reloid
              FROM worf.replication_set_table r,
                   worf.replication_set s,
                   worf.local_node n
             WHERE s.set_nodeid = n.node_id
               AND s.set_id = r.set_id
        ),
        user_tables AS (
            SELECT r.oid, n.nspname, r.relname, r.relreplident
              FROM pg_catalog.pg_class r,
                   pg_catalog.pg_namespace n
             WHERE r.relkind = 'r'
               AND r.relpersistence = 'p'
               AND n.oid = r.relnamespace
               AND n.nspname !~ '^pg_'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'worf'
        )
        SELECT r.oid AS relid, n.nspname, r.relname, s.set_name
          FROM pg_catalog.pg_namespace n,
               pg_catalog.pg_class r,
               set_relations s
         WHERE r.relkind = 'r'
           AND n.oid = r.relnamespace
           AND r.oid = s.set_reloid
         UNION
        SELECT t.oid AS relid, t.nspname, t.relname, NULL
          FROM user_tables t
         WHERE t.oid NOT IN (SELECT set_reloid FROM set_relations);
    """, Requires = ["replication-set-tables"])]

namespace Worf;

/// <summary>
/// A replication set.
/// </summary>
internal sealed class WorfRepSet
{
    /// <summary>
    /// Gets or sets the set's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the node the set belongs to.
    /// </summary>
    public uint NodeId { get; set; }

    /// <summary>
    /// Gets or sets the set's name.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Gets or sets whether the set replicates inserts.
    /// </summary>
    public bool ReplicateInsert { get; set; }

    /// <summary>
    /// Gets or sets whether the set replicates updates.
    /// </summary>
    public bool ReplicateUpdate { get; set; }

    /// <summary>
    /// Gets or sets whether the set replicates deletes.
    /// </summary>
    public bool ReplicateDelete { get; set; }

    /// <summary>
    /// Gets or sets whether the set replicates truncates.
    /// </summary>
    public bool ReplicateTruncate { get; set; }
}

/// <summary>
/// worf replication set manipulation functions.
/// </summary>
internal static class ReplicationSet
{
    /// <summary>
    /// The name of the default replication set.
    /// </summary>
    internal const string DefaultRepsetName = "default";

    /// <summary>
    /// The name of the default insert-only replication set.
    /// </summary>
    internal const string DefaultInsonlyRepsetName = "default_insert_only";

    /// <summary>
    /// The name of the replication set for DDL commands.
    /// </summary>
    internal const string DdlSqlRepsetName = "ddl_sql";

    private const string RepSetQuery =
        "SELECT set_id, set_nodeid, set_name::text, replicate_insert, replicate_update, replicate_delete, replicate_truncate " +
        "FROM worf.replication_set";

    private static uint s_repsetreloid;

    private static uint s_repsettablereloid;

    private static uint s_repsetseqreloid;

    /// <summary>
    /// Read the replication set.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <returns>The set.</returns>
    internal static WorfRepSet GetReplicationSet(uint setid)
    {
        SpiResult result = Spi.Select(RepSetQuery + " WHERE set_id = $1", SpiParameter.Create(setid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set {setid} not found");
        }

        return ReplicationSetFromRow(result[0]);
    }

    /// <summary>
    /// Find replication set by name.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="setname">The set's name.</param>
    /// <param name="missingOk">Whether to return null for a missing set.</param>
    /// <returns>The set.</returns>
    internal static WorfRepSet? GetReplicationSetByName(uint nodeid, string setname, bool missingOk)
    {
        SpiResult result = Spi.Select(RepSetQuery + " WHERE set_nodeid = $1 AND set_name = $2::name",
            SpiParameter.Create(nodeid), SpiParameter.Create(setname));
        if (result.Count == 0)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"replication set {setname} not found");
        }

        return ReplicationSetFromRow(result[0]);
    }

    /// <summary>
    /// Gets the replication sets of a node.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <returns>The sets.</returns>
    internal static List<WorfRepSet> GetNodeReplicationSets(uint nodeid)
    {
        SpiResult result = Spi.Select("SELECT set_id FROM worf.replication_set WHERE set_nodeid = $1",
            SpiParameter.Create(nodeid));
        List<WorfRepSet> replicationSets = [];
        foreach (SpiRow row in result)
        {
            replicationSets.Add(GetReplicationSet(row.Get<uint>(0)));
        }

        return replicationSets;
    }

    /// <summary>
    /// Gets a node's replication sets by name.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="replicationSetNames">The sets' names.</param>
    /// <param name="missingOk">Whether to skip missing sets.</param>
    /// <returns>The sets.</returns>
    internal static List<WorfRepSet> GetReplicationSets(uint nodeid, List<string> replicationSetNames, bool missingOk)
    {
        List<WorfRepSet> replicationSets = [];
        foreach (string setname in replicationSetNames)
        {
            SpiResult result = Spi.Select(RepSetQuery + " WHERE set_nodeid = $1 AND set_name = $2::name",
                SpiParameter.Create(nodeid), SpiParameter.Create(setname));
            if (result.Count == 0)
            {
                if (missingOk)
                {
                    continue;
                }

                throw new PgException(PgSqlStates.InvalidParameterValue, $"replication set {setname} not found");
            }

            replicationSets.Add(ReplicationSetFromRow(result[0]));
        }

        return replicationSets;
    }

    /// <summary>
    /// Gets the replication sets of a node that contain a table.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="reloid">The table.</param>
    /// <returns>The sets.</returns>
    internal static List<WorfRepSet> GetTableReplicationSets(uint nodeid, uint reloid)
    {
        SpiResult result = Spi.Select("SELECT set_id FROM worf.replication_set_table WHERE set_reloid = $1",
            SpiParameter.Create(reloid));
        List<WorfRepSet> replicationSets = [];
        foreach (SpiRow row in result)
        {
            WorfRepSet repset = GetReplicationSet(row.Get<uint>(0));
            if (repset.NodeId != nodeid)
            {
                continue;
            }

            replicationSets.Add(repset);
        }

        return replicationSets;
    }

    /// <summary>
    /// Gets the replication sets of a node that contain a sequence.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The sets.</returns>
    internal static List<WorfRepSet> GetSeqReplicationSets(uint nodeid, uint seqoid)
    {
        SpiResult result = Spi.Select("SELECT set_id FROM worf.replication_set_seq WHERE set_seqoid = $1",
            SpiParameter.Create(seqoid));
        List<WorfRepSet> replicationSets = [];
        foreach (SpiRow row in result)
        {
            WorfRepSet repset = GetReplicationSet(row.Get<uint>(0));
            if (repset.NodeId != nodeid)
            {
                continue;
            }

            replicationSets.Add(repset);
        }

        return replicationSets;
    }

    /// <summary>
    /// Add new tuple to the replication_sets catalog.
    /// </summary>
    /// <param name="repset">The set; its id is generated unless one was already specified.</param>
    internal static void CreateReplicationSet(WorfRepSet repset)
    {
        if (repset.Name.Length == 0)
        {
            throw new PgException(PgSqlStates.InvalidName, "replication set name cannot be empty");
        }

        if (GetReplicationSetByName(repset.NodeId, repset.Name, true) is not null)
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set {repset.Name} already exists");
        }

        // Generate new id unless one was already specified.
        if (repset.Id == 0)
        {
            repset.Id = Node.HashIds(repset.NodeId, Worf.HashAny(repset.Name));
        }

        Spi.Execute("INSERT INTO worf.replication_set (set_id, set_nodeid, set_name, replicate_insert, replicate_update, " +
            "replicate_delete, replicate_truncate) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            SpiParameter.Create(repset.Id), SpiParameter.Create(repset.NodeId), SpiParameter.Create(repset.Name),
            SpiParameter.Create(repset.ReplicateInsert), SpiParameter.Create(repset.ReplicateUpdate),
            SpiParameter.Create(repset.ReplicateDelete), SpiParameter.Create(repset.ReplicateTruncate));
    }

    /// <summary>
    /// Alter the existing replication set.
    /// </summary>
    /// <param name="repset">The set.</param>
    internal static void AlterReplicationSet(WorfRepSet repset)
    {
        if (!Spi.ExecuteScalar<bool>("SELECT EXISTS (SELECT FROM worf.replication_set WHERE set_id = $1)",
            SpiParameter.Create(repset.Id)))
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set {repset.Id} not found");
        }

        // Validate that replication is not being changed to replicate UPDATEs and DELETEs if it contains any
        // tables without replication identity.
        if (repset.ReplicateUpdate || repset.ReplicateDelete)
        {
            SpiResult tables = Spi.Select("SELECT set_reloid::oid FROM worf.replication_set_table WHERE set_id = $1",
                SpiParameter.Create(repset.Id));

            // Process every individual table in the set.
            foreach (SpiRow table in tables)
            {
                using PgRelation targetrel = PgRelation.Open(table.Get<uint>(0), PgLockMode.AccessShare);

                // Check of relation has replication index.
                if (targetrel.IsTable && GetReplicaIndex(targetrel) == 0)
                {
                    throw new PgException(PgSqlStates.InvalidParameterValue,
                        $"replication set {repset.Name} cannot be altered to replicate UPDATEs or DELETEs because it " +
                        "contains tables without PRIMARY KEY");
                }
            }
        }

        // Everything ok, form a new tuple.
        Spi.Execute("UPDATE worf.replication_set SET replicate_insert = $2, replicate_update = $3, " +
            "replicate_delete = $4, replicate_truncate = $5 WHERE set_id = $1",
            SpiParameter.Create(repset.Id), SpiParameter.Create(repset.ReplicateInsert),
            SpiParameter.Create(repset.ReplicateUpdate), SpiParameter.Create(repset.ReplicateDelete),
            SpiParameter.Create(repset.ReplicateTruncate));
    }

    /// <summary>
    /// Delete the tuple from replication sets catalog.
    /// </summary>
    /// <param name="setid">The set.</param>
    internal static void DropReplicationSet(uint setid)
    {
        uint? nodeid = Spi.ExecuteScalar<uint?>("SELECT set_nodeid FROM worf.replication_set WHERE set_id = $1",
            SpiParameter.Create(setid));
        if (nodeid is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set {setid} not found");
        }

        // Remove all tables and sequences associated with the repset.
        ReplicationSetRemoveTables(setid, nodeid.Value);
        ReplicationSetRemoveSeqs(setid, nodeid.Value);

        // Remove the tuple.
        Spi.Execute("DELETE FROM worf.replication_set WHERE set_id = $1", SpiParameter.Create(setid));

        // Cleanup.
        InvalidateRelcache(GetReplicationSetRelOid());
    }

    /// <summary>
    /// Drop all replication sets of a node.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    internal static void DropNodeReplicationSets(uint nodeid)
    {
        SpiResult result = Spi.Select("SELECT set_id, set_nodeid FROM worf.replication_set WHERE set_nodeid = $1",
            SpiParameter.Create(nodeid));

        // Remove matching tuples.
        foreach (SpiRow row in result)
        {
            uint setid = row.Get<uint>(0);

            // Remove all tables and sequences associated with the repset.
            ReplicationSetRemoveTables(setid, row.Get<uint>(1));
            ReplicationSetRemoveSeqs(setid, row.Get<uint>(1));

            // Remove the repset.
            Spi.Execute("DELETE FROM worf.replication_set WHERE set_id = $1", SpiParameter.Create(setid));
        }

        // Cleanup.
        InvalidateRelcache(GetReplicationSetRelOid());
    }

    /// <summary>
    /// Get list of table oids.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <returns>The tables.</returns>
    internal static List<uint> ReplicationSetGetTables(uint setid)
    {
        SpiResult result = Spi.Select("SELECT set_reloid::oid FROM worf.replication_set_table WHERE set_id = $1",
            SpiParameter.Create(setid));
        List<uint> res = [];
        foreach (SpiRow row in result)
        {
            res.Add(row.Get<uint>(0));
        }

        return res;
    }

    /// <summary>
    /// Get list of sequence oids.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <returns>The sequences.</returns>
    internal static List<uint> ReplicationSetGetSeqs(uint setid)
    {
        SpiResult result = Spi.Select("SELECT set_seqoid::oid FROM worf.replication_set_seq WHERE set_id = $1",
            SpiParameter.Create(setid));
        List<uint> res = [];
        foreach (SpiRow row in result)
        {
            res.Add(row.Get<uint>(0));
        }

        return res;
    }

    /// <summary>
    /// Remove existing replication set / table mapping.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <param name="reloid">The table.</param>
    /// <param name="fromDrop">Whether the table is being dropped.</param>
    internal static void ReplicationSetRemoveTable(uint setid, uint reloid, bool fromDrop)
    {
        // Remove the tuple if found, if not found report error unless this function was called as result of table
        // drop.
        long removed = Spi.Execute("DELETE FROM worf.replication_set_table WHERE set_id = $1 AND set_reloid = $2",
            SpiParameter.Create(setid), SpiParameter.Create(reloid));
        if (removed == 0 && !fromDrop)
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set table mapping {setid}:{reloid} not found");
        }

        // We can only invalidate the relcache when relation still exists.
        if (!fromDrop)
        {
            InvalidateRelcache(reloid);
        }

        // Dependency cleanup.
        Dependency.TryDropDependencies(Dependency.Address(GetReplicationSetTableRelOid(), setid, unchecked((int)reloid)), true);
    }

    /// <summary>
    /// Remove existing replication set / sequence mapping.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="fromDrop">Whether the sequence is being dropped.</param>
    internal static void ReplicationSetRemoveSeq(uint setid, uint seqoid, bool fromDrop)
    {
        WorfRepSet repset = GetReplicationSet(setid);

        // Remove the tuple if found, if not found report error uless this function was called as result of table
        // drop.
        long removed = Spi.Execute("DELETE FROM worf.replication_set_seq WHERE set_id = $1 AND set_seqoid = $2",
            SpiParameter.Create(setid), SpiParameter.Create(seqoid));
        if (removed == 0 && !fromDrop)
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set sequence mapping {setid}:{seqoid} not found");
        }

        // We can only invalidate the relcache when relation still exists.
        if (!fromDrop)
        {
            InvalidateRelcache(seqoid);
        }

        // Dependency cleanup.
        Dependency.TryDropDependencies(Dependency.Address(GetReplicationSetSeqRelOid(), setid, unchecked((int)seqoid)), true);

        if (fromDrop || !SequenceHasReplicationSets(repset.NodeId, seqoid))
        {
            Sequences.DropSequenceStateRecord(seqoid);
        }
    }

    /// <summary>
    /// Get (cached) oid of the replication set table.
    /// </summary>
    /// <returns>The oid.</returns>
    internal static uint GetReplicationSetRelOid()
    {
        if (s_repsetreloid == 0)
        {
            s_repsetreloid = Worf.GetWorfTableOid("replication_set");
        }

        return s_repsetreloid;
    }

    /// <summary>
    /// Get (cached) oid of the replication set table mapping table.
    /// </summary>
    /// <returns>The oid.</returns>
    internal static uint GetReplicationSetTableRelOid()
    {
        if (s_repsettablereloid == 0)
        {
            s_repsettablereloid = Worf.GetWorfTableOid("replication_set_table");
        }

        return s_repsettablereloid;
    }

    /// <summary>
    /// Get (cached) oid of the replication set sequence mapping table.
    /// </summary>
    /// <returns>The oid.</returns>
    internal static uint GetReplicationSetSeqRelOid()
    {
        if (s_repsetseqreloid == 0)
        {
            s_repsetseqreloid = Worf.GetWorfTableOid("replication_set_seq");
        }

        return s_repsetseqreloid;
    }

    /// <summary>
    /// Given a List of strings, return it as single comma separated string, quoting identifiers as needed. This
    /// is essentially the reverse of SplitIdentifierString.
    /// </summary>
    /// <param name="strings">The strings.</param>
    /// <returns>The identifiers.</returns>
    internal static string StringlistToIdentifierstr(IEnumerable<string> strings)
    {
        StringBuilder res = new();
        bool first = true;

        foreach (string str in strings)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                res.Append(',');
            }

            res.Append(Spi.QuoteIdentifier(str));
        }

        return res.ToString();
    }

    /// <summary>
    /// Gets the replica identity index of a table, as <c>rd_replidindex</c> holds it.
    /// </summary>
    /// <param name="relation">The table.</param>
    /// <returns>The index, or zero.</returns>
    internal static unsafe uint GetReplicaIndex(PgRelation relation)
        => NativeMethods.RelationGetReplicaIndex((RelationData*)relation.DangerousGetPointer());

    /// <summary>
    /// Invalidates a relation's relcache entry.
    /// </summary>
    /// <param name="relid">The relation.</param>
    internal static void InvalidateRelcache(uint relid)
    {
        unsafe
        {
            NativeMethods.CacheInvalidateRelcacheByRelid(relid);
        }
    }

    /// <summary>
    /// Remove all tables from replication set.
    /// </summary>
    private static void ReplicationSetRemoveTables(uint setid, uint nodeid)
    {
        _ = nodeid;
        SpiResult result = Spi.Select("SELECT ctid, set_reloid::oid FROM worf.replication_set_table WHERE set_id = $1",
            SpiParameter.Create(setid));

        foreach (SpiRow row in result)
        {
            uint reloid = row.Get<uint>(1);

            // Remove the tuple.
            Spi.Execute("DELETE FROM worf.replication_set_table WHERE ctid = $1", SpiParameter.Create(row.Get<PgItemPointer>(0)));
            InvalidateRelcache(reloid);

            // Dependency cleanup.
            Dependency.TryDropDependencies(Dependency.Address(GetReplicationSetTableRelOid(), setid, unchecked((int)reloid)), true);
        }
    }

    /// <summary>
    /// Remove all sequences from replication set.
    /// </summary>
    private static void ReplicationSetRemoveSeqs(uint setid, uint nodeid)
    {
        SpiResult result = Spi.Select("SELECT ctid, set_seqoid::oid FROM worf.replication_set_seq WHERE set_id = $1",
            SpiParameter.Create(setid));

        foreach (SpiRow row in result)
        {
            uint seqoid = row.Get<uint>(1);

            // Remove the tuple.
            Spi.Execute("DELETE FROM worf.replication_set_seq WHERE ctid = $1", SpiParameter.Create(row.Get<PgItemPointer>(0)));

            // Make sure the SequenceHasReplicationSets sees the changes.
            if (!SequenceHasReplicationSets(nodeid, seqoid))
            {
                Sequences.DropSequenceStateRecord(seqoid);
            }

            InvalidateRelcache(seqoid);

            // Dependency cleanup.
            Dependency.TryDropDependencies(Dependency.Address(GetReplicationSetSeqRelOid(), setid, unchecked((int)seqoid)), true);
        }
    }

    private static bool SequenceHasReplicationSets(uint nodeid, uint seqoid)
    {
        _ = nodeid;
        return Spi.ExecuteScalar<bool>("SELECT EXISTS (SELECT FROM worf.replication_set_seq WHERE set_seqoid = $1)",
            SpiParameter.Create(seqoid));
    }

    private static WorfRepSet ReplicationSetFromRow(SpiRow row) => new()
    {
        Id = row.Get<uint>(0),
        NodeId = row.Get<uint>(1),
        Name = row.Get<string>(2),
        ReplicateInsert = row.Get<bool>(3),
        ReplicateUpdate = row.Get<bool>(4),
        ReplicateDelete = row.Get<bool>(5),
        ReplicateTruncate = row.Get<bool>(6),
    };
}
