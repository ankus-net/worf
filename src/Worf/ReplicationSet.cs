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
/// worf replication set manipulation functions.
/// </summary>
internal static partial class ReplicationSet
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

    private static uint s_repsetreloid;

    private static Dictionary<uint, WorfTableRepInfo>? s_repSetTableHash;

    private static uint s_repsettablereloid;

    private static uint s_repsetseqreloid;

    /// <summary>
    /// Read the replication set.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <returns>The set.</returns>
    internal static unsafe WorfRepSet GetReplicationSet(uint setid)
    {
        RelationData* rel = OpenCatalog(CatalogRepset);

        // Search for repset record.
        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumRepsetId, BtEqualStrategyNumber, FOideq, setid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);
        if (tuple is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"replication set {setid} not found");
        }

        WorfRepSet repset = ReplicationSetFromTuple(tuple, rel->rd_att);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return repset;
    }

    /// <summary>
    /// Find replication set by name.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="setname">The set's name.</param>
    /// <param name="missingOk">Whether to return null for a missing set.</param>
    /// <returns>The set.</returns>
    internal static unsafe WorfRepSet? GetReplicationSetByName(uint nodeid, string setname, bool missingOk)
    {
        RelationData* rel = OpenCatalog(CatalogRepset);

        // Search for repset record.
        ScanKeyData* key = stackalloc ScanKeyData[2];
        NativeMethods.ScanKeyInit(&key[0], AnumRepsetNodeid, BtEqualStrategyNumber, FOideq, nodeid);
        NativeMethods.ScanKeyInit(&key[1], AnumRepsetName, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(setname));

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 2, key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);
        if (tuple is null)
        {
            if (missingOk)
            {
                NativeMethods.systable_endscan(scan);
                NativeMethods.table_close(rel, RowExclusiveLock);
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"replication set {setname} not found");
        }

        WorfRepSet repset = ReplicationSetFromTuple(tuple, rel->rd_att);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return repset;
    }

    /// <summary>
    /// Gets the replication sets of a node.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <returns>The sets.</returns>
    internal static unsafe List<WorfRepSet> GetNodeReplicationSets(uint nodeid)
    {
        List<WorfRepSet> replicationSets = [];

        RelationData* rel = OpenCatalog(CatalogRepset);

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumRepsetNodeid, BtEqualStrategyNumber, FOideq, nodeid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            bool isnull;
            uint setid = (uint)NativeMethods.heap_getattr(tuple, AnumRepsetId, rel->rd_att, &isnull);
            replicationSets.Add(GetReplicationSet(setid));
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return replicationSets;
    }

    /// <summary>
    /// Gets a node's replication sets by name.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="replicationSetNames">The sets' names.</param>
    /// <param name="missingOk">Whether to skip missing sets.</param>
    /// <returns>The sets.</returns>
    internal static unsafe List<WorfRepSet> GetReplicationSets(uint nodeid, List<string> replicationSetNames, bool missingOk)
    {
        List<WorfRepSet> replicationSets = [];

        RelationData* rel = OpenCatalog(CatalogRepset);

        // Setup common part of key.
        ScanKeyData* key = stackalloc ScanKeyData[2];
        NativeMethods.ScanKeyInit(&key[0], AnumRepsetNodeid, BtEqualStrategyNumber, FOideq, nodeid);

        foreach (string setname in replicationSetNames)
        {
            // Search for repset record.
            NativeMethods.ScanKeyInit(&key[1], AnumRepsetName, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(setname));

            // TODO: use index.
            SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 2, key);
            HeapTupleData* tuple = NativeMethods.systable_getnext(scan);
            if (tuple is null)
            {
                if (missingOk)
                {
                    NativeMethods.systable_endscan(scan);
                    continue;
                }

                throw new PgException(PgSqlStates.InvalidParameterValue, $"replication set {setname} not found");
            }

            replicationSets.Add(ReplicationSetFromTuple(tuple, rel->rd_att));
            NativeMethods.systable_endscan(scan);
        }

        NativeMethods.table_close(rel, RowExclusiveLock);

        return replicationSets;
    }

    /// <summary>
    /// Gets the replication sets of a node that contain a table.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="reloid">The table.</param>
    /// <returns>The sets.</returns>
    internal static List<WorfRepSet> GetTableReplicationSets(uint nodeid, uint reloid)
        => GetRelationReplicationSets(nodeid, CatalogRepsetTable, AnumRepsetTableReloid, reloid);

    /// <summary>
    /// Gets the replication sets of a node that contain a sequence.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The sets.</returns>
    internal static List<WorfRepSet> GetSeqReplicationSets(uint nodeid, uint seqoid)
        => GetRelationReplicationSets(nodeid, CatalogRepsetSeq, AnumRepsetSeqSeqoid, seqoid);

    private static unsafe List<WorfRepSet> GetRelationReplicationSets(uint nodeid, string catalog, short anumRelation, uint reloid)
    {
        List<WorfRepSet> replicationSets = [];

        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(Worf.ExtensionName), Worf.PStrDup(catalog), -1);
        uint relid = NativeMethods.RangeVarGetRelidExtended(rv, RowExclusiveLock, 1 /* RVR_MISSING_OK */, default, null);
        if (relid == 0)
        {
            throw new PgException(PgSqlStates.UndefinedTable, $"relation \"{Worf.ExtensionName}.{catalog}\" does not exist");
        }

        RelationData* rel = NativeMethods.table_open(relid, NoLock);

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, anumRelation, BtEqualStrategyNumber, FOideq, reloid);

        // TODO: use index
        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            bool isnull;
            WorfRepSet repset = GetReplicationSet((uint)NativeMethods.heap_getattr(tuple, 1, rel->rd_att, &isnull));
            if (repset.NodeId != nodeid)
            {
                continue;
            }

            replicationSets.Add(repset);
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

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
            repset.Id = WorfNode.HashIds(repset.NodeId, Worf.HashAny(repset.Name));
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
            SpiResult tables = Worf.SelectCatalog("SELECT set_reloid::oid FROM worf.replication_set_table WHERE set_id = $1",
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
        uint nodeid = Spi.ExecuteScalar<uint?>("SELECT set_nodeid FROM worf.replication_set WHERE set_id = $1",
            SpiParameter.Create(setid))
            ?? throw new PgException(PgSqlStates.InternalError, $"replication set {setid} not found");

        // Remove all tables and sequences associated with the repset.
        ReplicationSetRemoveTables(setid, nodeid);
        ReplicationSetRemoveSeqs(setid, nodeid);

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
        SpiResult result = Worf.SelectCatalog("SELECT set_id, set_nodeid FROM worf.replication_set WHERE set_nodeid = $1",
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
        SpiResult result = Worf.SelectCatalog("SELECT set_reloid::oid FROM worf.replication_set_table WHERE set_id = $1",
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
        SpiResult result = Worf.SelectCatalog("SELECT set_seqoid::oid FROM worf.replication_set_seq WHERE set_id = $1",
            SpiParameter.Create(setid));
        List<uint> res = [];
        foreach (SpiRow row in result)
        {
            res.Add(row.Get<uint>(0));
        }

        return res;
    }

    /// <summary>
    /// Add relation to replication set.
    /// </summary>
    /// <param name="setid">The replication set.</param>
    /// <param name="reloid">The table.</param>
    /// <param name="attList">The replicated columns, or an empty list for all.</param>
    /// <param name="rowFilter">The row filter, or null.</param>
    internal static unsafe void ReplicationSetAddTable(uint setid, uint reloid, List<string> attList, Node* rowFilter)
    {
        WorfRepSet repset = GetReplicationSet(setid);

        // Open the relation.
        RelationData* targetrel = NativeMethods.table_open(reloid, (int)PgLockMode.ShareRowExclusive);

        // UNLOGGED and TEMP relations cannot be part of replication set.
        if (targetrel->rd_rel->relpersistence != (sbyte)'p')
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "UNLOGGED and TEMP tables cannot be replicated");
        }

        if (NativeMethods.RelationGetReplicaIndex(targetrel) == 0 && (repset.ReplicateUpdate || repset.ReplicateDelete))
        {
            throw new PgException(PgSqlStates.InvalidParameterValue,
                $"table {Worf.FromServer((sbyte*)&targetrel->rd_rel->relname)} cannot be added to replication set {repset.Name}",
                detail: "table does not have PRIMARY KEY and given replication set is configured to replicate UPDATEs and/or DELETEs",
                hint: "Add a PRIMARY KEY to the table");
        }

        Queue.CreateTruncateTrigger(targetrel);

        NativeMethods.table_close(targetrel, (int)PgLockMode.None);

        // Open the catalog.
        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(Worf.ExtensionName), Worf.PStrDup("replication_set_table"), -1);
        RelationData* rel = NativeMethods.table_openrv(rv, (int)PgLockMode.RowExclusive);
        TupleDescData* tupDesc = rel->rd_att;

        // Form a tuple.
        ulong* values = stackalloc ulong[NattsRepsetTable];
        bool* nulls = stackalloc bool[NattsRepsetTable];
        new Span<bool>(nulls, NattsRepsetTable).Clear();

        values[AnumRepsetTableSetid - 1] = repset.Id;
        values[AnumRepsetTableReloid - 1] = reloid;

        if (attList.Count > 0)
        {
            values[AnumRepsetTableAttList - 1] = PgDatum.Create(attList.ToArray(), PgMemoryContext.Current).DangerousGetBits();
        }
        else
        {
            nulls[AnumRepsetTableAttList - 1] = true;
        }

        if (rowFilter is not null)
        {
            values[AnumRepsetTableRowFilter - 1] = (ulong)NativeMethods.cstring_to_text(NativeMethods.nodeToString(rowFilter));
        }
        else
        {
            nulls[AnumRepsetTableRowFilter - 1] = true;
        }

        HeapTupleData* tup = NativeMethods.heap_form_tuple(tupDesc, values, nulls);

        // Insert the tuple to the catalog.
        NativeMethods.CatalogTupleInsert(rel, tup);

        // Cleanup.
        InvalidateRelcache(reloid);
        NativeMethods.heap_freetuple(tup);

        ObjectAddress myself = Dependency.Address(GetReplicationSetTableRelOid(), setid, unchecked((int)reloid));
        ObjectAddress referenced = Dependency.Address(Dependency.RelationRelationId, reloid, 0);

        Dependency.RecordDependencyOn(myself, referenced, Dependency.DependencyNormal);

        // Make sure we record dependencies for the row_filter as well.
        if (rowFilter is not null)
        {
            Dependency.RecordDependencyOnSingleRelExpr(myself, rowFilter, reloid, Dependency.DependencyNormal,
                Dependency.DependencyNormal);
        }

        NativeMethods.table_close(rel, (int)PgLockMode.RowExclusive);

        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Add sequence to replication set.
    /// </summary>
    /// <param name="setid">The replication set.</param>
    /// <param name="seqoid">The sequence.</param>
    internal static unsafe void ReplicationSetAddSeq(uint setid, uint seqoid)
    {
        WorfRepSet repset = GetReplicationSet(setid);

        // Open the relation.
        RelationData* targetrel = NativeMethods.table_open(seqoid, (int)PgLockMode.ShareRowExclusive);

        // UNLOGGED and TEMP relations cannot be part of replication set.
        if (targetrel->rd_rel->relpersistence != (sbyte)'p')
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "UNLOGGED and TEMP sequences cannot be replicated");
        }

        // Ensure track the state of the sequence.
        Sequences.CreateSequenceStateRecord(seqoid);

        NativeMethods.table_close(targetrel, (int)PgLockMode.None);

        // Insert the tuple to the catalog.
        _ = Spi.Execute("INSERT INTO worf.replication_set_seq VALUES ($1, $2)",
            SpiParameter.Create(repset.Id), SpiParameter.Create(seqoid));

        // Cleanup.
        InvalidateRelcache(seqoid);

        ObjectAddress myself = Dependency.Address(GetReplicationSetSeqRelOid(), setid, unchecked((int)seqoid));
        ObjectAddress referenced = Dependency.Address(Dependency.RelationRelationId, seqoid, 0);

        Dependency.RecordDependencyOn(myself, referenced, Dependency.DependencyNormal);

        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Remove existing replication set / table mapping.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <param name="reloid">The table.</param>
    /// <param name="fromDrop">Whether the table is being dropped.</param>
    internal static unsafe void ReplicationSetRemoveTable(uint setid, uint reloid, bool fromDrop)
    {
        RelationData* rel = OpenCatalog(CatalogRepsetTable);

        // Search for the record.
        ScanKeyData* key = stackalloc ScanKeyData[2];
        NativeMethods.ScanKeyInit(&key[0], AnumRepsetTableSetid, BtEqualStrategyNumber, FOideq, setid);
        NativeMethods.ScanKeyInit(&key[1], AnumRepsetTableReloid, BtEqualStrategyNumber, FOideq, reloid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 2, key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        // Remove the tuple if found, if not found report error unless this function was called as result of table
        // drop.
        if (tuple is not null)
        {
            NativeMethods.simple_heap_delete(rel, &tuple->t_self);
        }
        else if (!fromDrop)
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

        // Make sure the has_relation_replication_sets sees the changes.
        NativeMethods.CommandCounterIncrement();

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
    }

    /// <summary>
    /// Remove existing replication set / sequence mapping.
    /// </summary>
    /// <param name="setid">The set.</param>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="fromDrop">Whether the sequence is being dropped.</param>
    internal static unsafe void ReplicationSetRemoveSeq(uint setid, uint seqoid, bool fromDrop)
    {
        WorfRepSet repset = GetReplicationSet(setid);

        RelationData* rel = OpenCatalog(CatalogRepsetSeq);

        // Search for the record.
        ScanKeyData* key = stackalloc ScanKeyData[2];
        NativeMethods.ScanKeyInit(&key[0], AnumRepsetSeqSetid, BtEqualStrategyNumber, FOideq, setid);
        NativeMethods.ScanKeyInit(&key[1], AnumRepsetSeqSeqoid, BtEqualStrategyNumber, FOideq, seqoid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 2, key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        // Remove the tuple if found, if not found report error uless this function was called as result of table
        // drop.
        if (tuple is not null)
        {
            NativeMethods.simple_heap_delete(rel, &tuple->t_self);
        }
        else if (!fromDrop)
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

        // Make sure the has_relation_replication_sets sees the changes.
        NativeMethods.CommandCounterIncrement();

        if (fromDrop || !SequenceHasReplicationSets(repset.NodeId, seqoid))
        {
            Sequences.DropSequenceStateRecord(seqoid);
        }

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);
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
        SpiResult result = Worf.SelectCatalog("SELECT ctid, set_reloid::oid FROM worf.replication_set_table WHERE set_id = $1",
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
        SpiResult result = Worf.SelectCatalog("SELECT ctid, set_seqoid::oid FROM worf.replication_set_seq WHERE set_id = $1",
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

    /// <summary>
    /// Get the replication info for a table, as the output plugin uses it.
    /// </summary>
    /// <param name="nodeid">The local node.</param>
    /// <param name="table">The table.</param>
    /// <param name="subsReplicationSets">The connection's replication sets.</param>
    /// <returns>The table's replication info.</returns>
    internal static unsafe WorfTableRepInfo GetTableReplicationInfo(uint nodeid, RelationData* table, List<WorfRepSet> subsReplicationSets)
    {
        _ = nodeid;
        uint reloid = table->rd_id;

        s_repSetTableHash ??= RepsetRelcacheInit();

        // It might seem that it's weird to use just reloid here for the cache key when we are searching for nodeid +
        // relation. But this function is only used by the output plugin which means the nodeid is always the same as
        // only one node is connected to current process.
        if (s_repSetTableHash.TryGetValue(reloid, out WorfTableRepInfo? entry) && entry.IsValid)
        {
            return entry;
        }

        // If an entry was found but entry is Invalid, free its row_filter nodes.
        if (entry is not null)
        {
            foreach (nint node in entry.RowFilter)
            {
                NativeMethods.pfree((void*)node);
            }
        }

        // Fill the entry
        entry = new WorfTableRepInfo { Reloid = reloid };
        s_repSetTableHash[reloid] = entry;

        // Check for match between table's replication sets and the subscription list of replication sets that was
        // given as parameter.
        //
        // Note that tables can have no replication sets. This will be commonly true for example for internal tables
        // which are created during table rewrites, so if we'll want to support replicating those, we'll have to have
        // special handling for them.
        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(Worf.ExtensionName), Worf.PStrDup("replication_set_table"), -1);
        uint repsetReloid = NativeMethods.RangeVarGetRelidExtended(rv, (int)PgLockMode.RowExclusive, 1 /* RVR_MISSING_OK */, default, null);
        if (repsetReloid == 0)
        {
            throw new PgException(PgSqlStates.UndefinedTable, $"relation \"{Worf.ExtensionName}.replication_set_table\" does not exist");
        }

        RelationData* repsetRel = NativeMethods.table_open(repsetReloid, (int)PgLockMode.None);
        TupleDescData* repsetRelDesc = repsetRel->rd_att;
        TupleDescData* tableDesc = table->rd_att;

        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumRepsetTableReloid, BtEqualStrategyNumber, FOideq, reloid);

        // TODO: use index
        SysScanDescData* scan = NativeMethods.systable_beginscan(repsetRel, 0, true, null, 1, &key);

        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            bool isnull;
            uint setid = (uint)NativeMethods.heap_getattr(tuple, AnumRepsetTableSetid, repsetRelDesc, &isnull);

            foreach (WorfRepSet repset in subsReplicationSets)
            {
                if (setid != repset.Id)
                {
                    continue;
                }

                // Update the action filter.
                if (repset.ReplicateInsert)
                {
                    entry.ReplicateInsert = true;
                }

                if (repset.ReplicateUpdate)
                {
                    entry.ReplicateUpdate = true;
                }

                if (repset.ReplicateDelete)
                {
                    entry.ReplicateDelete = true;
                }

                // Update replicated column map.
                ulong d = NativeMethods.heap_getattr(tuple, AnumRepsetTableAttList, repsetRelDesc, &isnull);
                if (!isnull)
                {
                    string?[] attList = PgDatum.DangerousCreate((nuint)d, (uint)PgBuiltInOid.TextArrayOid, PgMemoryContext.Current).Read<string?[]>();
                    entry.AttList ??= [];
                    foreach (string? attname in attList)
                    {
                        entry.AttList.Add(GetAttNumByName(tableDesc, attname!));
                    }
                }

                // Add row filter if any.
                d = NativeMethods.heap_getattr(tuple, AnumRepsetTableRowFilter, repsetRelDesc, &isnull);
                if (!isnull)
                {
                    MemoryContextData* olctx = NativeGlobals.CurrentMemoryContext;
                    NativeGlobals.CurrentMemoryContext = NativeGlobals.CacheMemoryContext;
                    try
                    {
                        entry.RowFilter.Add((nint)NativeMethods.stringToNode(NativeMethods.text_to_cstring((varlena*)d)));
                    }
                    finally
                    {
                        NativeGlobals.CurrentMemoryContext = olctx;
                    }
                }
            }
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(repsetRel, (int)PgLockMode.RowExclusive);
        entry.IsValid = true;

        return entry;
    }

    /// <summary>
    /// Reads a replication set from its catalog tuple.
    /// </summary>
    /// <param name="tuple">The tuple.</param>
    /// <param name="desc">The catalog's descriptor.</param>
    /// <returns>The replication set.</returns>
    internal static unsafe WorfRepSet ReplicationSetFromTuple(HeapTupleData* tuple, TupleDescData* desc)
    {
        // A deleted row's old tuple holds only its key.
        bool isnull;
        WorfRepSet repset = new() { Id = (uint)NativeMethods.heap_getattr(tuple, 1, desc, &isnull) };
        ulong d = NativeMethods.heap_getattr(tuple, 2, desc, &isnull);
        repset.NodeId = isnull ? 0 : (uint)d;
        d = NativeMethods.heap_getattr(tuple, 3, desc, &isnull);
        repset.Name = isnull ? "" : Worf.FromServer((sbyte*)d);
        d = NativeMethods.heap_getattr(tuple, 4, desc, &isnull);
        repset.ReplicateInsert = !isnull && (byte)d != 0;
        d = NativeMethods.heap_getattr(tuple, 5, desc, &isnull);
        repset.ReplicateUpdate = !isnull && (byte)d != 0;
        d = NativeMethods.heap_getattr(tuple, 6, desc, &isnull);
        repset.ReplicateDelete = !isnull && (byte)d != 0;
        d = NativeMethods.heap_getattr(tuple, 7, desc, &isnull);
        repset.ReplicateTruncate = !isnull && (byte)d != 0;
        return repset;
    }

    /// <summary>
    /// Finds a column's number by its name, or <c>FirstLowInvalidHeapAttributeNumber</c>.
    /// </summary>
    /// <param name="desc">The descriptor.</param>
    /// <param name="attname">The column's name.</param>
    /// <returns>The column's number.</returns>
    internal static unsafe int GetAttNumByName(TupleDescData* desc, string attname)
    {
        for (int i = 0; i < desc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);
            if (att->attisdropped)
            {
                continue;
            }

            if (Worf.FromServer((sbyte*)&att->attname) == attname)
            {
                return att->attnum;
            }
        }

        return FirstLowInvalidHeapAttributeNumber;
    }

    private static unsafe RelationData* OpenCatalog(string table)
    {
        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(Worf.ExtensionName), Worf.PStrDup(table), -1);
        return NativeMethods.table_openrv(rv, RowExclusiveLock);
    }

    private const string CatalogRepset = "replication_set";
    private const string CatalogRepsetSeq = "replication_set_seq";
    private const string CatalogRepsetTable = "replication_set_table";

    private const short AnumRepsetId = 1;
    private const short AnumRepsetNodeid = 2;
    private const short AnumRepsetName = 3;

    private const short AnumRepsetSeqSetid = 1;
    private const short AnumRepsetSeqSeqoid = 2;

    private const int NoLock = 0;
    private const int RowExclusiveLock = 3;
    private const uint FNameeq = 62;

    // Columns of worf.replication_set_table, and the scan key for its relation.
    private const short AnumRepsetTableSetid = 1;
    private const short AnumRepsetTableReloid = 2;
    private const short AnumRepsetTableAttList = 3;
    private const short AnumRepsetTableRowFilter = 4;
    private const int NattsRepsetTable = 4;
    private const ushort BtEqualStrategyNumber = 3;
    private const uint FOideq = 184;

    /// <summary>
    /// <c>FirstLowInvalidHeapAttributeNumber</c> from access/sysattr.h.
    /// </summary>
    internal const int FirstLowInvalidHeapAttributeNumber = -7;

    [PgNativeCallback(nameof(RepsetRelcacheInvalidateCallback))]
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction RepsetInvalidateCallback { get; }
#else
    private static partial RelSyncCallbackFunction RepsetInvalidateCallback { get; }
#endif

    private static void RepsetRelcacheInvalidateCallback(ulong arg, uint reloid)
    {
        _ = arg;

        // Just to be sure.
        if (s_repSetTableHash is null)
        {
            return;
        }

        if (reloid == 0)
        {
            foreach (WorfTableRepInfo entry in s_repSetTableHash.Values)
            {
                entry.IsValid = false;
            }
        }
        else if (s_repSetTableHash.TryGetValue(reloid, out WorfTableRepInfo? entry))
        {
            entry.IsValid = false;
        }
    }

    private static Dictionary<uint, WorfTableRepInfo> RepsetRelcacheInit()
    {
        // Watch for invalidation events fired when the relcache changes.
        //
        // Note that no invalidations are fired when the replication sets are created, destroyed, modified, or change
        // membership since there's no syscache management for user catalogs. We do our own invalidations for those
        // separately.
        unsafe
        {
            NativeMethods.CacheRegisterRelcacheCallback(RepsetInvalidateCallback, 0);
        }

        return [];
    }
}
