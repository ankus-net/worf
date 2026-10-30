using System.Globalization;
using System.Runtime.InteropServices;
using Ankus.Postgres;

[assembly: PgSql("node-tables", """
    CREATE TABLE worf.node (
        node_id oid NOT NULL PRIMARY KEY,
        node_name name NOT NULL UNIQUE
    ) WITH (user_catalog_table=true);

    CREATE TABLE worf.node_interface (
        if_id oid NOT NULL PRIMARY KEY,
        if_name name NOT NULL, -- default same as node name
        if_nodeid oid REFERENCES node(node_id),
        if_dsn text NOT NULL,
        UNIQUE (if_nodeid, if_name)
    );

    CREATE TABLE worf.local_node (
        node_id oid PRIMARY KEY REFERENCES node(node_id),
        node_local_interface oid NOT NULL REFERENCES node_interface(if_id)
    );

    CREATE TABLE worf.origin_writer (
        roident oid NOT NULL,
        writer_node oid NOT NULL,
        sub_id oid NOT NULL,
        since_xid xid8 NOT NULL,
        PRIMARY KEY (roident, since_xid)
    );

    COMMENT ON TABLE worf.origin_writer IS
        'The node whose changes each replication origin worf created applies, and the subscription it applies them for, from the transaction that created it, or zeros from the one that dropped it';

    CREATE TABLE worf.subscription (
        sub_id oid NOT NULL PRIMARY KEY,
        sub_name name NOT NULL UNIQUE,
        sub_origin oid NOT NULL REFERENCES node(node_id),
        sub_target oid NOT NULL REFERENCES node(node_id),
        sub_origin_if oid NOT NULL REFERENCES node_interface(if_id),
        sub_target_if oid NOT NULL REFERENCES node_interface(if_id),
        sub_enabled boolean NOT NULL DEFAULT true,
        sub_slot_name name NOT NULL,
        sub_replication_sets text[],
        sub_forward_origins text[],
        sub_apply_delay interval NOT NULL DEFAULT '0',
        sub_force_text_transfer boolean NOT NULL DEFAULT 'f'
    ) WITH (user_catalog_table=true);
    """)]

namespace Worf;

internal sealed partial class WorfNode
{
    /// <summary>
    /// The lock that <c>ShareUpdateExclusiveLock</c> names.
    /// </summary>
    internal const int ShareUpdateExclusiveLock = 4;

    /// <summary>
    /// The lock that <c>AccessExclusiveLock</c> names.
    /// </summary>
    internal const int AccessExclusiveLock = 8;

    /// <summary>
    /// Add new node to catalog.
    /// </summary>
    /// <param name="node">The node; its id is generated unless one was already specified.</param>
    internal static void CreateNode(WorfNode node)
    {
        if (GetNodeByName(node.Name, true) is not null)
        {
            throw new PgException(PgSqlStates.InternalError, $"node {node.Name} already exists");
        }

        // Generate new id unless one was already specified.
        if (node.Id == 0)
        {
            node.Id = Worf.HashAny(node.Name);
        }

        Spi.Execute("INSERT INTO worf.node (node_id, node_name) VALUES ($1, $2)",
            SpiParameter.Create(node.Id), SpiParameter.Create(node.Name));

        Worker.SubscriptionChanged(0, false);
    }

    /// <summary>
    /// Delete node from the catalog.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    internal static void DropNode(uint nodeid)
    {
        if (Spi.Execute("DELETE FROM worf.node WHERE node_id = $1", SpiParameter.Create(nodeid)) == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"node {nodeid} not found");
        }

        Worker.SubscriptionChanged(0, false);
    }

    /// <summary>
    /// Load the info for specific node.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <returns>The node.</returns>
    internal static unsafe WorfNode GetNode(uint nodeid)
    {
        RelationData* rel = OpenCatalog(CatalogNode, RowExclusiveLock);

        // Search for node record.
        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumNodeId, BtEqualStrategyNumber, FOideq, nodeid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        if (tuple is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"node {nodeid} not found");
        }

        WorfNode node = NodeFromTuple(tuple, rel->rd_att);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return node;
    }

    /// <summary>
    /// Load the info for specific node.
    /// </summary>
    /// <param name="name">The node's name.</param>
    /// <param name="missingOk">Whether to return null for a missing node.</param>
    /// <returns>The node.</returns>
    internal static unsafe WorfNode? GetNodeByName(string name, bool missingOk)
    {
        RelationData* rel = OpenCatalog(CatalogNode, RowExclusiveLock);

        // Search for node record.
        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumNodeName, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(name));

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        if (tuple is null)
        {
            if (missingOk)
            {
                NativeMethods.systable_endscan(scan);
                NativeMethods.table_close(rel, RowExclusiveLock);
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"node {name} not found");
        }

        WorfNode node = NodeFromTuple(tuple, rel->rd_att);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return node;
    }

    /// <summary>
    /// Add local node record to catalog.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="ifid">The node's interface.</param>
    internal static void CreateLocalNode(uint nodeid, uint ifid)
    {
        LockCatalog("local_node", AccessExclusiveLock);

        if (GetLocalNode(false, true) is not null)
        {
            throw new PgException(PgSqlStates.InternalError, "current database is already configured as worf node");
        }

        Spi.Execute("INSERT INTO worf.local_node (node_id, node_local_interface) VALUES ($1, $2)",
            SpiParameter.Create(nodeid), SpiParameter.Create(ifid));
    }

    /// <summary>
    /// Drop local node record from catalog.
    /// </summary>
    internal static void DropLocalNode()
    {
        LockCatalog("local_node", AccessExclusiveLock);

        // Find the local node tuple.
        if (Spi.Execute("DELETE FROM worf.local_node WHERE ctid = (SELECT ctid FROM worf.local_node LIMIT 1)") == 0)
        {
            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local node not found");
        }
    }

    /// <summary>
    /// Return local node.
    /// </summary>
    /// <param name="forUpdate">Whether to lock the local node against concurrent configuration changes.</param>
    /// <param name="missingOk">Whether to return null when this database is not a worf node.</param>
    /// <returns>The local node.</returns>
    internal static unsafe WorfLocalNode? GetLocalNode(bool forUpdate, bool missingOk)
    {
        uint relid = Worf.CatalogOid(CatalogLocalNode);
        RelationData* rel = relid == 0 ? null : NativeMethods.table_open(relid, forUpdate ? ShareUpdateExclusiveLock : RowExclusiveLock);

        if (rel is null)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local worf node not found");
        }

        // Find the local node tuple.
        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 0, null);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        // No local node record found.
        if (tuple is null)
        {
            if (missingOk)
            {
                NativeMethods.systable_endscan(scan);
                NativeMethods.table_close(rel, forUpdate ? NoLock : RowExclusiveLock);
                return null;
            }

            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local worf node not found");
        }

        TupleDescData* desc = rel->rd_att;
        bool isnull;
        uint nodeid = (uint)NativeMethods.heap_getattr(tuple, AnumNodeLocalId, desc, &isnull);
        uint nodeifid = (uint)NativeMethods.heap_getattr(tuple, AnumNodeLocalNodeIf, desc, &isnull);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, forUpdate ? NoLock : RowExclusiveLock);

        return new WorfLocalNode(GetNode(nodeid), GetNodeInterface(nodeifid));
    }

    /// <summary>
    /// Add new node interface to catalog.
    /// </summary>
    /// <param name="nodeif">The interface; its id is generated unless one was already specified.</param>
    internal static void CreateNodeInterface(WorfInterface nodeif)
    {
        // Generate new id unless one was already specified.
        if (nodeif.Id == 0)
        {
            nodeif.Id = HashIds(nodeif.NodeId, Worf.HashAny(nodeif.Name));
        }

        Spi.Execute("INSERT INTO worf.node_interface (if_id, if_name, if_nodeid, if_dsn) VALUES ($1, $2, $3, $4)",
            SpiParameter.Create(nodeif.Id), SpiParameter.Create(nodeif.Name), SpiParameter.Create(nodeif.NodeId),
            SpiParameter.Create(nodeif.Dsn));
    }

    /// <summary>
    /// Delete node interface from the catalog.
    /// </summary>
    /// <param name="ifid">The interface.</param>
    internal static void DropNodeInterface(uint ifid)
    {
        if (Spi.Execute("DELETE FROM worf.node_interface WHERE if_id = $1", SpiParameter.Create(ifid)) == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"node interface {ifid} not found");
        }
    }

    /// <summary>
    /// Delete all node interfaces from the catalog.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    internal static void DropNodeInterfaces(uint nodeid)
        => Spi.Execute("DELETE FROM worf.node_interface WHERE if_nodeid = $1", SpiParameter.Create(nodeid));

    /// <summary>
    /// Get the node interface from the catalog.
    /// </summary>
    /// <param name="ifid">The interface.</param>
    /// <returns>The interface.</returns>
    internal static unsafe WorfInterface GetNodeInterface(uint ifid)
    {
        RelationData* rel = OpenCatalog(CatalogNodeInterface, RowExclusiveLock);

        // Search for node record.
        ScanKeyData key;
        NativeMethods.ScanKeyInit(&key, AnumIfId, BtEqualStrategyNumber, FOideq, ifid);

        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 1, &key);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);

        if (tuple is null)
        {
            throw new PgException(PgSqlStates.InternalError, $"node interface {ifid} not found");
        }

        WorfInterface nodeif = InterfaceFromTuple(tuple, rel->rd_att);

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return nodeif;
    }

    /// <summary>
    /// Get the node interface by name.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="name">The interface's name.</param>
    /// <param name="missingOk">Whether to return null for a missing interface.</param>
    /// <returns>The interface.</returns>
    internal static unsafe WorfInterface? GetNodeInterfaceByName(uint nodeid, string name, bool missingOk)
    {
        RelationData* rel = OpenCatalog(CatalogNodeInterface, RowExclusiveLock);

        // Search for interface record.
        ScanKeyData* key = stackalloc ScanKeyData[2];
        NativeMethods.ScanKeyInit(&key[0], AnumIfNodeid, BtEqualStrategyNumber, FOideq, nodeid);
        NativeMethods.ScanKeyInit(&key[1], AnumIfName, BtEqualStrategyNumber, FNameeq, (ulong)Worf.PStrDup(name));

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

            throw new PgException(PgSqlStates.InternalError, $"node interface \"{name}\" not found for nod {nodeid}");
        }

        WorfInterface nodeif = InterfaceFromTuple(tuple, rel->rd_att);

        // Cleanup.
        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, RowExclusiveLock);

        return nodeif;
    }

    /// <summary>
    /// Load the info for specific subscriber.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <returns>The subscription.</returns>
    internal static WorfSubscription GetSubscription(uint subid)
    {
        SpiResult result = Worf.SelectCatalog(SubscriptionQuery + " WHERE sub_id = $1", SpiParameter.Create(subid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"subscription {subid} not found");
        }

        return SubscriptionFromRow(result[0]);
    }

    /// <summary>
    /// Load the info for specific subscriber.
    /// </summary>
    /// <param name="name">The subscription's name.</param>
    /// <param name="missingOk">Whether to return null for a missing subscription.</param>
    /// <returns>The subscription.</returns>
    internal static WorfSubscription? GetSubscriptionByName(string name, bool missingOk)
    {
        SpiResult result = Worf.SelectCatalog(SubscriptionQuery + " WHERE sub_name = $1::name", SpiParameter.Create(name));
        if (result.Count == 0)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"subscriber {name} not found");
        }

        return SubscriptionFromRow(result[0]);
    }

    /// <summary>
    /// Add new subscription to catalog.
    /// </summary>
    /// <param name="sub">The subscription, whose id is set when it has none.</param>
    internal static void CreateSubscription(WorfSubscription sub)
    {
        // Validate the new subscription name.
        ValidateSubscriptionName(sub.Name);

        if (GetSubscriptionByName(sub.Name, true) is not null)
        {
            throw new PgException(PgSqlStates.InternalError, $"subscription {sub.Name} already exists");
        }

        // Generate new id unless one was already specified.
        if (sub.Id == 0)
        {
            sub.Id = Worf.HashAny(sub.Name);
        }

        _ = Spi.Execute(
            "INSERT INTO worf.subscription VALUES ($1, $2::name, $3, $4, $5, $6, $7, $8::name, $9, $10, $11, $12)",
            SpiParameter.Create(sub.Id),
            SpiParameter.Create(sub.Name),
            SpiParameter.Create(sub.OriginIf.NodeId),
            SpiParameter.Create(sub.TargetIf.NodeId),
            SpiParameter.Create(sub.OriginIf.Id),
            SpiParameter.Create(sub.TargetIf.Id),
            SpiParameter.Create(sub.Enabled),
            SpiParameter.Create(sub.SlotName),
            SpiParameter.Create<string[]?>(sub.ReplicationSets.Count > 0 ? [.. sub.ReplicationSets] : null),
            SpiParameter.Create<string[]?>(sub.ForwardOrigins.Count > 0 ? [.. sub.ForwardOrigins] : null),
            SpiParameter.Create(sub.ApplyDelay),
            SpiParameter.Create(sub.ForceTextTransfer));

        NativeMethodsCommandCounterIncrement();

        Worker.SubscriptionChanged(sub.Id, true);
    }

    /// <summary>
    /// Change the subscription tuple.
    /// </summary>
    /// <param name="sub">The subscription.</param>
    internal static void AlterSubscription(WorfSubscription sub)
    {
        SpiResult result = Spi.Query("SELECT sub_name::text FROM worf.subscription WHERE sub_id = $1 FOR UPDATE", false, 0,
            SpiParameter.Create(sub.Id));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"subscription {sub.Id} not found");
        }

        if (result[0].Get<string>(0) != sub.Name)
        {
            PgLog.Log(new PgDiagnostic("subscription name change is not supported") { SqlState = PgSqlStates.FeatureNotSupported });
        }

        _ = Spi.Execute(
            "UPDATE worf.subscription SET sub_origin = $2, sub_target = $3, sub_origin_if = $4, sub_target_if = $5, " +
            "sub_enabled = $6, sub_slot_name = $7::name, sub_replication_sets = $8, sub_forward_origins = $9, " +
            "sub_apply_delay = $10, sub_force_text_transfer = $11 WHERE sub_id = $1",
            SpiParameter.Create(sub.Id),
            SpiParameter.Create(sub.OriginIf.NodeId),
            SpiParameter.Create(sub.TargetIf.NodeId),
            SpiParameter.Create(sub.OriginIf.Id),
            SpiParameter.Create(sub.TargetIf.Id),
            SpiParameter.Create(sub.Enabled),
            SpiParameter.Create(sub.SlotName),
            SpiParameter.Create<string[]?>(sub.ReplicationSets.Count > 0 ? [.. sub.ReplicationSets] : null),
            SpiParameter.Create<string[]?>(sub.ForwardOrigins.Count > 0 ? [.. sub.ForwardOrigins] : null),
            SpiParameter.Create(sub.ApplyDelay),
            SpiParameter.Create(sub.ForceTextTransfer));

        NativeMethodsCommandCounterIncrement();

        Worker.SubscriptionChanged(sub.Id, true);
    }

    /// <summary>
    /// Delete the tuple from subsription catalog.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    internal static void DropSubscription(uint subid)
    {
        if (Spi.Execute("DELETE FROM worf.subscription WHERE sub_id = $1", SpiParameter.Create(subid)) == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"subscription {subid} not found");
        }

        _ = Spi.Execute("DELETE FROM worf.exception_state WHERE sub_id = $1", SpiParameter.Create(subid));

        NativeMethodsCommandCounterIncrement();

        Worker.SubscriptionChanged(subid, true);
    }

    /// <summary>
    /// Return all target node subscriptions.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="origin">Whether to return the subscriptions to the node rather than from it.</param>
    /// <returns>The subscriptions.</returns>
    internal static List<WorfSubscription> GetNodeSubscriptions(uint nodeid, bool origin)
    {
        SpiResult result = Worf.SelectCatalog(SubscriptionQuery + (origin ? " WHERE sub_origin = $1" : " WHERE sub_target = $1"),
            SpiParameter.Create(nodeid));
        List<WorfSubscription> res = [];
        foreach (SpiRow row in result)
        {
            res.Add(SubscriptionFromRow(row));
        }

        return res;
    }

    /// <summary>
    /// Takes a lock on one of our catalog tables, as opening it with that lock mode does.
    /// </summary>
    /// <param name="table">The table's name.</param>
    /// <param name="lockmode">The lock mode.</param>
    internal static void LockCatalog(string table, int lockmode)
    {
        uint relid = Worf.GetWorfTableOid(table);
        unsafe
        {
            NativeMethods.LockRelationOid(relid, lockmode);
        }
    }

    /// <summary>
    /// Hashes two oids as <c>hash_any</c> over a two-element <c>uint32</c> array does.
    /// </summary>
    /// <param name="first">The first value.</param>
    /// <param name="second">The second value.</param>
    /// <returns>The hash.</returns>
    internal static uint HashIds(uint first, uint second)
    {
        ReadOnlySpan<uint> hashinput = [first, second];
        return Worf.HashAny(MemoryMarshal.AsBytes(hashinput));
    }

    private static void ValidateSubscriptionName(string name)
    {
        byte[] bytes = Worf.ToServer(name);
        if (bytes.Length == 0)
        {
            throw new PgException(PgSqlStates.InvalidName, $"subscription  name \"{name}\" is too short");
        }

        if (bytes.Length >= Worker.NameDataLen)
        {
            throw new PgException(PgSqlStates.NameTooLong, $"subscription name \"{name}\" is too long");
        }

        foreach (byte cp in bytes)
        {
            if (!((cp >= 'a' && cp <= 'z') || (cp >= '0' && cp <= '9') || cp == '_'))
            {
                throw new PgException(PgSqlStates.InvalidName, $"subscription name \"{name}\" contains invalid character",
                    hint: "Subscription names may only contain lower case letters, numbers, and the underscore character.");
            }
        }
    }

    private static void NativeMethodsCommandCounterIncrement()
    {
        unsafe
        {
            NativeMethods.CommandCounterIncrement();
        }
    }

    // The plan that records what a replication origin applies.
    private static SpiPreparedStatement? s_recordOriginWriter;

    private const string SubscriptionQuery =
        "SELECT sub_id, sub_name::text, sub_origin, sub_target, sub_origin_if, sub_target_if, sub_enabled, " +
        "sub_slot_name::text, sub_replication_sets, sub_forward_origins, sub_apply_delay, sub_force_text_transfer " +
        "FROM worf.subscription";

    /// <summary>
    /// Names the replication origin under which apply commits the changes its provider relays from another node, so
    /// that conflict resolution can tell which node wrote a row. Slot names, and so workers' own origins, can't
    /// contain the separator, so no subscription's origin can look like another's relay origin.
    /// </summary>
    /// <param name="originName">The apply or table sync worker's own origin.</param>
    /// <param name="nodeId">The node that wrote the changes.</param>
    /// <returns>The origin's name.</returns>
    internal static string RelayOriginName(string originName, uint nodeId)
        => $"{originName}{RelaySeparator}{nodeId.ToString(CultureInfo.InvariantCulture)}";

    /// <summary>
    /// Gets the node that wrote the changes a replication origin applies: the node a relay origin names, or the
    /// provider of the subscription whose apply or table sync worker owns the origin. This works while decoding too.
    /// </summary>
    /// <param name="originName">The origin's name.</param>
    /// <returns>The node's id, or null when no subscription owns the origin.</returns>
    internal static unsafe uint? GetOriginNodeId(string originName)
    {
        // A relay origin is a worker's origin followed by the id of the node that wrote its changes.
        string? relayed = null;
        uint writer = 0;
        int relay = originName.LastIndexOf(RelaySeparator, StringComparison.Ordinal);
        if (relay > 0 && uint.TryParse(originName.AsSpan(relay + RelaySeparator.Length), NumberStyles.None,
            CultureInfo.InvariantCulture, out writer))
        {
            relayed = originName[..relay];
        }

        RelationData* rel = OpenCatalog(CatalogSubscription, AccessShareLock);
        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, false, null, 0, null);
        uint? result = null;
        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            bool isnull;
            string slotName = Worf.FromServer((sbyte*)NativeMethods.heap_getattr(tuple, AnumSubSlotName, rel->rd_att, &isnull));
            if (IsWorkerOrigin(originName, slotName))
            {
                result = (uint)NativeMethods.heap_getattr(tuple, AnumSubOrigin, rel->rd_att, &isnull);
                break;
            }

            if (relayed is not null && IsWorkerOrigin(relayed, slotName))
            {
                result = writer;
            }
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, AccessShareLock);
        return result;
    }

    /// <summary>
    /// Creates a replication origin and records the node whose changes it applies, and the subscription it applies
    /// them for, from this transaction on, since PostgreSQL reuses a dropped origin's id while rows committed under it
    /// keep the id.
    /// </summary>
    /// <param name="originName">The origin.</param>
    /// <param name="writerNodeId">The node whose changes it applies.</param>
    /// <param name="subId">The subscription it applies them for.</param>
    /// <returns>The origin's id, and the full id of the transaction that created it.</returns>
    internal static (ushort Origin, ulong SinceXid) CreateOrigin(string originName, uint writerNodeId, uint subId)
    {
        ushort origin = PgReplicationOrigin.Create(originName);
        ulong sinceXid = RecordOriginWriter(origin, writerNodeId, subId);

        // An origin that a rolled back transaction created and used leaves its progress in shared memory, which an
        // origin created with its id would otherwise start from.
        if (PgReplicationOrigin.GetProgress(origin, false) != PgLsn.Invalid)
        {
            PgReplicationOrigin.Advance(origin, PgLsn.Invalid, PgLsn.Invalid, true, true);
        }

        return (origin, sinceXid);
    }

    /// <summary>
    /// Drops a replication origin that worf created, and records that worf applies nothing under its id from this
    /// transaction on, as an origin worf doesn't know can take the id next.
    /// </summary>
    /// <param name="originName">The origin.</param>
    internal static void DropOrigin(string originName)
    {
        if (PgReplicationOrigin.Find(originName) is ushort origin)
        {
            PgReplicationOrigin.Drop(originName, false);
            _ = RecordOriginWriter(origin, 0, 0);
        }
    }

    /// <summary>
    /// Gets the node whose changes a replication origin applied when a transaction ran, and the subscription it applied
    /// them for, from what worf recorded.
    /// </summary>
    /// <param name="originId">The origin's id.</param>
    /// <param name="xid">The transaction, within the last two billion.</param>
    /// <returns>The node and the subscription, zeros when worf had dropped the origin, or null when worf didn't record
    /// the origin.</returns>
    internal static (uint Writer, uint SubId)? OriginWriter(ushort originId, uint xid)
    {
        SpiResult result = Worf.Read("""
            SELECT w.writer_node, w.sub_id FROM worf.origin_writer w,
                (SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())::text::bigint AS next) n
            WHERE w.roident = $1 AND w.since_xid::text::bigint <= CASE WHEN $2 <= n.next & 4294967295 THEN n.next >> 32 << 32 | $2
                ELSE (n.next >> 32) - 1 << 32 | $2 END
            ORDER BY w.since_xid DESC
            LIMIT 1
            """, 1, SpiParameter.Create((uint)originId), SpiParameter.Create((long)xid));
        return result.Count == 0 ? null : (result[0].Get<uint>(0), result[0].Get<uint>(1));
    }

    /// <summary>
    /// Lists a worker's own replication origin and its relay origins that apply its subscription's changes now, with the
    /// transaction that created each. Only the worker drops them while it runs.
    /// </summary>
    /// <param name="subId">The subscription.</param>
    /// <param name="originName">The worker's origin.</param>
    /// <returns>Each origin's id and the full id of the transaction that created it.</returns>
    internal static Dictionary<ushort, ulong> WorkerOrigins(uint subId, string originName)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT o.roident, w.since_xid::text::bigint FROM pg_catalog.pg_replication_origin o,
                LATERAL (SELECT sub_id, since_xid FROM worf.origin_writer WHERE roident = o.roident ORDER BY since_xid DESC LIMIT 1) w
            WHERE w.sub_id = $1 AND (o.roname = $2 OR starts_with(o.roname, $3))
            """, SpiParameter.Create(subId), SpiParameter.Create(originName), SpiParameter.Create(originName + RelaySeparator));
        return Enumerable.Range(0, result.Count).ToDictionary(i => (ushort)result[i].Get<uint>(0), i => (ulong)result[i].Get<long>(1));
    }

    /// <summary>
    /// Drops a worker's replication origin and its relay origins.
    /// </summary>
    /// <param name="originName">The worker's origin.</param>
    internal static void DropOrigins(string originName)
    {
        DropOrigin(originName);
        foreach ((_, string relay) in RelayOrigins(originName))
        {
            DropOrigin(relay);
        }
    }

    /// <summary>
    /// Lists a worker's relay origins.
    /// </summary>
    /// <param name="originName">The worker's origin.</param>
    /// <returns>The relay origins' ids and names.</returns>
    internal static List<(ushort Id, string Name)> RelayOrigins(string originName)
    {
        string prefix = originName + RelaySeparator;
        SpiResult result = Worf.SelectCatalog(
            "SELECT roident, roname FROM pg_catalog.pg_replication_origin WHERE starts_with(roname, $1)", SpiParameter.Create(prefix));
        return [.. Enumerable.Range(0, result.Count)
            .Select(i => ((ushort)result[i].Get<uint>(0), result[i].Get<string>(1)))
            .Where(relay => uint.TryParse(relay.Item2.AsSpan(prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out _))];
    }

    /// <summary>
    /// Records what a replication origin applies from this transaction on: the node whose changes and the subscription
    /// it applies them for, or zeros for nothing. A record that later ones supersede for every transaction since the
    /// oldest that any database hasn't frozen is forgotten, as rows older than that carry no commit origin. Apply runs
    /// this for each relay origin it creates and drops, so the statement is planned once.
    /// </summary>
    /// <returns>The full id of this transaction.</returns>
    private static ulong RecordOriginWriter(ushort origin, uint writerNodeId, uint subId)
        => Elevated.Run(() =>
        {
            s_recordOriginWriter ??= Spi.Prepare("""
                WITH recorded AS (
                    INSERT INTO worf.origin_writer VALUES ($1, $2, $3, pg_catalog.pg_current_xact_id())
                    ON CONFLICT (roident, since_xid) DO UPDATE SET writer_node = excluded.writer_node, sub_id = excluded.sub_id
                    RETURNING since_xid),
                kept AS (
                    SELECT since_xid FROM worf.origin_writer
                    WHERE roident = $1 AND since_xid <= (
                        SELECT min(CASE WHEN f <= n & 4294967295 THEN n >> 32 << 32 | f ELSE (n >> 32) - 1 << 32 | f END)
                        FROM (SELECT datfrozenxid::text::bigint AS f, pg_catalog.pg_current_xact_id()::text::bigint AS n
                              FROM pg_catalog.pg_database) d)::text::xid8
                    ORDER BY since_xid DESC
                    LIMIT 1),
                pruned AS (
                    DELETE FROM worf.origin_writer w USING kept WHERE w.roident = $1 AND w.since_xid < kept.since_xid)
                SELECT since_xid::text::bigint FROM recorded
                """, [typeof(uint), typeof(uint), typeof(uint)]).Keep();
            return (ulong)s_recordOriginWriter.ExecuteScalar<long>(SpiParameter.Create((uint)origin), SpiParameter.Create(writerNodeId),
                SpiParameter.Create(subId));
        });

    /// <summary>
    /// Gets a replication origin's name.
    /// </summary>
    /// <param name="originId">The origin's id.</param>
    /// <returns>The name, or null when no origin has the id.</returns>
    internal static unsafe string? OriginName(ushort originId)
    {
        sbyte* name;
        return NativeMethods.replorigin_by_oid(originId, true, &name) ? Worf.FromServer(name) : null;
    }

    /// <summary>
    /// Checks whether every replication origin slot in shared memory is taken, so that a new subscription, table sync
    /// or relay origin can't be tracked.
    /// </summary>
    /// <returns>Whether they are.</returns>
    internal static bool OriginSlotsFull()
        => Worf.ReadScalar<bool>("""
            SELECT count(*) >= coalesce(pg_catalog.current_setting('max_active_replication_origins', true),
                pg_catalog.current_setting('max_replication_slots'))::integer
            FROM pg_catalog.pg_replication_origin_status
            """);

    /// <summary>
    /// Checks whether an origin is the apply or a table sync worker's own origin for a subscription: the slot's name,
    /// or for a table sync worker, the slot's name followed by the table's hash.
    /// </summary>
    private static bool IsWorkerOrigin(string originName, string slotName)
        => originName == slotName ||
            (originName.Length == slotName.Length + 9 && originName.StartsWith(slotName + "_", StringComparison.Ordinal) &&
                uint.TryParse(originName.AsSpan(slotName.Length + 1), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out _));

    private static unsafe RelationData* OpenCatalog(string table, int lockmode) => Worf.OpenCatalog(table, lockmode);

    private static unsafe WorfNode NodeFromTuple(HeapTupleData* tuple, TupleDescData* desc)
    {
        bool isnull;
        return new WorfNode
        {
            Id = (uint)NativeMethods.heap_getattr(tuple, AnumNodeId, desc, &isnull),
            Name = Worf.FromServer((sbyte*)NativeMethods.heap_getattr(tuple, AnumNodeName, desc, &isnull)),
        };
    }

    private static unsafe WorfInterface InterfaceFromTuple(HeapTupleData* tuple, TupleDescData* desc)
    {
        bool isnull;
        return new WorfInterface
        {
            Id = (uint)NativeMethods.heap_getattr(tuple, AnumIfId, desc, &isnull),
            Name = Worf.FromServer((sbyte*)NativeMethods.heap_getattr(tuple, AnumIfName, desc, &isnull)),
            NodeId = (uint)NativeMethods.heap_getattr(tuple, AnumIfNodeid, desc, &isnull),
            Dsn = Worf.FromServer(NativeMethods.text_to_cstring((varlena*)NativeMethods.heap_getattr(tuple, AnumIfDsn, desc, &isnull))),
        };
    }

    private const string CatalogNode = "node";
    private const string CatalogSubscription = "subscription";
    private const string RelaySeparator = "@";
    private const string CatalogLocalNode = "local_node";
    private const string CatalogNodeInterface = "node_interface";

    private const short AnumNodeId = 1;
    private const short AnumNodeName = 2;

    private const short AnumNodeLocalId = 1;
    private const short AnumNodeLocalNodeIf = 2;

    private const short AnumIfId = 1;
    private const short AnumIfName = 2;
    private const short AnumIfNodeid = 3;
    private const short AnumIfDsn = 4;

    private const short AnumSubOrigin = 3;
    private const short AnumSubSlotName = 8;

    private const int NoLock = 0;
    private const int AccessShareLock = 1;
    private const int RowExclusiveLock = 3;
    private const ushort BtEqualStrategyNumber = 3;
    private const uint FOideq = 184;
    private const uint FNameeq = 62;

    private static WorfSubscription SubscriptionFromRow(SpiRow row)
    {
        WorfSubscription sub = new()
        {
            Id = row.Get<uint>(0),
            Name = row.Get<string>(1),
            Enabled = row.Get<bool>(6),
            SlotName = row.Get<string>(7),
        };
        sub.Origin = GetNode(row.Get<uint>(2));
        sub.Target = GetNode(row.Get<uint>(3));
        sub.OriginIf = GetNodeInterface(row.Get<uint>(4));
        sub.TargetIf = GetNodeInterface(row.Get<uint>(5));

        // Get replication sets.
        string?[]? replicationSets = row.Get<string?[]?>(8);
        sub.ReplicationSets = replicationSets is null ? [] : Worf.TextArrayToList(replicationSets);

        // Get origin forwarding.
        string?[]? forwardOrigins = row.Get<string?[]?>(9);
        sub.ForwardOrigins = forwardOrigins is null ? [] : Worf.TextArrayToList(forwardOrigins);

        // Get apply_delay.
        sub.ApplyDelay = row.Get<PgInterval?>(10);

        // Get force_text_transfer.
        sub.ForceTextTransfer = row.Get<bool?>(11) ?? false;
        return sub;
    }
}
