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
    );
    """)]

namespace Worf;

/// <summary>
/// A node.
/// </summary>
internal sealed class WorfNode
{
    /// <summary>
    /// Gets or sets the node's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the node's name.
    /// </summary>
    public string Name { get; set; } = "";
}

/// <summary>
/// A node's connection interface.
/// </summary>
internal sealed class WorfInterface
{
    /// <summary>
    /// Gets or sets the interface's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the interface's name.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Gets or sets the node the interface belongs to.
    /// </summary>
    public uint NodeId { get; set; }

    /// <summary>
    /// Gets or sets the interface's connection string.
    /// </summary>
    public string Dsn { get; set; } = "";
}

/// <summary>
/// The local node and its interface.
/// </summary>
/// <param name="node">The node.</param>
/// <param name="nodeIf">The node's interface.</param>
internal sealed class WorfLocalNode(WorfNode node, WorfInterface nodeIf)
{
    /// <summary>
    /// Gets the node.
    /// </summary>
    public WorfNode Node { get; } = node;

    /// <summary>
    /// Gets the node's interface.
    /// </summary>
    public WorfInterface NodeIf { get; } = nodeIf;
}

/// <summary>
/// A subscription.
/// </summary>
internal sealed class WorfSubscription
{
    /// <summary>
    /// Gets or sets the subscription's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the subscription's name.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Gets or sets the provider node.
    /// </summary>
    public WorfNode Origin { get; set; } = new();

    /// <summary>
    /// Gets or sets the subscriber node.
    /// </summary>
    public WorfNode Target { get; set; } = new();

    /// <summary>
    /// Gets or sets the provider's interface.
    /// </summary>
    public WorfInterface OriginIf { get; set; } = new();

    /// <summary>
    /// Gets or sets the subscriber's interface.
    /// </summary>
    public WorfInterface TargetIf { get; set; } = new();

    /// <summary>
    /// Gets or sets whether the subscription is enabled.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the apply delay, or null.
    /// </summary>
    public PgInterval? ApplyDelay { get; set; }

    /// <summary>
    /// Gets or sets the slot name.
    /// </summary>
    public string SlotName { get; set; } = "";

    /// <summary>
    /// Gets or sets the subscribed replication sets.
    /// </summary>
    public List<string> ReplicationSets { get; set; } = [];

    /// <summary>
    /// Gets or sets the forwarded origins.
    /// </summary>
    public List<string> ForwardOrigins { get; set; } = [];

    /// <summary>
    /// Gets or sets whether to transfer data as text.
    /// </summary>
    public bool ForceTextTransfer { get; set; }
}

/// <summary>
/// worf node and subscription catalog manipulation functions.
/// </summary>
internal static class Node
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

        Worker.SubscriptionChanged(0);
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

        Worker.SubscriptionChanged(0);
    }

    /// <summary>
    /// Load the info for specific node.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <returns>The node.</returns>
    internal static WorfNode GetNode(uint nodeid)
    {
        SpiResult result = Spi.Select("SELECT node_id, node_name::text FROM worf.node WHERE node_id = $1",
            SpiParameter.Create(nodeid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"node {nodeid} not found");
        }

        return NodeFromRow(result[0]);
    }

    /// <summary>
    /// Load the info for specific node.
    /// </summary>
    /// <param name="name">The node's name.</param>
    /// <param name="missingOk">Whether to return null for a missing node.</param>
    /// <returns>The node.</returns>
    internal static WorfNode? GetNodeByName(string name, bool missingOk)
    {
        SpiResult result = Spi.Select("SELECT node_id, node_name::text FROM worf.node WHERE node_name = $1::name",
            SpiParameter.Create(name));
        if (result.Count == 0)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"node {name} not found");
        }

        return NodeFromRow(result[0]);
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
    /// <param name="missingOk">Whether to return null when this database is not a node.</param>
    /// <returns>The local node.</returns>
    internal static WorfLocalNode? GetLocalNode(bool forUpdate, bool missingOk)
    {
        uint? relid = Spi.ExecuteScalar<uint?>("SELECT pg_catalog.to_regclass('worf.local_node')::oid");
        if (relid is null)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local worf node not found");
        }

        if (forUpdate)
        {
            unsafe
            {
                NativeMethods.LockRelationOid(relid.Value, ShareUpdateExclusiveLock);
            }
        }

        // Find the local node tuple.
        SpiResult result = Spi.Select("SELECT node_id, node_local_interface FROM worf.local_node LIMIT 1");
        if (result.Count == 0)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local worf node not found");
        }

        uint nodeid = result[0].Get<uint>(0);
        uint nodeifid = result[0].Get<uint>(1);
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
    internal static WorfInterface GetNodeInterface(uint ifid)
    {
        SpiResult result = Spi.Select(
            "SELECT if_id, if_name::text, if_nodeid, if_dsn FROM worf.node_interface WHERE if_id = $1",
            SpiParameter.Create(ifid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.InternalError, $"node interface {ifid} not found");
        }

        return InterfaceFromRow(result[0]);
    }

    /// <summary>
    /// Get the node interface by name.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="name">The interface's name.</param>
    /// <param name="missingOk">Whether to return null for a missing interface.</param>
    /// <returns>The interface.</returns>
    internal static WorfInterface? GetNodeInterfaceByName(uint nodeid, string name, bool missingOk)
    {
        SpiResult result = Spi.Select(
            "SELECT if_id, if_name::text, if_nodeid, if_dsn FROM worf.node_interface WHERE if_nodeid = $1 AND if_name = $2::name",
            SpiParameter.Create(nodeid), SpiParameter.Create(name));
        if (result.Count == 0)
        {
            if (missingOk)
            {
                return null;
            }

            throw new PgException(PgSqlStates.InternalError, $"node interface \"{name}\" not found for nod {nodeid}");
        }

        return InterfaceFromRow(result[0]);
    }

    /// <summary>
    /// Load the info for specific subscriber.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <returns>The subscription.</returns>
    internal static WorfSubscription GetSubscription(uint subid)
    {
        SpiResult result = Spi.Select(SubscriptionQuery + " WHERE sub_id = $1", SpiParameter.Create(subid));
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
        SpiResult result = Spi.Select(SubscriptionQuery + " WHERE sub_name = $1::name", SpiParameter.Create(name));
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
    /// Return all target node subscriptions.
    /// </summary>
    /// <param name="nodeid">The node.</param>
    /// <param name="origin">Whether to return the subscriptions to the node rather than from it.</param>
    /// <returns>The subscriptions.</returns>
    internal static List<WorfSubscription> GetNodeSubscriptions(uint nodeid, bool origin)
    {
        SpiResult result = Spi.Select(SubscriptionQuery + (origin ? " WHERE sub_origin = $1" : " WHERE sub_target = $1"),
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
        return Worf.HashAny(System.Runtime.InteropServices.MemoryMarshal.AsBytes(hashinput));
    }

    private const string SubscriptionQuery =
        "SELECT sub_id, sub_name::text, sub_origin, sub_target, sub_origin_if, sub_target_if, sub_enabled, " +
        "sub_slot_name::text, sub_replication_sets, sub_forward_origins, sub_apply_delay, sub_force_text_transfer " +
        "FROM worf.subscription";

    private static WorfNode NodeFromRow(SpiRow row) => new() { Id = row.Get<uint>(0), Name = row.Get<string>(1) };

    private static WorfInterface InterfaceFromRow(SpiRow row) => new()
    {
        Id = row.Get<uint>(0),
        Name = row.Get<string>(1),
        NodeId = row.Get<uint>(2),
        Dsn = row.Get<string>(3),
    };

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
