using Ankus.Postgres;
namespace Worf;

/// <summary>
/// worf SQL visible interfaces.
/// </summary>
internal static class Functions
{
    /// <summary>
    /// Create new node.
    /// </summary>
    /// <param name="nodeName">The node's name.</param>
    /// <param name="dsn">The node's connection string.</param>
    /// <returns>The node's id.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.create_node(node_name name, dsn text)
        RETURNS oid STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static uint CreateNode([PgSqlType("name", Schema = "pg_catalog")] PgDatum nodeName, string dsn)
    {
        WorfNode node = new() { Id = 0, Name = NameArg(nodeName) };
        Node.CreateNode(node);

        WorfInterface nodeif = new() { Id = 0, Name = node.Name, NodeId = node.Id, Dsn = dsn };
        Node.CreateNodeInterface(nodeif);

        // Create predefined repsets.
        ReplicationSet.CreateReplicationSet(new WorfRepSet
        {
            Id = 0,
            NodeId = node.Id,
            Name = ReplicationSet.DefaultRepsetName,
            ReplicateInsert = true,
            ReplicateUpdate = true,
            ReplicateDelete = true,
            ReplicateTruncate = true,
        });

        ReplicationSet.CreateReplicationSet(new WorfRepSet
        {
            Id = 0,
            NodeId = node.Id,
            Name = ReplicationSet.DefaultInsonlyRepsetName,
            ReplicateInsert = true,
            ReplicateUpdate = false,
            ReplicateDelete = false,
            ReplicateTruncate = true,
        });

        ReplicationSet.CreateReplicationSet(new WorfRepSet
        {
            Id = 0,
            NodeId = node.Id,
            Name = ReplicationSet.DdlSqlRepsetName,
            ReplicateInsert = true,
            ReplicateUpdate = false,
            ReplicateDelete = false,
            ReplicateTruncate = false,
        });

        Node.CreateLocalNode(node.Id, nodeif.Id);

        return node.Id;
    }

    /// <summary>
    /// Drop the named node.
    /// </summary>
    /// <param name="nodeName">The node's name.</param>
    /// <param name="ifexists">Whether a missing node is not an error.</param>
    /// <returns>Whether the node existed.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.drop_node(node_name name, ifexists boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool DropNode([PgSqlType("name", Schema = "pg_catalog")] PgDatum nodeName, bool ifexists)
    {
        string name = NameArg(nodeName);
        WorfNode? node = Node.GetNodeByName(name, ifexists);

        if (node is not null)
        {
            List<WorfSubscription> osubs = Node.GetNodeSubscriptions(node.Id, true);
            List<WorfSubscription> tsubs = Node.GetNodeSubscriptions(node.Id, false);
            if (osubs.Count != 0 || tsubs.Count != 0)
            {
                throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                    $"cannot drop node \"{name}\" because it still has subscriptions associated with it",
                    hint: "drop the subscriptions first");
            }

            // If the node is local node, drop the record as well.
            WorfLocalNode? localNode = Node.GetLocalNode(true, true);
            if (localNode is not null && localNode.Node.Id == node.Id)
            {
                // Also drop all the slots associated with the node.
                try
                {
                    _ = Spi.Select("SELECT pg_catalog.pg_drop_replication_slot(slot_name)" +
                        "  FROM pg_catalog.pg_replication_slots" +
                        " WHERE plugin = 'worf'" +
                        "   AND database = current_database()" +
                        "   AND slot_name ~ 'worf_.*'");
                }
                catch (PgException)
                {
                    throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                        $"cannot drop node \"{name}\" because one or more replication slots for the node are still active",
                        hint: "drop the subscriptions connected to the node first");
                }

                // And drop the local node association as well.
                Node.DropLocalNode();
            }

            // Drop all the interfaces.
            Node.DropNodeInterfaces(node.Id);

            // Drop replication sets associated with the node.
            ReplicationSet.DropNodeReplicationSets(node.Id);

            // Drop the node itself.
            Node.DropNode(node.Id);
        }

        return node is not null;
    }

    /// <summary>
    /// Add interface to a node.
    /// </summary>
    /// <param name="nodeName">The node's name.</param>
    /// <param name="interfaceName">The interface's name.</param>
    /// <param name="dsn">The interface's connection string.</param>
    /// <returns>The interface's id.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_node_add_interface(node_name name, interface_name name, dsn text)
        RETURNS oid STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static uint AlterNodeAddInterface([PgSqlType("name", Schema = "pg_catalog")] PgDatum nodeName,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum interfaceName, string dsn)
    {
        string name = NameArg(nodeName);
        string ifName = NameArg(interfaceName);

        WorfNode? node = Node.GetNodeByName(name, false);
        if (node is null)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"node \"{name}\" not found");
        }

        WorfInterface? oldif = Node.GetNodeInterfaceByName(node.Id, ifName, true);
        if (oldif is not null)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"node \"{name}\" already has interface named \"{ifName}\"");
        }

        WorfInterface newif = new() { Id = 0, Name = ifName, NodeId = node.Id, Dsn = dsn };
        Node.CreateNodeInterface(newif);

        return newif.Id;
    }

    /// <summary>
    /// Drop interface from a node.
    /// </summary>
    /// <param name="nodeName">The node's name.</param>
    /// <param name="interfaceName">The interface's name.</param>
    /// <returns>True.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_node_drop_interface(node_name name, interface_name name)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterNodeDropInterface([PgSqlType("name", Schema = "pg_catalog")] PgDatum nodeName,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum interfaceName)
    {
        string name = NameArg(nodeName);
        string ifName = NameArg(interfaceName);

        WorfNode? node = Node.GetNodeByName(name, false);
        if (node is null)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"node \"{name}\" not found");
        }

        WorfInterface? oldif = Node.GetNodeInterfaceByName(node.Id, ifName, true);
        if (oldif is null)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"interface \"{ifName}\" for node node \"{name}\" not found");
        }

        List<WorfSubscription> otherSubs = Node.GetNodeSubscriptions(node.Id, true);
        foreach (WorfSubscription sub in otherSubs)
        {
            if (oldif.Id == sub.OriginIf.Id)
            {
                throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                    $"cannot drop interface \"{oldif.Name}\" for node \"{node.Name}\" because subscription \"{sub.Name}\" is using it",
                    hint: "change the subscription interface first");
            }
        }

        Node.DropNodeInterface(oldif.Id);

        return true;
    }

    /// <summary>
    /// Returns information about the local node.
    /// </summary>
    /// <returns>The node's id, name, system identifier, database and replication sets.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.worf_node_info(OUT node_id oid, OUT node_name text, OUT sysid text, OUT dbname text, OUT replication_sets text)
        RETURNS record
        STABLE STRICT LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple WorfNodeInfo()
    {
        WorfLocalNode node = Node.GetLocalNode(false, false)!;

        ulong sysid;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
        }

        List<WorfRepSet> repsets = ReplicationSet.GetNodeReplicationSets(node.Node.Id);

        return PgHeapTuple.Create(
            ("node_id", SpiParameter.Create(node.Node.Id)),
            ("node_name", SpiParameter.Create(node.Node.Name)),
            ("sysid", SpiParameter.Create(sysid.ToString(System.Globalization.CultureInfo.InvariantCulture))),
            ("dbname", SpiParameter.Create(Spi.ExecuteScalar<string>("SELECT pg_catalog.current_database()::text"))),
            ("replication_sets", SpiParameter.Create(ReplicationSet.StringlistToIdentifierstr(repsets.Select(static repset => repset.Name)))));
    }

    /// <summary>
    /// Generates a slot name.
    /// </summary>
    /// <param name="dbname">The subscriber's database.</param>
    /// <param name="providerNodeName">The provider node's name.</param>
    /// <param name="subscriptionName">The subscription's name.</param>
    /// <returns>The slot name.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.worf_gen_slot_name(name, name, name)
        RETURNS name
        IMMUTABLE STRICT LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    [return: PgSqlType("name", Schema = "pg_catalog")]
    internal static PgDatum WorfGenSlotName([PgSqlType("name", Schema = "pg_catalog")] PgDatum dbname,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum providerNodeName,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName)
    {
        string slotName = GenSlotName(NameArg(dbname), NameArg(providerNodeName), NameArg(subscriptionName));
        return PgFunctions.CallRaw("pg_catalog.name", PgMemoryContext.Current, PgFunctionArgument.Create(slotName));
    }

    /// <summary>
    /// Returns the worf version.
    /// </summary>
    /// <returns>The version.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf_version() RETURNS text
        LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static string WorfVersion() => Worf.Version;

    /// <summary>
    /// Returns the worf version number.
    /// </summary>
    /// <returns>The version number.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf_version_num() RETURNS integer
        LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static int WorfVersionNum() => Worf.VersionNum;

    /// <summary>
    /// Returns the newest protocol version.
    /// </summary>
    /// <returns>The protocol version.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf_max_proto_version() RETURNS integer
        LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static int WorfMaxProtoVersion() => Worf.MaxProtoVersionNum;

    /// <summary>
    /// Returns the oldest protocol version.
    /// </summary>
    /// <returns>The protocol version.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf_min_proto_version() RETURNS integer
        LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static int WorfMinProtoVersion() => Worf.MinProtoVersionNum;

    /// <summary>
    /// Generate slot name (used also for origin identifier). The current format is
    /// <c>worf_&lt;subscriber database name&gt;_&lt;provider node name&gt;_&lt;subscription name&gt;</c>. Note that
    /// we want to leave enough free space for 8 bytes of suffix which in practice means 9 bytes including the
    /// underscore.
    /// </summary>
    /// <param name="dbname">The subscriber's database.</param>
    /// <param name="providerNode">The provider node's name.</param>
    /// <param name="subscriptionName">The subscription's name.</param>
    /// <returns>The slot name.</returns>
    internal static string GenSlotName(string dbname, string providerNode, string subscriptionName)
    {
        List<byte> name = [.. "worf_"u8];
        name.AddRange(Worf.ShortenHash(Worf.ToServer(dbname), 16));
        name.Add((byte)'_');
        name.AddRange(Worf.ShortenHash(Worf.ToServer(providerNode), 16));
        name.Add((byte)'_');
        name.AddRange(Worf.ShortenHash(Worf.ToServer(subscriptionName), 16));
        if (name.Count > 63)
        {
            name.RemoveRange(63, name.Count - 63);
        }

        // Replace all the invalid characters in slot name with underscore.
        char[] slot = new char[name.Count];
        for (int i = 0; i < name.Count; i++)
        {
            byte cp = name[i];
            slot[i] = (cp >= 'a' && cp <= 'z') || (cp >= '0' && cp <= '9') || cp == '_' ? (char)cp : '_';
        }

        return new string(slot);
    }

    /// <summary>
    /// Reads a <c>name</c> argument.
    /// </summary>
    /// <param name="value">The argument.</param>
    /// <returns>The name's text.</returns>
    internal static string NameArg(PgDatum value) => value.ToPostgresString()!;

    /// <summary>
    /// Checks that this database is a worf node.
    /// </summary>
    /// <param name="forUpdate">Whether to lock the local node.</param>
    /// <returns>The local node.</returns>
    internal static WorfLocalNode CheckLocalNode(bool forUpdate)
    {
        WorfLocalNode? node = Node.GetLocalNode(forUpdate, true);
        if (node is null)
        {
            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "current database is not configured as worf node",
                hint: "create worf node first");
        }

        return node;
    }
}
