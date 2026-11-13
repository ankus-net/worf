using System.Globalization;
using System.Text;
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
        WorfNode.CreateNode(node);

        WorfInterface nodeif = new() { Id = 0, Name = node.Name, NodeId = node.Id, Dsn = dsn };
        WorfNode.CreateNodeInterface(nodeif);

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

        WorfNode.CreateLocalNode(node.Id, nodeif.Id);

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
        WorfNode? node = WorfNode.GetNodeByName(name, ifexists);

        if (node is not null)
        {
            List<WorfSubscription> osubs = WorfNode.GetNodeSubscriptions(node.Id, true);
            List<WorfSubscription> tsubs = WorfNode.GetNodeSubscriptions(node.Id, false);
            if (osubs.Count != 0 || tsubs.Count != 0)
            {
                throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                    $"cannot drop node \"{name}\" because it still has subscriptions associated with it",
                    hint: "drop the subscriptions first");
            }

            // If the node is local node, drop the record as well.
            WorfLocalNode? localNode = WorfNode.GetLocalNode(true, true);
            if (localNode is not null && localNode.Node.Id == node.Id)
            {
                // Also drop all the slots associated with the node.
                try
                {
                    _ = Worf.Select("SELECT pg_catalog.pg_drop_replication_slot(slot_name)" +
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
                WorfNode.DropLocalNode();
            }

            // Drop all the interfaces.
            WorfNode.DropNodeInterfaces(node.Id);

            // Drop replication sets associated with the node.
            ReplicationSet.DropNodeReplicationSets(node.Id);

            // Drop the node itself.
            WorfNode.DropNode(node.Id);
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

        WorfNode node = WorfNode.GetNodeByName(name, false)
            ?? throw new PgException(PgSqlStates.InvalidParameterValue, $"node \"{name}\" not found");

        WorfInterface? oldif = WorfNode.GetNodeInterfaceByName(node.Id, ifName, true);
        if (oldif is not null)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, $"node \"{name}\" already has interface named \"{ifName}\"");
        }

        WorfInterface newif = new() { Id = 0, Name = ifName, NodeId = node.Id, Dsn = dsn };
        WorfNode.CreateNodeInterface(newif);

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

        WorfNode node = WorfNode.GetNodeByName(name, false)
            ?? throw new PgException(PgSqlStates.InvalidParameterValue, $"node \"{name}\" not found");

        WorfInterface oldif = WorfNode.GetNodeInterfaceByName(node.Id, ifName, true)
            ?? throw new PgException(PgSqlStates.InvalidParameterValue, $"interface \"{ifName}\" for node node \"{name}\" not found");

        List<WorfSubscription> otherSubs = WorfNode.GetNodeSubscriptions(node.Id, true);
        foreach (WorfSubscription sub in otherSubs)
        {
            if (oldif.Id == sub.OriginIf.Id)
            {
                throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                    $"cannot drop interface \"{oldif.Name}\" for node \"{node.Name}\" because subscription \"{sub.Name}\" is using it",
                    hint: "change the subscription interface first");
            }
        }

        WorfNode.DropNodeInterface(oldif.Id);

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
        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;

        ulong sysid;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
        }

        List<WorfRepSet> repsets = ReplicationSet.GetNodeReplicationSets(node.Node.Id);

        return PgHeapTuple.Create(
            ("node_id", SpiParameter.Create(node.Node.Id)),
            ("node_name", Worf.TextParameter(node.Node.Name)),
            ("sysid", SpiParameter.Create(sysid.ToString(CultureInfo.InvariantCulture))),
            ("dbname", Worf.TextParameter(Worf.ReadScalar<string>("SELECT pg_catalog.current_database()::text"))),
            ("replication_sets", Worf.TextParameter(ReplicationSet.StringlistToIdentifierstr(repsets.Select(static repset => repset.Name)))));
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
    /// Connect two existing nodes.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.create_subscription(subscription_name name, provider_dsn text,
            replication_sets text[] = '{default,default_insert_only,ddl_sql}', synchronize_structure boolean = false,
            synchronize_data boolean = true, forward_origins text[] = '{all}', apply_delay interval DEFAULT '0',
            force_text_transfer boolean = false)
        RETURNS oid STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static uint CreateSubscription([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        string providerDsn, string?[] replicationSets, bool synchronizeStructure, bool synchronizeData,
        string?[] forwardOrigins, PgInterval applyDelay, bool forceTextTransfer)
    {
        string subName = NameArg(subscriptionName);

        // Check that this is actually a node.
        WorfLocalNode localnode = WorfNode.GetLocalNode(true, false)!;

        // Now, fetch info about remote node.
        WorfNode origin;
        using (WorfConnection conn = Worf.Connect(providerDsn, subName, "create"))
        {
            (uint nodeId, string nodeName, _, _, _) = Rpc.RemoteNodeInfo(conn);
            origin = new WorfNode { Id = nodeId, Name = nodeName };

            (bool subscribes, string? remoteResolution) = Rpc.RemoteSubscribesTo(conn, localnode.Node.Id);
            if (subscribes && remoteResolution is not null)
            {
                Conflict.CheckTwoWayResolution(localnode.Node.Name, origin.Name, remoteResolution);
            }
        }

        // Check that we can connect remotely also in replication mode.
        Worf.ConnectReplica(providerDsn, subName, "create").Dispose();

        // Check that local connection works.
        Worf.Connect(localnode.NodeIf.Dsn, subName, "create").Dispose();

        // Check for existing local representation of remote node and interface and lock it if it already exists.
        WorfNode? existingOrigin = WorfNode.GetNodeByName(origin.Name, true);

        // If not found, crate local representation of remote node and interface.
        WorfInterface originif;
        if (existingOrigin is null)
        {
            WorfNode.CreateNode(origin);

            originif = new WorfInterface { Id = 0, Name = origin.Name, NodeId = origin.Id, Dsn = providerDsn };
            WorfNode.CreateNodeInterface(originif);
        }
        else
        {
            WorfInterface existingif = WorfNode.GetNodeInterfaceByName(origin.Id, origin.Name, false)!;
            if (existingif.Dsn != providerDsn)
            {
                throw new PgException(PgSqlStates.InvalidParameterValue,
                    $"dsn \"{providerDsn}\" points to existing node \"{origin.Name}\" with different dsn \"{existingif.Dsn}\"");
            }

            originif = existingif;
        }

        // Check for overlapping replication sets.
        //
        // Note that we can't use exclusion constraints as we use the subscriptions table in same manner as system
        // catalog.
        List<string> replicationSetList = Worf.TextArrayToList(replicationSets);
        foreach (WorfSubscription esub in WorfNode.GetNodeSubscriptions(originif.NodeId, true))
        {
            foreach (string existingset in esub.ReplicationSets)
            {
                foreach (string newset in replicationSetList)
                {
                    if (newset == existingset)
                    {
                        throw new PgException(PgSqlStates.InvalidParameterValue,
                            $"existing subscription \"{esub.Name}\" to node \"{origin.Name}\" already subscribes to replication set \"{newset}\"");
                    }
                }
            }
        }

        // Create the subscription.
        //
        // Note for now we don't care much about the target interface so we fake it here to be invalid.
        WorfInterface targetif = new() { Id = localnode.NodeIf.Id, NodeId = localnode.Node.Id };
        string dbname;
        unsafe
        {
            dbname = Worf.FromServer(NativeMethods.get_database_name(NativeGlobals.MyDatabaseId));
        }

        WorfSubscription sub = new()
        {
            Id = 0,
            Name = subName,
            OriginIf = originif,
            TargetIf = targetif,
            ReplicationSets = replicationSetList,
            ForwardOrigins = Worf.TextArrayToList(forwardOrigins),
            Enabled = true,
            SlotName = GenSlotName(dbname, origin.Name, subName),
            ApplyDelay = applyDelay,
            ForceTextTransfer = forceTextTransfer,
        };

        // Names that differ only in characters a slot's name can't hold, or in its end, can give the same slot, and
        // replication origin, which two subscriptions can't share: neither can a subscription of another database of
        // the cluster, whose replication origins this one's share, nor a replication origin left of that name, which
        // the subscription would take as its own progress.
        if (Worf.ReadScalar<string?>("SELECT sub_name::text FROM worf.subscription WHERE sub_slot_name = $1::name", SpiParameter.Create(sub.SlotName))
            is string sharing)
        {
            throw new PgException(PgSqlStates.DuplicateObject,
                $"subscription \"{subName}\" would use slot \"{sub.SlotName}\", which subscription \"{sharing}\" uses",
                hint: "Choose a subscription name that differs in letters, digits or underscores.");
        }

        // An origin this transaction created is the caller's to start the subscription from, as worf_create_subscriber
        // creates it at the position its restore point reached.
        if (Worf.ReadScalar<uint?>("SELECT xmin::text::pg_catalog.int8::pg_catalog.oid FROM pg_catalog.pg_replication_origin WHERE roname = $1",
            SpiParameter.Create(sub.SlotName)) is uint creator && !CreatedHere(creator))
        {
            throw new PgException(PgSqlStates.DuplicateObject,
                $"subscription \"{subName}\" would use replication origin \"{sub.SlotName}\", which already exists",
                detail: "Another database's subscription may use it, or it was left behind.",
                hint: "Choose a subscription name that differs in letters, digits or underscores, or drop the origin with pg_replication_origin_drop if nothing uses it. An origin created in the same transaction as the subscription is taken as its own.");
        }

        WorfNode.CreateSubscription(sub);

        // The subscription's replication origin is made now, rather than once its worker has made the slot, so that a
        // subscription of another database whose slot's name comes out the same, as names joined with underscores that
        // hold underscores can, finds it and is refused, whatever the first's worker is waiting for.
        if (PgReplicationOrigin.Find(sub.SlotName) is null)
        {
            _ = WorfNode.CreateOrigin(sub.SlotName, origin.Id, sub.Id);
        }

        // Create synchronization status for the subscription.
        Sync.CreateLocalSyncStatus(new WorfSyncStatus
        {
            Kind = synchronizeStructure && synchronizeData ? Sync.SyncKindFull
                : synchronizeStructure ? Sync.SyncKindStructure
                : synchronizeData ? Sync.SyncKindDataOnly
                : Sync.SyncKindInit,
            Subid = sub.Id,
            Status = Sync.SyncStatusInit,
        });

        return sub.Id;
    }

    /// <summary>
    /// Remove subscribption.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.drop_subscription(subscription_name name, ifexists boolean DEFAULT false)
        RETURNS oid STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    [return: PgSqlType("oid", Schema = "pg_catalog")]
    internal static PgDatum DropSubscription([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName, bool ifexists)
    {
        WorfSubscription? sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), ifexists);

        if (sub is not null)
        {
            WorfLocalNode node = WorfNode.GetLocalNode(true, false)!;
            WorfLocks.LockDrop(sub.Id);

            // First drop the status, after noting the slots its tables' syncs create, which may be left on the provider.
            List<string> syncSlotNames = Sync.SyncSlotNames(sub);
            WorfRows recordedSyncSlots = Worf.Read("SELECT slot_name::text FROM worf.sync_slot WHERE sub_id = $1 ORDER BY 1", 0,
                SpiParameter.Create(sub.Id));
            string syncSlotsLeft = string.Join(", ", recordedSyncSlots.Select(row => $"\"{row.Get<string>(0)}\""));
            Sync.DropSubscriptionSyncStatus(sub.Id);
            _ = Worf.Execute("DELETE FROM worf.sync_slot WHERE sub_id = $1", SpiParameter.Create(sub.Id));

            // Drop the actual subscription.
            WorfNode.DropSubscription(sub.Id);

            // The rest is different depending on if we are doing this on provider or subscriber.
            //
            // For now on provider we just exist (there should be no records of subscribers on their provider node).
            if (sub.Origin.Id == node.Node.Id)
            {
                return BoolDatum(true);
            }

            // If the provider node record existed only for the dropped, subscription, it should be dropped as well,
            // along with the fences its DDL kept here, since nothing would bring the DDL.
            if (WorfNode.GetNodeSubscriptions(sub.Origin.Id, true).Count == 0)
            {
                DdlLock.ForgetOrigin(sub.Origin.Id);
                WorfNode.DropNodeInterfaces(sub.Origin.Id);
                WorfNode.DropNode(sub.Origin.Id);
            }

            uint myDatabaseId;
            unsafe
            {
                myDatabaseId = NativeGlobals.MyDatabaseId;
            }

            // Kill the apply and table sync workers to unlock the resources.
            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                foreach (int worker in RunningSubscriptionWorkers(guard, myDatabaseId, sub.Id))
                {
                    Worker.WorkerKill(guard, worker);
                }
            }

            // Wait for them to die. They look for the request to stop between statements, so an apply that waits on a
            // row this transaction deleted, as one that is initializing the subscription does, never sees it until its
            // statement is cancelled. Only an apply this transaction blocks is cancelled: it can't exit before this
            // transaction ends, so it's still there to cancel. A table sync, whose copy can wait on any session's lock,
            // is cancelled whatever it waits for, and its cleanup drops its slot. A cancellation is no error of the
            // data, so exception_behaviour leaves it be.
            for (int waits = 0; ; waits++)
            {
                List<(int Pid, bool Sync)> pids = [];
                using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
                {
                    foreach (int worker in RunningSubscriptionWorkers(guard, myDatabaseId, sub.Id))
                    {
                        unsafe
                        {
                            pids.Add((((PGPROC*)guard[worker].Proc)->pid, guard[worker].WorkerType == WorfWorkerType.Sync));
                        }
                    }
                }

                if (pids.Count == 0)
                {
                    break;
                }

                if (waits > 0)
                {
                    foreach ((int pid, bool sync) in pids)
                    {
                        _ = Elevated.Run(() => Worf.Execute(
                            "SELECT pg_catalog.pg_cancel_backend($1) WHERE $2 OR pg_catalog.pg_backend_pid() = ANY (pg_catalog.pg_blocking_pids($1))",
                            SpiParameter.Create(pid), SpiParameter.Create(sync)));
                    }
                }

                PgInterrupts.Check();

                // Wait for worker termination. On Windows, use a shorter interval for faster cleanup since process
                // termination can be slower.
                Worker.WaitLatch(OperatingSystem.IsWindows() ? 200 : 1000);
            }

            // Drop the slot on remote side, and those its table syncs left.
            //
            bool shared = Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.subscription WHERE sub_slot_name = $1::name AND sub_id <> $2)",
                SpiParameter.Create(sub.SlotName), SpiParameter.Create(sub.Id));
            if (shared)
            {
                WorfLog.Warning($"subscription \"{sub.Name}\" shares slot \"{sub.SlotName}\" with another subscription, which keeps it and its replication origin");
            }

            // Note, we can't fail here since we can't assume that the remote node is still reachable or even alive.
            try
            {
                PgTransaction.RunInSubtransaction(() =>
                {
                    using WorfConnection originConn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "cleanup");

                    // The walsenders of the workers that just exited can hold the slots a moment longer, as when one is
                    // still finishing a large transaction. Wait for them, as the apply does when it starts.
                    long deadline = Worker.GetCurrentTimestamp() + 5000000L;
                    List<string> syncSlots;
                    while ((syncSlots = Sync.DropSyncSlots(originConn, syncSlotNames, new HashSet<string>())).Count > 0 &&
                        Worker.GetCurrentTimestamp() < deadline)
                    {
                        PgInterrupts.Check();
                        Worker.WaitLatch(50);
                    }

                    if (syncSlots.Count > 0)
                    {
                        WorfLog.Warning($"could not drop slots {string.Join(", ", syncSlots.Select(slot => $"\"{slot}\""))} on provider, you will probably have to drop them manually");
                    }

                    while (Worker.GetCurrentTimestamp() < deadline && Rpc.RemoteSlotActive(originConn, sub.SlotName))
                    {
                        PgInterrupts.Check();
                        Worker.WaitLatch(50);
                    }

                    SequenceRangeExchange.Leave(originConn, node, sub);
                    if (!shared)
                    {
                        Rpc.DropRemoteSlot(originConn, sub.SlotName);
                    }
                });
            }
            catch (PgException)
            {
                WorfLog.Warning($"could not drop slot \"{sub.SlotName}\" on provider, you will probably have to drop it manually" +
                    (syncSlotsLeft.Length == 0 ? "" : $", and those its table syncs may have left, {syncSlotsLeft}") +
                    ". Its sequence range fences wait for this node's reports: delete this node's rows in worf.sequence_range_link there while it has no fence up, so that later ones don't, and one that is up waits until this node subscribes again and reports, or worf.activate_sequence_range naming this node stops the wait for it, which reissues any values this node holds that the provider lost");
            }

            // Drop the origin tracking locally, unless another subscription made before names were told apart shares it.
            if (!shared)
            {
                WorfNode.DropOrigins(sub.SlotName);
            }

            // Sequence range fences no longer wait on the provider, unless it still calls this node. A reply that this
            // node's exchange worker is taking in locks the reports of the provider, and of the nodes it relays, in the
            // order of the sequences' names, and this deletes them in the order they're stored, so it waits for the
            // take-in to commit first, as one exchange does for another; one that comes later finds the subscription
            // held, or gone. The turn is taken last, after the calls to the provider above, so that no node holds its
            // turn while it waits on another, and let go once the deletes are done, rather than as the transaction ends.
            SequenceRangeExchange.WithTurn(() => SequenceRanges.ForgetNeighbor(sub.Origin.Id));
        }

        return BoolDatum(sub is not null);
    }

    /// <summary>
    /// Lists a subscription's apply and table sync workers that have a process. The caller holds the lock exclusively.
    /// </summary>
    private static List<int> RunningSubscriptionWorkers(PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard, uint dboid, uint subId)
        => guard.Read((in _, workers) =>
        {
            List<int> found = Worker.SyncFindAll(workers, dboid, subId);
            found.Add(Worker.ApplyFind(workers, dboid, subId));
            List<int> running = [];
            for (int i = 0; i < found.Count; i++)
            {
                if (found[i] >= 0 && workers[found[i]].Proc != 0)
                {
                    running.Add(found[i]);
                }
            }

            return running;
        });

    /// <summary>
    /// Disable subscription.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_disable(subscription_name name, immediate boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionDisable([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName, bool immediate)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;

        // XXX: Only used for locking purposes.
        _ = WorfNode.GetLocalNode(true, false);

        sub.Enabled = false;

        WorfNode.AlterSubscription(sub);

        if (immediate)
        {
            bool inBlock;
            uint myDatabaseId;
            unsafe
            {
                inBlock = NativeMethods.IsTransactionBlock() || NativeMethods.IsSubTransaction();
                myDatabaseId = NativeGlobals.MyDatabaseId;
            }

            if (inBlock)
            {
                throw new PgException(PgSqlStates.ActiveSqlTransaction,
                    "alter_subscription_disable with immediate = true cannot be run inside a transaction block");
            }

            using PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive();
            int apply = guard.Read((in _, workers) => Worker.ApplyFind(workers, myDatabaseId, sub.Id));
            Worker.WorkerKill(guard, apply);
        }

        return true;
    }

    /// <summary>
    /// Enable subscription.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_enable(subscription_name name, immediate boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionEnable([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName, bool immediate)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;

        // XXX: Only used for locking purposes.
        _ = WorfNode.GetLocalNode(true, false);

        sub.Enabled = true;

        WorfNode.AlterSubscription(sub);

        // There is nothing more to immediate here than running it outside of transaction.
        bool inBlock;
        unsafe
        {
            inBlock = NativeMethods.IsTransactionBlock() || NativeMethods.IsSubTransaction();
        }

        if (immediate && inBlock)
        {
            throw new PgException(PgSqlStates.ActiveSqlTransaction,
                "alter_subscription_enable with immediate = true cannot be run inside a transaction block");
        }

        return true;
    }

    /// <summary>
    /// Switch interface the subscription is using.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_interface(subscription_name name, interface_name name)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionInterface([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum interfaceName)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;

        // XXX: Only used for locking purposes.
        _ = WorfNode.GetLocalNode(true, false);

        WorfInterface newIf = WorfNode.GetNodeInterfaceByName(sub.Origin.Id, NameArg(interfaceName), false)!;

        if (newIf.Id == sub.OriginIf.Id)
        {
            return false;
        }

        sub.OriginIf = newIf;
        WorfNode.AlterSubscription(sub);

        return true;
    }

    /// <summary>
    /// Add replication set to subscription.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_add_replication_set(subscription_name name, replication_set name)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionAddReplicationSet([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum replicationSet)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;
        string repsetName = NameArg(replicationSet);

        if (sub.ReplicationSets.Contains(repsetName))
        {
            return false;
        }

        sub.ReplicationSets.Add(repsetName);
        WorfNode.AlterSubscription(sub);

        return true;
    }

    /// <summary>
    /// Remove replication set to subscription.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_remove_replication_set(subscription_name name, replication_set name)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionRemoveReplicationSet([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        [PgSqlType("name", Schema = "pg_catalog")] PgDatum replicationSet)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;
        string repsetName = NameArg(replicationSet);

        if (sub.ReplicationSets.Remove(repsetName))
        {
            WorfNode.AlterSubscription(sub);

            return true;
        }

        return false;
    }

    /// <summary>
    /// Synchronize all the missing tables.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_synchronize(subscription_name name, truncate boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionSynchronize([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName, bool truncate)
    {
        string subName = NameArg(subscriptionName);
        WorfSubscription sub = WorfNode.GetSubscriptionByName(subName, false)!;

        // Read table list from provider.
        List<WorfRemoteRel> remoteTables;
        using (WorfConnection conn = Worf.Connect(sub.OriginIf.Dsn, subName, "sync"))
        {
            remoteTables = Rpc.GetRemoteRepsetTables(conn, sub.ReplicationSets);
        }

        List<WorfSyncStatus> localTables = Sync.GetSubscriptionTables(sub.Id);

        // Compare with sync status on subscription. And add missing ones.
        foreach (WorfRemoteRel remoterel in remoteTables)
        {
            WorfSyncStatus? oldsync = localTables.Find(tablesync =>
                tablesync.Nspname == remoterel.Nspname && tablesync.Relname == remoterel.Relname);

            if (oldsync is not null)
            {
                _ = localTables.Remove(oldsync);
            }
            else
            {
                Sync.CreateLocalSyncStatus(new WorfSyncStatus
                {
                    Kind = Sync.SyncKindDataOnly,
                    Subid = sub.Id,
                    Nspname = remoterel.Nspname,
                    Relname = remoterel.Relname,
                    Status = Sync.SyncStatusInit,
                });

                if (truncate)
                {
                    Sync.TruncateTable(remoterel.Nspname, remoterel.Relname);
                }
            }
        }

        // Any leftover local tables should not be replicated, remove the status for them.
        foreach (WorfSyncStatus tablesync in localTables)
        {
            Sync.DropTableSyncStatusForSub(tablesync.Subid, tablesync.Nspname, tablesync.Relname);
        }

        // Tell apply to re-read sync statuses.
        Worker.SubscriptionChanged(sub.Id, false);

        return true;
    }

    /// <summary>
    /// Resynchronize one existing table.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_subscription_resynchronize_table(subscription_name name, relation regclass,
        	truncate boolean DEFAULT true)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AlterSubscriptionResynchronizeTable([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation, bool truncate)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;
        string nspname;
        string relname;

        using (PgRelation rel = PgRelation.Open(Oid(relation), PgLockMode.AccessShare))
        {
            unsafe
            {
                nspname = Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(rel.Oid)));
                relname = Worf.FromServer(NativeMethods.get_rel_name(rel.Oid));
            }

            // Reset sync status of the table.
            WorfSyncStatus? oldsync = Sync.GetTableSyncStatus(sub.Id, nspname, relname, true);
            if (oldsync is not null)
            {
                // A sync that keeps failing, whose worker waits to start again, starts over at once.
                if (oldsync.Status != Sync.SyncStatusReady &&
                    oldsync.Status != Sync.SyncStatusSyncdone &&
                    oldsync.Status != Sync.SyncStatusNone &&
                    SyncWorkerRuns(sub.Id, nspname, relname))
                {
                    throw new PgException(PgSqlStates.InternalError, $"table {nspname}.{relname} is already being synchronized");
                }

                Sync.SetTableSyncStatus(sub.Id, nspname, relname, Sync.SyncStatusInit, 0);
            }
            else
            {
                Sync.CreateLocalSyncStatus(new WorfSyncStatus
                {
                    Kind = Sync.SyncKindDataOnly,
                    Subid = sub.Id,
                    Nspname = nspname,
                    Relname = relname,
                    Status = Sync.SyncStatusInit,
                });
            }

            // Keep the lock until the transaction ends, as closing with NoLock does.
            unsafe
            {
                NativeMethods.LockRelationOid(rel.Oid, (int)PgLockMode.AccessShare);
            }
        }

        if (truncate)
        {
            Sync.TruncateTable(nspname, relname);
        }

        // Tell apply to re-read sync statuses.
        Worker.SubscriptionChanged(sub.Id, false);

        return true;
    }

    /// <summary>
    /// Checks whether a table's sync worker runs.
    /// </summary>
    private static bool SyncWorkerRuns(uint subId, string nspname, string relname)
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        byte[] serverNspname = Worf.ToServer(nspname);
        byte[] serverRelname = Worf.ToServer(relname);
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        int worker = guard.Read((in _, workers) => Worker.SyncFind(workers, dboid, subId, serverNspname, serverRelname));
        return worker >= 0 && guard[worker].Proc != 0;
    }

    /// <summary>
    /// Synchronize one sequence.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.synchronize_sequence(relation regclass)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool SynchronizeSequence([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation)
    {
        // Check that this is actually a node.
        _ = WorfNode.GetLocalNode(true, false);

        RejectManagedSequence(Oid(relation));
        Sequences.SynchronizeSequence(Oid(relation));

        return true;
    }

    /// <summary>
    /// Show info about one table.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.show_subscription_table(subscription_name name, relation regclass, OUT nspname text, OUT relname text, OUT status text)
        RETURNS record STRICT STABLE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple ShowSubscriptionTable([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation)
    {
        WorfSubscription sub = WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!;
        uint reloid = Oid(relation);
        string nspname;
        string relname;
        unsafe
        {
            nspname = Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(reloid)));
            relname = Worf.FromServer(NativeMethods.get_rel_name(reloid));
        }

        // Reset sync status of the table.
        WorfSyncStatus? sync = Sync.GetTableSyncStatus(sub.Id, nspname, relname, true);
        string syncStatus = sync is not null ? SyncStatusToString(sync.Status) : "unknown";

        return PgHeapTuple.Create(
            ("nspname", Worf.TextParameter(nspname)),
            ("relname", Worf.TextParameter(relname)),
            ("status", SpiParameter.Create(syncStatus)));
    }

    /// <summary>
    /// Show info about subscribption.
    /// </summary>
    [PgFunction(SetMode = PgSetMode.Materialize, Sql = """
        CREATE FUNCTION worf.show_subscription_status(subscription_name name DEFAULT NULL,
            OUT subscription_name text, OUT status text, OUT provider_node text,
            OUT provider_dsn text, OUT slot_name text, OUT replication_sets text[],
            OUT forward_origins text[])
        RETURNS SETOF record STABLE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static IEnumerable<PgHeapTuple?> ShowSubscriptionStatus([PgSqlType("name", Schema = "pg_catalog")] PgDatum? subscriptionName)
    {
        WorfLocalNode node = CheckLocalNode(false);
        List<WorfSubscription> subscriptions = subscriptionName is null || subscriptionName.IsNull
            ? WorfNode.GetNodeSubscriptions(node.Node.Id, false)
            : [WorfNode.GetSubscriptionByName(NameArg(subscriptionName), false)!];

        uint myDatabaseId;
        unsafe
        {
            myDatabaseId = NativeGlobals.MyDatabaseId;
        }

        List<PgHeapTuple?> rows = [];
        foreach (WorfSubscription sub in subscriptions)
        {
            string status;
            int apply;
            bool running;

            using (PgLwLockExclusiveGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Exclusive())
            {
                apply = guard.Read((in _, workers) => Worker.ApplyFind(workers, myDatabaseId, sub.Id));
                // An apply told to stop, as when the subscription's interface changes, is no longer replicating even
                // before it exits, and reporting it as down until its replacement starts lets callers wait for that.
                running = apply >= 0 && guard[apply].Proc != 0 && !guard[apply].TerminateRequested;
            }

            if (running)
            {
                WorfSyncStatus? sync = Sync.GetSubscriptionSyncStatus(sub.Id, true);

                status = sync is null ? "unknown"
                    : sync.Status == Sync.SyncStatusReady ? "replicating"
                    : "initializing";
            }
            else if (!sub.Enabled)
            {
                status = "disabled";
            }
            else
            {
                // The worker slot exists but its worker isn't attached yet, or there's no slot at all, either because
                // the subscription was just created and manager hasn't registered a worker yet, or because the
                // worker was killed and its slot cleared.
                status = "down";
            }

            rows.Add(PgHeapTuple.Create(
                ("subscription_name", Worf.TextParameter(sub.Name)),
                ("status", SpiParameter.Create(status)),
                ("provider_node", Worf.TextParameter(sub.Origin.Name)),
                ("provider_dsn", Worf.TextParameter(sub.OriginIf.Dsn)),
                ("slot_name", Worf.TextParameter(sub.SlotName)),
                ("replication_sets", Worf.TextArrayParameter(sub.ReplicationSets.Count > 0 ? [.. sub.ReplicationSets] : null)),
                ("forward_origins", Worf.TextArrayParameter(sub.ForwardOrigins.Count > 0 ? [.. sub.ForwardOrigins] : null))));
        }

        return rows;
    }

    /// <summary>
    /// Create new replication set.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.create_replication_set(set_name name,
            replicate_insert boolean = true, replicate_update boolean = true,
            replicate_delete boolean = true, replicate_truncate boolean = true)
        RETURNS oid STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static uint CreateReplicationSet([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName,
        bool replicateInsert, bool replicateUpdate, bool replicateDelete, bool replicateTruncate)
    {
        WorfLocalNode node = CheckLocalNode(true);

        WorfRepSet repset = new()
        {
            Id = 0,
            NodeId = node.Node.Id,
            Name = NameArg(setName),
            ReplicateInsert = replicateInsert,
            ReplicateUpdate = replicateUpdate,
            ReplicateDelete = replicateDelete,
            ReplicateTruncate = replicateTruncate,
        };

        ReplicationSet.CreateReplicationSet(repset);

        return repset.Id;
    }

    /// <summary>
    /// Alter existing replication set.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.alter_replication_set(set_name name,
            replicate_insert boolean DEFAULT NULL, replicate_update boolean DEFAULT NULL,
            replicate_delete boolean DEFAULT NULL, replicate_truncate boolean DEFAULT NULL)
        RETURNS oid CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static uint AlterReplicationSet([PgSqlType("name", Schema = "pg_catalog")] PgDatum? setName,
        bool? replicateInsert, bool? replicateUpdate, bool? replicateDelete, bool? replicateTruncate)
    {
        if (setName is null || setName.IsNull)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "set_name cannot be NULL");
        }

        WorfLocalNode node = CheckLocalNode(true);

        WorfRepSet repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, NameArg(setName), false)!;

        if (replicateInsert is bool insert)
        {
            repset.ReplicateInsert = insert;
        }

        if (replicateUpdate is bool update)
        {
            repset.ReplicateUpdate = update;
        }

        if (replicateDelete is bool delete)
        {
            repset.ReplicateDelete = delete;
        }

        if (replicateTruncate is bool truncate)
        {
            repset.ReplicateTruncate = truncate;
        }

        ReplicationSet.AlterReplicationSet(repset);

        return repset.Id;
    }

    /// <summary>
    /// Drop existing replication set.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.drop_replication_set(set_name name, ifexists boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool DropReplicationSet([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName, bool ifexists)
    {
        WorfLocalNode node = CheckLocalNode(true);

        WorfRepSet? repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, NameArg(setName), ifexists);

        if (repset is not null)
        {
            ReplicationSet.DropReplicationSet(repset.Id);
        }

        return repset is not null;
    }

    /// <summary>
    /// Add replication set / table mapping.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replication_set_add_table(set_name name, relation regclass, synchronize_data boolean DEFAULT false,
        	columns text[] DEFAULT NULL, row_filter text DEFAULT NULL)
        RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static unsafe bool ReplicationSetAddTable([PgSqlType("name", Schema = "pg_catalog")] PgDatum? setName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum? relation, bool? synchronizeData, string?[]? columns,
        string? rowFilter)
    {
        // Proccess for required parameters.
        if (setName is null || setName.IsNull)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "set_name cannot be NULL");
        }

        if (relation is null || relation.IsNull)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "relation cannot be NULL");
        }

        if (synchronizeData is not bool synchronize)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "synchronize_data cannot be NULL");
        }

        uint reloid = Oid(relation);

        // standard check for node.
        WorfLocalNode node = CheckLocalNode(true);

        // Find the replication set.
        WorfRepSet repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, NameArg(setName), false)!;

        // Make sure the relation exists (lock mode has to be the same one as in replication_set_add_relation).
        RelationData* rel = NativeMethods.table_open(reloid, (int)PgLockMode.ShareRowExclusive);
        TupleDescData* tupDesc = rel->rd_att;

        string nspname = Worf.FromServer(NativeMethods.get_namespace_name(rel->rd_rel->relnamespace));
        string relname = Worf.FromServer((sbyte*)&rel->rd_rel->relname);

        // Proccess att_list.
        List<string> attList = [];
        if (columns is not null)
        {
            // fetch bitmap of REPLICATION IDENTITY attributes
            Bitmapset* idattrs = NativeMethods.RelationGetIndexAttrBitmap(rel, IndexAttrBitmapKind.INDEX_ATTR_BITMAP_IDENTITY_KEY);

            attList = Worf.TextArrayToList(columns);
            foreach (string attname in attList)
            {
                int attnum = ReplicationSet.GetAttNumByName(tupDesc, attname);

                if (attnum < 0)
                {
                    throw new PgException(PgSqlStates.SyntaxError,
                        $"table {Worf.QuoteQualifiedIdentifier(nspname, relname)} does not have column {attname}");
                }

                idattrs = NativeMethods.bms_del_member(idattrs, attnum - ReplicationSet.FirstLowInvalidHeapAttributeNumber);
            }

            if (!NativeMethods.bms_is_empty(idattrs))
            {
                throw new PgException(PgSqlStates.SyntaxError, "REPLICA IDENTITY columns must be replicated");
            }
        }

        // Proccess row_filter if any.
        Node* rowFilterNode = null;
        if (rowFilter is not null)
        {
            rowFilterNode = ParseRowFilter(rel, rowFilter);
        }

        ReplicationSet.ReplicationSetAddTable(repset.Id, reloid, attList, rowFilterNode);

        if (synchronize)
        {
            // It's easier to construct json manually than via Jsonb API...
            StringBuilder json = new("{\"schema_name\": ");
            Executor.EscapeJson(json, nspname);
            json.Append(",\"table_name\": ");
            Executor.EscapeJson(json, relname);
            json.Append('}');

            // Queue the synchronize request for replication.
            Queue.QueueMessage([repset.Name], NativeMethods.GetUserId(), Queue.QueueCommandTypeTablesync, json.ToString());
        }

        // Cleanup.
        NativeMethods.table_close(rel, (int)PgLockMode.None);

        return true;
    }

    /// <summary>
    /// Add replication set / sequence mapping.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replication_set_add_sequence(set_name name, relation regclass, synchronize_data boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static unsafe bool ReplicationSetAddSequence([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation, bool synchronizeData)
    {
        uint reloid = Oid(relation);

        WorfLocalNode node = CheckLocalNode(true);
        RejectManagedSequence(reloid);

        // Find the replication set.
        WorfRepSet repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, NameArg(setName), false)!;

        // Make sure the relation exists (lock mode has to be the same one as in replication_set_add_relation).
        RelationData* rel = NativeMethods.table_open(reloid, (int)PgLockMode.ShareRowExclusive);

        ReplicationSet.ReplicationSetAddSeq(repset.Id, reloid);

        if (synchronizeData)
        {
            string nspname = Worf.FromServer(NativeMethods.get_namespace_name(rel->rd_rel->relnamespace));
            string relname = Worf.FromServer((sbyte*)&rel->rd_rel->relname);

            // It's easier to construct json manually than via Jsonb API...
            StringBuilder json = new("{\"schema_name\": ");
            Executor.EscapeJson(json, nspname);
            json.Append(",\"sequence_name\": ");
            Executor.EscapeJson(json, relname);
            json.Append(CultureInfo.InvariantCulture, $",\"last_value\": \"{Sequences.SequenceGetLastValue(reloid)}\"");
            json.Append('}');

            // Add sequence to the queue.
            Queue.QueueMessage([repset.Name], NativeMethods.GetUserId(), Queue.QueueCommandTypeSequence, json.ToString());
        }

        // Cleanup.
        NativeMethods.table_close(rel, (int)PgLockMode.None);

        return true;
    }

    /// <summary>
    /// Add replication set / table mapping based on schemas.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replication_set_add_all_tables(set_name name, schema_names text[], synchronize_data boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool ReplicationSetAddAllTables([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName,
        string?[] schemaNames, bool synchronizeData)
        => ReplicationSetAddAllRelations(NameArg(setName), schemaNames, synchronizeData, 'r');

    /// <summary>
    /// Add replication set / sequence mapping based on schemas.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replication_set_add_all_sequences(set_name name, schema_names text[], synchronize_data boolean DEFAULT false)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool ReplicationSetAddAllSequences([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName,
        string?[] schemaNames, bool synchronizeData)
        => ReplicationSetAddAllRelations(NameArg(setName), schemaNames, synchronizeData, 'S');

    /// <summary>
    /// Remove replication set / table mapping. Unlike the add function, this function does not care if table is
    /// valid or not, as we are just removing the record from repset.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replication_set_remove_table(set_name name, relation regclass)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool ReplicationSetRemoveTable([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation)
    {
        uint reloid = Oid(relation);

        WorfLocalNode node = CheckLocalNode(true);

        // Find the replication set.
        WorfRepSet repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, NameArg(setName), false)!;

        ReplicationSet.ReplicationSetRemoveTable(repset.Id, reloid, false);

        return true;
    }

    /// <summary>
    /// Remove replication set / sequence mapping.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replication_set_remove_sequence(set_name name, relation regclass)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool ReplicationSetRemoveSequence([PgSqlType("name", Schema = "pg_catalog")] PgDatum setName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation)
    {
        uint seqoid = Oid(relation);

        WorfLocalNode node = CheckLocalNode(true);

        // Find the replication set.
        WorfRepSet repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, NameArg(setName), false)!;

        ReplicationSet.ReplicationSetRemoveSeq(repset.Id, seqoid, false);

        return true;
    }

    /// <summary>
    /// Queues the input SQL for replication.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.replicate_ddl_command(command text, replication_sets text[] DEFAULT '{ddl_sql}')
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static unsafe bool ReplicateDdlCommand(string command, string?[] replicationSets)
    {
        WorfLocalNode node = CheckLocalNode(false);

        List<string> replicationSetList = Worf.TextArrayToList(replicationSets);

        // Validate replication sets.
        foreach (string setname in replicationSetList)
        {
            _ = ReplicationSet.GetReplicationSetByName(node.Node.Id, setname, false);
        }

        // Apply runs the command in its own transaction, so the command can neither end nor split the transaction.
        List* statements = NativeMethods.raw_parser(Worf.PStrDup(command), RawParseMode.RAW_PARSE_DEFAULT);
        for (int i = 0; statements is not null && i < statements->length; i++)
        {
            if (((RawStmt*)statements->elements[i].ptr_value)->stmt->type == NodeTag.T_TransactionStmt)
            {
                throw new PgException(PgSqlStates.InvalidTransactionTermination, "replicate_ddl_command cannot run transaction control statements");
            }
        }

        int saveNestlevel = NativeMethods.NewGUCNestLevel();

        // Force everything in the query to be fully qualified.
        fixed (byte* name = Worf.ToServerCString("search_path"))
        fixed (byte* value = Worf.ToServerCString(""))
        {
            _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)value, GucContext.PGC_USERSET, GucSource.PGC_S_SESSION,
                GucAction.GUC_ACTION_SAVE, true, 0, false);
        }

        // Convert the query to json string.
        StringBuilder cmd = new();
        Executor.EscapeJson(cmd, command);

        // Queue the query for replication.
        uint userId = NativeMethods.GetUserId();
        Queue.QueueMessage(replicationSetList, userId, Queue.QueueCommandTypeSql, cmd.ToString());

        // Execute the query locally, which the queued command replicates, with the indexes on this node alone out of its
        // way, as every node runs it, so that each chooses the same names and none copies or uses those indexes.
        string role = Worf.FromServer(NativeMethods.GetUserNameFromId(userId, false));
        DdlReplication.Suppressed = true;
        LocalIndex.Applying = true;
        try
        {
            Apply.ExecuteSqlCommand(command, role, false, applying: false);
        }
        finally
        {
            DdlReplication.Suppressed = false;
            LocalIndex.Applying = false;
        }

        // Restore the GUC variables we set above.
        NativeMethods.AtEOXact_GUC(true, saveNestlevel);

        return true;
    }

    /// <summary>
    /// Trigger which queues the TRUNCATE command. This function only writes to internal list, actual queueing is
    /// done when the TRUNCATE finishes.
    /// </summary>
    /// <param name="context">The trigger's context.</param>
    /// <returns>Null.</returns>
    [PgTrigger]
    [PgFunction(Sql = """
        CREATE OR REPLACE FUNCTION worf.queue_truncate()
        RETURNS trigger LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple? QueueTruncate(PgTriggerContext context)
    {
        const string Funcname = "queue_truncate";

        // Return if this function was called from apply process.
        if (Worker.MyWorfWorker >= 0)
        {
            return null;
        }

        // Make sure this is being called as an AFTER TRUNCTATE trigger.
        if (context.Timing != PgTriggerTiming.After || context.Operation != PgTriggerOperation.Truncate)
        {
            throw new PgException("39P01", $"function \"{Funcname}\" must be fired AFTER TRUNCATE");
        }

        // If this is not worf node, don't do anything.
        if (WorfNode.GetLocalNode(false, true) is null)
        {
            return null;
        }

        Executor.TruncatedTables.Add(context.RelationOid);

        return null;
    }

    /// <summary>
    /// Returns the replication info of a table for the given replication sets.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.show_repset_table_info(relation regclass, repsets text[], OUT relid oid, OUT nspname text,
        	OUT relname text, OUT att_list text[], OUT has_row_filter boolean)
        RETURNS record STRICT STABLE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static unsafe PgHeapTuple ShowRepsetTableInfo([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation,
        string?[] repsets)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;

        RelationData* rel = NativeMethods.table_open(Oid(relation), (int)PgLockMode.AccessShare);
        TupleDescData* reldesc = rel->rd_att;
        List<WorfRepSet> replicationSets = ReplicationSet.GetReplicationSets(node.Node.Id, Worf.TextArrayToList(repsets), false);

        string nspname = Worf.FromServer(NativeMethods.get_namespace_name(rel->rd_rel->relnamespace));
        string relname = Worf.FromServer((sbyte*)&rel->rd_rel->relname);

        // Build the replication info for the table.
        WorfTableRepInfo tableinfo = ReplicationSet.GetTableReplicationInfo(node.Node.Id, rel, replicationSets);

        // Build the column list.
        List<string> attList = [];
        for (int i = 0; i < reldesc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(reldesc, i);

            // Skip dropped columns.
            if (att->attisdropped)
            {
                continue;
            }

            // Skip filtered columns if any.
            if (tableinfo.AttList is not null && !tableinfo.AttList.Contains(att->attnum))
            {
                continue;
            }

            attList.Add(Worf.FromServer((sbyte*)&att->attname));
        }

        PgHeapTuple result = PgHeapTuple.Create(
            ("relid", SpiParameter.Create(rel->rd_id)),
            ("nspname", Worf.TextParameter(nspname)),
            ("relname", Worf.TextParameter(relname)),
            ("att_list", Worf.TextArrayParameter([.. attList])),
            ("has_row_filter", SpiParameter.Create(tableinfo.RowFilter.Count > 0)));

        NativeMethods.table_close(rel, (int)PgLockMode.None);

        return result;
    }

    /// <summary>
    /// Returns the type each column of a table's values are sent in, as RELATION messages name them, so that a
    /// subscriber's initial copy converts them as replication does.
    /// </summary>
    [PgFunction(SetMode = PgSetMode.Materialize, Sql = """
        CREATE FUNCTION worf.show_table_column_types(relation regclass, OUT attname text, OUT type_oid oid,
        	OUT type_nspname text, OUT type_name text, OUT type_definition text)
        RETURNS SETOF record STRICT STABLE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static unsafe IEnumerable<PgHeapTuple?> ShowTableColumnTypes(
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation)
    {
        RelationData* rel = NativeMethods.table_open(Oid(relation), (int)PgLockMode.AccessShare);
        TupleDescData* reldesc = rel->rd_att;
        List<PgHeapTuple?> rows = [];
        for (int i = 0; i < reldesc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(reldesc, i);
            if (att->attisdropped)
            {
                continue;
            }

            WorfRemoteType type = ProtoNative.ColumnType(att->atttypid);
            rows.Add(PgHeapTuple.Create(
                ("attname", Worf.TextParameter(Worf.FromServer((sbyte*)&att->attname))),
                ("type_oid", SpiParameter.Create(type.Oid)),
                ("type_nspname", Worf.TextParameter(type.Nspname)),
                ("type_name", Worf.TextParameter(type.Typname)),
                ("type_definition", Worf.TextParameter(type.Definition))));
        }

        NativeMethods.table_close(rel, (int)PgLockMode.None);
        return rows;
    }

    /// <summary>
    /// Do sequential table scan and return all rows that pass the row filter(s) defined in speficied replication
    /// set(s) for a table. This is called by downstream sync worker on the upstream to obtain filtered data for
    /// initial COPY.
    /// </summary>
    [PgFunction(SetMode = PgSetMode.Materialize, Sql = """
        CREATE FUNCTION worf.table_data_filtered(reltyp anyelement, relation regclass, repsets text[])
        RETURNS SETOF anyelement CALLED ON NULL INPUT STABLE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static unsafe IEnumerable<PgAnyElement?> TableDataFiltered(PgAnyElement? reltyp,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum? relation, string?[]? repsets, PgMemoryContext owner)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;

        // Validate parameter.
        if (relation is null || relation.IsNull)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "relation cannot be NULL");
        }

        if (repsets is null)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "repsets cannot be NULL");
        }

        if (reltyp is not null && !NativeMethods.type_is_rowtype(reltyp.TypeOid))
        {
            throw new PgException(PgSqlStates.DatatypeMismatch, $"first argument of {"worf_table_data_filtered"} must be a row type");
        }

        // Check output type and table row type are the same.
        RelationData* rel = NativeMethods.table_open(Oid(relation), (int)PgLockMode.AccessShare);
        TupleDescData* reltupdesc = rel->rd_att;
        if (reltyp is not null && reltyp.TypeOid != rel->rd_rel->reltype)
        {
            throw new PgException(PgSqlStates.DatatypeMismatch,
                $"return type of {"worf_table_data_filtered"} must be same as row type of the relation");
        }

        // Build the replication info for the table.
        List<WorfRepSet> replicationSets = ReplicationSet.GetReplicationSets(node.Node.Id, Worf.TextArrayToList(repsets), false);
        WorfTableRepInfo tableinfo = ReplicationSet.GetTableReplicationInfo(node.Node.Id, rel, replicationSets);

        // Prepare executor.
        EState* estate = Executor.CreateEstateForRelation(rel, false);
        ExprContext* econtext = Executor.PreparePerTupleEcontext(estate, reltupdesc);

        // Prepare the row filter expression. The filters are prepared, which inlines the SQL functions they call, and
        // run under the path the database and role set, as the WAL sender's are.
        int nestLevel = Worf.DefaultSearchPath();
        List<nint> rowFilterList = [];
        try
        {
            foreach (nint rowFilter in tableinfo.RowFilter)
            {
                rowFilterList.Add((nint)Executor.PrepareRowFilter((Node*)rowFilter));
            }
        }
        catch
        {
            Worf.EndDefaultSearchPath(nestLevel);
            throw;
        }

        // Scan the table.
        List<PgAnyElement?> rows = [];
#if ANKUS_PG19
        TableScanDescData* scandesc = NativeMethods.table_beginscan(rel, NativeMethods.GetActiveSnapshot(), 0, null, 0);
#else
        TableScanDescData* scandesc = NativeMethods.table_beginscan(rel, NativeMethods.GetActiveSnapshot(), 0, null);
#endif
        ulong* values = (ulong*)NativeMethods.palloc((ulong)(reltupdesc->natts * sizeof(ulong)));
        bool* nulls = (bool*)NativeMethods.palloc((ulong)reltupdesc->natts);

        try
        {
            HeapTupleData* htup;
            while ((htup = NativeMethods.heap_getnext(scandesc, ScanDirection.ForwardScanDirection)) is not null)
            {
                // Create a new version of our current HeapTuple. We can't just reuse the current one since it might
                // be possible that not every current attribute is correctly filled (e.g when atthasmissing is true).
                NativeMethods.heap_deform_tuple(htup, reltupdesc, values, nulls);
                HeapTupleData* newHtup = NativeMethods.heap_form_tuple(reltupdesc, values, nulls);

                if (!FilterTuple(newHtup, econtext, rowFilterList))
                {
                    continue;
                }

                MemoryContextData* old = NativeGlobals.CurrentMemoryContext;
                NativeGlobals.CurrentMemoryContext = (MemoryContextData*)owner.DangerousGetPointer();
                ulong datum = NativeMethods.heap_copy_tuple_as_datum(newHtup, reltupdesc);
                NativeGlobals.CurrentMemoryContext = old;

                rows.Add(new PgAnyElement(PgDatum.DangerousCreate((nuint)datum, rel->rd_rel->reltype, owner)));
            }
        }
        finally
        {
            Worf.EndDefaultSearchPath(nestLevel);
        }

        NativeMethods.pfree(values);
        NativeMethods.pfree(nulls);

        // Cleanup.
        NativeMethods.ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
        NativeMethods.FreeExecutorState(estate);

        NativeMethods.table_endscan(scandesc);
        NativeMethods.table_close(rel, (int)PgLockMode.None);

        return rows;
    }

    /// <summary>
    /// Wait for subscription and initial sync to complete.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.wait_for_subscription_sync_complete(subscription_name name)
        RETURNS void RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void WaitForSubscriptionSyncComplete([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName)
        => WaitForSyncComplete(NameArg(subscriptionName), null, null);

    /// <summary>
    /// Wait for sync to complete for a specific table.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.wait_for_table_sync_complete(subscription_name name, relation regclass)
        RETURNS void RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void WaitForTableSyncComplete([PgSqlType("name", Schema = "pg_catalog")] PgDatum subscriptionName,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum relation)
    {
        uint relid = Oid(relation);
        string relname;
        string relnamespace;
        unsafe
        {
            relname = Worf.FromServer(NativeMethods.get_rel_name(relid));
            relnamespace = Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(relid)));
        }

        WaitForSyncComplete(NameArg(subscriptionName), relnamespace, relname);
    }

    /// <summary>
    /// Like pg_xact_commit_timestamp but extended for replorigin too.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.xact_commit_timestamp_origin("xid" xid, OUT "timestamp" timestamptz, OUT "roident" oid)
        RETURNS record RETURNS NULL ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple XactCommitTimestampOrigin([PgSqlType("xid", Schema = "pg_catalog")] PgDatum xid)
    {
        long ts;
        ushort origin;
        bool found;
        unsafe
        {
            found = NativeMethods.TransactionIdGetCommitTsData((uint)xid.DangerousGetBits(), &ts, &origin);
        }

        return found
            ? PgHeapTuple.Create(
                ("timestamp", SpiParameter.Create(new PgTimestampTz(ts))),
                ("roident", SpiParameter.Create((uint)origin)))
            : PgHeapTuple.Create(
                ("timestamp", SpiParameter.Create<PgTimestampTz?>(null)),
                ("roident", SpiParameter.Create<uint?>(null)));
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
    /// <c>worf_&lt;subscriber database name&gt;_&lt;provider node name&gt;_&lt;subscription name&gt;</c>, at most 54
    /// bytes, which leaves room in a 63-byte name for a table sync's suffix of an underscore and 8 hex digits. The
    /// <c>worf_</c> prefix is a byte longer than pglogical's <c>pgl_</c>, so the database's name takes 15 bytes at
    /// most, rather than 16. A name that a slot's characters can't hold whole, as one with a capital or a hyphen, is
    /// written with an underscore for each such character and a hash of the whole name after, as a name too long is,
    /// so that names that differ, as those of a cluster's databases do, give slots and replication origins that differ.
    /// </summary>
    /// <param name="dbname">The subscriber's database.</param>
    /// <param name="providerNode">The provider node's name.</param>
    /// <param name="subscriptionName">The subscription's name.</param>
    /// <returns>The slot name.</returns>
    internal static string GenSlotName(string dbname, string providerNode, string subscriptionName)
    {
        List<byte> name = [.. "worf_"u8];
        name.AddRange(SlotNamePart(Worf.ToServer(dbname), 15));
        name.Add((byte)'_');
        name.AddRange(SlotNamePart(Worf.ToServer(providerNode), 16));
        name.Add((byte)'_');
        name.AddRange(SlotNamePart(Worf.ToServer(subscriptionName), 16));
        char[] slot = [.. name.Select(b => (char)b)];

        // A name that ends as a table sync's slot's does, an underscore and eight hex digits, would be taken for another
        // subscription's, whose cleanup drops it, so its last character becomes an underscore.
        if (slot.Length >= 9 && slot[^9] == '_' && Array.TrueForAll(slot[^8..], char.IsAsciiHexDigitLower))
        {
            slot[^1] = '_';
        }

        return new string(slot);
    }

    // Whether a transaction is this one, or one of its subtransactions.
    private static bool CreatedHere(uint xid)
    {
        unsafe
        {
            return NativeMethods.TransactionIdIsCurrentTransactionId(xid);
        }
    }

    /// <summary>
    /// Writes one of the names a slot's name is made of in the characters a slot's name holds, lowercase letters,
    /// digits and underscores, in at most a number of bytes: as it is, shortened with a hash when it's too long, or
    /// with an underscore for each other character and a hash of the whole name after. A name that's written as it
    /// is never ends as one with a hash does, in seven hex digits after at most the bytes a hash leaves before them;
    /// such a name is written with a hash of its own, so that it can't give the part another name's hash gives.
    /// </summary>
    private static byte[] SlotNamePart(byte[] part, int maxlen)
    {
        static bool Held(byte c) => c is (>= (byte)'a' and <= (byte)'z') or (>= (byte)'0' and <= (byte)'9') or (byte)'_';
        static bool Hex(byte c) => c is (>= (byte)'a' and <= (byte)'f') or (>= (byte)'0' and <= (byte)'9');
        bool hashedLike = part.Length >= 7 && part.Length <= maxlen - 1 && Array.TrueForAll(part[^7..], Hex);
        if (Array.TrueForAll(part, Held) && !hashedLike)
        {
            return Worf.ShortenHash(part, maxlen);
        }

        byte[] held = [.. part.Take(maxlen - 8).Select(c => Held(c) ? c : (byte)'_')];
        return [.. held, .. Encoding.ASCII.GetBytes(Worf.HashAny(part).ToString("x8", CultureInfo.InvariantCulture)[..7])];
    }

    /// <summary>
    /// Reads a <c>name</c> argument.
    /// </summary>
    /// <param name="value">The argument.</param>
    /// <returns>The name's text.</returns>
    internal static unsafe string NameArg(PgDatum? value) => Worf.FromServer((sbyte*)value!.DangerousGetBits());

    /// <summary>
    /// Checks that this database is a worf node.
    /// </summary>
    /// <param name="forUpdate">Whether to lock the local node.</param>
    /// <returns>The local node.</returns>
    internal static WorfLocalNode CheckLocalNode(bool forUpdate)
    {
        return WorfNode.GetLocalNode(forUpdate, true)
            ?? throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "current database is not configured as worf node",
                hint: "create worf node first");
    }

    private static string SyncStatusToString(char status) => status switch
    {
        Sync.SyncStatusInit => "sync_init",
        Sync.SyncStatusStructure => "sync_structure",
        Sync.SyncStatusData => "sync_data",
        Sync.SyncStatusConstraints => "sync_constraints",
        Sync.SyncStatusSyncwait => "sync_waiting",
        Sync.SyncStatusCatchup => "catchup",
        Sync.SyncStatusSyncdone => "synchronized",
        Sync.SyncStatusReady => "replicating",
        _ => "unknown",
    };

    private static uint Oid(PgDatum? value) => (uint)value!.DangerousGetBits();

    /// <summary>
    /// Rejects replicating the state of a sequence under range management, whose setval would put it into another
    /// node's block.
    /// </summary>
    private static void RejectManagedSequence(uint seqoid)
        => SequenceRanges.RejectReplicatedState(seqoid);

    /// <summary>
    /// Returns a boolean as the oid that pglogical's drop_subscription returns it as.
    /// </summary>
    private static PgDatum BoolDatum(bool value)
    {
        unsafe
        {
            return PgDatum.DangerousCreate(value ? 1u : 0u, (uint)PgBuiltInOid.OidOid, PgMemoryContext.Current);
        }
    }

    /// <summary>
    /// Parses a row filter into an expression over the table, as a CHECK constraint is parsed.
    /// </summary>
    private static unsafe Node* ParseRowFilter(RelationData* rel, string rowFilterStr)
    {
        string nspname = Worf.FromServer(NativeMethods.get_namespace_name(rel->rd_rel->relnamespace));
        string relname = Worf.FromServer((sbyte*)&rel->rd_rel->relname);

        // Build fake query which includes the expression so that we can pass it to the parser.
        string buf = $"SELECT {rowFilterStr} FROM {Worf.QuoteQualifiedIdentifier(nspname, relname)}";

        // Parse it, providing proper error context, without the position in the fake query.
        List* rawParsetreeList = null;
        using (PgErrorContext.Push(Worf.Shown($"invalid row_filter expression \"{rowFilterStr}\"")))
        {
            try
            {
                PgTransaction.RunInSubtransaction(() => rawParsetreeList = NativeMethods.pg_parse_query(Worf.PStrDup(buf)));
            }
            catch (PgException error)
            {
                throw new PgException(error.SqlState, error.Message, error.Detail, error.Hint);
            }
        }

        // Validate the output from the parser.
        if (rawParsetreeList is null || rawParsetreeList->length != 1)
        {
            throw InvalidRowFilter(rowFilterStr);
        }

        SelectStmt* stmt = (SelectStmt*)((RawStmt*)rawParsetreeList->elements[0].ptr_value)->stmt;
        if (stmt is null ||
            stmt->type != NodeTag.T_SelectStmt ||
            stmt->distinctClause is not null ||
            stmt->intoClause is not null ||
            stmt->whereClause is not null ||
            stmt->groupClause is not null ||
            stmt->havingClause is not null ||
            stmt->windowClause is not null ||
            stmt->valuesLists is not null ||
            stmt->sortClause is not null ||
            stmt->limitOffset is not null ||
            stmt->limitCount is not null ||
            stmt->lockingClause is not null ||
            stmt->withClause is not null ||
            stmt->op != SetOperation.SETOP_NONE)
        {
            throw InvalidRowFilter(rowFilterStr);
        }

        if (stmt->targetList is null || stmt->targetList->length != 1)
        {
            throw InvalidRowFilter(rowFilterStr);
        }

        ResTarget* restarget = (ResTarget*)stmt->targetList->elements[0].ptr_value;
        if (restarget is null ||
            restarget->type != NodeTag.T_ResTarget ||
            restarget->name is not null ||
            restarget->indirection is not null ||
            restarget->val is null)
        {
            throw InvalidRowFilter(rowFilterStr);
        }

        Node* rowFilter = restarget->val;

        // Create a dummy ParseState and insert the target relation as its sole rangetable entry. We need a
        // ParseState for transformExpr.
        ParseState* pstate = NativeMethods.make_parsestate(null);
        ParseNamespaceItem* nsitem = NativeMethods.addRangeTableEntryForRelation(pstate, rel, (int)PgLockMode.AccessShare,
            null, false, true);
        NativeMethods.addNSItemToQuery(pstate, nsitem, true, true, true);

        // Transform the expression and check it follows limits of row_filter which are same as those of CHECK
        // constraint so we can use the builtin checks for that.
        rowFilter = NativeMethods.transformExpr(pstate, rowFilter, ParseExprKind.EXPR_KIND_CHECK_CONSTRAINT);
        rowFilter = NativeMethods.coerce_to_boolean(pstate, rowFilter, Worf.PStrDup("row_filter"));
        NativeMethods.assign_expr_collations(pstate, rowFilter);
        if (pstate->p_rtable is null || pstate->p_rtable->length != 1)
        {
            throw new PgException(PgSqlStates.InvalidColumnReference, $"only table \"{relname}\" can be referenced in row_filter");
        }

        return rowFilter;
    }

    private static PgException InvalidRowFilter(string rowFilterStr)
        => new(PgSqlStates.SyntaxError, $"invalid row_filter expression \"{rowFilterStr}\"");

    /// <summary>
    /// Common function for adding replication set / relation mapping based on schemas.
    /// </summary>
    private static unsafe bool ReplicationSetAddAllRelations(string repsetName, string?[] nspNames, bool synchronize, char relkind)
    {
        WorfLocalNode node = CheckLocalNode(true);

        // Find the replication set.
        WorfRepSet repset = ReplicationSet.GetReplicationSetByName(node.Node.Id, repsetName, false)!;

        HashSet<uint> existingRelations = [.. ReplicationSet.ReplicationSetGetTables(repset.Id)];
        existingRelations.UnionWith(ReplicationSet.ReplicationSetGetSeqs(repset.Id));

        RelationData* rel = NativeMethods.table_open(Dependency.RelationRelationId, (int)PgLockMode.RowExclusive);

        foreach (string nspname in Worf.TextArrayToList(nspNames))
        {
            uint nspoid;
            fixed (byte* cnspname = Worf.ToServerCString(nspname))
            {
                nspoid = NativeMethods.LookupExplicitNamespace((sbyte*)cnspname, false);
            }

            // Only add logged relations which are not system relations (catalog, toast).
            WorfRows relations = Worf.Select(
                "SELECT c.oid, c.relname::text FROM pg_catalog.pg_class c WHERE c.relnamespace = $1 AND c.relkind = $2 " +
                "AND c.relpersistence = 'p' AND c.oid >= 16384 ORDER BY c.relname",
                SpiParameter.Create(nspoid), SpiParameter.Create((sbyte)relkind));

            foreach (WorfRow reltup in relations)
            {
                uint reloid = reltup.Get<uint>(0);
                if (existingRelations.Contains(reloid))
                {
                    continue;
                }

                // Each node's own blocks keep a managed sequence's values apart, so its state isn't replicated.
                if (relkind == 'S' && SequenceRanges.IsManaged(reloid))
                {
                    WorfLog.Notice($"sequence {Worf.QuoteQualifiedIdentifier(nspname, reltup.Get<string>(1))} is left out, since it is under sequence range management");
                    continue;
                }

                if (relkind == 'r')
                {
                    ReplicationSet.ReplicationSetAddTable(repset.Id, reloid, [], null);
                }
                else
                {
                    ReplicationSet.ReplicationSetAddSeq(repset.Id, reloid);
                }

                if (synchronize)
                {
                    string relname = reltup.Get<string>(1);
                    char cmdtype;

                    // It's easier to construct json manually than via Jsonb API...
                    StringBuilder json = new("{\"schema_name\": ");
                    Executor.EscapeJson(json, nspname);
                    if (relkind == 'r')
                    {
                        json.Append(",\"table_name\": ");
                        Executor.EscapeJson(json, relname);
                        cmdtype = Queue.QueueCommandTypeTablesync;
                    }
                    else
                    {
                        json.Append(",\"sequence_name\": ");
                        Executor.EscapeJson(json, relname);
                        json.Append(CultureInfo.InvariantCulture,
                            $",\"last_value\": \"{Sequences.SequenceGetLastValue(reloid)}\"");
                        cmdtype = Queue.QueueCommandTypeSequence;
                    }

                    json.Append('}');

                    // Queue the truncate for replication.
                    Queue.QueueMessage([repset.Name], NativeMethods.GetUserId(), cmdtype, json.ToString());
                }
            }
        }

        NativeMethods.table_close(rel, (int)PgLockMode.RowExclusive);

        return true;
    }

    /// <summary>
    /// Decide if to return tuple or not.
    /// </summary>
    private static unsafe bool FilterTuple(HeapTupleData* htup, ExprContext* econtext, List<nint> rowFilterList)
    {
        _ = NativeMethods.ExecStoreHeapTuple(htup, econtext->ecxt_scantuple, false);

        foreach (nint exprstate in rowFilterList)
        {
            ulong res = Executor.ExecEvalExpr((ExprState*)exprstate, econtext, out bool isnull);

            // NULL is same as false for our use.
            if (isnull)
            {
                return false;
            }

            if ((res & 0xFF) == 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Wait for subscription and initial sync to complete, or, if relation info is given, for sync to complete for a
    /// specific table.
    /// </summary>
    private static void WaitForSyncComplete(string subscriptionName, string? relnamespace, string? relname)
    {
        // If we wait in SERIALIZABLE, then the next snapshot after we return won't reflect the new state.
        int xactIsoLevel;
        unsafe
        {
            xactIsoLevel = NativeGlobals.XactIsoLevel;
        }

        if (xactIsoLevel >= 2)
        {
            throw new PgException(PgSqlStates.InternalError, "cannot wait for sync in REPEATABLE READ or SERIALIZABLE isolation");
        }

        WorfSubscription sub = WorfNode.GetSubscriptionByName(subscriptionName, false)!;

        while (true)
        {
            bool isdone;

            // We need to see the latest rows
            unsafe
            {
                NativeMethods.PushActiveSnapshot(NativeMethods.GetLatestSnapshot());
            }

            try
            {
                WorfSyncStatus? subsync = Sync.GetSubscriptionSyncStatus(sub.Id, true);
                isdone = subsync is not null && subsync.Status == Sync.SyncStatusReady;

                if (isdone)
                {
                    // Subscription itself is synced, but what about separately synced tables?
                    if (relname is not null)
                    {
                        // Use missing_ok=true because the sync status entry may not exist yet if the apply worker
                        // hasn't processed the TABLESYNC queue message. In that case, we continue waiting.
                        WorfSyncStatus? table = Sync.GetTableSyncStatus(sub.Id, relnamespace!, relname, true);
                        isdone = table is not null && table.Status == Sync.SyncStatusReady;
                    }
                    else
                    {
                        isdone = Sync.GetUnsyncedTables(sub.Id).Count == 0;
                    }
                }
            }
            finally
            {
                unsafe
                {
                    NativeMethods.PopActiveSnapshot();
                }
            }

            if (isdone)
            {
                // On Windows, add a small delay after sync completes to allow the streaming connection to be fully
                // established. Without this, pg_replication_slots may show the slot as inactive when checked
                // immediately after sync completes.
                if (OperatingSystem.IsWindows())
                {
                    unsafe
                    {
                        NativeMethods.pg_usleep(500000);  // 500ms
                    }
                }

                break;
            }

            PgInterrupts.Check();

            // some kind of backoff could be useful here
            Worker.WaitLatch(200);
        }
    }
}
