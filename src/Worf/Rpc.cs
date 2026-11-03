using System.Globalization;
using System.Text;

namespace Worf;

/// <summary>
/// Remote calls.
/// </summary>
internal static class Rpc
{
    /// <summary>
    /// Fetch list of tables that are grouped in specified replication sets.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="replicationSets">The replication sets.</param>
    /// <returns>The tables.</returns>
    internal static List<WorfRemoteRel> GetRemoteRepsetTables(WorfConnection conn, List<string> replicationSets)
    {
        string repsetarr = RepsetArray(replicationSets);
        string query = RemoteFunctionExists(conn, Worf.ExtensionName, "show_repset_table_info", 2, null)
            ? "SELECT i.relid, i.nspname, i.relname, i.att_list," +
                    "       i.has_row_filter" +
                    "  FROM (SELECT DISTINCT relid FROM worf.tables WHERE set_name = ANY(ARRAY[" + repsetarr + "])) t," +
                    "       LATERAL worf.show_repset_table_info(t.relid, ARRAY[" + repsetarr + "]) i"
            : "SELECT r.oid AS relid, t.nspname, t.relname, ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = r.oid AND NOT attisdropped AND attnum > 0) AS att_list," +
                    "       false AS has_row_filter" +
                    "  FROM worf.tables t, pg_catalog.pg_class r, pg_catalog.pg_namespace n" +
                    " WHERE t.set_name = ANY(ARRAY[" + repsetarr + "]) AND r.relname = t.relname AND n.oid = r.relnamespace AND n.nspname = t.nspname";

        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query(query, 5);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not get table list: ERROR:  {error.Message}\n");
        }

        List<WorfRemoteRel> tables = [];
        foreach (string?[] row in res)
        {
            tables.Add(RemoteRelFromRow(row));
        }

        return tables;
    }

    /// <summary>
    /// Like <see cref="GetRemoteRepsetTables"/> but for one table.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="schemaname">The table's schema.</param>
    /// <param name="relname">The table's name.</param>
    /// <param name="replicationSets">The replication sets.</param>
    /// <returns>The table.</returns>
    internal static WorfRemoteRel GetRemoteRepsetTable(WorfConnection conn, string schemaname, string relname,
        List<string> replicationSets)
    {
        string qualified = Worf.QuoteQualifiedIdentifier(schemaname, relname);
        string repsetarr = RepsetArray(replicationSets);
        string query = RemoteFunctionExists(conn, Worf.ExtensionName, "show_repset_table_info", 2, null)
            ? "SELECT i.relid, i.nspname, i.relname, i.att_list," +
                    "       i.has_row_filter" +
                    "  FROM worf.show_repset_table_info(" + Worf.QuoteLiteral(qualified) + "::regclass, ARRAY[" + repsetarr + "]) i"
            : "SELECT r.oid AS relid, t.nspname, t.relname, ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = r.oid AND NOT attisdropped AND attnum > 0) AS att_list," +
                    "       false AS has_row_filter" +
                    "  FROM worf.tables t, pg_catalog.pg_class r, pg_catalog.pg_namespace n" +
                    " WHERE r.oid = " + Worf.QuoteLiteral(qualified) + "::regclass AND t.set_name = ANY(ARRAY[" + repsetarr + "]) AND r.relname = t.relname AND n.oid = r.relnamespace AND n.nspname = t.nspname";

        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query(query, 5);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not get table list: ERROR:  {error.Message}\n");
        }

        if (res.Count != 1)
        {
            throw new PgException(PgSqlStates.InternalError, "could not get table list: ");
        }

        return RemoteRelFromRow(res[0]);
    }

    /// <summary>
    /// Gets the type each column of a remote table's values are sent in, as RELATION messages name them.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="remoterel">The table.</param>
    /// <returns>Each of its replicated columns' types, or null when the provider doesn't tell them.</returns>
    internal static WorfRemoteType?[]? GetRemoteColumnTypes(WorfConnection conn, WorfRemoteRel remoterel)
    {
        if (!RemoteFunctionExists(conn, Worf.ExtensionName, "show_table_column_types", 1, null))
        {
            return null;
        }

        string qualified = Worf.QuoteQualifiedIdentifier(remoterel.Nspname, remoterel.Relname);
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query("SELECT attname, type_oid, type_nspname, type_name, type_definition" +
                "  FROM worf.show_table_column_types(" + Worf.QuoteLiteral(qualified) + "::regclass)", 5);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not get column types: ERROR:  {error.Message}\n");
        }

        Dictionary<string, WorfRemoteType> types = [];
        foreach (string?[] row in res)
        {
            types[row[0]!] = new WorfRemoteType(Atooid(row[1]), row[2], row[3], row[4]);
        }

        WorfRemoteType?[] columns = new WorfRemoteType?[remoterel.Attnames.Count];
        for (int i = 0; i < columns.Length; i++)
        {
            columns[i] = types.TryGetValue(remoterel.Attnames[i], out WorfRemoteType type) ? type : null;
        }

        return columns;
    }

    /// <summary>
    /// Gets how the provider's session formats money, as <see cref="OutputConfig.MoneyFormat"/> shows it, and keeps
    /// the session's lc_monetary, in the same statement, so that a reload of the provider's configuration, which takes
    /// effect between statements, can't change how the copies on the connection format money afterwards.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <returns>The text.</returns>
    internal static string GetRemoteMoneyFormat(WorfConnection conn)
    {
        try
        {
            return conn.Query("""
                SELECT '-1234567'::pg_catalog.money::pg_catalog.text || ' = ' || '-1234567'::pg_catalog.money::pg_catalog.numeric::pg_catalog.text
                FROM pg_catalog.set_config('lc_monetary', pg_catalog.current_setting('lc_monetary'), false)
                """, 1)[0][0]!;
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not get how the provider formats money: ERROR:  {error.Message}\n");
        }
    }

    /// <summary>
    /// Is the remote slot active?.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    /// <returns>Whether a process is streaming from the slot.</returns>
    internal static bool RemoteSlotActive(WorfConnection conn, string slotName)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query("SELECT plugin, active " +
                "FROM pg_catalog.pg_replication_slots " +
                "WHERE slot_name = " + Worf.QuoteLiteral(slotName), 2);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "getting remote slot info failed",
                detail: $"SELECT FROM pg_catalog.pg_replication_slots failed with: ERROR:  {error.Message}\n");
        }

        // Slot not found return false
        if (res.Count == 0)
        {
            return false;
        }

        // Slot found, validate that it's worf slot
        if (res[0][0] is null)
        {
            throw new PgException(PgSqlStates.InternalError, "Unexpectedly null field plugin");
        }

        if (res[0][0] != Worf.ExtensionName)
        {
            throw new PgException(PgSqlStates.InternalError, $"slot {slotName} is not worf slot");
        }

        return res[0][1] == "t";
    }

    /// <summary>
    /// Checks whether a remote replication slot can stream: it exists, and PostgreSQL hasn't invalidated it, as for
    /// <c>max_slot_wal_keep_size</c>, which every version from 15 shows as a lost WAL status.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    /// <returns>Why it can't stream, or null when it can.</returns>
    internal static string? RemoteSlotLost(WorfConnection conn, string slotName)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query("SELECT wal_status FROM pg_catalog.pg_replication_slots WHERE slot_name = " + Worf.QuoteLiteral(slotName), 1);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "getting remote slot info failed",
                detail: $"SELECT FROM pg_catalog.pg_replication_slots failed with: ERROR:  {error.Message}\n");
        }

        return res.Count == 0 ? $"Replication slot \"{slotName}\" does not exist on the provider."
            : res[0][0] == "lost" ? $"Replication slot \"{slotName}\" on the provider was invalidated."
            : null;
    }

    /// <summary>
    /// Gets the process that holds a remote replication slot.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    /// <returns>The process's id, or zero when nothing holds the slot.</returns>
    internal static int RemoteSlotHolder(WorfConnection conn, string slotName)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query("SELECT active_pid FROM pg_catalog.pg_replication_slots " +
                "WHERE slot_name = " + Worf.QuoteLiteral(slotName) + " AND active", 1);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "getting remote slot info failed",
                detail: $"SELECT FROM pg_catalog.pg_replication_slots failed with: ERROR:  {error.Message}\n");
        }

        return res.Count == 0 || res[0][0] is null ? 0 : int.Parse(res[0][0]!, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Stops the process that holds a remote replication slot when it's a walsender of an earlier connection with this
    /// one's user and an application name, as one whose connection has closed still is while it decodes a transaction.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    /// <param name="holder">The process that held the slot.</param>
    /// <param name="applicationName">The application name of the connections whose walsenders are stopped.</param>
    /// <returns>Why the process wasn't stopped, or null when it was.</returns>
    internal static string? TerminateSlotHolder(WorfConnection conn, string slotName, int holder, string applicationName)
    {
        try
        {
            IReadOnlyList<string?[]> res = conn.Query($"""
                SELECT pg_catalog.pg_terminate_backend(a.pid)
                FROM pg_catalog.pg_stat_activity a
                    JOIN pg_catalog.pg_replication_slots s ON s.active_pid = a.pid
                WHERE s.slot_name = {Worf.QuoteLiteral(slotName)} AND a.pid = {holder.ToString(CultureInfo.InvariantCulture)}
                    AND a.pid <> pg_catalog.pg_backend_pid() AND a.backend_type = 'walsender' AND a.usename = session_user
                    AND a.application_name = {Worf.QuoteLiteral(applicationName)}
                """, 1);
            return res.Count == 0 ? "It isn't a walsender of this table sync's earlier connections, which the sync stops."
                : res[0][0] == "t" ? null
                : "It couldn't be signaled.";
        }
        catch (PgRemoteException error)
        {
            return $"Stopping it failed: {error.Message}";
        }
    }

    /// <summary>
    /// Lists which of the slots a subscription's table syncs create are in the provider's database. The names are those
    /// of the subscription's tables, rather than any that look like one, which another subscription's own slot may.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="names">The names of the slots the subscription's table syncs create.</param>
    /// <returns>Each slot's name, and whether a process holds it.</returns>
    internal static List<(string Name, bool Active)> RemoteSyncSlots(WorfConnection conn, IReadOnlyCollection<string> names)
    {
        if (names.Count == 0)
        {
            return [];
        }

        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query($"""
                SELECT slot_name, active FROM pg_catalog.pg_replication_slots
                WHERE database = pg_catalog.current_database() AND plugin = {Worf.QuoteLiteral(Worf.ExtensionName)}
                    AND slot_name OPERATOR(pg_catalog.=) ANY (ARRAY[{string.Join(", ", names.Select(Worf.QuoteLiteral))}]::pg_catalog.name[])
                """, 2);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "getting remote slot info failed",
                detail: $"SELECT FROM pg_catalog.pg_replication_slots failed with: ERROR:  {error.Message}\n");
        }

        return [.. res.Select(row => (row[0]!, row[1] == "t"))];
    }

    /// <summary>
    /// Drops replication slot on remote node that has been used by the local node.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    internal static void DropRemoteSlot(WorfConnection conn, string slotName)
    {
        IReadOnlyList<string?[]> res;

        // Check if the slot exists
        try
        {
            res = conn.Query("SELECT plugin " +
                "FROM pg_catalog.pg_replication_slots " +
                "WHERE slot_name = " + Worf.QuoteLiteral(slotName), 1);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "getting remote slot info failed",
                detail: $"SELECT FROM pg_catalog.pg_replication_slots failed with: ERROR:  {error.Message}\n");
        }

        // Slot not found return false
        if (res.Count == 0)
        {
            return;
        }

        // Slot found, validate that it's worf slot
        if (res[0][0] is null)
        {
            throw new PgException(PgSqlStates.InternalError, "Unexpectedly null field plugin");
        }

        if (res[0][0] != Worf.ExtensionName)
        {
            throw new PgException(PgSqlStates.InternalError, $"slot {slotName} is not worf slot");
        }

        // And finally, drop the slot.
        try
        {
            _ = conn.Query("SELECT pg_drop_replication_slot(" + Worf.QuoteLiteral(slotName) + ")", 1);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, "remote slot drop failed",
                detail: $"SELECT pg_drop_replication_slot() failed with: ERROR:  {error.Message}\n");
        }
    }

    /// <summary>
    /// Fetches the remote node's identity.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <returns>The node's id, name, system identifier, database and replication sets.</returns>
    internal static (uint NodeId, string NodeName, string Sysid, string Dbname, string ReplicationSets) RemoteNodeInfo(
        WorfConnection conn)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query("SELECT node_id, node_name, sysid, dbname, replication_sets FROM worf.worf_node_info()", 5);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not fetch remote node info: ERROR:  {error.Message}\n\n");
        }

        // No nodes found?
        if (res.Count == 0)
        {
            throw new PgException(PgSqlStates.InternalError, "the remote database is not configured as a worf node.\n");
        }

        if (res.Count > 1)
        {
            throw new PgException(PgSqlStates.InternalError,
                "the remote database has multiple nodes configured. That is not supported with current version of worf.\n");
        }

        string?[] row = res[0];
        return (Atooid(row[0]), row[1] ?? "", row[2] ?? "", row[3] ?? "", row[4] ?? "");
    }

    /// <summary>
    /// Fetches whether the remote node subscribes to a node, and the remote node's conflict resolution.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="nodeId">The node.</param>
    /// <returns>Whether the remote node subscribes to the node, and its <c>worf.conflict_resolution</c>, or null when
    /// its session doesn't have the setting.</returns>
    internal static (bool Subscribes, string? ConflictResolution) RemoteSubscribesTo(WorfConnection conn, uint nodeId)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            string?[] privileges = conn.Query("""
                SELECT pg_catalog.has_table_privilege('pg_catalog.pg_file_settings', 'SELECT'),
                    pg_catalog.has_function_privilege('pg_catalog.pg_read_file(text, bigint, bigint, boolean)', 'EXECUTE')
                """, 2)[0];
            res = conn.Query($"SELECT EXISTS (SELECT 1 FROM worf.subscription WHERE sub_origin = {nodeId.ToString(CultureInfo.InvariantCulture)}), ({Conflict.WorkerResolutionSql(privileges[0] == "t", privileges[1] == "t")})", 2);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not fetch remote subscriptions: ERROR:  {error.Message}\n\n");
        }

        return (res[0][0] == "t", res[0][1]);
    }

    /// <summary>
    /// Checks whether the remote node replicates a sequence of a name in any of the replication sets.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="nspname">The sequence's schema.</param>
    /// <param name="relname">The sequence's name.</param>
    /// <param name="replicationSets">The replication sets.</param>
    /// <returns>Whether one of the sets has the sequence.</returns>
    internal static bool RemoteReplicatesSequence(WorfConnection conn, string nspname, string relname, List<string> replicationSets)
    {
        string query = "SELECT 1" +
            "  FROM worf.replication_set_seq q, worf.replication_set s, worf.local_node l, pg_catalog.pg_class c, pg_catalog.pg_namespace n" +
            " WHERE s.set_id = q.set_id AND s.set_nodeid = l.node_id AND c.oid = q.set_seqoid AND n.oid = c.relnamespace" +
            "   AND n.nspname = " + Worf.QuoteLiteral(nspname) + " AND c.relname = " + Worf.QuoteLiteral(relname) +
            "   AND s.set_name = ANY(ARRAY[" + RepsetArray(replicationSets) + "]::name[])";

        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query(query, 1);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not fetch remote sequence info: ERROR:  {error.Message}\n\n");
        }

        return res.Count > 0;
    }

    /// <summary>
    /// Checks whether a function exists on the remote node.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="nspname">The function's schema.</param>
    /// <param name="proname">The function's name.</param>
    /// <param name="nargs">The function's number of arguments, or -1 for any.</param>
    /// <param name="argname">An argument name the function must have, or null.</param>
    /// <returns>Whether the function exists.</returns>
    internal static bool RemoteFunctionExists(WorfConnection conn, string nspname, string proname, int nargs, string? argname)
    {
        StringBuilder query = new();
        query.Append("SELECT oid " +
            "  FROM pg_catalog.pg_proc " +
            " WHERE proname = ").Append(Worf.QuoteLiteral(proname)).Append(
            "   AND pronamespace = " +
            "       (SELECT oid " +
            "          FROM pg_catalog.pg_namespace " +
            "         WHERE nspname = ").Append(Worf.QuoteLiteral(nspname)).Append(')');

        if (nargs >= 0)
        {
            query.Append(CultureInfo.InvariantCulture, $"   AND pronargs = '{nargs}'");
        }

        if (argname is not null)
        {
            query.Append("   AND ").Append(Worf.QuoteLiteral(argname)).Append(" = ANY (proargnames)");
        }

        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query(query.ToString(), 1);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not fetch remote function info: ERROR:  {error.Message}\n\n");
        }

        return res.Count > 0;
    }

    /// <summary>
    /// Parses an oid as <c>strtoul</c> does.
    /// </summary>
    /// <param name="value">The text.</param>
    /// <returns>The oid.</returns>
    internal static uint Atooid(string? value)
    {
        int end = 0;
        while (value is not null && end < value.Length && char.IsAsciiDigit(value[end]))
        {
            end++;
        }

        return end == 0 || !uint.TryParse(value.AsSpan(0, end), NumberStyles.None, CultureInfo.InvariantCulture, out uint oid) ? 0 : oid;
    }

    private static string RepsetArray(List<string> replicationSets)
    {
        StringBuilder repsetarr = new();
        bool first = true;
        foreach (string repsetName in replicationSets)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                repsetarr.Append(',');
            }

            repsetarr.Append(Worf.QuoteLiteral(repsetName));
        }

        return repsetarr.ToString();
    }

    private static WorfRemoteRel RemoteRelFromRow(string?[] row)
    {
        WorfRemoteRel remoterel = new()
        {
            Relid = Atooid(row[0]),
            Nspname = row[1] ?? "",
            Relname = row[2] ?? "",
        };
        if (row[3] is null || !Worf.ParsePGArray(row[3]!, out List<string> attnames))
        {
            throw new PgException(PgSqlStates.InternalError, "could not parse column list for table");
        }

        remoterel.Attnames = attnames;
        remoterel.HasRowFilter = row[4] == "t";
        return remoterel;
    }
}
