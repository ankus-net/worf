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
    internal static List<WorfRemoteRel> GetRemoteRepsetTables(PgReplicationConnection conn, List<string> replicationSets)
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
    internal static WorfRemoteRel GetRemoteRepsetTable(PgReplicationConnection conn, string schemaname, string relname,
        List<string> replicationSets)
    {
        string qualified = Spi.QuoteQualifiedIdentifier(schemaname, relname);
        string repsetarr = RepsetArray(replicationSets);
        string query = RemoteFunctionExists(conn, Worf.ExtensionName, "show_repset_table_info", 2, null)
            ? "SELECT i.relid, i.nspname, i.relname, i.att_list," +
                    "       i.has_row_filter" +
                    "  FROM worf.show_repset_table_info(" + Spi.QuoteLiteral(qualified) + "::regclass, ARRAY[" + repsetarr + "]) i"
            : "SELECT r.oid AS relid, t.nspname, t.relname, ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = r.oid AND NOT attisdropped AND attnum > 0) AS att_list," +
                    "       false AS has_row_filter" +
                    "  FROM worf.tables t, pg_catalog.pg_class r, pg_catalog.pg_namespace n" +
                    " WHERE r.oid = " + Spi.QuoteLiteral(qualified) + "::regclass AND t.set_name = ANY(ARRAY[" + repsetarr + "]) AND r.relname = t.relname AND n.oid = r.relnamespace AND n.nspname = t.nspname";

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
    /// Is the remote slot active?.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    /// <returns>Whether a process is streaming from the slot.</returns>
    internal static bool RemoteSlotActive(PgReplicationConnection conn, string slotName)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query("SELECT plugin, active " +
                "FROM pg_catalog.pg_replication_slots " +
                "WHERE slot_name = " + Spi.QuoteLiteral(slotName), 2);
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
    /// Drops replication slot on remote node that has been used by the local node.
    /// </summary>
    /// <param name="conn">The provider connection, inside a local transaction.</param>
    /// <param name="slotName">The slot.</param>
    internal static void DropRemoteSlot(PgReplicationConnection conn, string slotName)
    {
        IReadOnlyList<string?[]> res;

        // Check if the slot exists
        try
        {
            res = conn.Query("SELECT plugin " +
                "FROM pg_catalog.pg_replication_slots " +
                "WHERE slot_name = " + Spi.QuoteLiteral(slotName), 1);
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
            _ = conn.Query("SELECT pg_drop_replication_slot(" + Spi.QuoteLiteral(slotName) + ")", 1);
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
        PgReplicationConnection conn)
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
    internal static (bool Subscribes, string? ConflictResolution) RemoteSubscribesTo(PgReplicationConnection conn, uint nodeId)
    {
        IReadOnlyList<string?[]> res;
        try
        {
            res = conn.Query($"SELECT EXISTS (SELECT 1 FROM worf.subscription WHERE sub_origin = {nodeId.ToString(CultureInfo.InvariantCulture)}), ({Conflict.WorkerResolutionSql})", 2);
        }
        catch (PgRemoteException error)
        {
            throw new PgException(PgSqlStates.InternalError, $"could not fetch remote subscriptions: ERROR:  {error.Message}\n\n");
        }

        return (res[0][0] == "t", res[0][1]);
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
    internal static bool RemoteFunctionExists(PgReplicationConnection conn, string nspname, string proname, int nargs, string? argname)
    {
        StringBuilder query = new();
        query.Append("SELECT oid " +
            "  FROM pg_catalog.pg_proc " +
            " WHERE proname = ").Append(Spi.QuoteLiteral(proname)).Append(
            "   AND pronamespace = " +
            "       (SELECT oid " +
            "          FROM pg_catalog.pg_namespace " +
            "         WHERE nspname = ").Append(Spi.QuoteLiteral(nspname)).Append(')');

        if (nargs >= 0)
        {
            query.Append(CultureInfo.InvariantCulture, $"   AND pronargs = '{nargs}'");
        }

        if (argname is not null)
        {
            query.Append("   AND ").Append(Spi.QuoteLiteral(argname)).Append(" = ANY (proargnames)");
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

            repsetarr.Append(Spi.QuoteLiteral(repsetName));
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
