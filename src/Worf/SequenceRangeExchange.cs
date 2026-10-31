using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The conversation over each link, an enabled subscription, through which nodes learn of managed sequences, report
/// how far their values reach, request blocks from their parent, and grant and acknowledge blocks. The subscriber end
/// calls <c>worf.sequence_range_exchange</c> on the provider end, which answers with the same from its side.
/// </summary>
internal static class SequenceRangeExchange
{
    // A longer route counts as none, so stale routes stop counting upward once an owner disappears.
    private const int MaxDistance = 32;

    // Seconds from the Unix epoch to PostgreSQL's, 2000-01-01.
    private const double UnixToPostgres = 946_684_800;

    // How far a neighbor's WAL position moves before the tables record it, a default WAL segment.
    private const ulong LsnRecordedEvery = 16 * 1024 * 1024;

    // When each subscription's provider is next tried after a failed exchange, and the delay before that.
    private static readonly Dictionary<uint, (long At, long Delay)> s_retries = [];

    // When each link last exchanged, by subscription, and the links with acknowledgements to send.
    private static readonly Dictionary<uint, long> s_exchangedAt = [];
    private static readonly HashSet<uint> s_acksWaiting = [];
    private static long s_requested;

    /// <summary>
    /// Answers a neighbor's exchange on the provider end of its subscription.
    /// </summary>
    /// <param name="message">The neighbor's message.</param>
    /// <returns>This node's message to the neighbor.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_exchange(message text)
        RETURNS text STRICT VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.sequence_range_exchange(text) FROM public;
        """)]
    internal static string Exchange(string message)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;
        using JsonDocument document = JsonDocument.Parse(message);
        JsonElement root = document.RootElement;
        uint senderId = root.GetProperty("node_id").GetUInt32();
        Authenticate(root.GetProperty("slot").GetString() ?? "", senderId);

        // Backends reload their blocks, which an activation or a grant may have changed, once this commits.
        _ = Process(node, root, senderId, root.GetProperty("node_name").GetString() ?? "", true);
        SequenceRanges.ChangedOnCommit();
        (string reply, List<Guid> refusals) = Build(node, senderId, null);
        AcknowledgeRefusals(refusals);
        return reply;
    }

    /// <summary>
    /// Ties a call to the subscription whose slot it names. The slot must be worf's, in this database, and streamed by
    /// the calling role, or, while nothing streams it, bound to that role. The first call through a slot binds it to the
    /// node that makes it and to the role that streams it, until the slot is dropped. A node speaks through the slots of
    /// one role, and a role, unless it is a superuser, for one node, so that one subscriber's role can't speak for another
    /// node: a provider tells its subscribers apart by their roles alone.
    /// </summary>
    private static void Authenticate(string slot, uint senderId)
    {
        uint role;
        unsafe
        {
            role = NativeMethods.GetSessionUserId();
        }

        SpiParameter name = SpiParameter.Create(slot);
        SpiResult slots = Worf.SelectCatalog("""
            SELECT s.active_pid IS NOT NULL, a.usesysid
            FROM pg_catalog.pg_replication_slots s LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid = s.active_pid
            WHERE s.slot_name = $1 AND s.plugin = 'worf' AND s.database = pg_catalog.current_database()
            """, name);
        if (slots.Count == 0)
        {
            throw new PgException("42501", $"no worf replication slot \"{slot}\" carries this sequence range exchange");
        }

        bool active = slots[0].Get<bool>(0);
        SpiResult link = Worf.SelectCatalog("SELECT node_id, role_id FROM worf.sequence_range_link WHERE slot_name = $1", name);
        if (active ? slots[0].Get<uint?>(1) != role : link.Count == 0 || link[0].Get<uint>(1) != role)
        {
            throw new PgException("42501", active ? $"replication slot \"{slot}\" is streamed by another role than this sequence range exchange's"
                : link.Count == 0 ? $"replication slot \"{slot}\" isn't streamed yet"
                : $"replication slot \"{slot}\" is bound to another role than this sequence range exchange's");
        }

        if (link.Count > 0)
        {
            if (link[0].Get<uint>(0) != senderId)
            {
                throw new PgException("42501", $"replication slot \"{slot}\" belongs to another node than this sequence range exchange's");
            }

            // The subscriber's connection string names another role now.
            if (link[0].Get<uint>(1) != role)
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_link SET role_id = $2 WHERE slot_name = $1", name, SpiParameter.Create(role));
            }

            return;
        }

        // A dropped slot frees its name for the next subscription that takes it.
        if (Worf.ReadScalar<bool>("""
            SELECT EXISTS (SELECT 1 FROM worf.sequence_range_link l WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = l.slot_name))
            """))
        {
            _ = Spi.Execute("""
                DELETE FROM worf.sequence_range_link l
                WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = l.slot_name)
                """);
        }

        bool superuser;
        unsafe
        {
            superuser = NativeMethods.superuser_arg(role);
        }

        SpiResult bound = Worf.SelectCatalog("""
            SELECT node_id, role_id, pg_catalog.pg_get_userbyid(role_id)::text FROM worf.sequence_range_link WHERE node_id = $1 OR role_id = $2
            """, SpiParameter.Create(senderId), SpiParameter.Create(role));
        for (int i = 0; i < bound.Count; i++)
        {
            if (bound[i].Get<uint>(0) == senderId && bound[i].Get<uint>(1) != role)
            {
                throw new PgException("42501", $"node {senderId} speaks through the slots of role \"{bound[i].Get<string>(2)}\" here, not this sequence range exchange's role");
            }

            if (bound[i].Get<uint>(0) != senderId && !superuser)
            {
                throw new PgException("42501", $"role \"{bound[i].Get<string>(2)}\" already speaks for node {bound[i].Get<uint>(0)} here",
                    hint: "Give each subscriber node a role of its own in its provider connection strings.");
            }
        }

        _ = Spi.Execute("INSERT INTO worf.sequence_range_link VALUES ($1, $2, $3)", name, SpiParameter.Create(senderId), SpiParameter.Create(role));
    }

    /// <summary>
    /// Exchanges with the provider of each enabled subscription whose turn has come, or with which something is
    /// waiting: a request, a grant or an acknowledgement. Each call is a transaction of its own, committed before the
    /// next call, so that a block this node grants or receives is never sent on, or acknowledged, before it is
    /// committed, and no row this node locks waits on another node.
    /// </summary>
    /// <returns>Whether this node's blocks changed.</returns>
    internal static bool ExchangeAll()
    {
        // A backend that answered an exchange learned of a node or sequence the neighbors should hear of.
        long requested = SequenceRanges.ExchangeRequested.Value;
        if (requested != s_requested)
        {
            s_requested = requested;
            s_exchangedAt.Clear();
        }

        (WorfLocalNode Node, List<WorfSubscription> Due)? links = PgBackgroundWorker.RunTransaction(DueLinks);
        if (links is not (WorfLocalNode node, List<WorfSubscription> due))
        {
            return false;
        }

        bool changed = false;
        foreach (WorfSubscription sub in due)
        {
            long now = Worker.GetCurrentTimestamp();
            try
            {
                changed |= ExchangeWith(node, sub);
                s_exchangedAt[sub.Id] = now;
                _ = s_retries.Remove(sub.Id);
            }
            catch (Exception error) when (error is PgException or PgRemoteException or PgQueryCanceledException)
            {
                // A failed exchange is tried again after a delay that doubles, up to the exchange interval. A call the
                // watcher cancelled has run out of time.
                long delay = s_retries.TryGetValue(sub.Id, out (long At, long Delay) retry) ? retry.Delay * 2 : 0;
                delay = Math.Min(Math.Max(delay, SequenceRanges.CheckInterval * 1000L), SequenceRanges.ExchangeInterval * 1000L);
                s_retries[sub.Id] = (now + delay, delay);
                string failure = error is PgQueryCanceledException
                    ? $"node {sub.Origin.Name} didn't answer within {SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)} ms"
                    : error.Message;
                PgLog.Warning($"sequence range exchange with node {sub.Origin.Name} failed: {failure}");
            }
        }

        return changed;
    }

    /// <summary>
    /// Lists the links whose turn has come.
    /// </summary>
    private static (WorfLocalNode Node, List<WorfSubscription> Due)? DueLinks()
    {
        if (WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
        {
            return null;
        }

        List<WorfSubscription> due = [];
        long now = Worker.GetCurrentTimestamp();
        foreach (WorfSubscription sub in WorfNode.GetNodeSubscriptions(node.Node.Id, false).Where(sub => sub.Enabled))
        {
            bool retrying = s_retries.TryGetValue(sub.Id, out (long At, long Delay) retry) && now < retry.At;
            if (!retrying && (!s_exchangedAt.TryGetValue(sub.Id, out long at) || now - at >= SequenceRanges.ExchangeInterval * 1000L ||
                s_acksWaiting.Contains(sub.Id) || Waiting(sub.Origin.Id)))
            {
                due.Add(sub);
            }
        }

        return (node, due);
    }

    /// <summary>
    /// Exchanges with a subscription's provider. The exchange worker's watcher cancels a connection or call that takes
    /// longer than <c>worf.sequence_range_timeout</c>, and the provider ends a call that runs that long. Each waits in
    /// a transaction, which the cancellation aborts, so that the worker goes on.
    /// </summary>
    private static bool ExchangeWith(WorfLocalNode node, WorfSubscription sub)
    {
        RequireReachable(sub.OriginIf.Dsn, sub.Origin.Name);
        using PgReplicationConnection conn = PgBackgroundWorker.RunTransaction(() => SequenceRangeExchangeWorker.Bounded(() =>
        {
            PgReplicationConnection opened = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "range");
            try
            {
                opened.Execute($"SET statement_timeout = {SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)}");
                return opened;
            }
            catch
            {
                opened.Dispose();
                throw;
            }
        }));
        if (!PgBackgroundWorker.RunTransaction(() => SequenceRangeExchangeWorker.Bounded(() => Rpc.RemoteFunctionExists(conn, "worf", "sequence_range_exchange", 1, null))))
        {
            return false;
        }

        bool changed = false;

        // A grant this node makes from the provider's request goes back in a second call, after this node commits it.
        for (int call = 0; call < 2; call++)
        {
            (bool granted, bool received) = PgBackgroundWorker.RunTransaction(() => Call(node, sub, conn));
            changed |= received;
            if (received)
            {
                _ = s_acksWaiting.Add(sub.Id);
            }
            else if (call == 0)
            {
                _ = s_acksWaiting.Remove(sub.Id);
            }

            if (!granted)
            {
                break;
            }
        }

        return changed;
    }

    /// <summary>
    /// Waits, no longer than <c>worf.sequence_range_timeout</c>, for a host the provider's connection string names to
    /// accept a connection. libpq keeps no timeout of its own for the connection this backend waits for on its latch, so
    /// a host that never answers would hold the worker, and the exchanges with every other provider, until the
    /// operating system gives up. A provider reached over a local socket needs no check.
    /// </summary>
    private static void RequireReachable(string dsn, string nodeName)
    {
        if (Fe.ConninfoParse(dsn, out _) is not Dictionary<string, string> options)
        {
            return;
        }

        string[] hosts = (options.GetValueOrDefault("hostaddr") is { Length: > 0 } addresses ? addresses : options.GetValueOrDefault("host") ?? "")
            .Split(',');
        if (hosts.Any(host => host.Length == 0 || host[0] is '/' or '@'))
        {
            return;
        }

        string[] ports = (options.GetValueOrDefault("port") is { Length: > 0 } port ? port
            : Environment.GetEnvironmentVariable("PGPORT") is { Length: > 0 } environment ? environment : "5432").Split(',');
        long deadline = Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 1000L);
        for (int i = 0; i < hosts.Length; i++)
        {
            if (int.TryParse(ports[ports.Length == 1 ? 0 : Math.Min(i, ports.Length - 1)], NumberStyles.None, CultureInfo.InvariantCulture, out int number) &&
                Reaches(hosts[i].Trim(), number, deadline))
            {
                return;
            }
        }

        throw new PgException(PgSqlStates.ConnectionFailure,
            $"could not reach node {nodeName} within {SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)} ms");
    }

    /// <summary>
    /// Checks whether a host accepts a TCP connection on a port before a deadline.
    /// </summary>
    private static bool Reaches(string host, int port, long deadline)
    {
        IPAddress[] addresses;
        try
        {
            addresses = IPAddress.TryParse(host, out IPAddress? address) ? [address] : Dns.GetHostAddresses(host);
        }
        catch (SocketException)
        {
            return false;
        }

        foreach (IPAddress address in addresses)
        {
            using Socket socket = new(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { Blocking = false };
            bool pending;
            try
            {
                socket.Connect(address, port);
                return true;
            }
            catch (SocketException error)
            {
                pending = error.SocketErrorCode is SocketError.WouldBlock or SocketError.InProgress;
            }

            while (pending && Worker.GetCurrentTimestamp() < deadline)
            {
                PgInterrupts.Check();
                if (socket.Poll(100_000, SelectMode.SelectWrite))
                {
                    if (socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error) is 0)
                    {
                        return true;
                    }

                    pending = false;
                }
                else if (socket.Poll(0, SelectMode.SelectError))
                {
                    pending = false;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Makes one call: sends this node's message and takes in the reply, in the caller's transaction. The message only
    /// reads, so that this node holds no row lock while the provider works, which may wait on that provider's own
    /// calls to other nodes.
    /// </summary>
    private static (bool Granted, bool Received) Call(WorfLocalNode node, WorfSubscription sub, PgReplicationConnection conn)
    {
        // This node's calls carry its WAL position in order.
        _ = Worf.Read("SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('worf.sequence_range_exchange'))", 0);
        (string message, List<Guid> refusals) = Build(node, sub.Origin.Id, sub.SlotName);
        string reply = SequenceRangeExchangeWorker.Bounded(() => conn.Query("SELECT worf.sequence_range_exchange(" + Spi.QuoteLiteral(message) + ")", 1))[0][0] ?? "{}";
        AcknowledgeRefusals(refusals);
        using JsonDocument document = JsonDocument.Parse(reply);
        return Process(node, document.RootElement, sub.Origin.Id, sub.Origin.Name, false);
    }

    /// <summary>
    /// Records refusals as delivered: a refusal needs no acknowledgement, since a request that lost it is sent again
    /// and refused again.
    /// </summary>
    private static void AcknowledgeRefusals(List<Guid> refusals)
    {
        if (refusals.Count > 0)
        {
            _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = ANY ($1) AND state = 'sent'",
                SpiParameter.Create(refusals.ToArray()));
        }
    }

    /// <summary>
    /// Checks whether this node has a request open with a neighbor, a grant the neighbor hasn't acknowledged, or a
    /// reset to ask of it or acknowledge, or whether either is a fenced allocator, which waits on its neighbors' reports.
    /// </summary>
    private static bool Waiting(uint neighbor)
        => Worf.SelectCatalog("""
            SELECT 1 FROM worf.sequence_range_request WHERE parent_node = $1 AND state = 'open'
            UNION ALL
            SELECT 1 FROM worf.sequence_range_grant WHERE grantee_id = $1 AND state = 'sent'
            UNION ALL
            SELECT 1 FROM worf.sequence_range_node WHERE node_id = $1 AND (state = 'fenced' OR reset_lsn IS NOT NULL)
            UNION ALL
            SELECT 1 FROM worf.sequence_range WHERE fenced_at IS NOT NULL
            LIMIT 1
            """, SpiParameter.Create(neighbor)).Count > 0;

    /// <summary>
    /// Builds this node's message to a neighbor, with its sequences in the order of their names, which each node
    /// processes them in, so that two transactions on a node lock their rows in the same order.
    /// </summary>
    /// <returns>The message, and the refusals it carries.</returns>
    private static (string Message, List<Guid> Refusals) Build(WorfLocalNode node, uint neighbor, string? slot)
    {
        List<Guid> refusals = [];
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
        {
            ulong sysid;
            uint timeline;
            ulong insert;
            unsafe
            {
                sysid = NativeMethods.GetSystemIdentifier();
                timeline = NativeMethods.GetWALInsertionTimeLine();
                insert = NativeMethods.GetXLogInsertRecPtr();
            }

            writer.WriteStartObject();
            writer.WriteNumber("node_id", node.Node.Id);
            writer.WriteString("node_name", node.Node.Name);
            writer.WriteString("sysid", sysid.ToString(CultureInfo.InvariantCulture));
            writer.WriteNumber("timeline", timeline);
            writer.WriteString("wal_lsn", Worf.FormatLsn(insert));
            if (slot is not null)
            {
                writer.WriteString("slot", slot);
            }

            writer.WriteStartArray("sequences");
            SpiResult sequences = Worf.SelectCatalog("""
                SELECT r.seqoid::oid, pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname), r.owner_node,
                    r.epoch, r.range_size, r.max_range_size, r.distance, r.high_water, r.watermark, r.parent_node,
                    CASE WHEN r.fenced_at IS NOT NULL THEN 'fenced' ELSE r.state END, r.root_start, r.root_end
                FROM worf.sequence_range r
                    JOIN pg_catalog.pg_class c ON c.oid = r.seqoid
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                ORDER BY (pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)) COLLATE "C"
                """);
            for (int i = 0; i < sequences.Count; i++)
            {
                WriteSequence(writer, sequences[i], neighbor, refusals);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return (Encoding.UTF8.GetString(stream.ToArray()), refusals);
    }

    /// <summary>
    /// Writes one managed sequence's part of this node's message to a neighbor.
    /// </summary>
    private static void WriteSequence(Utf8JsonWriter writer, SpiRow row, uint neighbor, List<Guid> refusals)
    {
        uint seqoid = row.Get<uint>(0);
        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter peer = SpiParameter.Create(neighbor);
        writer.WriteStartObject();
        writer.WriteString("name", row.Get<string>(1));
        writer.WriteNumber("owner_id", row.Get<uint>(2));
        writer.WriteNumber("epoch", row.Get<int>(3));
        writer.WriteNumber("range_size", row.Get<long>(4));
        writer.WriteNumber("max_range_size", row.Get<long>(5));
        WriteNullable(writer, "distance", row.Get<int?>(6));
        WriteNullable(writer, "high_water", row.Get<long?>(7));
        WriteNullable(writer, "watermark", row.Get<long?>(8));
        writer.WriteString("state", row.Get<string>(10));
        WriteNullable(writer, "root_start", row.Get<long?>(11));
        WriteNullable(writer, "root_end", row.Get<long?>(12));
        bool snowflake = seqoid == SnowflakeIds.NodeSequence();
        if (snowflake)
        {
            WriteHolds(writer, OwnHolds(seqoid), SnowflakeIds.ReservedClock());
        }


        // A reset this node asks of the neighbor, whose WAL position moved backwards, and one it honored.
        SpiResult neighborRow = Worf.SelectCatalog(
            "SELECT reset_lsn::text, reset_done::text, state = 'fenced' FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2",
            seq, peer);
        bool neighborFenced = neighborRow.Count > 0 && neighborRow[0].Get<bool?>(2) == true;
        WriteNullable(writer, "reset", neighborRow.Count > 0 ? neighborRow[0].Get<string?>(0) : null);
        WriteNullable(writer, "reset_done", neighborRow.Count > 0 ? neighborRow[0].Get<string?>(1) : null);

        // The blocks this node holds from the neighbor: every one while the neighbor is a fenced allocator, so
        // that it can rebuild its ledger, and otherwise those abandoned lately, which it marks so.
        writer.WriteBoolean("held_complete", neighborFenced);
        WriteArray(writer, "held", Worf.SelectCatalog("""
            SELECT block_id, kind::text, lo, hi, state = 'abandoned' FROM worf.sequence_range_block
            WHERE seqoid = $1 AND from_node = $2 AND kind IN ('n', 'p', 'i')
                AND ($3 OR state = 'abandoned' AND retired_at > now() - interval '10 minutes')
            """, seq, peer, SpiParameter.Create(neighborFenced)), held =>
        {
            writer.WriteStartObject();
            writer.WriteString("id", held.Get<Guid>(0));
            writer.WriteString("kind", held.Get<string>(1));
            writer.WriteNumber("lo", held.Get<long>(2));
            writer.WriteNumber("hi", held.Get<long>(3));
            writer.WriteBoolean("abandoned", held.Get<bool>(4));
            writer.WriteEndObject();
        });

        // The open requests, for a node block or a pool block, when the neighbor is this node's parent.
        WriteArray(writer, "requests", Worf.SelectCatalog(
            "SELECT request_id, size, kind::text, lo, hi FROM worf.sequence_range_request WHERE seqoid = $1 AND parent_node = $2 AND state = 'open' AND $3",
            seq, peer, SpiParameter.Create(row.Get<uint?>(9) == neighbor)), request =>
        {
            writer.WriteStartObject();
            writer.WriteString("id", request.Get<Guid>(0));
            writer.WriteNumber("size", request.Get<long>(1));
            writer.WriteString("kind", request.Get<string>(2));
            WriteNullable(writer, "lo", request.Get<long?>(3));
            WriteNullable(writer, "hi", request.Get<long?>(4));
            writer.WriteEndObject();
        });

        // Every node this node has heard from, so that reports reach the owner hop by hop, each with when its node
        // made it, so that a node that stopped reporting ages out everywhere rather than being relayed back and forth.
        WriteArray(writer, "reports", Worf.SelectCatalog("""
            SELECT n.node_id, n.node_name::text, n.high_water, n.watermark, n.epoch,
                CASE WHEN n.node_id <> l.node_id THEN n.state WHEN r.fenced_at IS NOT NULL THEN 'fenced' ELSE r.state END,
                extract(epoch FROM CASE WHEN n.node_id = l.node_id THEN now() ELSE worf.sequence_range_heard(n.node_id, false, n.reported_at, false) END)::float8,
                n.node_id = l.node_id, n.holds, n.clock
            FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid CROSS JOIN worf.local_node l
            WHERE n.seqoid = $1 AND n.node_id <> $2
            """, seq, peer), report =>
        {
            writer.WriteStartObject();
            writer.WriteNumber("node_id", report.Get<uint>(0));
            writer.WriteString("node_name", report.Get<string>(1));
            WriteNullable(writer, "high_water", report.Get<long?>(2));
            WriteNullable(writer, "watermark", report.Get<long?>(3));
            WriteNullable(writer, "epoch", report.Get<int?>(4));
            WriteNullable(writer, "state", report.Get<string?>(5));
            writer.WriteNumber("reported_at", report.Get<double>(6));
            if (snowflake)
            {
                WriteHolds(writer, report.Get<bool>(7) ? OwnHolds(seqoid) : report.Get<long[]?>(8) ?? [],
                    report.Get<bool>(7) ? SnowflakeIds.ReservedClock() : report.Get<long?>(9));
            }

            writer.WriteEndObject();
        });

        // Grants for the neighbor that it hasn't acknowledged, and acknowledgements of its grants.
        bool refused = false;
        WriteArray(writer, "grants", Worf.SelectCatalog(
            "SELECT grant_id, request_id, lo, hi, kind::text, not_before FROM worf.sequence_range_grant WHERE seqoid = $1 AND grantee_id = $2 AND state = 'sent'",
            seq, peer), grant =>
        {
            writer.WriteStartObject();
            writer.WriteString("id", grant.Get<Guid>(0));
            writer.WriteString("request_id", grant.Get<Guid>(1));
            writer.WriteNumber("lo", grant.Get<long>(2));
            writer.WriteNumber("hi", grant.Get<long>(3));
            writer.WriteString("kind", grant.Get<string>(4));
            WriteNullable(writer, "not_before", grant.Get<long?>(5));
            writer.WriteEndObject();
            if (grant.Get<string>(4) == "x")
            {
                refusals.Add(grant.Get<Guid>(0));
            }
        });

        WriteArray(writer, "acks", Worf.SelectCatalog(
            "SELECT block_id FROM worf.sequence_range_block WHERE seqoid = $1 AND from_node = $2 AND kind IN ('n', 'p', 'i') AND state IN ('next', 'active', 'open', 'imported')",
            seq, peer), received => writer.WriteStringValue(received.Get<Guid>(0)));
        writer.WriteEndObject();
    }

    private static void WriteArray(Utf8JsonWriter writer, string name, SpiResult rows, Action<SpiRow> write)
    {
        writer.WriteStartArray(name);
        for (int i = 0; i < rows.Count; i++)
        {
            write(rows[i]);
        }

        writer.WriteEndArray();
    }

    /// <summary>
    /// Takes in a neighbor's message, each sequence in a subtransaction of its own, so that one sequence's failure,
    /// such as a root with no values left, doesn't hold up the others.
    /// </summary>
    /// <param name="node">This node.</param>
    /// <param name="root">The message.</param>
    /// <param name="senderId">The neighbor's node id.</param>
    /// <param name="senderName">The neighbor's name.</param>
    /// <param name="call">Whether the neighbor called this node, rather than answered its call.</param>
    /// <returns>Whether this node granted the neighbor a block, and whether it received one.</returns>
    private static (bool Granted, bool Received) Process(WorfLocalNode node, JsonElement root, uint senderId, string senderName, bool call)
    {
        bool granted = false;
        bool received = false;
        bool learned = false;

        // The neighbor is heard from now, in shared memory, which an exchange that changes nothing else is all it writes to.
        SequenceRangeHeard.Record(senderId, true, Worker.GetCurrentTimestamp());
        if (!root.TryGetProperty("sequences", out JsonElement sequences))
        {
            return (false, false);
        }

        foreach (JsonElement entry in sequences.EnumerateArray())
        {
            try
            {
                if (!Unchanged(node, root, entry, senderId, call))
                {
                    PgTransaction.RunInSubtransaction(() =>
                    {
                        (bool g, bool r, bool l) = ProcessSequence(node, root, entry, senderId, senderName, call);
                        granted |= g;
                        received |= r;
                        learned |= l;
                    });
                }

                // The reports the neighbor relays, each as of when its node made it, once taken in.
                if (entry.TryGetProperty("reports", out JsonElement reports))
                {
                    uint localId = node.Node.Id;
                    foreach ((uint reported, long at) in reports.EnumerateArray()
                        .Select(report => (Node: report.GetProperty("node_id").GetUInt32(), At: ReportedAt(report)))
                        .Where(report => report.At is not null && report.Node != localId && report.Node != senderId)
                        .Select(report => (report.Node, report.At!.Value)))
                    {
                        SequenceRangeHeard.Record(reported, false, Math.Min(at, Worker.GetCurrentTimestamp()));
                    }
                }
            }
            catch (PgException error)
            {
                PgLog.Warning($"sequence range exchange with node {senderName} skipped sequence {entry.GetProperty("name").GetString()}: {error.Message}");
            }
        }

        // The neighbor's WAL position, once compared with the last one.
        if (Worf.TryParseLsn(root.GetProperty("wal_lsn").GetString() ?? "", out ulong lsn))
        {
            SequenceRangeHeard.RecordLsn(senderId, ulong.Parse(root.GetProperty("sysid").GetString() ?? "0", CultureInfo.InvariantCulture),
                root.GetProperty("timeline").GetInt64(), call, lsn);
        }

        // Neighbors hear of what this node learned at their next exchange, which happens now, from the exchange worker the
        // manager starts if none is running.
        if (learned)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () =>
            {
                _ = PgAtomic.Add(SequenceRanges.ExchangeRequested, 1L);
                Worker.WakeManager();
            });
        }

        return (granted, received);
    }

    /// <summary>
    /// Checks, only reading, whether one sequence of a neighbor's message would change nothing here: the neighbor reports
    /// what it reported before, relays nothing new, asks for, grants and gives up nothing, acknowledges nothing new, and
    /// leaves this node's route as it is, while neither node is fenced or collecting. Such a message is only noted in
    /// shared memory, so that exchanges between idle nodes write nothing and use no transaction IDs.
    /// </summary>
    private static bool Unchanged(WorfLocalNode node, JsonElement root, JsonElement entry, uint senderId, bool call)
    {
        if (Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(entry.GetProperty("name").GetString())) is not uint seqoid)
        {
            return true;
        }

        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter sender = SpiParameter.Create(senderId);
        SpiResult local = Worf.Read("""
            SELECT epoch, state, fenced_at IS NOT NULL, root_start, watermark, owner_node, parent_node, distance FROM worf.sequence_range WHERE seqoid = $1
            """, 1, seq);
        int epoch = entry.GetProperty("epoch").GetInt32();
        string? state = Text(entry, "state");
        if (local.Count == 0)
        {
            return state == "disabled";
        }

        if (local[0].Get<int>(0) != epoch)
        {
            return local[0].Get<int>(0) > epoch;
        }

        if (local[0].Get<string>(1) == "collecting" || local[0].Get<bool>(2) || state == "fenced" || (state == "disabled" && local[0].Get<string>(1) != "disabled") ||
            entry.GetProperty("requests").GetArrayLength() > 0 || entry.GetProperty("grants").GetArrayLength() > 0)
        {
            return false;
        }

        SequenceInfo info = SequenceRanges.GetInfo(seqoid);
        bool covers(long? have, long? value) => value is not long v || (have is long h && (info.Ascending ? h >= v : h <= v));
        if (!covers(local[0].Get<long?>(3), Nullable(entry, "root_start")) || !covers(local[0].Get<long?>(4), Nullable(entry, "watermark")))
        {
            return false;
        }

        // The neighbor's own report, the resets each node asked of the other, and its WAL position at this end of the
        // conversation: not behind the last one, nor a WAL segment past the one the tables recorded.
        string sysid = root.GetProperty("sysid").GetString() ?? "";
        long timeline = root.GetProperty("timeline").GetInt64();
        SpiResult own = Worf.Read("""
            SELECT CASE WHEN $6 THEN wal_lsn ELSE reply_lsn END::text
            FROM worf.sequence_range_node
            WHERE seqoid = $1 AND node_id = $2 AND via_node = node_id AND node_name = $3 AND sysid = $4::numeric AND timeline = $5
                AND epoch IS NOT DISTINCT FROM $7 AND state IS NOT DISTINCT FROM $8 AND distance IS NOT DISTINCT FROM $9
                AND high_water IS NOT DISTINCT FROM $10 AND watermark IS NOT DISTINCT FROM $11 AND holds IS NOT DISTINCT FROM $12
                AND clock IS NOT DISTINCT FROM $13 AND ($14::pg_lsn IS NULL OR reset_done = $14::pg_lsn)
                AND ($15::pg_lsn IS NULL OR reset_lsn IS DISTINCT FROM $15::pg_lsn)
            """, 1, seq, sender, SpiParameter.Create(root.GetProperty("node_name").GetString()), SpiParameter.Create(sysid),
            SpiParameter.Create(timeline), SpiParameter.Create(call), SpiParameter.Create(epoch), SpiParameter.Create(state),
            SpiParameter.Create(Nullable32(entry, "distance")), SpiParameter.Create(Nullable(entry, "high_water")),
            SpiParameter.Create(Nullable(entry, "watermark")), SpiParameter.Create(Holds(entry)), SpiParameter.Create(Nullable(entry, "clock")),
            SpiParameter.Create(Text(entry, "reset")), SpiParameter.Create(Text(entry, "reset_done")));
        if (own.Count == 0 || !Worf.TryParseLsn(own[0].Get<string?>(0) ?? "", out ulong recorded) ||
            !Worf.TryParseLsn(root.GetProperty("wal_lsn").GetString() ?? "", out ulong position) ||
            position < Math.Max(recorded, SequenceRangeHeard.LatestLsn(senderId, ulong.Parse(sysid, CultureInfo.InvariantCulture), timeline, call)) ||
            position - recorded >= LsnRecordedEvery)
        {
            return false;
        }

        // Each relayed report is older than what this node has, or relayed instead of a live report, or says the same.
        if (entry.TryGetProperty("reports", out JsonElement reports))
        {
            foreach (JsonElement report in reports.EnumerateArray())
            {
                uint reported = report.GetProperty("node_id").GetUInt32();
                if (reported != node.Node.Id && reported != senderId && !Worf.ReadScalar<bool>("""
                    SELECT CASE
                        WHEN least(coalesce(pg_catalog.to_timestamp($3), now()), now()) <= worf.sequence_range_heard(n.node_id, false, n.reported_at, false) THEN true
                        WHEN n.via_node = n.node_id THEN worf.sequence_range_heard(n.node_id, true, n.reported_at, false) >= now() - $4::interval
                        ELSE n.node_name = $5 AND n.high_water IS NOT DISTINCT FROM $6 AND n.watermark IS NOT DISTINCT FROM $7
                            AND n.epoch IS NOT DISTINCT FROM $8 AND n.state IS NOT DISTINCT FROM $9 AND n.holds IS NOT DISTINCT FROM $10
                            AND n.clock IS NOT DISTINCT FROM $11 END
                    FROM worf.sequence_range_node n WHERE n.seqoid = $1 AND n.node_id = $2
                    """, seq, SpiParameter.Create(reported),
                    SpiParameter.Create(report.TryGetProperty("reported_at", out JsonElement at) && at.ValueKind == JsonValueKind.Number ? at.GetDouble() : (double?)null),
                    SpiParameter.Create(SequenceRanges.StaleAfter()), SpiParameter.Create(report.GetProperty("node_name").GetString()),
                    SpiParameter.Create(Nullable(report, "high_water")), SpiParameter.Create(Nullable(report, "watermark")),
                    SpiParameter.Create(Nullable32(report, "epoch")), SpiParameter.Create(Text(report, "state")), SpiParameter.Create(Holds(report)),
                    SpiParameter.Create(Nullable(report, "clock"))))
                {
                    return false;
                }
            }
        }

        // Blocks the neighbor gave up that this node granted, and grants it acknowledges or this node has no record of.
        Guid[] abandoned = [.. entry.GetProperty("held").EnumerateArray().Where(block => block.GetProperty("abandoned").GetBoolean())
            .Select(block => block.GetProperty("id").GetGuid())];
        Guid[] acks = [.. entry.GetProperty("acks").EnumerateArray().Select(ack => ack.GetGuid())];
        if (Worf.ReadScalar<bool>("""
            SELECT EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE grant_id = ANY ($1) AND state <> 'abandoned')
                OR EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE grant_id = ANY ($2) AND grantee_id = $3 AND state = 'sent')
                OR EXISTS (SELECT 1 FROM unnest($2::uuid[]) a(id) WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant g WHERE g.grant_id = a.id))
            """, SpiParameter.Create(abandoned), SpiParameter.Create(acks), sender))
        {
            return false;
        }

        // The route stays as it is.
        if (local[0].Get<uint>(5) == node.Node.Id)
        {
            return true;
        }

        (uint? parent, int? distance) = Nearest(seqoid, node);
        string routed = parent is null && local[0].Get<string>(1) is "quarantined" or "no_route" ? "no_route"
            : local[0].Get<string>(1) == "no_route" ? "quarantined" : local[0].Get<string>(1);
        return parent == local[0].Get<uint?>(6) && distance == local[0].Get<int?>(7) && routed == local[0].Get<string>(1) &&
            (parent is null || !Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = $1 AND parent_node <> $2)",
                seq, SpiParameter.Create(parent.Value)));
    }

    /// <summary>
    /// Takes in one sequence of a neighbor's message: joins it if it exists here, records the neighbor's reports,
    /// answers its requests when this node is its parent, records grants for this node and the neighbor's
    /// acknowledgements.
    /// </summary>
    /// <returns>Whether this node granted the neighbor a block, received one, and learned of a node or sequence.</returns>
    private static (bool Granted, bool Received, bool Learned) ProcessSequence(WorfLocalNode node, JsonElement root, JsonElement entry,
        uint senderId, string senderName, bool call)
    {
        bool granted = false;
        bool received = false;
        bool learned = false;
        uint? found = Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(entry.GetProperty("name").GetString()));
        if (found is not uint seqoid)
        {
            return (false, false, false);
        }

        // The range row is locked first, as every change to the sequence's ranges does.
        int epoch = entry.GetProperty("epoch").GetInt32();
        uint owner = entry.GetProperty("owner_id").GetUInt32();
        SpiResult local = Spi.Query("SELECT owner_node, epoch, state FROM worf.sequence_range WHERE seqoid = $1 FOR NO KEY UPDATE", false, 1,
            SpiParameter.Create(seqoid));
        if (local.Count == 0)
        {
            if (entry.GetProperty("state").GetString() == "disabled")
            {
                return (false, false, false);
            }

            SequenceRanges.JoinFromNeighbor(seqoid, owner, epoch, entry.GetProperty("range_size").GetInt64(),
                entry.GetProperty("max_range_size").GetInt64(), Nullable(entry, "root_start"), Nullable(entry, "root_end"), node);
        }
        else if (local[0].Get<int>(1) > epoch)
        {
            // Grants from an older epoch than this node knows are ignored.
            return (false, false, false);
        }
        else if (local[0].Get<int>(1) < epoch)
        {
            SequenceRanges.TakeEpoch(seqoid, owner, epoch, node);
            learned = true;
        }
        else if (entry.GetProperty("state").GetString() == "disabled" && local[0].Get<string>(2) != "disabled")
        {
            // The owner disabled the epoch, and the word spreads from node to node.
            SequenceRanges.Disable(seqoid);
            learned = true;
        }

        SequenceInfo info = SequenceRanges.GetInfo(seqoid);
        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter sender = SpiParameter.Create(senderId);
        learned |= local.Count == 0 || Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2", seq, sender).Count == 0;
        long? high = Nullable(entry, "high_water");
        long? watermark = Nullable(entry, "watermark");
        string sysid = root.GetProperty("sysid").GetString() ?? "";
        long timeline = root.GetProperty("timeline").GetInt64();
        string walLsn = root.GetProperty("wal_lsn").GetString() ?? "0/0";

        // The root's start, once the owner has started it, reaches every node, so that each allocator refuses a node
        // that joins with values reaching into it.
        if (Nullable(entry, "root_start") is long rootStart)
        {
            _ = Spi.Execute($"UPDATE worf.sequence_range SET root_start = {(info.Ascending ? "greatest" : "least")}(coalesce(root_start, $2), $2) WHERE seqoid = $1 AND epoch = $3",
                seq, SpiParameter.Create(rootStart), SpiParameter.Create(epoch));
        }

        // A neighbor whose WAL position moved backwards on the same timeline was rolled back, by a restored backup or
        // snapshot, and may issue values it issued before, or grant blocks it granted before. It must reset. The
        // positions in its calls, which it makes one at a time, are compared with each other, and those in its answers
        // to this node's calls, which this node makes one at a time, with each other: an answer may pass a call the
        // neighbor sent before it. The neighbor's row is locked first, so that the position compared is the one this
        // report replaces, and none written in between is lost.
        // The position compared is the later of the one the tables recorded and the one shared memory holds, which the
        // tables record only once it moves by a WAL segment.
        _ = Spi.Execute("SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2 FOR UPDATE", seq, sender);
        SpiResult stored = Worf.Read(
            "SELECT CASE WHEN $5 THEN wal_lsn ELSE reply_lsn END::text FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2 AND sysid = $3::numeric AND timeline = $4",
            1, seq, sender, SpiParameter.Create(sysid), SpiParameter.Create(timeline), SpiParameter.Create(call));
        ulong latest = stored.Count > 0 ? Math.Max(Worf.TryParseLsn(stored[0].Get<string?>(0) ?? "", out ulong recorded) ? recorded : 0,
            SequenceRangeHeard.LatestLsn(senderId, ulong.Parse(sysid, CultureInfo.InvariantCulture), timeline, call)) : 0;
        string? rolledBack = null;
        if (Worf.TryParseLsn(walLsn, out ulong position) && latest > position)
        {
            rolledBack = Worf.FormatLsn(latest);
            _ = Spi.Execute("UPDATE worf.sequence_range_node SET reset_lsn = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2", seq, sender,
                SpiParameter.Create(rolledBack));
        }
        // An allocator that grants values this node already holds lost the grant it made them in, as when it went back too
        // little for its WAL position to show it, and must reset too. A fenced allocator is taking its lost grants back
        // into its ledger.
        string? lost = rolledBack is not null || entry.GetProperty("state").GetString() == "fenced" ? null : Regrants(seqoid, entry);
        if (lost is not null)
        {
            rolledBack = walLsn;
            _ = Spi.Execute("UPDATE worf.sequence_range_node SET reset_lsn = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2 AND reset_lsn IS NULL", seq,
                sender, SpiParameter.Create(walLsn));
            PgLog.Warning($"sequence {info.Name}: node {senderName} {lost}, so it must reset its blocks");
        }
        else if (rolledBack is not null)
        {
            PgLog.Warning($"sequence {info.Name}: node {senderName}'s WAL position moved back from {rolledBack} to {walLsn}, so it must reset its blocks");
        }

        // Until it resets, a neighbor that went back may answer from a ledger that lost grants, and ask again for blocks
        // it already had, so this message's grants are left alone, its requests unanswered, and this node's requests to
        // it are made again later, under new ids.
        bool wentBack = rolledBack is not null;
        if (wentBack)
        {
            _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND parent_node = $2", seq, sender);
        }

        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, sysid, timeline, wal_lsn, reply_lsn, epoch, state, distance,
                high_water, watermark, reported_at, holds, clock)
            VALUES ($1, $2, $3, $2, $4::numeric, $5, CASE WHEN $12 THEN $6::pg_lsn END, CASE WHEN NOT $12 THEN $6::pg_lsn END, $7, $8, $9, $10, $11, now(),
                $13, $14)
            ON CONFLICT (seqoid, node_id) DO UPDATE SET node_name = excluded.node_name, via_node = excluded.via_node,
                sysid = excluded.sysid, timeline = excluded.timeline,
                wal_lsn = CASE WHEN $12 THEN excluded.wal_lsn ELSE sequence_range_node.wal_lsn END,
                reply_lsn = CASE WHEN NOT $12 THEN excluded.reply_lsn ELSE sequence_range_node.reply_lsn END, epoch = excluded.epoch,
                state = excluded.state, distance = excluded.distance, high_water = excluded.high_water, watermark = excluded.watermark,
                reported_at = excluded.reported_at, holds = excluded.holds, clock = excluded.clock
            """, seq, sender, SpiParameter.Create(senderName),
            SpiParameter.Create(sysid), SpiParameter.Create(timeline), SpiParameter.Create(walLsn), SpiParameter.Create(epoch),
            SpiParameter.Create(entry.GetProperty("state").GetString()), SpiParameter.Create(Nullable32(entry, "distance")),
            SpiParameter.Create(high), SpiParameter.Create(watermark), SpiParameter.Create(call), SpiParameter.Create(Holds(entry)),
            SpiParameter.Create(Nullable(entry, "clock")));

        // The neighbor asks this node to reset, once per rollback it saw, or has reset as this node asked.
        if (Text(entry, "reset") is string reset && Spi.Execute(
            "UPDATE worf.sequence_range_node SET reset_done = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2 AND reset_done IS DISTINCT FROM $3::pg_lsn",
            seq, sender, SpiParameter.Create(reset)) > 0)
        {
            PgLog.Warning($"sequence {info.Name}: node {senderName} saw this node's WAL position move backwards, so it abandons its blocks and requests new ones");
            SequenceRanges.Reset(seqoid, node, false);
            received = true;
        }

        if (Text(entry, "reset_done") is string done)
        {
            _ = Spi.Execute("UPDATE worf.sequence_range_node SET reset_lsn = NULL WHERE seqoid = $1 AND node_id = $2 AND reset_lsn = $3::pg_lsn",
                seq, sender, SpiParameter.Create(done));
        }

        // A watermark that rises here goes back to the neighbors at once, so that an owner that granted itself a block hears
        // that its neighbors know of it, even one that only answers calls.
        if (watermark is long mark)
        {
            string raised = info.Ascending ? "greatest" : "least";
            learned |= Spi.Execute($"""
                UPDATE worf.sequence_range SET watermark = {raised}(coalesce(watermark, $2), $2)
                WHERE seqoid = $1 AND watermark IS DISTINCT FROM {raised}(coalesce(watermark, $2), $2)
                """, seq, SpiParameter.Create(mark)) > 0;
        }

        // Reports of other nodes, relayed toward the owner, each as old as its node made it. A fresher report replaces a
        // relayed one, and the neighbor's own report replaces one it relayed only once that has gone stale.
        if (entry.TryGetProperty("reports", out JsonElement reports))
        {
            foreach (JsonElement report in reports.EnumerateArray())
            {
                // The neighbor's own entry, above, is its live report.
                uint reported = report.GetProperty("node_id").GetUInt32();
                if (reported == node.Node.Id || reported == senderId)
                {
                    continue;
                }

                learned |= Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2",
                    seq, SpiParameter.Create(reported)).Count == 0;

                _ = Spi.Execute("""
                    INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, high_water, watermark, epoch, state, reported_at,
                        holds, clock)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, least(coalesce(pg_catalog.to_timestamp($9), now()), now()), $11, $12)
                    ON CONFLICT (seqoid, node_id) DO UPDATE SET node_name = excluded.node_name, via_node = excluded.via_node,
                        high_water = excluded.high_water, watermark = excluded.watermark, epoch = excluded.epoch, state = excluded.state,
                        reported_at = excluded.reported_at, holds = excluded.holds, clock = excluded.clock
                    WHERE excluded.reported_at > worf.sequence_range_heard(sequence_range_node.node_id, false, sequence_range_node.reported_at, false)
                        AND (sequence_range_node.via_node <> sequence_range_node.node_id
                            OR worf.sequence_range_heard(sequence_range_node.node_id, true, sequence_range_node.reported_at, false) < now() - $10::interval)
                    """, seq, SpiParameter.Create(reported), SpiParameter.Create(report.GetProperty("node_name").GetString()),
                    sender, SpiParameter.Create(Nullable(report, "high_water")), SpiParameter.Create(Nullable(report, "watermark")),
                    SpiParameter.Create(Nullable32(report, "epoch")), SpiParameter.Create(Text(report, "state")),
                    SpiParameter.Create(report.TryGetProperty("reported_at", out JsonElement at) && at.ValueKind == JsonValueKind.Number ? at.GetDouble() : (double?)null),
                    SpiParameter.Create(SequenceRanges.StaleAfter()), SpiParameter.Create(Holds(report)), SpiParameter.Create(Nullable(report, "clock")));
            }
        }

        // Blocks the neighbor holds from this node. A fenced allocator takes back into its ledger the ones it lost.
        bool fenced = Worf.ReadScalar<bool>("SELECT fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = $1", seq);
        if (entry.TryGetProperty("held", out JsonElement heldBlocks))
        {
            foreach (JsonElement block in heldBlocks.EnumerateArray())
            {
                SequenceRanges.RecordHeld(seqoid, info, block.GetProperty("id").GetGuid(), (block.GetProperty("kind").GetString() ?? "n")[0],
                    block.GetProperty("lo").GetInt64(), block.GetProperty("hi").GetInt64(), block.GetProperty("abandoned").GetBoolean(), fenced,
                    senderId, senderName);
            }

            // A grant the neighbor doesn't list among every block it holds from this fenced allocator never reached it, or
            // was left alone, as when it came from the ledger this allocator went back from: its values were never used.
            if (fenced && entry.TryGetProperty("held_complete", out JsonElement complete) && complete.GetBoolean())
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_node SET held_at = now() WHERE seqoid = $1 AND node_id = $2", seq, sender);
                _ = Spi.Execute("""
                    UPDATE worf.sequence_range_grant SET state = 'unclaimed'
                    WHERE seqoid = $1 AND grantee_id = $2 AND state = 'sent' AND kind <> 'x' AND NOT grant_id = ANY ($3)
                    """, seq, sender, SpiParameter.Create(heldBlocks.EnumerateArray().Select(block => block.GetProperty("id").GetGuid()).ToArray()));
            }
        }

        // A neighbor that holds a block this node granted, which this node's ledger has no record of, shows that this node
        // went back and lost the grant: every grant is committed before it is sent. This node resets, which fences its
        // root and pool blocks until the neighbors report what they hold.
        Guid[] acks = [.. entry.GetProperty("acks").EnumerateArray().Select(ack => ack.GetGuid())];
        _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = ANY ($1) AND grantee_id = $2 AND state = 'sent'",
            SpiParameter.Create(acks), sender);
        if (!fenced && !wentBack && Worf.ReadScalar<long>(
            "SELECT count(*) FROM unnest($1::uuid[]) a(id) WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant g WHERE g.grant_id = a.id)",
            SpiParameter.Create(acks)) is long unknown and > 0)
        {
            PgLog.Warning($"sequence {info.Name}: node {senderName} holds {unknown} blocks this node granted and has no record of, so this node went back; it resets");
            SequenceRanges.Reset(seqoid, node, false);
            received = true;
            fenced = Worf.ReadScalar<bool>("SELECT fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = $1", seq);
        }

        bool isOwner = owner == node.Node.Id;
        if (!isOwner)
        {
            Route(seqoid, node);
        }

        // The neighbor asks this node, its parent, for blocks. The owner grants from the root, and another node from
        // its pool blocks, asking its own parent for a pool block before they run short. A fenced allocator grants
        // nothing.
        if (!fenced && !wentBack && entry.TryGetProperty("requests", out JsonElement requests))
        {
            foreach (JsonElement request in requests.EnumerateArray())
            {
                string kind = Text(request, "kind") ?? "n";
                if (kind is "i" or "f")
                {
                    granted |= GrantExact(seqoid, info, epoch, request, kind[0], senderId, senderName, isOwner);
                    continue;
                }

                long size = request.GetProperty("size").GetInt64();
                bool? answered = Grant(seqoid, info, epoch, request, senderId, senderName, high, isOwner);
                granted |= answered != false;
                if (answered is not null && !isOwner && PoolRequestsLeft(seqoid, info, size) < 2)
                {
                    RequestPool(seqoid, size);
                }
            }
        }

        if (!wentBack)
        {
            foreach (JsonElement grant in entry.GetProperty("grants").EnumerateArray())
            {
                received |= Receive(seqoid, epoch, grant, senderId, senderName, node);
            }
        }

        if (isOwner)
        {
            SequenceRanges.ActivateWhenReported(seqoid, node);
        }

        // An answer that changed nothing is noted in shared memory alone, and one that did here, under the range row's lock,
        // which this transaction takes for each sequence in turn, in the order every exchange takes them.
        if (!call)
        {
            _ = Spi.Execute("UPDATE worf.sequence_range SET exchanged_at = now() WHERE seqoid = $1", seq);
        }

        return (granted, received, learned);
    }

    /// <summary>
    /// Grants a neighbor's request for a node or pool block, unless the neighbor joined with values that reach into
    /// the root, which every allocator refuses, as the root's start that the owner spreads tells it. An allocator knows
    /// that start once it holds the root, or a pool block, whose grant carried it.
    /// </summary>
    /// <returns>Whether it granted, false to have the neighbor wait, or null when it refused.</returns>
    private static bool? Grant(uint seqoid, SequenceInfo info, int epoch, JsonElement request, uint granteeId, string granteeName, long? high,
        bool isOwner)
    {
        Guid requestId = request.GetProperty("id").GetGuid();
        if (Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'sent' WHERE request_id = $1 AND kind = 'x'", SpiParameter.Create(requestId)) > 0)
        {
            return null;
        }

        SpiResult range = Worf.SelectCatalog("""
            SELECT root_start, EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = $1 AND epoch = $2 AND kind = $4::"char"),
                EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE seqoid = $1 AND grantee_id = $3 AND kind <> 'x')
            FROM worf.sequence_range WHERE seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(epoch), SpiParameter.Create(granteeId), SpiParameter.Create(isOwner ? "r" : "p"));
        if (!range[0].Get<bool>(1))
        {
            return false;
        }

        if (!range[0].Get<bool>(2) && high is long h && range[0].Get<long?>(0) is long start && (info.Ascending ? h >= start : h <= start) &&
            seqoid != SnowflakeIds.NodeSequence())
        {
            _ = Spi.Execute("""
                INSERT INTO worf.sequence_range_grant VALUES (gen_random_uuid(), $1, $2, NULL, $3, $4, 'x', $5, $5, 'sent', now())
                ON CONFLICT (request_id) DO UPDATE SET state = 'sent'
                """, SpiParameter.Create(requestId), SpiParameter.Create(seqoid), SpiParameter.Create(granteeId),
                SpiParameter.Create(granteeName), SpiParameter.Create(h));
            PgLog.Warning($"sequence {info.Name}: node {granteeName} joined with values up to {h}, which reach into the root; it is refused");
            return null;
        }

        string kind = request.TryGetProperty("kind", out JsonElement value) ? value.GetString() ?? "n" : "n";
        return SequenceRanges.Allocate(seqoid, info, epoch, request.GetProperty("size").GetInt64(), granteeId, granteeName, requestId, kind[0]) is not null;
    }

    /// <summary>
    /// Finds a grant in a neighbor's message of values this node holds or held under another id.
    /// </summary>
    /// <returns>What shows it, or null.</returns>
    private static string? Regrants(uint seqoid, JsonElement entry)
    {
        foreach (JsonElement grant in entry.GetProperty("grants").EnumerateArray())
        {
            long lo = grant.GetProperty("lo").GetInt64();
            long hi = grant.GetProperty("hi").GetInt64();
            if (Text(grant, "kind") != "x" && Overlaps(seqoid, grant.GetProperty("id").GetGuid(), lo, hi))
            {
                return $"granted [{lo}, {hi}], which this node already holds";
            }
        }

        return null;
    }

    /// <summary>
    /// Checks whether values overlap a block this node holds or held, other than the block of the given id. A Snowflake
    /// node number comes back into use a day after its holder gave it up, so only a number this node holds, or gave up
    /// within that day, counts.
    /// </summary>
    private static bool Overlaps(uint seqoid, Guid id, long lo, long hi)
        => Worf.SelectCatalog("""
            SELECT 1 FROM worf.sequence_range_block
            WHERE seqoid = $1 AND kind IN ('n', 'p', 'i') AND lo <= $3 AND $2 <= hi AND block_id <> $4
                AND (seqoid <> 'worf.snowflake_node'::regclass OR state IN ('next', 'active') OR state = 'abandoned' AND retired_at > now() - $5::interval)
            LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(lo), SpiParameter.Create(hi), SpiParameter.Create(id),
            SpiParameter.Create(SequenceRanges.SnowflakeReuseAfter())).Count > 0;

    /// <summary>
    /// Counts how many requests of a size this node's pool blocks can still serve.
    /// </summary>
    private static decimal PoolRequestsLeft(uint seqoid, SequenceInfo info, long size)
        => Worf.ReadScalar<decimal>("""
            SELECT coalesce(sum(floor(((f.hi::numeric - f.lo) / $2 + 1) / $3)), 0)
            FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
            WHERE b.seqoid = $1 AND b.kind = 'p' AND b.state = 'open'
            """, SpiParameter.Create(seqoid), SpiParameter.Create(Math.Abs(info.Increment)), SpiParameter.Create(size));

    /// <summary>
    /// Answers a request for exact values. The owner grants them from the root or refuses them. Another node grants
    /// them from a pool block that holds them, which its own parent granted for this request, passes on its parent's
    /// refusal, or else forwards the request.
    /// </summary>
    private static bool GrantExact(uint seqoid, SequenceInfo info, int epoch, JsonElement request, char kind, uint granteeId, string granteeName,
        bool isOwner)
    {
        long lo = request.GetProperty("lo").GetInt64();
        long hi = request.GetProperty("hi").GetInt64();
        Guid requestId = request.GetProperty("id").GetGuid();
        if (SequenceRanges.AllocateExact(seqoid, info, epoch, lo, hi, granteeId, granteeName, requestId, kind, isOwner) is not null)
        {
            return true;
        }

        if (isOwner)
        {
            return true;
        }

        SpiParameter seq = SpiParameter.Create(seqoid);
        if (Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND kind = 'f' AND lo = $2 AND hi = $3 AND state = 'refused'",
            seq, SpiParameter.Create(lo), SpiParameter.Create(hi)) > 0)
        {
            _ = Spi.Execute("""
                INSERT INTO worf.sequence_range_grant VALUES (gen_random_uuid(), $1, $2, NULL, $3, $4, 'x', $5, $6, 'sent', now())
                ON CONFLICT (request_id) DO UPDATE SET state = 'sent'
                """, SpiParameter.Create(requestId), seq, SpiParameter.Create(granteeId), SpiParameter.Create(granteeName),
                SpiParameter.Create(lo), SpiParameter.Create(hi));
            return true;
        }

        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_request (request_id, seqoid, parent_node, kind, size, lo, hi, created_at)
            SELECT gen_random_uuid(), r.seqoid, r.parent_node, 'f', $4, $2, $3, now()
            FROM worf.sequence_range r
            WHERE r.seqoid = $1 AND r.parent_node IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request q WHERE q.seqoid = r.seqoid AND q.kind = 'f' AND q.lo = $2 AND q.hi = $3)
            """, seq, SpiParameter.Create(lo), SpiParameter.Create(hi), SpiParameter.Create(SequenceRanges.Steps(info, lo, hi)));
        return false;
    }

    /// <summary>
    /// Asks this node's parent for a pool block sixteen times a child's request, unless one is already open.
    /// </summary>
    private static void RequestPool(uint seqoid, long childSize)
    {
        _ = Spi.Execute("""
            INSERT INTO worf.sequence_range_request (request_id, seqoid, parent_node, kind, size, created_at)
            SELECT gen_random_uuid(), r.seqoid, r.parent_node, 'p', least($2::numeric * 16, 4611686018427387903)::bigint, now()
            FROM worf.sequence_range r
            WHERE r.seqoid = $1 AND r.parent_node IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request q WHERE q.seqoid = r.seqoid AND q.kind = 'p' AND q.state = 'open')
            """, SpiParameter.Create(seqoid), SpiParameter.Create(childSize));
    }

    /// <summary>
    /// Records a block a neighbor granted this node, once, and closes the request it answers. A grant for no request
    /// open here, as one from a parent this node has left, or for a request it made before a rollback, is recorded as
    /// abandoned, so that the block is never used and its allocator hears so. A refusal of a node block means the node
    /// joined with values that reach into the root.
    /// </summary>
    private static bool Receive(uint seqoid, int epoch, JsonElement grant, uint from, string fromName, WorfLocalNode node)
    {
        Guid id = grant.GetProperty("id").GetGuid();
        Guid requestId = grant.GetProperty("request_id").GetGuid();
        string kind = Text(grant, "kind") ?? "n";
        if (kind == "x")
        {
            string? refused = Spi.ExecuteScalar<string?>(
                "UPDATE worf.sequence_range_request SET state = 'refused' WHERE request_id = $1 AND state = 'open' RETURNING kind::text",
                SpiParameter.Create(requestId));
            if (refused == "n")
            {
                _ = Spi.Execute("""
                    UPDATE worf.sequence_range SET state = 'refused',
                        detail = 'node ' || $2 || ' refused this node: its values reach into the root; disable and enable the sequence again on its owner to start a root above them'
                    WHERE seqoid = $1
                    """, SpiParameter.Create(seqoid), SpiParameter.Create(fromName));
            }

            return refused is not null;
        }

        if (Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_block WHERE block_id = $1", SpiParameter.Create(id)).Count > 0)
        {
            return false;
        }

        long lo = grant.GetProperty("lo").GetInt64();
        long hi = grant.GetProperty("hi").GetInt64();

        // A grant of values this node already holds comes from an allocator that lost the grant it made them in. It is
        // never recorded, not even as abandoned, so that the allocator, once fenced, finds it missing from this node's
        // blocks and lets it go.
        if (Overlaps(seqoid, id, lo, hi))
        {
            return false;
        }

        SpiResult open = Worf.SelectCatalog("SELECT reserved FROM worf.sequence_range_request WHERE request_id = $1 AND parent_node = $2 AND state = 'open'",
            SpiParameter.Create(requestId), SpiParameter.Create(from));
        if (open.Count == 0)
        {
            _ = SequenceRanges.InsertBlock(seqoid, epoch, kind == "i" ? 'i' : kind is "p" or "f" ? 'p' : 'n', lo, hi, "abandoned", from, node, id);
            PgLog.Log($"sequence {SequenceRanges.GetInfo(seqoid).Name}: block [{lo}, {hi}] from node {fromName} answers no open request, so it is abandoned");
            return false;
        }

        // A pool block, or exact values to grant on, are this node's to grant from. Exact values for this node are an
        // imported block, which the sequence never switches to.
        bool pool = kind is "p" or "f";
        (char blockKind, string state) = kind switch
        {
            "p" or "f" => ('p', "open"),
            "i" => ('i', "imported"),
            _ => ('n', "next"),
        };
        _ = SequenceRanges.InsertBlock(seqoid, epoch, blockKind, lo, hi, state, from, node, id, blockKind == 'n' && open[0].Get<bool>(0),
            Nullable(grant, "not_before"));
        if (pool)
        {
            _ = Spi.Execute("INSERT INTO worf.sequence_range_free VALUES ($1, $2, $3)", SpiParameter.Create(id), SpiParameter.Create(lo),
                SpiParameter.Create(hi));
        }

        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE request_id = $1", SpiParameter.Create(requestId));
        _ = Spi.Execute("SET LOCAL synchronous_commit = on");
        return true;
    }

    /// <summary>
    /// Sets this node's distance to the owner, one more than its nearest neighbor's, and its parent, that neighbor,
    /// with ties going to the lowest name. Only neighbors that still exchange count, so that a dropped link's node stops
    /// being the parent. A request open with another parent is dropped once a new parent is found.
    /// </summary>
    private static void Route(uint seqoid, WorfLocalNode node)
    {
        (uint? parent, int? distance) = Nearest(seqoid, node);
        _ = Spi.Execute("""
            UPDATE worf.sequence_range SET parent_node = $2, distance = $3,
                state = CASE WHEN $2 IS NULL AND state IN ('quarantined', 'no_route') THEN 'no_route'
                    WHEN state = 'no_route' THEN 'quarantined' ELSE state END
            WHERE seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(parent), SpiParameter.Create(distance));
        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND $2::oid IS NOT NULL AND parent_node <> $2",
            SpiParameter.Create(seqoid), SpiParameter.Create(parent));
    }

    /// <summary>
    /// Finds this node's parent, its nearest neighbor that still exchanges, and its distance to the owner.
    /// </summary>
    private static (uint? Parent, int? Distance) Nearest(uint seqoid, WorfLocalNode node)
    {
        SpiResult nearest = Worf.SelectCatalog("""
            SELECT n.node_id, n.distance FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid AND r.epoch = n.epoch
            WHERE n.seqoid = $1 AND n.via_node = n.node_id AND n.node_id <> $2 AND n.distance IS NOT NULL AND n.distance < $3
                AND worf.sequence_range_heard(n.node_id, true, n.reported_at, false) > now() - $4::interval
            ORDER BY n.distance, n.node_name
            LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(MaxDistance),
            SpiParameter.Create(SequenceRanges.StaleAfter()));
        return nearest.Count > 0 ? (nearest[0].Get<uint>(0), nearest[0].Get<int>(1) + 1) : (null, null);
    }

    /// <summary>
    /// Lists the Snowflake node numbers this node holds: the ones it uses or is about to, and for a day the ones it gave
    /// up, against a copy of this node that may still use them.
    /// </summary>
    private static long[] OwnHolds(uint seqoid)
    {
        SpiResult held = Worf.SelectCatalog("""
            SELECT lo FROM worf.sequence_range_block
            WHERE seqoid = $1 AND kind = 'n' AND (state IN ('next', 'active') OR state = 'abandoned' AND retired_at > now() - $2::interval)
            """, SpiParameter.Create(seqoid), SpiParameter.Create(SequenceRanges.SnowflakeReuseAfter()));
        long[] holds = new long[held.Count];
        for (int i = 0; i < held.Count; i++)
        {
            holds[i] = held[i].Get<long>(0);
        }

        return holds;
    }

    private static void WriteHolds(Utf8JsonWriter writer, long[] holds, long? clock)
    {
        writer.WriteStartArray("holds");
        foreach (long number in holds)
        {
            writer.WriteNumberValue(number);
        }

        writer.WriteEndArray();
        WriteNullable(writer, "clock", clock);
    }

    private static long[]? Holds(JsonElement entry)
        => entry.TryGetProperty("holds", out JsonElement holds) && holds.ValueKind == JsonValueKind.Array
            ? [.. holds.EnumerateArray().Select(number => number.GetInt64())]
            : null;

    private static void WriteNullable(Utf8JsonWriter writer, string name, long? value)
    {
        if (value is long v)
        {
            writer.WriteNumber(name, v);
        }
        else
        {
            writer.WriteNull(name);
        }
    }

    private static void WriteNullable(Utf8JsonWriter writer, string name, int? value)
    {
        if (value is int v)
        {
            writer.WriteNumber(name, v);
        }
        else
        {
            writer.WriteNull(name);
        }
    }

    private static void WriteNullable(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is not null)
        {
            writer.WriteString(name, value);
        }
        else
        {
            writer.WriteNull(name);
        }
    }

    /// <summary>
    /// Reads when a report's node made it, in microseconds since 2000.
    /// </summary>
    private static long? ReportedAt(JsonElement report)
        => report.TryGetProperty("reported_at", out JsonElement at) && at.ValueKind == JsonValueKind.Number
            ? (long)((at.GetDouble() - UnixToPostgres) * 1e6)
            : null;

    private static string? Text(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static long? Nullable(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number ? value.GetInt64() : null;

    private static int? Nullable32(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number ? value.GetInt32() : null;
}
