using System.Globalization;
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
        RETURNS text STRICT VOLATILE SECURITY DEFINER LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.sequence_range_exchange(text) FROM public;
        """)]
    internal static string Exchange(string message)
    {
        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;
        using JsonDocument document = JsonDocument.Parse(message);
        JsonElement root = document.RootElement;

        // Each call is tied to a real subscription by its slot here.
        string slot = root.GetProperty("slot").GetString() ?? "";
        if (Worf.SelectCatalog("SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND plugin = 'worf'",
            SpiParameter.Create(slot)).Count == 0)
        {
            throw new PgException("42501", $"no worf replication slot \"{slot}\" carries this sequence range exchange");
        }

        uint senderId = root.GetProperty("node_id").GetUInt32();
        _ = Process(node, root, senderId, root.GetProperty("node_name").GetString() ?? "");
        return Build(node, senderId, null);
    }

    /// <summary>
    /// Exchanges with the provider of each enabled subscription whose turn has come, or with which something is
    /// waiting: a request, a grant or an acknowledgement.
    /// </summary>
    /// <param name="node">This node.</param>
    /// <returns>Whether this node's blocks changed.</returns>
    internal static bool ExchangeAll(WorfLocalNode node)
    {
        // A backend that answered an exchange learned of a node or sequence the neighbors should hear of.
        long requested = SequenceRanges.ExchangeRequested.Value;
        if (requested != s_requested)
        {
            s_requested = requested;
            s_exchangedAt.Clear();
        }

        bool changed = false;
        foreach (WorfSubscription sub in WorfNode.GetNodeSubscriptions(node.Node.Id, false))
        {
            long now = Worker.GetCurrentTimestamp();
            bool due = !s_exchangedAt.TryGetValue(sub.Id, out long at) || now - at >= SequenceRanges.ExchangeInterval * 1000L ||
                s_acksWaiting.Contains(sub.Id) || Waiting(sub.Origin.Id);
            if (!sub.Enabled || !due)
            {
                continue;
            }

            try
            {
                PgTransaction.RunInSubtransaction(() => changed |= ExchangeWith(node, sub));
                s_exchangedAt[sub.Id] = now;
            }
            catch (PgException error)
            {
                PgLog.Warning($"sequence range exchange with node {sub.Origin.Name} failed: {error.Message}");
            }
        }

        return changed;
    }

    private static bool ExchangeWith(WorfLocalNode node, WorfSubscription sub)
    {
        using PgReplicationConnection conn = Worf.Connect(sub.OriginIf.Dsn, sub.Name, "range");
        if (!Rpc.RemoteFunctionExists(conn, "worf", "sequence_range_exchange", 1, null))
        {
            return false;
        }

        _ = conn.Query($"SELECT pg_catalog.set_config('statement_timeout', '{SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)}', false)", 1);
        bool changed = false;

        // A grant this node makes from the provider's request goes back in a second call, after this node commits it.
        for (int call = 0; call < 2; call++)
        {
            string message = Build(node, sub.Origin.Id, sub.SlotName);
            string reply = conn.Query("SELECT worf.sequence_range_exchange(" + Spi.QuoteLiteral(message) + ")", 1)[0][0] ?? "{}";
            using JsonDocument document = JsonDocument.Parse(reply);
            (bool granted, bool received) = Process(node, document.RootElement, sub.Origin.Id, sub.Origin.Name);
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

        _ = Spi.Execute("UPDATE worf.sequence_range SET exchanged_at = now()");
        return changed;
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
    /// Builds this node's message to a neighbor.
    /// </summary>
    private static string Build(WorfLocalNode node, uint neighbor, string? slot)
    {
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
                """);
            for (int i = 0; i < sequences.Count; i++)
            {
                SpiRow row = sequences[i];
                uint seqoid = row.Get<uint>(0);
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

                // A reset this node asks of the neighbor, whose WAL position moved backwards, and one it honored.
                SpiResult neighborRow = Worf.SelectCatalog(
                    "SELECT reset_lsn::text, reset_done::text, state = 'fenced' FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2",
                    SpiParameter.Create(seqoid), SpiParameter.Create(neighbor));
                bool neighborFenced = false;
                if (neighborRow.Count > 0)
                {
                    WriteNullable(writer, "reset", neighborRow[0].Get<string?>(0));
                    WriteNullable(writer, "reset_done", neighborRow[0].Get<string?>(1));
                    neighborFenced = neighborRow[0].Get<bool?>(2) == true;
                }

                // The blocks this node holds from the neighbor: every one while the neighbor is a fenced allocator, so
                // that it can rebuild its ledger, and otherwise those abandoned lately, which it marks so.
                writer.WriteBoolean("held_complete", neighborFenced);
                writer.WriteStartArray("held");
                SpiResult held = Worf.SelectCatalog("""
                    SELECT block_id, kind::text, lo, hi, state = 'abandoned' FROM worf.sequence_range_block
                    WHERE seqoid = $1 AND from_node = $2 AND kind IN ('n', 'p', 'i')
                        AND ($3 OR state = 'abandoned' AND retired_at > now() - interval '10 minutes')
                    """, SpiParameter.Create(seqoid), SpiParameter.Create(neighbor), SpiParameter.Create(neighborFenced));
                for (int h = 0; h < held.Count; h++)
                {
                    writer.WriteStartObject();
                    writer.WriteString("id", held[h].Get<Guid>(0));
                    writer.WriteString("kind", held[h].Get<string>(1));
                    writer.WriteNumber("lo", held[h].Get<long>(2));
                    writer.WriteNumber("hi", held[h].Get<long>(3));
                    writer.WriteBoolean("abandoned", held[h].Get<bool>(4));
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                // The open requests, for a node block or a pool block, when the neighbor is this node's parent.
                writer.WriteStartArray("requests");
                if (row.Get<uint?>(9) == neighbor)
                {
                    SpiResult requests = Worf.SelectCatalog(
                        "SELECT request_id, size, kind::text, lo, hi FROM worf.sequence_range_request WHERE seqoid = $1 AND parent_node = $2 AND state = 'open'",
                        SpiParameter.Create(seqoid), SpiParameter.Create(neighbor));
                    for (int r = 0; r < requests.Count; r++)
                    {
                        writer.WriteStartObject();
                        writer.WriteString("id", requests[r].Get<Guid>(0));
                        writer.WriteNumber("size", requests[r].Get<long>(1));
                        writer.WriteString("kind", requests[r].Get<string>(2));
                        WriteNullable(writer, "lo", requests[r].Get<long?>(3));
                        WriteNullable(writer, "hi", requests[r].Get<long?>(4));
                        writer.WriteEndObject();
                    }
                }

                writer.WriteEndArray();

                // Every node this node has heard from, so that reports reach the owner hop by hop.
                writer.WriteStartArray("reports");
                SpiResult reports = Worf.SelectCatalog("""
                    SELECT n.node_id, n.node_name::text, n.high_water, n.watermark, n.epoch,
                        CASE WHEN n.node_id <> l.node_id THEN n.state WHEN r.fenced_at IS NOT NULL THEN 'fenced' ELSE r.state END
                    FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid CROSS JOIN worf.local_node l
                    WHERE n.seqoid = $1 AND n.node_id <> $2
                    """, SpiParameter.Create(seqoid), SpiParameter.Create(neighbor));
                for (int r = 0; r < reports.Count; r++)
                {
                    writer.WriteStartObject();
                    writer.WriteNumber("node_id", reports[r].Get<uint>(0));
                    writer.WriteString("node_name", reports[r].Get<string>(1));
                    WriteNullable(writer, "high_water", reports[r].Get<long?>(2));
                    WriteNullable(writer, "watermark", reports[r].Get<long?>(3));
                    WriteNullable(writer, "epoch", reports[r].Get<int?>(4));
                    WriteNullable(writer, "state", reports[r].Get<string?>(5));
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                // Grants for the neighbor that it hasn't acknowledged, and acknowledgements of its grants.
                writer.WriteStartArray("grants");
                SpiResult grants = Worf.SelectCatalog(
                    "SELECT grant_id, request_id, lo, hi, kind::text FROM worf.sequence_range_grant WHERE seqoid = $1 AND grantee_id = $2 AND state = 'sent'",
                    SpiParameter.Create(seqoid), SpiParameter.Create(neighbor));
                for (int g = 0; g < grants.Count; g++)
                {
                    writer.WriteStartObject();
                    writer.WriteString("id", grants[g].Get<Guid>(0));
                    writer.WriteString("request_id", grants[g].Get<Guid>(1));
                    writer.WriteNumber("lo", grants[g].Get<long>(2));
                    writer.WriteNumber("hi", grants[g].Get<long>(3));
                    writer.WriteString("kind", grants[g].Get<string>(4));
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                // A refusal needs no acknowledgement: a request that lost it is sent again and refused again.
                _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE seqoid = $1 AND grantee_id = $2 AND kind = 'x' AND state = 'sent'",
                    SpiParameter.Create(seqoid), SpiParameter.Create(neighbor));
                writer.WriteStartArray("acks");
                SpiResult received = Worf.SelectCatalog(
                    "SELECT block_id FROM worf.sequence_range_block WHERE seqoid = $1 AND from_node = $2 AND kind IN ('n', 'p', 'i') AND state IN ('next', 'active', 'open', 'imported')",
                    SpiParameter.Create(seqoid), SpiParameter.Create(neighbor));
                for (int b = 0; b < received.Count; b++)
                {
                    writer.WriteStringValue(received[b].Get<Guid>(0));
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// Takes in a neighbor's message: joins the sequences it manages that exist here, records its reports, answers
    /// its request when this node is its parent, records grants for this node and the neighbor's acknowledgements.
    /// </summary>
    /// <returns>Whether this node granted the neighbor a block, and whether it received one.</returns>
    private static (bool Granted, bool Received) Process(WorfLocalNode node, JsonElement root, uint senderId, string senderName)
    {
        bool granted = false;
        bool received = false;
        bool learned = false;
        if (!root.TryGetProperty("sequences", out JsonElement sequences))
        {
            return (false, false);
        }

        foreach (JsonElement entry in sequences.EnumerateArray())
        {
            uint? found = Spi.ExecuteScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(entry.GetProperty("name").GetString()));
            if (found is not uint seqoid)
            {
                continue;
            }

            int epoch = entry.GetProperty("epoch").GetInt32();
            uint owner = entry.GetProperty("owner_id").GetUInt32();
            SpiResult local = Worf.SelectCatalog("SELECT owner_node, epoch, state FROM worf.sequence_range WHERE seqoid = $1",
                SpiParameter.Create(seqoid));
            if (local.Count == 0)
            {
                if (entry.GetProperty("state").GetString() == "disabled")
                {
                    continue;
                }

                SequenceRanges.JoinFromNeighbor(seqoid, owner, epoch, entry.GetProperty("range_size").GetInt64(),
                    entry.GetProperty("max_range_size").GetInt64(), Nullable(entry, "root_start"), Nullable(entry, "root_end"), node);
            }
            else if (local[0].Get<int>(1) > epoch)
            {
                // Grants from an older epoch than this node knows are ignored.
                continue;
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
            learned |= local.Count == 0 || Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2",
                SpiParameter.Create(seqoid), SpiParameter.Create(senderId)).Count == 0;
            long? high = Nullable(entry, "high_water");
            long? watermark = Nullable(entry, "watermark");
            string sysid = root.GetProperty("sysid").GetString() ?? "";
            long timeline = root.GetProperty("timeline").GetInt64();
            string walLsn = root.GetProperty("wal_lsn").GetString() ?? "0/0";

            // A neighbor whose WAL position moved backwards on the same timeline was rolled back, by a restored backup
            // or snapshot, and may issue values it issued before. It must reset its blocks.
            string? rolledBack = Spi.ExecuteScalar<string?>("""
                UPDATE worf.sequence_range_node SET reset_lsn = wal_lsn
                WHERE seqoid = $1 AND node_id = $2 AND via_node = node_id AND sysid = $3::numeric AND timeline = $4 AND wal_lsn > $5::pg_lsn
                RETURNING wal_lsn::text
                """, SpiParameter.Create(seqoid), SpiParameter.Create(senderId), SpiParameter.Create(sysid), SpiParameter.Create(timeline),
                SpiParameter.Create(walLsn));
            if (rolledBack is not null)
            {
                PgLog.Warning($"sequence {info.Name}: node {senderName}'s WAL position moved back from {rolledBack} to {walLsn}, so it must reset its blocks");
            }

            _ = Spi.Execute("""
                INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, sysid, timeline, wal_lsn, epoch, state, distance,
                    high_water, watermark, reported_at)
                VALUES ($1, $2, $3, $2, $4::numeric, $5, $6::pg_lsn, $7, $8, $9, $10, $11, now())
                ON CONFLICT (seqoid, node_id) DO UPDATE SET node_name = excluded.node_name, via_node = excluded.via_node,
                    sysid = excluded.sysid, timeline = excluded.timeline, wal_lsn = excluded.wal_lsn, epoch = excluded.epoch,
                    state = excluded.state, distance = excluded.distance, high_water = excluded.high_water, watermark = excluded.watermark,
                    reported_at = excluded.reported_at
                """, SpiParameter.Create(seqoid), SpiParameter.Create(senderId), SpiParameter.Create(senderName),
                SpiParameter.Create(sysid), SpiParameter.Create(timeline), SpiParameter.Create(walLsn), SpiParameter.Create(epoch),
                SpiParameter.Create(entry.GetProperty("state").GetString()), SpiParameter.Create(Nullable32(entry, "distance")),
                SpiParameter.Create(high), SpiParameter.Create(watermark));

            // The neighbor asks this node to reset, once per rollback it saw, or has reset as this node asked.
            if (Text(entry, "reset") is string reset && Spi.Execute(
                "UPDATE worf.sequence_range_node SET reset_done = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2 AND reset_done IS DISTINCT FROM $3::pg_lsn",
                SpiParameter.Create(seqoid), SpiParameter.Create(senderId), SpiParameter.Create(reset)) > 0)
            {
                PgLog.Warning($"sequence {info.Name}: node {senderName} saw this node's WAL position move backwards, so it abandons its blocks and requests new ones");
                SequenceRanges.Reset(seqoid, node);
                received = true;
            }

            if (Text(entry, "reset_done") is string done)
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_node SET reset_lsn = NULL WHERE seqoid = $1 AND node_id = $2 AND reset_lsn = $3::pg_lsn",
                    SpiParameter.Create(seqoid), SpiParameter.Create(senderId), SpiParameter.Create(done));
            }

            if (watermark is long mark)
            {
                _ = Spi.Execute($"UPDATE worf.sequence_range SET watermark = {(info.Ascending ? "greatest" : "least")}(coalesce(watermark, $2), $2) WHERE seqoid = $1",
                    SpiParameter.Create(seqoid), SpiParameter.Create(mark));
            }

            // Reports of other nodes, relayed toward the owner. A neighbor's own report outranks a relayed one.
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
                        SpiParameter.Create(seqoid), SpiParameter.Create(reported)).Count == 0;

                    _ = Spi.Execute("""
                        INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, high_water, watermark, epoch, state, reported_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                        ON CONFLICT (seqoid, node_id) DO UPDATE SET high_water = excluded.high_water, watermark = excluded.watermark,
                            epoch = excluded.epoch, state = excluded.state, reported_at = excluded.reported_at
                        WHERE sequence_range_node.via_node = excluded.via_node
                        """, SpiParameter.Create(seqoid), SpiParameter.Create(reported), SpiParameter.Create(report.GetProperty("node_name").GetString()),
                        SpiParameter.Create(senderId), SpiParameter.Create(Nullable(report, "high_water")), SpiParameter.Create(Nullable(report, "watermark")),
                        SpiParameter.Create(Nullable32(report, "epoch")), SpiParameter.Create(Text(report, "state")));
                }
            }

            // Blocks the neighbor holds from this node. A fenced allocator takes back into its ledger the ones it lost.
            bool fenced = Spi.ExecuteScalar<bool>("SELECT fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = $1", SpiParameter.Create(seqoid));
            if (entry.TryGetProperty("held", out JsonElement heldBlocks))
            {
                foreach (JsonElement block in heldBlocks.EnumerateArray())
                {
                    SequenceRanges.RecordHeld(seqoid, info, block.GetProperty("id").GetGuid(), (block.GetProperty("kind").GetString() ?? "n")[0],
                        block.GetProperty("lo").GetInt64(), block.GetProperty("hi").GetInt64(), block.GetProperty("abandoned").GetBoolean(), fenced,
                        senderId, senderName);
                }

                if (fenced && entry.TryGetProperty("held_complete", out JsonElement complete) && complete.GetBoolean())
                {
                    _ = Spi.Execute("UPDATE worf.sequence_range_node SET held_at = now() WHERE seqoid = $1 AND node_id = $2",
                        SpiParameter.Create(seqoid), SpiParameter.Create(senderId));
                }
            }

            bool isOwner = owner == node.Node.Id;
            if (!isOwner)
            {
                Route(seqoid, node);
            }

            // The neighbor asks this node, its parent, for blocks. The owner grants from the root, and another node
            // from its pool blocks, asking its own parent for a pool block when they can't serve the request. A fenced
            // allocator grants nothing.
            if (!fenced && entry.TryGetProperty("requests", out JsonElement requests))
            {
                foreach (JsonElement request in requests.EnumerateArray())
                {
                    string kind = Text(request, "kind") ?? "n";
                    if (kind is "i" or "f")
                    {
                        granted |= GrantExact(seqoid, info, epoch, request, kind[0], senderId, senderName, isOwner);
                        continue;
                    }

                    bool answered = Grant(seqoid, info, epoch, request, senderId, senderName, high, isOwner);
                    granted |= answered;
                    if (!answered && !isOwner)
                    {
                        RequestPool(seqoid, request.GetProperty("size").GetInt64());
                    }
                }
            }

            foreach (JsonElement grant in entry.GetProperty("grants").EnumerateArray())
            {
                received |= Receive(seqoid, epoch, grant, senderId, node);
            }

            foreach (JsonElement ack in entry.GetProperty("acks").EnumerateArray())
            {
                _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = $1 AND grantee_id = $2 AND state = 'sent'",
                    SpiParameter.Create(ack.GetGuid()), SpiParameter.Create(senderId));
            }

            if (isOwner)
            {
                SequenceRanges.ActivateWhenReported(seqoid, node);
            }
        }

        // Neighbors hear of what this node learned at their next exchange, which happens now.
        if (learned)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () => _ = PgAtomic.Add(SequenceRanges.ExchangeRequested, 1L));
        }

        return (granted, received);
    }

    /// <summary>
    /// Grants a neighbor's request, unless its values might already lie in the root, which happens when it joins
    /// after the root started and its high-water mark reaches into it.
    /// </summary>
    private static bool Grant(uint seqoid, SequenceInfo info, int epoch, JsonElement request, uint granteeId, string granteeName, long? high,
        bool isOwner)
    {
        // The owner refuses a node that joins after the root started, with values that reach into it.
        if (isOwner)
        {
            SpiResult root = Worf.SelectCatalog(
                "SELECT lo, hi FROM worf.sequence_range_block WHERE seqoid = $1 AND epoch = $2 AND kind = 'r'",
                SpiParameter.Create(seqoid), SpiParameter.Create(epoch));
            if (root.Count == 0)
            {
                return false;
            }

            bool holds = Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_grant WHERE seqoid = $1 AND grantee_id = $2",
                SpiParameter.Create(seqoid), SpiParameter.Create(granteeId)).Count > 0;
            if (!holds && high is long h && (info.Ascending ? h >= root[0].Get<long>(0) : h <= root[0].Get<long>(1)))
            {
                PgLog.Warning($"sequence {info.Name}: node {granteeName} joined with values up to {h}, which reach into the root; it stays quarantined");
                return false;
            }
        }

        string kind = request.TryGetProperty("kind", out JsonElement value) ? value.GetString() ?? "n" : "n";
        return SequenceRanges.Allocate(seqoid, info, epoch, request.GetProperty("size").GetInt64(), granteeId, granteeName,
            request.GetProperty("id").GetGuid(), kind[0]) is not null;
    }

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
    /// Records a block a neighbor granted this node as its next block, once, and closes the request it answers.
    /// </summary>
    private static bool Receive(uint seqoid, int epoch, JsonElement grant, uint from, WorfLocalNode node)
    {
        Guid id = grant.GetProperty("id").GetGuid();
        string kind = Text(grant, "kind") ?? "n";
        if (kind == "x")
        {
            return Spi.Execute("UPDATE worf.sequence_range_request SET state = 'refused' WHERE request_id = $1 AND state = 'open'",
                SpiParameter.Create(grant.GetProperty("request_id").GetGuid())) > 0;
        }

        if (Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_block WHERE block_id = $1", SpiParameter.Create(id)).Count > 0)
        {
            return false;
        }

        // A pool block, or exact values to grant on, are this node's to grant from. Exact values for this node are an
        // imported block, which the sequence never switches to.
        long lo = grant.GetProperty("lo").GetInt64();
        long hi = grant.GetProperty("hi").GetInt64();
        bool pool = kind is "p" or "f";
        (char blockKind, string state) = kind switch
        {
            "p" or "f" => ('p', "open"),
            "i" => ('i', "imported"),
            _ => ('n', "next"),
        };
        _ = SequenceRanges.InsertBlock(seqoid, epoch, blockKind, lo, hi, state, from, node, id);
        if (pool)
        {
            _ = Spi.Execute("INSERT INTO worf.sequence_range_free VALUES ($1, $2, $3)", SpiParameter.Create(id), SpiParameter.Create(lo),
                SpiParameter.Create(hi));
        }

        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE request_id = $1", SpiParameter.Create(grant.GetProperty("request_id").GetGuid()));
        _ = Spi.Execute("SET LOCAL synchronous_commit = on");
        return true;
    }

    /// <summary>
    /// Sets this node's distance to the owner, one more than its nearest neighbor's, and its parent, that neighbor,
    /// with ties going to the lowest name. A request open with another parent is dropped.
    /// </summary>
    private static void Route(uint seqoid, WorfLocalNode node)
    {
        SpiResult nearest = Worf.SelectCatalog("""
            SELECT n.node_id, n.distance FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid AND r.epoch = n.epoch
            WHERE n.seqoid = $1 AND n.via_node = n.node_id AND n.node_id <> $2 AND n.distance IS NOT NULL AND n.distance < $3
            ORDER BY n.distance, n.node_name
            LIMIT 1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(MaxDistance));
        uint? parent = nearest.Count > 0 ? nearest[0].Get<uint>(0) : null;
        int? distance = nearest.Count > 0 ? nearest[0].Get<int>(1) + 1 : null;
        _ = Spi.Execute("""
            UPDATE worf.sequence_range SET parent_node = $2, distance = $3,
                state = CASE WHEN $2 IS NULL AND state IN ('quarantined', 'no_route') THEN 'no_route'
                    WHEN state = 'no_route' THEN 'quarantined' ELSE state END
            WHERE seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(parent), SpiParameter.Create(distance));
        _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND parent_node IS DISTINCT FROM $2",
            SpiParameter.Create(seqoid), SpiParameter.Create(parent));
    }

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

    private static string? Text(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static long? Nullable(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number ? value.GetInt64() : null;

    private static int? Nullable32(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number ? value.GetInt32() : null;
}
