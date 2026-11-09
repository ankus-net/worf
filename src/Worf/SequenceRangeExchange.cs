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

    // The Snowflake node numbers there are, more than which no node can hold.
    private const int SnowflakeNodes = 1024;

    // Seconds from the Unix epoch to PostgreSQL's, 2000-01-01.
    private const double UnixToPostgres = 946_684_800;

    // How far a neighbor's WAL position moves before the tables record it, a default WAL segment.
    private const ulong LsnRecordedEvery = 16 * 1024 * 1024;

    // How many check intervals a link waiting on its provider's fence waits at most between asks, since it hears of the
    // fence lifting only by asking.
    private const int FencedAskEvery = 10;

    // When each subscription's provider is next tried after a failed exchange, and the delay before that, and the
    // subscriptions whose last call ran out of time, which back off up to a minute.
    private static readonly Dictionary<uint, (long At, long Delay)> s_retries = [];
    private static readonly HashSet<uint> s_timedOut = [];

    // The exchanges under way, by subscription, which the worker takes on without waiting on them.
    private static readonly Dictionary<uint, WorfPendingExchange> s_pending = [];

    // undefined_function, which a provider without the exchange function answers a call with.
    private const string UndefinedFunction = "42883";

    // The sequences, with the prepared transactions holding their range rows, that this process has warned of.
    private static readonly HashSet<(uint Seqoid, string Gid)> s_preparedHolders = [];

    // The neighbors whose messages the current transaction took in, with when their servers started and whether they
    // made them knowing this server was back, noted once it commits.
    private static readonly List<(uint Sender, long? StartedAt, bool Confirmed)> s_onCommit = [];
    private const long TimedOutBackoff = 60_000_000;
    private const long TimedOutFirstBackoff = 10_000_000;

    // When each link last exchanged, by subscription, and the links with acknowledgements to send.
    private static readonly Dictionary<uint, long> s_exchangedAt = [];
    private static readonly HashSet<uint> s_acksWaiting = [];
    private static long s_requested;

    // What each link waited on at its last exchange that changed nothing, when it next asks for that, and the delay
    // before that, which doubles from the check interval up to the exchange interval while nothing changes.
    private static readonly Dictionary<uint, (string What, long At, long Delay)> s_waits = [];


    // The subscriptions whose provider answered the last exchange that their slot isn't streamed yet, those whose
    // provider answered the last call that it has no such slot, and those whose last exchange failed so, logged only
    // if the next fails so too, as a subscription that is being dropped no longer has one.
    private static readonly HashSet<uint> s_notStreamed = [];
    private static readonly HashSet<uint> s_noSlot = [];
    private static readonly HashSet<uint> s_noSlotOnce = [];

    // The connection to each subscription's provider that this worker's exchanges go over, kept from one exchange to the
    // next, as apply keeps its own, rather than a session made and ended for each, with the connection string and the
    // timeout it was made with; one that fails, or a call that runs out of time, is closed.
    private static readonly Dictionary<uint, (WorfConnection Conn, string Dsn, int Timeout)> s_connections = [];

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
        // A standby, or a node still finishing its promotion, can write nothing and has no timeline to report, so it
        // takes in no sequence of the call.
        if (SequenceRanges.Recovering())
        {
            throw new PgException("25006", "cannot exchange sequence ranges during recovery");
        }

        WorfLocalNode node = WorfNode.GetLocalNode(false, false)!;
        using JsonDocument document = JsonDocument.Parse(message);
        JsonElement root = document.RootElement;
        uint senderId = root.GetProperty("node_id").GetUInt32();
        string slot = root.GetProperty("slot").GetWorfString() ?? "";

        // A subscriber that drops its subscription leaves the slot, if it ever called through it, and this node's fences no
        // longer wait on it.
        bool leaving = root.TryGetProperty("leave", out JsonElement leave) && leave.ValueKind == JsonValueKind.True;
        if (leaving && Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_link WHERE slot_name = $1", SpiParameter.Create(slot)).Count == 0)
        {
            return "{}";
        }

        // A slot that isn't streamed yet, which a subscription's first exchanges can find before its apply worker
        // streams it, is no failure: the subscriber asks again soon. Nor is one that isn't there, as before the apply
        // worker makes it, or once the subscription is dropped, which the subscriber logs if it lasts, so that this node
        // logs no error for it; one that a subscriber leaves must be there.
        switch (Authenticate(slot, senderId))
        {
            case null when !leaving:
                return """{"streamed":false,"slot":false}""";
            case null:
                throw new PgException("42501", $"no worf replication slot \"{slot}\" carries this sequence range exchange");
            case false:
                return """{"streamed":false}""";
            default:
                break;
        }

        TakeTurn();
        if (leaving)
        {
            _ = Worf.Execute("DELETE FROM worf.sequence_range_link WHERE slot_name = $1", SpiParameter.Create(slot));
            SequenceRanges.ForgetNeighbor(senderId);
            return "{}";
        }

        // Backends reload their blocks, which an activation or a grant may have changed, once this commits.
        _ = Process(node, root, senderId, root.GetProperty("node_name").GetWorfString() ?? "", true);
        SequenceRanges.ChangedOnCommit();
        (string reply, List<Guid> refusals) = Build(node, senderId, null);
        AcknowledgeRefusals(refusals);
        return reply;
    }

    /// <summary>
    /// Ties a call to the subscription whose slot it names. The slot must be worf's, in this database, and streamed by
    /// the calling role, or, while nothing streams it, bound to that role. The first call through a slot binds it to the
    /// node that makes it and to the role that streams it, and a call from another role that streams it binds it to
    /// that role. A node speaks through the slots of one role, and a role, unless it is a superuser, for one node, the
    /// first it calls for, for good, whatever becomes of its slots, until its row in <c>worf.sequence_range_role</c> is
    /// deleted, so that one subscriber's role can't speak for another node: a provider tells its subscribers apart by
    /// their roles alone. A node re-created under another name, with the same subscription name and role, takes over
    /// the slot its old node was bound to once the role may speak for it, or, for a superuser, once the old node has
    /// been silent for as long as a neighbor counts as in touch, and the old node is forgotten.
    /// </summary>
    /// <returns>Whether the slot is streamed or bound, false while it's neither, or null when there's no such slot.</returns>
    private static bool? Authenticate(string slot, uint senderId)
    {
        uint role;
        unsafe
        {
            role = NativeMethods.GetSessionUserId();
        }

        SpiParameter name = SpiParameter.Create(slot);
        WorfRows slots = Worf.SelectCatalog("""
            SELECT s.active_pid IS NOT NULL, a.usesysid
            FROM pg_catalog.pg_replication_slots s LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid = s.active_pid
            WHERE s.slot_name = $1 AND s.plugin = 'worf' AND s.database = pg_catalog.current_database()
            """, name);
        if (slots.Count == 0)
        {
            return null;
        }

        bool active = slots[0].Get<bool>(0);
        WorfRows link = Worf.SelectCatalog("SELECT node_id, role_id FROM worf.sequence_range_link WHERE slot_name = $1", name);
        if (!active && link.Count == 0)
        {
            return false;
        }

        if (active ? slots[0].Get<uint?>(1) != role : link[0].Get<uint>(1) != role)
        {
            throw new PgException("42501", active ? $"replication slot \"{slot}\" is streamed by another role than this sequence range exchange's"
                : $"replication slot \"{slot}\" is bound to another role than this sequence range exchange's");
        }

        bool superuser;
        unsafe
        {
            superuser = NativeMethods.superuser_arg(role);
        }

        // A role speaks for the node it first called for, which a dropped slot's link leaves recorded. A role dropped
        // since leaves its oid to the next role that takes it.
        if (!superuser)
        {
            WorfRows spoken = Worf.SelectCatalog(
                "SELECT node_id, pg_catalog.pg_get_userbyid(role_id)::text FROM worf.sequence_range_role WHERE role_id = $1", SpiParameter.Create(role));
            if (spoken.Count > 0 && spoken[0].Get<uint>(0) != senderId)
            {
                throw new PgException("42501", $"role \"{spoken[0].Get<string>(1)}\" already speaks for node {spoken[0].Get<uint>(0)} here",
                    hint: "Give each subscriber node a role of its own in its provider connection strings.");
            }

            if (spoken.Count == 0)
            {
                _ = Worf.Execute("DELETE FROM worf.sequence_range_role r WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles a WHERE a.oid = r.role_id)");
                _ = Worf.Execute("INSERT INTO worf.sequence_range_role VALUES ($1, $2)", SpiParameter.Create(role), SpiParameter.Create(senderId));
            }
        }

        // A slot bound to another node passes to this one, as a node re-created under another name takes its old node's
        // subscription name: once the role may speak for this node, or for a superuser, once the old node is silent.
        if (link.Count > 0 && link[0].Get<uint>(0) != senderId)
        {
            uint old = link[0].Get<uint>(0);
            if (link[0].Get<uint>(1) != role || (superuser && !Silent(old)))
            {
                throw new PgException("42501", $"replication slot \"{slot}\" belongs to another node than this sequence range exchange's");
            }

            _ = Worf.Execute("UPDATE worf.sequence_range_link SET node_id = $2 WHERE slot_name = $1", name, SpiParameter.Create(senderId));
            SequenceRanges.ForgetNeighbor(old);
            WorfLog.Log($"node {senderId} takes over replication slot \"{slot}\" from node {old}, which this node forgets");
            return true;
        }

        if (link.Count > 0 && link[0].Get<uint>(1) == role)
        {
            return true;
        }

        // A slot bound already, whose subscriber's connection string names another role now, takes that role, as long as
        // the role speaks for no other node. Its node's other slots keep theirs.
        WorfRows bound = Worf.SelectCatalog("""
            SELECT node_id, role_id, pg_catalog.pg_get_userbyid(role_id)::text FROM worf.sequence_range_link
            WHERE (node_id = $1 OR role_id = $2) AND slot_name <> $3
            """, SpiParameter.Create(senderId), SpiParameter.Create(role), name);
        for (int i = 0; i < bound.Count; i++)
        {
            if (link.Count == 0 && bound[i].Get<uint>(0) == senderId && bound[i].Get<uint>(1) != role)
            {
                throw new PgException("42501", $"node {senderId} speaks through the slots of role \"{bound[i].Get<string>(2)}\" here, not this sequence range exchange's role");
            }

            if (bound[i].Get<uint>(0) != senderId && !superuser)
            {
                throw new PgException("42501", $"role \"{bound[i].Get<string>(2)}\" already speaks for node {bound[i].Get<uint>(0)} here",
                    hint: "Give each subscriber node a role of its own in its provider connection strings.");
            }
        }

        _ = link.Count > 0
            ? Worf.Execute("UPDATE worf.sequence_range_link SET role_id = $2 WHERE slot_name = $1", name, SpiParameter.Create(role))
            : Worf.Execute("INSERT INTO worf.sequence_range_link VALUES ($1, $2, $3)", name, SpiParameter.Create(senderId), SpiParameter.Create(role));

        return true;
    }

    /// <summary>
    /// Checks whether a node has been silent here for as long as a neighbor counts as in touch: neither its calls nor its
    /// answers, nor its reports that others relay, reached this node.
    /// </summary>
    private static bool Silent(uint nodeId)
        => Worf.ReadScalar<bool>("""
            SELECT coalesce(max(worf.sequence_range_heard(node_id, false, reported_at, false)), worf.sequence_range_heard($1, false, '-infinity', false))
                < now() - $2::interval
            FROM worf.sequence_range_node WHERE node_id = $1
            """, SpiParameter.Create(nodeId), SpiParameter.Create(SequenceRanges.StaleAfter()));

    /// <summary>
    /// Tells a subscription's provider, as the subscription is dropped, that this node leaves the subscription's slot, so
    /// that the provider's sequence range fences no longer wait on it. A provider that can't be told, or manages no
    /// sequence, is left as it is.
    /// </summary>
    /// <param name="conn">A connection to the provider.</param>
    /// <param name="node">This node.</param>
    /// <param name="sub">The subscription.</param>
    internal static void Leave(WorfConnection conn, WorfLocalNode node, WorfSubscription sub)
    {
        try
        {
            if (!Rpc.RemoteFunctionExists(conn, "worf", "sequence_range_exchange", 1, null))
            {
                return;
            }

            using MemoryStream stream = new();
            using (Utf8JsonWriter writer = new(stream))
            {
                writer.WriteStartObject();
                writer.WriteNumber("node_id", node.Node.Id);
                writer.WriteString("node_name", WorfJson.Encode(node.Node.Name));
                writer.WriteString("slot", WorfJson.Encode(sub.SlotName));
                writer.WriteBoolean("leave", true);
                writer.WriteEndObject();
            }

            conn.Execute($"SET statement_timeout = {SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)}");
            _ = conn.Query("SELECT worf.sequence_range_exchange(" + Worf.QuoteLiteral(Encoding.UTF8.GetString(stream.ToArray())) + ")", 1);
            conn.Execute("SET statement_timeout = 0");
        }
        catch (Exception error) when (error is PgException or PgRemoteException)
        {
            WorfLog.Log($"could not tell node {sub.Origin.Name} that this node leaves slot \"{sub.SlotName}\", so its sequence range fences may wait on this node: {error.Message}");
        }
    }

    /// <summary>
    /// Exchanges with the provider of each enabled subscription whose turn has come, or with which something is
    /// waiting: a request, a grant or an acknowledgement, less often while those exchanges change nothing. The worker
    /// waits on no provider: it starts each exchange's connection, if it keeps none for the provider, and its calls, and
    /// takes them on as their sockets let it, this round or a later one, so that a provider that never answers, or
    /// answers slowly, holds up none of the exchanges with the others. Each call's message is made in a transaction of
    /// its own, and its reply taken in in another, committed before the next call, so that a block this node grants or
    /// receives is never sent on, or acknowledged, before it is committed, and no row this node locks waits on another
    /// node.
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
            s_waits.Clear();
        }

        SequenceRangeExchangeWorker.CheckWatcher();
        (WorfLocalNode Node, List<(WorfSubscription Sub, string? Waiting, bool Fenced)> Due)? links = PgBackgroundWorker.RunTransaction(DueLinks);
        if (links is not (WorfLocalNode node, List<(WorfSubscription Sub, string? Waiting, bool Fenced)> due))
        {
            foreach (uint subId in s_pending.Keys.ToList())
            {
                Abandon(subId);
            }

            return false;
        }

        // The exchanges under way go as far as they can, and those that ended are taken in; then the links due that had
        // none under way start theirs, one at a time with each provider, which takes this node's calls in the order of
        // the WAL positions they carry. A link whose exchange ends now is due again by the delay that set, next round.
        bool changed = TakeExchanges(node);
        foreach ((WorfSubscription sub, string? waiting, bool fenced) in due.Where(link => !s_pending.ContainsKey(link.Sub.Id)))
        {
            if (s_pending.Values.Any(other => other.Sub.Origin.Id == sub.Origin.Id))
            {
                continue;
            }

            // A worker asked to stop, or whose watcher stopped, starts no more exchanges.
            if (Worker.GotSigterm || SequenceRangeExchangeWorker.Unwatched)
            {
                break;
            }

            _ = s_noSlot.Remove(sub.Id);
            WorfPendingExchange pending = new(sub, waiting, fenced, Worker.GetCurrentTimestamp());
            s_pending[sub.Id] = pending;
            WorfLog.Debug2($"sequence range exchange with node {sub.Origin.Name} starting");
            if (s_connections.TryGetValue(sub.Id, out (WorfConnection Conn, string Dsn, int Timeout) kept) && kept.Dsn == sub.OriginIf.Dsn &&
                kept.Timeout == SequenceRanges.ExchangeTimeout)
            {
                pending.Reused = true;
                Call(node, pending, kept.Conn);
            }
            else
            {
                Connect(pending);
            }
        }

        bool took = TakeExchanges(node);
        return changed || took;
    }

    /// <summary>
    /// Waits up to a time for the latch, or, while exchanges are under way, for their sockets, taking each on as its
    /// socket lets it, and returns once one has a result to take in, or has run out of time.
    /// </summary>
    /// <param name="milliseconds">The time.</param>
    internal static void Wait(int milliseconds)
    {
        if (s_pending.Count == 0)
        {
            _ = PgBackgroundWorker.Wait(TimeSpan.FromMilliseconds(milliseconds));
            return;
        }

        long until = Worker.GetCurrentTimestamp() + (milliseconds * 1000L);
        while (!Worker.GotSigterm)
        {
            long now = Worker.GetCurrentTimestamp();
            if (now >= until || s_pending.Values.Any(pending => Ready(pending, now)))
            {
                return;
            }

            // One exchange's socket is waited on at a time, for a moment while others are under way.
            WorfPendingExchange waited = s_pending.Values.First();
            long left = Math.Min(until, Deadline(waited)) - now;
            long slice = Math.Max((s_pending.Count > 1 ? Math.Min(left, 20_000L) : left) / 1000L, 1);
            bool latched = waited.Query is WorfPendingQuery query ? query.Wait(slice) : waited.Probe?.Wait(slice) ?? false;
            if (latched)
            {
                PgInterrupts.Check();
                return;
            }

            foreach (WorfPendingExchange pending in s_pending.Values)
            {
                pending.Probe?.Advance();
                pending.Query?.Advance();
            }
        }
    }

    // Whether an exchange under way has something to take in: a connection made or failed, a call's result, or its time
    // run out.
    private static bool Ready(WorfPendingExchange pending, long now)
        => now >= Deadline(pending) || pending.Probe is { Done: true } || pending.Query is { Done: true } || (pending.Probe is null && pending.Query is null);

    // When the step an exchange is at runs out of time.
    private static long Deadline(WorfPendingExchange pending)
        => pending.Query?.Deadline ?? pending.Probe?.Deadline ?? long.MinValue;

    /// <summary>
    /// Takes each exchange under way as far as it goes, and ends those that are done.
    /// </summary>
    /// <returns>Whether one that ended received a block.</returns>
    private static bool TakeExchanges(WorfLocalNode node)
    {
        List<bool> received = [.. s_pending.Values.ToList().Select(pending => TakeExchange(node, pending))];
        return received.Contains(true);
    }

    // Takes an exchange under way as far as it goes, and ends it if it's done, returning whether it ended having received
    // a block.
    private static bool TakeExchange(WorfLocalNode node, WorfPendingExchange pending)
    {
        if (Step(node, pending) is not bool received)
        {
            return false;
        }

        _ = s_pending.Remove(pending.Sub.Id);
        return received;
    }

    /// <summary>
    /// Starts an exchange's connection to its provider, with the exchange's time, as a probe of a provider whose last call
    /// ran out of time. Its connection string is read as a call's is.
    /// </summary>
    private static void Connect(WorfPendingExchange pending)
    {
        WorfSubscription sub = pending.Sub;
        Disconnect(sub.Id);
        pending.Probing = s_timedOut.Contains(sub.Id);
        int timeout = SequenceRanges.ExchangeTimeout;
        (string conninfo, string applicationName) = Worf.ConnectionString(sub.OriginIf.Dsn, sub.Name, "range");
        pending.Probe = WorfProbe.Start(conninfo, applicationName, $"node {pending.Sub.Origin.Name}", Worf.SessionSetup($"SET statement_timeout = {timeout.ToString(CultureInfo.InvariantCulture)}"),
            Worker.GetCurrentTimestamp() + (timeout * 1000L));
        WorfLog.Debug2($"sequence range exchange with node {sub.Origin.Name} connecting{(pending.Probing ? ", as a probe" : "")}");
    }

    /// <summary>
    /// Makes an exchange's call: this node's message, made in a transaction of its own, is sent, and the reply is taken
    /// in once it has come. The message only reads, so that this node holds no row lock while the provider works, which
    /// may wait on that provider's own calls to other nodes.
    /// </summary>
    private static void Call(WorfLocalNode node, WorfPendingExchange pending, WorfConnection conn)
    {
        WorfSubscription sub = pending.Sub;
        try
        {
            // This node's calls carry its WAL position in order. How far this node applied the provider's changes is read
            // before the call, so that the provider's answer, which it writes later, can't be behind it unless the
            // provider went back.
            (ulong applied, string message, List<Guid> refusals) = PgBackgroundWorker.RunTransaction(() =>
            {
                _ = Worf.Read("SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('worf.sequence_range_exchange'))", 0);
                ulong applied = Applied(sub);
                (string message, List<Guid> refusals) = Build(node, sub.Origin.Id, sub.SlotName);
                return (applied, message, refusals);
            });
            pending.Applied = applied;
            pending.Refusals = refusals;
            pending.Calls++;
            pending.Query = WorfPendingQuery.Start(conn, "SELECT worf.sequence_range_exchange(" + Worf.QuoteLiteral(message) + ")",
                Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 1000L));
        }
        catch (PgException error)
        {
            pending.Query = null;
            Failed(pending, error.Message, false, error.Detail);
        }
    }

    /// <summary>
    /// Takes an exchange as far as it goes now: a connection made goes on to its call, a call answered is taken in, and,
    /// for a grant this node made from the provider's request, which goes back in a second call once this node has
    /// committed it, makes that call. A kept connection that fails, as once the provider restarted, is made again, once,
    /// before the exchange counts as failed.
    /// </summary>
    /// <returns>Null while the exchange goes on, or once it ends, whether this node received a block.</returns>
    private static bool? Step(WorfLocalNode node, WorfPendingExchange pending)
    {
        WorfSubscription sub = pending.Sub;
        long now = Worker.GetCurrentTimestamp();
        if (pending.Probe is WorfProbe probe)
        {
            probe.Advance();
            if (!probe.Done && now < probe.Deadline)
            {
                return null;
            }

            pending.Probe = null;
            if (!probe.Answered)
            {
                string failure = probe.Failure ?? $"node {sub.Origin.Name} didn't answer within {SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)} ms";
                probe.Dispose();
                Failed(pending, failure, probe.Failure is null, null);
                return false;
            }

            // A provider whose last call ran out of time keeps its delay, which a call that fails again doubles.
            WorfConnection conn = probe.Take();
            s_connections[sub.Id] = (conn, sub.OriginIf.Dsn, SequenceRanges.ExchangeTimeout);
            if (pending.Probing)
            {
                _ = s_timedOut.Remove(sub.Id);
                WorfLog.Debug1($"sequence range exchange with node {sub.Origin.Name} answered a probe, and is called again");
            }

            Call(node, pending, conn);
        }

        if (pending.Query is not WorfPendingQuery query)
        {
            return pending.Probe is null ? false : null;
        }

        query.Advance();
        if (!query.Done && now < query.Deadline)
        {
            return null;
        }

        pending.Query = null;
        if (!query.Done)
        {
            Disconnect(sub.Id);
            Failed(pending, $"node {sub.Origin.Name} didn't answer within {SequenceRanges.ExchangeTimeout.ToString(CultureInfo.InvariantCulture)} ms", true, null);
            return false;
        }

        if (query.Failure is string failed)
        {
            if (query.Broken && pending.Reused)
            {
                pending.Reused = false;
                Connect(pending);
                return null;
            }

            Disconnect(sub.Id);

            // A provider without the exchange function manages no sequence.
            if (query.SqlState == UndefinedFunction)
            {
                return Succeeded(pending);
            }

            Failed(pending, failed, query.SqlState == PgSqlStates.QueryCanceled, null);
            return false;
        }

        try
        {
            (bool granted, bool received) = PgBackgroundWorker.RunTransaction(() => TakeReply(node, pending, query.Value ?? "{}"));
            pending.Received |= received;
            pending.Granted |= granted;
            if (received)
            {
                _ = s_acksWaiting.Add(sub.Id);
            }
            else if (pending.Calls == 1)
            {
                _ = s_acksWaiting.Remove(sub.Id);
            }

            if (granted && pending.Calls < 2 && s_connections.TryGetValue(sub.Id, out (WorfConnection Conn, string Dsn, int Timeout) kept))
            {
                Call(node, pending, kept.Conn);
                return pending.Query is null ? false : null;
            }
        }
        catch (Exception error) when (error is PgException or PgRemoteException)
        {
            Disconnect(sub.Id);
            Failed(pending, error.Message, false, (error as PgException)?.Detail);
            return false;
        }

        return Succeeded(pending);
    }

    /// <summary>
    /// Takes in a call's reply, in the caller's transaction, once this node's other exchanges have taken theirs in.
    /// </summary>
    private static (bool Granted, bool Received) TakeReply(WorfLocalNode node, WorfPendingExchange pending, string reply)
    {
        WorfSubscription sub = pending.Sub;
        TakeTurn();

        // A reply that comes once the subscription is dropped or disabled, as when the provider answered only after
        // taking in the drop's word that this node leaves its slot, is no news of a neighbor this node still links to, so
        // it's taken in as one that never came.
        if (!WorfNode.GetNodeSubscriptions(node.Node.Id, false).Exists(current => current.Id == sub.Id && current.Enabled))
        {
            return (false, false);
        }

        AcknowledgeRefusals(pending.Refusals);
        using JsonDocument document = JsonDocument.Parse(reply);
        if (document.RootElement.TryGetProperty("streamed", out JsonElement streamed) && streamed.ValueKind == JsonValueKind.False)
        {
            // A provider without the slot answers so, rather than failing the call, which would log an error there too.
            if (document.RootElement.TryGetProperty("slot", out JsonElement slot) && slot.ValueKind == JsonValueKind.False)
            {
                _ = s_noSlot.Add(sub.Id);
                throw new PgException("42501", $"no worf replication slot \"{sub.SlotName}\" carries this sequence range exchange");
            }

            _ = s_notStreamed.Add(sub.Id);
            return (false, false);
        }

        _ = s_notStreamed.Remove(sub.Id);
        return Process(node, document.RootElement, sub.Origin.Id, sub.Origin.Name, false, pending.Applied);
    }

    /// <summary>
    /// Notes an exchange that ended well, or found a provider that manages nothing.
    /// </summary>
    /// <returns>Whether this node received a block.</returns>
    private static bool Succeeded(WorfPendingExchange pending)
    {
        WorfSubscription sub = pending.Sub;
        long now = pending.StartedAt;
        WorfLog.Debug2($"sequence range exchange with node {sub.Origin.Name} done");

        // A provider whose slot isn't streamed yet is asked again after a check interval.
        if (s_notStreamed.Contains(sub.Id))
        {
            s_retries[sub.Id] = (Worker.GetCurrentTimestamp() + (SequenceRanges.CheckInterval * 1000L), SequenceRanges.CheckInterval * 1000L);
            WorfLog.Debug1($"sequence range exchange with node {sub.Origin.Name} waits for replication slot \"{sub.SlotName}\" to be streamed");
            return false;
        }

        s_exchangedAt[sub.Id] = now;
        _ = s_noSlotOnce.Remove(sub.Id);
        _ = s_retries.Remove(sub.Id);
        _ = s_timedOut.Remove(sub.Id);

        // An exchange that changed nothing while something waits makes the next one wait longer, up to the exchange
        // interval, since the neighbor may not answer it for long, as when a root can't start or a fence waits on a node
        // that is down, though no more than ten check intervals while the fence of the provider, or of an allocator behind
        // it, holds, or a request waits on a pool holder.
        if (pending.Received || pending.Granted || pending.Waiting is null)
        {
            _ = s_waits.Remove(sub.Id);
        }
        else
        {
            long delay = s_waits.TryGetValue(sub.Id, out (string What, long At, long Delay) wait) && wait.What == pending.Waiting ? wait.Delay * 2 : 0;
            long most = SequenceRanges.ExchangeInterval * 1000L;
            if (pending.Fenced)
            {
                most = Math.Min(most, FencedAskEvery * SequenceRanges.CheckInterval * 1000L);
            }

            delay = Math.Min(Math.Max(delay, SequenceRanges.CheckInterval * 1000L), most);
            s_waits[sub.Id] = (pending.Waiting, now + delay, delay);
        }

        return pending.Received;
    }

    /// <summary>
    /// Notes an exchange that failed, tried again after a delay that doubles, up to the exchange interval, or half the
    /// exchange timeout while this node waits on the neighbor, as for a block, so that inserts waiting at a block's end,
    /// up to twice the timeout, get it soon after the neighbor is back. One that ran out of time, or a probe of a
    /// provider whose last call did, backs off from ten seconds up to a minute, or the exchange interval if that is
    /// longer, whatever waits on the neighbor, and is probed next.
    /// </summary>
    private static void Failed(WorfPendingExchange pending, string failure, bool timedOut, string? detail)
    {
        WorfSubscription sub = pending.Sub;
        bool slow = timedOut || pending.Probing;
        long most = pending.Waiting is not null && !slow
            ? Math.Min(SequenceRanges.ExchangeInterval * 1000L, SequenceRanges.ExchangeTimeout * 500L)
            : slow ? Math.Max(SequenceRanges.ExchangeInterval * 1000L, TimedOutBackoff)
            : SequenceRanges.ExchangeInterval * 1000L;
        if (slow)
        {
            _ = s_timedOut.Add(sub.Id);
        }

        // The delay counts from the exchange's end, so that one that ran out of time waits it out too.
        long delay = s_retries.TryGetValue(sub.Id, out (long At, long Delay) retry) ? retry.Delay * 2 : 0;
        long least = slow ? Math.Min(TimedOutFirstBackoff, most) : SequenceRanges.CheckInterval * 1000L;
        delay = Math.Min(Math.Max(delay, Math.Max(least, SequenceRanges.CheckInterval * 1000L)), Math.Max(most, SequenceRanges.CheckInterval * 1000L));
        s_retries[sub.Id] = (Worker.GetCurrentTimestamp() + delay, delay);
        WorfLog.Debug1($"sequence range exchange with node {sub.Origin.Name} is made again in {(delay / 1000).ToString(CultureInfo.InvariantCulture)} ms");

        // While the subscription's first sync hasn't finished, its slot may not be there yet, or be made again, so a
        // failure then is no news, and neither is the first exchange that finds the slot gone, as the subscription may be
        // being dropped, which drops it before the drop commits.
        PgDiagnostic diagnostic = new($"sequence range exchange with node {sub.Origin.Name} failed: {failure}")
        {
            Detail = detail ?? (pending.Probing && !timedOut ? "The probe of a node whose last call ran out of time failed." : null),
        };
        bool noSlot = s_noSlot.Contains(sub.Id);
        if (!noSlot)
        {
            _ = s_noSlotOnce.Remove(sub.Id);
        }

        if ((noSlot && s_noSlotOnce.Add(sub.Id)) ||
            PgBackgroundWorker.RunTransaction(() => Sync.GetSubscriptionSyncStatus(sub.Id, true) is { } sync && sync.Status != Sync.SyncStatusReady))
        {
            WorfLog.Debug1(diagnostic.Message);
        }
        else
        {
            WorfLog.Warning(diagnostic);
        }
    }

    // Drops an exchange under way, and its connection.
    private static void Abandon(uint subId)
    {
        if (s_pending.Remove(subId, out WorfPendingExchange? pending))
        {
            pending.Probe?.Dispose();
        }

        Disconnect(subId);
    }

    /// <summary>
    /// Gets a value indicating whether exchanges are under way, whose answers the worker waits for.
    /// </summary>
    internal static bool Pending => s_pending.Count > 0;

    /// <summary>
    /// Gets whether a subscription was still making its slot at the last look at the links due, so that its first
    /// exchange waits a check interval rather than an exchange interval.
    /// </summary>
    internal static bool Starting { get; private set; }

    /// <summary>
    /// Lists the links whose turn has come, with what each waits on, and whether the provider is fenced.
    /// </summary>
    private static (WorfLocalNode Node, List<(WorfSubscription Sub, string? Waiting, bool Fenced)> Due)? DueLinks()
    {
        if (WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
        {
            return null;
        }

        List<(WorfSubscription Sub, string? Waiting, bool Fenced)> due = [];
        long now = Worker.GetCurrentTimestamp();
        Starting = false;
        List<WorfSubscription> enabled = [.. WorfNode.GetNodeSubscriptions(node.Node.Id, false).Where(sub => sub.Enabled)];

        // The connections kept for subscriptions dropped or disabled since go, and so do their probes.
        foreach (uint gone in s_connections.Keys.Where(id => !enabled.Exists(sub => sub.Id == id)).ToList())
        {
            Disconnect(gone);
        }

        foreach (uint gone in s_pending.Keys.Where(id => !enabled.Exists(sub => sub.Id == id)).ToList())
        {
            Abandon(gone);
        }

        _ = s_noSlotOnce.RemoveWhere(id => !enabled.Exists(sub => sub.Id == id));

        // A neighbor this node no longer exchanges with stays forgotten, though a reply of its came as its subscription
        // was dropped, and was taken in after the drop forgot it, as once it waited for a range row the drop held.
        if (Worf.CatalogOid("sequence_range_absent") != 0)
        {
            foreach (WorfRow stale in Worf.Read("""
                SELECT DISTINCT n.via_node FROM worf.sequence_range_node n
                WHERE n.via_node <> $1 AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_link k WHERE k.node_id = n.via_node)
                    AND NOT EXISTS (SELECT 1 FROM worf.subscription s WHERE s.sub_origin = n.via_node AND s.sub_target = $1)
                """, 0, SpiParameter.Create(node.Node.Id)))
            {
                SequenceRanges.ForgetNeighbor(stale.Get<uint>(0));
            }
        }

        foreach (WorfSubscription sub in enabled.Where(sub => !s_pending.ContainsKey(sub.Id)))
        {
            // The neighbor ties each exchange to the subscription's slot, which a new subscription makes as it starts,
            // and which its apply worker has to have streamed.
            WorfSyncStatus? sync = Sync.GetSubscriptionSyncStatus(sub.Id, true);
            if (s_notStreamed.Contains(sub.Id))
            {
                Starting = true;
            }

            if (sync is null || sync.Status == Sync.SyncStatusInit)
            {
                Starting = true;
                continue;
            }

            bool retrying = s_retries.TryGetValue(sub.Id, out (long At, long Delay) retry) && now < retry.At;
            (string? waiting, bool fenced) = Waiting(sub.Origin.Id);
            bool asks = waiting is not null &&
                (!s_waits.TryGetValue(sub.Id, out (string What, long At, long Delay) wait) || wait.What != waiting || now >= wait.At);

            // A statement that waits to draw from a block the neighbor granted, until this server hears from it since it
            // started, makes the exchange due, once a second at most, and a failed one's retry due half
            // worf.sequence_range_timeout after it ended, if its delay is longer, so that the statement, which waits up
            // to twice that, gets an exchange soon after the neighbor is back.
            bool exchanged = s_exchangedAt.TryGetValue(sub.Id, out long at);
            bool wanted = (!exchanged || now - at >= 1_000_000) && SequenceRangeHeard.Wanted(sub.Origin.Id, now - 2_000_000);
            bool cut = wanted && retrying && now >= retry.At - retry.Delay + Math.Min(retry.Delay, SequenceRanges.ExchangeTimeout * 500L);

            if ((!retrying || cut) &&
                (!exchanged || now - at >= SequenceRanges.ExchangeInterval * 1000L || s_acksWaiting.Contains(sub.Id) || asks || wanted))
            {
                due.Add((sub, waiting, fenced));
            }
        }

        return (node, due);
    }

    /// <summary>
    /// Closes the connection kept for a subscription's provider, if there is one.
    /// </summary>
    private static void Disconnect(uint subId)
    {
        if (s_connections.Remove(subId, out (WorfConnection Conn, string Dsn, int Timeout) kept))
        {
            kept.Conn.Dispose();
        }
    }

    /// <summary>
    /// Waits for this node's other exchanges to finish taking in their messages, so that no two lock its ranges in
    /// different orders, as when a sequence's name names another sequence on a neighbor. A call takes its turn only once
    /// the neighbor has answered, so that no node waits on another's turn while that node waits on its own.
    /// </summary>
    private static void TakeTurn() => _ = Worf.Read("SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('worf.sequence_range_exchange_turn'))", 0);

    /// <summary>
    /// Gets the furthest WAL position of the provider whose changes this node has applied through a subscription, under
    /// the subscription's origin or one of its relay origins. Replication origins record it with each commit, so it
    /// survives a restart or crash of this node, and an idle provider never moves it. Before its first commit, a
    /// subscription that worf_create_subscriber made has applied up to the position its sync status holds, which its
    /// apply worker starts from.
    /// </summary>
    /// <returns>The position, or zero.</returns>
    private static ulong Applied(WorfSubscription sub)
    {
        ulong applied = WorfNode.RelayOrigins(sub.SlotName).Select(relay => relay.Id).Append(PgReplicationOrigin.Find(sub.SlotName) ?? 0)
            .Where(origin => origin != 0)
            .Select(origin => PgReplicationOrigin.GetProgress(origin, false))
            .Where(progress => progress.IsValid)
            .Select(progress => progress.Value)
            .DefaultIfEmpty()
            .Max();
        return applied != 0 ? applied : Sync.GetSubscriptionSyncStatus(sub.Id, true)?.Statuslsn ?? 0;
    }

    /// <summary>
    /// Gets the WAL position a neighbor's answer can't be behind of: how far this node applied its changes, unless the
    /// neighbor already reset for going back past that, as it reports for each sequence.
    /// </summary>
    private static ulong AppliedFloor(ulong applied, JsonElement entry)
        => Text(entry, "reset_done") is string done && Worf.TryParseLsn(done, out ulong reset) && applied <= reset ? 0 : applied;

    /// <summary>
    /// Records refusals as delivered: a refusal needs no acknowledgement, since a request that lost it is sent again
    /// and refused again.
    /// </summary>
    private static void AcknowledgeRefusals(List<Guid> refusals)
    {
        if (refusals.Count > 0)
        {
            _ = Worf.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = ANY ($1) AND state = 'sent'",
                SpiParameter.Create(refusals.ToArray()));
        }
    }

    /// <summary>
    /// Lists what this node waits on with a neighbor: its requests open with the neighbor, its grants the neighbor
    /// hasn't acknowledged, and resets to ask of it or acknowledge, and whether something it can't see as it changes
    /// holds it up: a fenced allocator, which waits on its neighbors' reports, the neighbor or one that a request open
    /// with the neighbor waits on in turn, or a pool holder, whose own parent, as a fence that begins and ends between
    /// two of this node's exchanges, may hold up its answer.
    /// </summary>
    /// <returns>What waits, or null when nothing does, and whether something it can't see holds it up.</returns>
    private static (string? What, bool Fenced) Waiting(uint neighbor)
    {
        WorfRows waiting = Worf.Read("""
            SELECT string_agg(what, ' ' ORDER BY what), coalesce(bool_or(fenced), false) FROM (
                SELECT q.request_id::text, EXISTS (SELECT 1 FROM worf.sequence_range_node n WHERE n.seqoid = q.seqoid AND n.state = 'fenced')
                    OR EXISTS (SELECT 1 FROM worf.sequence_range r WHERE r.seqoid = q.seqoid AND r.owner_node <> q.parent_node)
                FROM worf.sequence_range_request q WHERE q.parent_node = $1 AND q.state = 'open'
                UNION ALL
                SELECT grant_id::text, false FROM worf.sequence_range_grant WHERE grantee_id = $1 AND state = 'sent'
                UNION ALL
                SELECT seqoid::oid::text || ' ' || coalesce(state, '') || ' ' || coalesce(reset_lsn::text, ''), state = 'fenced' FROM worf.sequence_range_node
                WHERE node_id = $1 AND (state = 'fenced' OR reset_lsn IS NOT NULL)
                UNION ALL
                SELECT seqoid::oid::text || ' fenced ' || fenced_at::text, false FROM worf.sequence_range WHERE fenced_at IS NOT NULL) w(what, fenced)
            """, 1, SpiParameter.Create(neighbor));
        return (waiting[0].Get<string?>(0), waiting[0].Get<bool>(1));
    }

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
                timeline = SequenceRanges.Timeline();
                insert = NativeMethods.GetXLogInsertRecPtr();
            }

            writer.WriteStartObject();
            writer.WriteNumber("node_id", node.Node.Id);
            writer.WriteString("node_name", WorfJson.Encode(node.Node.Name));
            writer.WriteString("sysid", sysid.ToString(CultureInfo.InvariantCulture));
            writer.WriteNumber("timeline", timeline);
            writer.WriteString("wal_lsn", Worf.FormatLsn(insert));
            if (slot is not null)
            {
                writer.WriteString("slot", WorfJson.Encode(slot));
            }

            // When this server started, and when the neighbor's did, as it last said, which tells it that a message
            // this server makes from now on was made knowing it's back.
            writer.WriteNumber("started_at", SequenceRangeHeard.Started);
            writer.WriteNumber("heard_started_at", PendingStart(neighbor) ?? SequenceRangeHeard.StartedOf(neighbor));

            writer.WriteStartArray("sequences");
            // An epoch this node owned of a sequence it dropped goes out as disabled, with nothing else.
            WorfRows sequences = Worf.SelectCatalog("""
                SELECT * FROM (
                    SELECT r.seqoid::oid, pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) AS name, r.owner_node,
                        r.epoch, r.range_size, r.max_range_size, r.distance, r.high_water, r.watermark, r.parent_node,
                        CASE WHEN r.fenced_at IS NOT NULL THEN 'fenced' ELSE r.state END, r.root_start, r.root_end,
                        CASE WHEN r.distance IS NULL THEN NULL WHEN r.parent_node IS NULL THEN '{}'::pg_catalog.int8[]
                            ELSE ARRAY[r.parent_node::pg_catalog.int8] || coalesce((SELECT p.route FROM worf.sequence_range_node p
                                WHERE p.seqoid = r.seqoid AND p.node_id = r.parent_node AND p.via_node = p.node_id), '{}') END
                    FROM worf.sequence_range r
                        JOIN pg_catalog.pg_class c ON c.oid = r.seqoid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    UNION ALL
                    SELECT NULL, d.name, d.owner_node, d.epoch, d.range_size, d.max_range_size, 0, NULL, d.watermark, NULL, 'disabled', d.root_start, d.root_end,
                        '{}'::pg_catalog.int8[]
                    FROM worf.sequence_range_dropped d
                    WHERE d.owned AND NOT EXISTS (SELECT 1 FROM worf.sequence_range r WHERE r.seqoid = pg_catalog.to_regclass(d.name))) s
                ORDER BY name COLLATE "C"
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
    private static void WriteSequence(Utf8JsonWriter writer, WorfRow row, uint neighbor, List<Guid> refusals)
    {
        uint? managed = row.Get<uint?>(0);
        uint seqoid = managed ?? 0;
        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter peer = SpiParameter.Create(neighbor);
        writer.WriteStartObject();
        writer.WriteString("name", WorfJson.Encode(row.Get<string>(1)));
        writer.WriteNumber("owner_id", row.Get<uint>(2));
        writer.WriteNumber("epoch", row.Get<int>(3));
        writer.WriteNumber("range_size", row.Get<long>(4));
        writer.WriteNumber("max_range_size", row.Get<long>(5));
        // A neighbor that is this node's parent hears of no distance from it, as this node's route to the owner goes
        // through the neighbor, so that the neighbor never takes this node as its own parent, as it would once it no
        // longer heard from its own, and lengthen its route through this node in turn. The others hear the route, the
        // nodes from this node's parent to the owner, so that none takes a route that goes through itself, as one
        // around a ring would once the owner is gone.
        bool poisoned = row.Get<uint?>(9) == neighbor;
        WriteNullable(writer, "distance", poisoned ? null : row.Get<int?>(6));
        if (!poisoned && row.Get<long[]?>(13) is long[] route)
        {
            writer.WriteStartArray("route");
            foreach (long hop in route)
            {
                writer.WriteNumberValue(hop);
            }

            writer.WriteEndArray();
        }

        WriteNullable(writer, "high_water", row.Get<long?>(7));
        WriteNullable(writer, "watermark", row.Get<long?>(8));
        writer.WriteString("state", row.Get<string>(10));
        WriteNullable(writer, "root_start", row.Get<long?>(11));
        WriteNullable(writer, "root_end", row.Get<long?>(12));
        if (managed is null)
        {
            writer.WriteNull("reset");
            writer.WriteNull("reset_done");
            writer.WriteBoolean("held_complete", false);
            foreach (string name in new[] { "held", "requests", "reports", "grants", "acks" })
            {
                writer.WriteStartArray(name);
                writer.WriteEndArray();
            }

            writer.WriteEndObject();
            return;
        }

        bool snowflake = seqoid == SnowflakeIds.NodeSequence();
        if (snowflake)
        {
            WriteHolds(writer, OwnHolds(seqoid), SnowflakeIds.ReservedClock());
        }


        // A reset this node asks of the neighbor, whose WAL position moved backwards, and one it honored.
        WorfRows neighborRow = Worf.SelectCatalog(
            "SELECT reset_lsn::text, reset_done::text, state = 'fenced' FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2",
            seq, peer);
        bool neighborFenced = neighborRow.Count > 0 && neighborRow[0].Get<bool?>(2) == true;
        string? askedReset = neighborRow.Count > 0 ? neighborRow[0].Get<string?>(0) : null;
        WriteNullable(writer, "reset", askedReset);
        WriteNullable(writer, "reset_done", neighborRow.Count > 0 ? neighborRow[0].Get<string?>(1) : null);

        // A fence's lift names the grants this node made the neighbor that it hasn't heard the neighbor give up, of which
        // the neighbor must hold one for the lift to reset it at once.
        if (askedReset is not null && IsLift(askedReset))
        {
            WriteArray(writer, "reset_grants", Worf.SelectCatalog("""
                SELECT grant_id FROM worf.sequence_range_grant WHERE seqoid = $1 AND grantee_id = $2 AND kind <> 'x' AND state <> 'abandoned'
                """, seq, peer), grant => writer.WriteStringValue(grant.Get<Guid>(0)));
        }

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
            writer.WriteString("node_name", WorfJson.Encode(report.Get<string>(1)));
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

        // Grants for the neighbor that it hasn't acknowledged.
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

        // The blocks this node holds of the epoch, which the neighbor's ledger has a record of unless it went back, while
        // an earlier epoch's root, as one whose sequence its owner dropped, may have none.
        WriteArray(writer, "acks", Worf.SelectCatalog("""
            SELECT block_id FROM worf.sequence_range_block
            WHERE seqoid = $1 AND from_node = $2 AND kind IN ('n', 'p', 'i') AND state IN ('next', 'active', 'open', 'imported') AND epoch = $3
            """, seq, peer, SpiParameter.Create(row.Get<int>(3))), received => writer.WriteStringValue(received.Get<Guid>(0)));
        writer.WriteEndObject();
    }

    private static void WriteArray(Utf8JsonWriter writer, string name, WorfRows rows, Action<WorfRow> write)
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
    /// <param name="applied">How far this node applied the neighbor's changes before its call, or zero.</param>
    /// <returns>Whether this node granted the neighbor a block, and whether it received one.</returns>
    private static (bool Granted, bool Received) Process(WorfLocalNode node, JsonElement root, uint senderId, string senderName, bool call,
        ulong applied = 0)
    {
        bool granted = false;
        bool received = false;
        bool learned = false;

        // The neighbor is heard from now, in shared memory, which an exchange that changes nothing else is all it writes to,
        // unless shared memory has no room for it, and then its report is written to the tables.
        bool heard = SequenceRangeHeard.Record(senderId, senderId, Worker.GetCurrentTimestamp());
        bool knowsStart = root.TryGetProperty("heard_started_at", out JsonElement heardStarted) && heardStarted.TryGetInt64(out long heardStartedAt) &&
            heardStartedAt == SequenceRangeHeard.Started;
        long? startedAt = root.TryGetProperty("started_at", out JsonElement started) && started.TryGetInt64(out long since) ? since : null;
        if (!root.TryGetProperty("sequences", out JsonElement sequences))
        {
            OnCommit(senderId, startedAt, knowsStart);
            return (false, false);
        }

        // A sequence this node is fenced on that the neighbor's message doesn't carry has no block on the neighbor.
        WorfRows fencedOn = Worf.Read("SELECT seqoid::oid FROM worf.sequence_range WHERE fenced_at IS NOT NULL", 0);
        HashSet<uint> absent = [.. Enumerable.Range(0, fencedOn.Count).Select(i => fencedOn[i].Get<uint>(0))];
        List<long> carriedSequences = [];
        bool undone = false;
        foreach (JsonElement entry in sequences.EnumerateArray())
        {
            uint? processed = Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(entry.GetProperty("name").GetWorfString()));
            if (processed is uint carried)
            {
                carriedSequences.Add(carried);
                _ = absent.Remove(carried);
            }

            // A sequence whose range row a prepared transaction holds, which it may hold across restarts, is left out, so
            // that the exchange doesn't wait on it while the other sequences' blocks wait on the exchange.
            if (processed is uint locked && PreparedHolder(locked) is string gid)
            {
                if (s_preparedHolders.Add((locked, gid)))
                {
                    WorfLog.Warning(new PgDiagnostic($"sequence {entry.GetProperty("name").GetWorfString()}: prepared transaction \"{gid}\" holds its range row, so it exchanges no blocks until the transaction ends")
                    {
                        Hint = $"Commit or roll back the prepared transaction with COMMIT PREPARED or ROLLBACK PREPARED '{gid}'.",
                    });
                }

                undone |= Text(entry, "reset") is not null;
                continue;
            }

            try
            {
                if (!heard || !Unchanged(node, root, entry, senderId, call, applied))
                {
                    PgTransaction.RunInSubtransaction(() =>
                    {
                        (bool g, bool r, bool l) = ProcessSequence(node, root, entry, senderId, senderName, call, applied);
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
                        _ = SequenceRangeHeard.Record(reported, senderId, Math.Min(at, Worker.GetCurrentTimestamp()));
                    }
                }
            }
            catch (PgException error)
            {
                undone |= Text(entry, "reset") is not null;

                // A sequence dropped here since the exchange began goes unmentioned, though one of its name was created again,
                // and one whose range row stayed locked is taken in at a later exchange.
                if (error.SqlState == "55P03")
                {
                    WorfLog.Debug1($"sequence range exchange with node {senderName} left out sequence {entry.GetProperty("name").GetWorfString()}, whose range row another transaction holds");
                }
                else if (processed is uint seqoid && SequenceRanges.SequenceExists(seqoid))
                {
                    WorfLog.Warning(new PgDiagnostic($"sequence range exchange with node {senderName} skipped sequence {entry.GetProperty("name").GetWorfString()}: {error.Message}")
                    {
                        Detail = error.Detail,
                    });
                }
            }
        }

        foreach (uint seqoid in absent)
        {
            _ = Worf.Execute("""
                INSERT INTO worf.sequence_range_absent VALUES ($1, $2, now()) ON CONFLICT (seqoid, node_id) DO UPDATE SET absent_at = excluded.absent_at
                """, SpiParameter.Create(seqoid), SpiParameter.Create(senderId));
        }

        // The neighbor's start is said back only once this node has asked all it makes of the neighbor's return: a
        // sequence left out, as one whose range row a prepared transaction holds, or whose part of the message failed,
        // may still owe the neighbor a request to reset, after a fence's lift that stopped waiting for it. And the message
        // counts as made knowing this server is back only if every request to reset it carries was taken in.
        bool asking = carriedSequences.Count > 0 && Worf.ReadScalar<bool>("""
            SELECT EXISTS (SELECT 1 FROM worf.sequence_range_absent a JOIN worf.sequence_range r ON r.seqoid = a.seqoid
                WHERE a.node_id = $1 AND a.seqoid = ANY ($2::oid[]) AND r.fenced_at IS NULL)
            """, SpiParameter.Create(senderId), SpiParameter.Create(carriedSequences.ToArray()));
        OnCommit(senderId, asking ? null : startedAt, knowsStart && !undone);

        // The neighbor's WAL position, once compared with the last one.
        if (Worf.TryParseLsn(root.GetProperty("wal_lsn").GetWorfString() ?? "", out ulong lsn))
        {
            SequenceRangeHeard.RecordLsn(senderId, ulong.Parse(root.GetProperty("sysid").GetWorfString() ?? "0", CultureInfo.InvariantCulture),
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
    /// Checks, only reading, whether one sequence of a neighbor's message would change nothing here: the neighbor
    /// reports what it reported before, relays nothing new, asks for nothing that this node can answer yet, as an
    /// owner still collecting reports can't, grants and gives up nothing, acknowledges nothing new, and leaves this
    /// node's route as it is, while neither node is fenced and this node owes the neighbor no request to reset after a
    /// fence's lift that stopped waiting for it. Such a message is only noted in shared memory, so that
    /// exchanges between idle nodes, or toward an owner whose root can't start, write nothing and use no transaction
    /// IDs.
    /// </summary>
    private static bool Unchanged(WorfLocalNode node, JsonElement root, JsonElement entry, uint senderId, bool call, ulong applied)
    {
        if (Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(entry.GetProperty("name").GetWorfString())) is not uint seqoid)
        {
            return DroppedEpochFollowed(entry) is null;
        }

        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter sender = SpiParameter.Create(senderId);
        WorfRows local = Worf.Read("""
            SELECT epoch, state, fenced_at IS NOT NULL, root_start, watermark, owner_node, parent_node, distance FROM worf.sequence_range WHERE seqoid = $1
            """, 1, seq);
        int epoch = entry.GetProperty("epoch").GetInt32();
        string? state = Text(entry, "state");
        if (local.Count == 0)
        {
            return state == "disabled" || entry.GetProperty("owner_id").GetUInt32() == node.Node.Id;
        }

        if (local[0].Get<int>(0) != epoch)
        {
            return local[0].Get<int>(0) > epoch;
        }

        // A neighbor that a fence's lift stopped waiting for is asked to reset once it's heard from again, whatever it reports.
        SequenceInfo info = SequenceRanges.GetInfo(seqoid);
        if (local[0].Get<bool>(2) || state == "fenced" || (state == "disabled" && local[0].Get<string>(1) != "disabled") ||
            Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_absent WHERE seqoid = $1 AND node_id = $2)", seq, sender) ||
            (entry.GetProperty("requests").GetArrayLength() > 0 && local[0].Get<string>(1) != "collecting" &&
                !Unanswerable(seqoid, info, epoch, entry.GetProperty("requests"), local[0].Get<uint>(5) == node.Node.Id)) ||
            entry.GetProperty("grants").GetArrayLength() > 0)
        {
            return false;
        }

        bool covers(long? have, long? value) => value is not long v || (have is long h && (info.Ascending ? h >= v : h <= v));
        long? watermark = Bounded(seqoid, info, entry);
        if (!covers(local[0].Get<long?>(3), Nullable(entry, "root_start")) || !covers(local[0].Get<long?>(4), watermark))
        {
            return false;
        }

        // The neighbor's own report, the resets each node asked of the other, and its WAL position at this end of the
        // conversation: not behind the last one, or the changes of its that this node applied, nor a WAL segment past the
        // one the tables recorded.
        string sysid = root.GetProperty("sysid").GetWorfString() ?? "";
        long timeline = root.GetProperty("timeline").GetInt64();
        WorfRows own = Worf.Read("""
            SELECT CASE WHEN $6 THEN wal_lsn ELSE reply_lsn END::text
            FROM worf.sequence_range_node
            WHERE seqoid = $1 AND node_id = $2 AND via_node = node_id AND node_name = $3 AND sysid = $4::numeric AND timeline = $5
                AND epoch IS NOT DISTINCT FROM $7 AND state IS NOT DISTINCT FROM $8 AND distance IS NOT DISTINCT FROM $9
                AND high_water IS NOT DISTINCT FROM $10 AND watermark IS NOT DISTINCT FROM $11 AND holds IS NOT DISTINCT FROM $12
                AND (clock IS NOT DISTINCT FROM $13 OR clock >= $13) AND ($14::pg_lsn IS NULL OR reset_done = $14::pg_lsn)
                AND ($15::pg_lsn IS NULL OR reset_lsn IS DISTINCT FROM $15::pg_lsn) AND route IS NOT DISTINCT FROM $16
            """, 1, seq, sender, SpiParameter.Create(root.GetProperty("node_name").GetWorfString()), SpiParameter.Create(sysid),
            SpiParameter.Create(timeline), SpiParameter.Create(call), SpiParameter.Create(epoch), SpiParameter.Create(state),
            SpiParameter.Create(ReportedRoute(entry).Distance), SpiParameter.Create(Nullable(entry, "high_water")),
            SpiParameter.Create(watermark), SpiParameter.Create(Holds(entry)), SpiParameter.Create(Nullable(entry, "clock")),
            SpiParameter.Create(Text(entry, "reset")), SpiParameter.Create(Text(entry, "reset_done")), SpiParameter.Create(ReportedRoute(entry).Route));
        if (own.Count == 0 || !Worf.TryParseLsn(own[0].Get<string?>(0) ?? "", out ulong recorded) ||
            !Worf.TryParseLsn(root.GetProperty("wal_lsn").GetWorfString() ?? "", out ulong position) ||
            position < Math.Max(Math.Max(recorded, AppliedFloor(applied, entry)),
                SequenceRangeHeard.LatestLsn(senderId, ulong.Parse(sysid, CultureInfo.InvariantCulture), timeline, call)) ||
            position - recorded >= LsnRecordedEvery)
        {
            return false;
        }

        // Each relayed report is older than what this node has, or relayed instead of a live report, or says the same and
        // is of a node shared memory holds, which notes when it was made, or is of a node this node has no room to take in,
        // and the neighbor still relays every report it relayed before.
        if (entry.TryGetProperty("reports", out JsonElement reports))
        {
            bool? full = null;
            List<long> still = [];
            foreach (JsonElement report in reports.EnumerateArray())
            {
                uint reported = report.GetProperty("node_id").GetUInt32();
                if (reported == node.Node.Id || reported == senderId)
                {
                    continue;
                }

                still.Add(reported);

                WorfRows known = Worf.Read("""
                    SELECT CASE
                            WHEN least(coalesce(pg_catalog.to_timestamp($3), now()), now()) <= worf.sequence_range_heard(n.node_id, false, n.reported_at, false) THEN true
                            WHEN n.via_node = n.node_id THEN worf.sequence_range_heard(n.node_id, true, n.reported_at, false) >= now() - $4::interval END,
                        n.node_name = $5 AND n.high_water IS NOT DISTINCT FROM $6 AND n.watermark IS NOT DISTINCT FROM $7
                            AND n.epoch IS NOT DISTINCT FROM $8 AND n.state IS NOT DISTINCT FROM $9 AND n.holds IS NOT DISTINCT FROM $10
                            AND (n.clock IS NOT DISTINCT FROM $11 OR n.clock >= $11)
                    FROM worf.sequence_range_node n WHERE n.seqoid = $1 AND n.node_id = $2
                    """, 1, seq, SpiParameter.Create(reported),
                    SpiParameter.Create(report.TryGetProperty("reported_at", out JsonElement at) && at.ValueKind == JsonValueKind.Number ? at.GetDouble() : (double?)null),
                    SpiParameter.Create(SequenceRanges.StaleAfter()), SpiParameter.Create(report.GetProperty("node_name").GetWorfString()),
                    SpiParameter.Create(Nullable(report, "high_water")), SpiParameter.Create(Bounded(seqoid, info, report)),
                    SpiParameter.Create(Nullable32(report, "epoch")), SpiParameter.Create(Text(report, "state")), SpiParameter.Create(Holds(report)),
                    SpiParameter.Create(Nullable(report, "clock")));
                if (known.Count == 0
                    ? !(full ??= Worf.ReadScalar<bool>("""
                        SELECT count(*) >= $2 OR count(*) FILTER (WHERE via_node = $3 AND node_id <> via_node) >= $4
                        FROM worf.sequence_range_node WHERE seqoid = $1
                        """, seq, SpiParameter.Create(SequenceRangeHeard.MaxNodes), sender, SpiParameter.Create(SequenceRangeHeard.MaxRelayed)))
                    : known[0].Get<bool?>(0) is bool skip ? !skip : !known[0].Get<bool>(1) || !SequenceRangeHeard.Holds(reported))
                {
                    return false;
                }
            }

            if (Worf.ReadScalar<bool>("""
                SELECT EXISTS (SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND via_node = $2 AND node_id <> via_node AND NOT node_id = ANY ($3::oid[]))
                """, seq, sender, SpiParameter.Create(still.ToArray())))
            {
                return false;
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
    /// Checks, only reading, whether this node would leave a neighbor's requests for blocks as they are, as while the
    /// root has run out, or, on a node between it and the owner, can't start: none was answered before, and no free
    /// span of this node's root or pool blocks holds the smallest, while this node, unless it is the owner, has asked its
    /// parent for a pool block already, or this node, a Snowflake pool holder, hasn't heard from the owner lately.
    /// </summary>
    private static bool Unanswerable(uint seqoid, SequenceInfo info, int epoch, JsonElement requests, bool isOwner)
    {
        List<Guid> ids = [];
        long smallest = long.MaxValue;
        foreach (JsonElement request in requests.EnumerateArray())
        {
            if (Text(request, "kind") is not (null or "n" or "p"))
            {
                return false;
            }

            ids.Add(request.GetProperty("id").GetGuid());
            smallest = Math.Min(smallest, request.GetProperty("size").GetInt64());
        }

        ulong sysid;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
        }

        bool silent = !isOwner && seqoid == SnowflakeIds.NodeSequence() && !SequenceRanges.SnowflakeOwnerHeard(seqoid);
        return Worf.ReadScalar<bool>("""
            SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE request_id = ANY ($2))
                AND ($8 OR NOT EXISTS (
                        SELECT 1 FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
                        WHERE b.seqoid = $1 AND b.epoch = $3 AND b.kind IN ('r', 'p') AND b.state = 'open' AND b.sysid = $4 AND b.timeline = $5
                            AND f.hi::numeric - f.lo >= $6)
                    AND ($7 OR EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = $1 AND kind = 'p' AND state = 'open')))
            """, SpiParameter.Create(seqoid), SpiParameter.Create(ids.ToArray()), SpiParameter.Create(epoch),
            SpiParameter.Create((decimal)sysid), SpiParameter.Create((long)SequenceRanges.Timeline()),
            SpiParameter.Create(((decimal)smallest - 1) * Math.Abs(info.Increment)), SpiParameter.Create(isOwner), SpiParameter.Create(silent));
    }

    /// <summary>
    /// Keeps the record of a managed sequence this node dropped, which a sequence created again under its name joins,
    /// up to date with a neighbor's word of its epoch: once the epoch is disabled the record goes, so that no sequence
    /// joins a disabled epoch, as no neighbor's report brings one to join, and a later epoch replaces it. It reads first,
    /// so that an exchange that changes nothing writes nothing.
    /// </summary>
    private static void FollowDroppedEpoch(JsonElement entry)
    {
        bool? disabled = DroppedEpochFollowed(entry);
        if (disabled is null)
        {
            return;
        }

        SpiParameter name = SpiParameter.Create(entry.GetProperty("name").GetWorfString());
        int epoch = entry.GetProperty("epoch").GetInt32();
        _ = disabled.Value
            ? Worf.Execute("DELETE FROM worf.sequence_range_dropped WHERE name = $1 AND NOT owned", name)
            : Worf.Execute("""
                UPDATE worf.sequence_range_dropped SET owner_node = $2, epoch = $3, range_size = $4, max_range_size = $5, root_start = $6,
                    root_end = coalesce($7, root_end)
                WHERE name = $1 AND NOT owned
                """, name, SpiParameter.Create(entry.GetProperty("owner_id").GetUInt32()), SpiParameter.Create(epoch),
                SpiParameter.Create(entry.GetProperty("range_size").GetInt64()), SpiParameter.Create(entry.GetProperty("max_range_size").GetInt64()),
                SpiParameter.Create(Nullable(entry, "root_start")), SpiParameter.Create(Nullable(entry, "root_end")));
    }

    /// <summary>
    /// Finds how a neighbor's word of an epoch changes this node's record of the managed sequence it dropped, if at all.
    /// </summary>
    /// <returns>Whether the record goes, as its epoch is disabled, or false when a later epoch replaces it, or null when
    /// nothing changes.</returns>
    private static bool? DroppedEpochFollowed(JsonElement entry)
    {
        if (Worf.CatalogOid("sequence_range_dropped") == 0 ||
            Worf.ReadScalar<int?>("SELECT epoch FROM worf.sequence_range_dropped WHERE name = $1 AND NOT owned",
                SpiParameter.Create(entry.GetProperty("name").GetWorfString())) is not int dropped)
        {
            return null;
        }

        int epoch = entry.GetProperty("epoch").GetInt32();
        return entry.GetProperty("state").GetWorfString() == "disabled" && epoch >= dropped ? true : epoch > dropped ? false : null;
    }

    /// <summary>
    /// Takes in one sequence of a neighbor's message: joins it if it exists here, records the neighbor's reports,
    /// answers its requests when this node is its parent, records grants for this node and the neighbor's
    /// acknowledgements.
    /// </summary>
    /// <returns>Whether this node granted the neighbor a block, received one, and learned of a node or sequence.</returns>
    private static (bool Granted, bool Received, bool Learned) ProcessSequence(WorfLocalNode node, JsonElement root, JsonElement entry,
        uint senderId, string senderName, bool call, ulong applied)
    {
        bool granted = false;
        bool received = false;
        bool learned = false;
        uint? found = Worf.ReadScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(entry.GetProperty("name").GetWorfString()));
        if (found is not uint seqoid)
        {
            FollowDroppedEpoch(entry);
            return (false, false, false);
        }

        // The range row is locked first, as every change to the sequence's ranges does, waiting a quarter of
        // worf.sequence_range_timeout at most, up to a second, as a transaction that ran a block out holds it until it
        // ends, so that the exchange, which holds the node's turn, goes on with the other sequences.
        int epoch = entry.GetProperty("epoch").GetInt32();
        uint owner = entry.GetProperty("owner_id").GetUInt32();
        WorfRows local;
        int nestLevel;
        unsafe
        {
            nestLevel = NativeMethods.NewGUCNestLevel();
        }

        try
        {
            _ = Worf.Execute($"SET LOCAL lock_timeout = {Math.Min(1000, SequenceRanges.ExchangeTimeout / 4).ToString(CultureInfo.InvariantCulture)}");
            local = Worf.Query("SELECT owner_node, epoch, state FROM worf.sequence_range WHERE seqoid = $1 FOR NO KEY UPDATE", false, 1,
                SpiParameter.Create(seqoid));
        }
        finally
        {
            unsafe
            {
                NativeMethods.AtEOXact_GUC(true, nestLevel);
            }
        }

        if (local.Count == 0)
        {
            // An epoch this node owns and no longer manages, as after it dropped the sequence, is no epoch to join.
            if (entry.GetProperty("state").GetWorfString() == "disabled" || owner == node.Node.Id)
            {
                return (false, false, false);
            }

            SequenceRanges.JoinFromNeighbor(seqoid, owner, epoch, entry.GetProperty("range_size").GetInt64(),
                entry.GetProperty("max_range_size").GetInt64(), Nullable(entry, "root_start"), Nullable(entry, "root_end"), node,
                entry.GetProperty("state").GetWorfString() != "collecting");
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
        else if (entry.GetProperty("state").GetWorfString() == "disabled" && local[0].Get<string>(2) != "disabled")
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
        if (watermark is long claimed && SequenceRanges.BoundWatermark(seqoid, info, claimed) is (long bounded, true))
        {
            WorfLog.Warning($"sequence {info.Name}: node {senderName} reports grants up to {claimed}, beyond any a node could have heard of, so this node takes them only up to {bounded}");
            watermark = bounded;
        }

        string sysid = root.GetProperty("sysid").GetWorfString() ?? "";
        long timeline = root.GetProperty("timeline").GetInt64();
        string walLsn = root.GetProperty("wal_lsn").GetWorfString() ?? "0/0";

        // The root's start, once the owner has started it, reaches every node, so that each allocator refuses a node
        // that joins with values reaching into it.
        if (Nullable(entry, "root_start") is long rootStart)
        {
            _ = Worf.Execute($"UPDATE worf.sequence_range SET root_start = {(info.Ascending ? "greatest" : "least")}(coalesce(root_start, $2), $2) WHERE seqoid = $1 AND epoch = $3",
                seq, SpiParameter.Create(rootStart), SpiParameter.Create(epoch));
        }

        // A neighbor whose WAL position moved backwards on the same timeline was rolled back, by a restored backup or
        // snapshot, and may issue values it issued before, or grant blocks it granted before. It must reset. The
        // positions in its calls, which it makes one at a time, are compared with each other, and those in its answers
        // to this node's calls, which this node makes one at a time, with each other: an answer may pass a call the
        // neighbor sent before it. The neighbor's row is locked first, so that the position compared is the one this
        // report replaces, and none written in between is lost.
        // The position compared is the latest of the one the tables recorded, the one shared memory holds, which the
        // tables record only once it moves by a WAL segment, and, in an answer, how far this node applied the neighbor's
        // changes, which neither its restart nor its crash forgets, and which stays put while the neighbor is idle.
        _ = Worf.Execute("SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2 FOR UPDATE", seq, sender);
        WorfRows stored = Worf.Read(
            "SELECT CASE WHEN $5 THEN wal_lsn ELSE reply_lsn END::text FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2 AND sysid = $3::numeric AND timeline = $4",
            1, seq, sender, SpiParameter.Create(sysid), SpiParameter.Create(timeline), SpiParameter.Create(call));
        ulong latest = Math.Max(AppliedFloor(applied, entry),
            stored.Count > 0 ? Math.Max(Worf.TryParseLsn(stored[0].Get<string?>(0) ?? "", out ulong recorded) ? recorded : 0,
                SequenceRangeHeard.LatestLsn(senderId, ulong.Parse(sysid, CultureInfo.InvariantCulture), timeline, call)) : 0);
        string? rolledBack = null;
        if (Worf.TryParseLsn(walLsn, out ulong position) && latest > position)
        {
            rolledBack = Worf.FormatLsn(latest);
            _ = Worf.Execute("UPDATE worf.sequence_range_node SET reset_lsn = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2", seq, sender,
                SpiParameter.Create(rolledBack));
        }
        // An allocator that grants values this node already holds lost the grant it made them in, as when it went back too
        // little for its WAL position to show it, and must reset too. A fenced allocator is taking its lost grants back
        // into its ledger.
        string? lost = rolledBack is not null || entry.GetProperty("state").GetWorfString() == "fenced" ? null : Regrants(seqoid, entry);
        if (lost is not null)
        {
            rolledBack = walLsn;
            _ = Worf.Execute("UPDATE worf.sequence_range_node SET reset_lsn = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2 AND reset_lsn IS NULL", seq,
                sender, SpiParameter.Create(walLsn));
            WorfLog.Warning($"sequence {info.Name}: node {senderName} {lost}, so it must reset its blocks");
        }
        else if (rolledBack is not null)
        {
            WorfLog.Warning($"sequence {info.Name}: node {senderName}'s WAL position moved back from {rolledBack} to {walLsn}, so it must reset its blocks");
        }

        // Until it resets, a neighbor that went back may answer from a ledger that lost grants, and ask again for blocks
        // it already had, so this message's grants are left alone, its requests unanswered, and this node's requests to
        // it are made again later, under new ids.
        bool wentBack = rolledBack is not null;
        if (wentBack)
        {
            _ = Worf.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND parent_node = $2", seq, sender);
        }

        _ = Worf.Execute("""
            INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, sysid, timeline, wal_lsn, reply_lsn, epoch, state, distance,
                high_water, watermark, reported_at, holds, clock, route)
            VALUES ($1, $2, $3, $2, $4::numeric, $5, CASE WHEN $12 THEN $6::pg_lsn END, CASE WHEN NOT $12 THEN $6::pg_lsn END, $7, $8, $9, $10, $11, now(),
                $13, $14, $15)
            ON CONFLICT (seqoid, node_id) DO UPDATE SET node_name = excluded.node_name, via_node = excluded.via_node,
                sysid = excluded.sysid, timeline = excluded.timeline,
                wal_lsn = CASE WHEN $12 THEN excluded.wal_lsn ELSE sequence_range_node.wal_lsn END,
                reply_lsn = CASE WHEN NOT $12 THEN excluded.reply_lsn ELSE sequence_range_node.reply_lsn END, epoch = excluded.epoch,
                state = excluded.state, distance = excluded.distance, high_water = excluded.high_water, watermark = excluded.watermark,
                reported_at = excluded.reported_at, holds = excluded.holds, clock = greatest(sequence_range_node.clock, excluded.clock),
                route = excluded.route
            """, seq, sender, SpiParameter.Create(senderName),
            SpiParameter.Create(sysid), SpiParameter.Create(timeline), SpiParameter.Create(walLsn), SpiParameter.Create(epoch),
            SpiParameter.Create(entry.GetProperty("state").GetWorfString()), SpiParameter.Create(ReportedRoute(entry).Distance),
            SpiParameter.Create(high), SpiParameter.Create(watermark), SpiParameter.Create(call), SpiParameter.Create(Holds(entry)),
            SpiParameter.Create(Nullable(entry, "clock")), SpiParameter.Create(ReportedRoute(entry).Route));

        // A neighbor that worf.activate_sequence_range stopped waiting for, as gone, may hold values that the fence's lift
        // gave out again, so once it's heard from again after the lift, it's asked to reset, at a WAL position of its own,
        // past any real one; while the fence is still up, its report counts as any other's.
        if (Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_absent WHERE seqoid = $1 AND node_id = $2)", seq, sender))
        {
            _ = Worf.Execute("""
                WITH gone AS (DELETE FROM worf.sequence_range_absent WHERE seqoid = $1 AND node_id = $2 RETURNING absent_at)
                UPDATE worf.sequence_range_node n SET reset_lsn = $3::pg_lsn
                FROM worf.sequence_range r
                WHERE n.seqoid = $1 AND n.node_id = $2 AND r.seqoid = $1 AND r.fenced_at IS NULL AND EXISTS (SELECT 1 FROM gone)
                """, seq, sender, SpiParameter.Create($"FFFFFFFE/{(uint)Worker.GetCurrentTimestamp():X8}"));
        }

        // The neighbor asks this node to reset, once per rollback it saw, or has reset as this node asked. A node that
        // has neither started drawing from a block nor granted one since it last reset has nothing it could issue or
        // grant twice, so a request then, unless it shows this node's WAL position behind where the neighbor saw it, as a
        // rollback leaves it, or comes from a fence's lift, which may have given the blocks it holds out again, only
        // stops the neighbor asking. Any other resets it, as often as
        // SequenceRangeHeard.MayReset lets the neighbor, whatever WAL position it claims, and the sequence once in those
        // ten minutes, however many requests come in the minute that a rollback's resets take.
        if (Text(entry, "reset") is string reset && Worf.ReadScalar<bool>(
            "SELECT reset_done IS DISTINCT FROM $3::pg_lsn FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2", seq, sender, SpiParameter.Create(reset)))
        {
            ulong insert;
            unsafe
            {
                insert = NativeMethods.GetXLogInsertRecPtr();
            }

            bool behind = Worf.TryParseLsn(reset, out ulong seen) && seen > insert;

            // A fence's lift asks the holders of the grants it gave out again, or that overlap, to reset, which the
            // node that granted them may ask, when it names a block this node holds from it, in a window of its own, so
            // that another reset doesn't hold it up, which starts at ten seconds and grows as the other does, as one
            // made up may name the blocks this node's answers list; the reset gives them up. Any other request waits for
            // SequenceRangeHeard.MayReset's other window.
            Guid[] named = entry.TryGetProperty("reset_grants", out JsonElement grants) && grants.ValueKind == JsonValueKind.Array
                ? [.. grants.EnumerateArray().Select(grant => grant.ValueKind == JsonValueKind.String && grant.TryGetGuid(out Guid id) ? id : Guid.Empty)]
                : [];
            bool fromGrantor = IsLift(reset) && named.Length > 0 && Worf.ReadScalar<bool>("""
                SELECT r.parent_node = $2 AND EXISTS (
                    SELECT 1 FROM worf.sequence_range_block b
                    WHERE b.seqoid = $1 AND b.from_node = $2 AND b.block_id = ANY ($3) AND b.state <> 'abandoned')
                FROM worf.sequence_range r WHERE r.seqoid = $1
                """, seq, sender, SpiParameter.Create(named));
            if (!behind && !IsLift(reset) && !SequenceRanges.ActedSinceReset(seqoid, node))
            {
                _ = Worf.Execute("UPDATE worf.sequence_range_node SET reset_done = $3::pg_lsn WHERE seqoid = $1 AND node_id = $2", seq, sender,
                    SpiParameter.Create(reset));
            }
            else if (SequenceRangeHeard.MayReset(senderId, fromGrantor) is long window && Worf.Execute("""
                UPDATE worf.sequence_range_node SET reset_done = $3::pg_lsn, reset_at = pg_catalog.clock_timestamp()
                WHERE seqoid = $1 AND node_id = $2 AND (reset_at IS NULL OR reset_at < $4)
                """, seq, sender, SpiParameter.Create(reset), SpiParameter.Create(new PgTimestampTz(window))) > 0)
            {
                WorfLog.Warning(reset.StartsWith("FFFFFFFF/", StringComparison.Ordinal)
                    ? $"sequence {info.Name}: node {senderName} lifted a fence over grants of this node's that overlap another's, so it abandons its blocks and requests new ones"
                    : reset.StartsWith("FFFFFFFE/", StringComparison.Ordinal)
                    ? $"sequence {info.Name}: node {senderName} lifted its fence without this node's report, so it abandons its blocks and requests new ones"
                    : behind
                    ? $"sequence {info.Name}: node {senderName} saw this node's WAL position move backwards, so it abandons its blocks and requests new ones"
                    : $"sequence {info.Name}: node {senderName} asked this node to reset, so it abandons its blocks and requests new ones");
                SequenceRanges.Reset(seqoid, node, false);
                received = true;
            }
        }

        if (Text(entry, "reset_done") is string done)
        {
            _ = Worf.Execute("UPDATE worf.sequence_range_node SET reset_lsn = NULL WHERE seqoid = $1 AND node_id = $2 AND reset_lsn = $3::pg_lsn",
                seq, sender, SpiParameter.Create(done));
        }

        // A watermark that rises here goes back to the neighbors at once, so that an owner that granted itself a block hears
        // that its neighbors know of it, even one that only answers calls.
        if (watermark is long mark)
        {
            // Every value of a started root that any node has heard of granted, the owner granted, so a neighbor's watermark
            // in the root beyond the owner's own shows grants the owner lost, as when it went back on its timeline by less
            // than its WAL position shows. It resets, which fences the root until the nodes it granted to report, and the
            // root then gives up its free values up to the watermark it takes in here.
            string raised = info.Ascending ? "greatest" : "least";
            if (!wentBack && LostGrants(seqoid, info, node, mark) && SequenceRangeHeard.MayReset(senderId) is long window && Worf.Execute("""
                UPDATE worf.sequence_range_node SET reset_at = pg_catalog.clock_timestamp() WHERE seqoid = $1 AND node_id = $2 AND (reset_at IS NULL OR reset_at < $3)
                """, seq, sender, SpiParameter.Create(new PgTimestampTz(window))) > 0)
            {
                WorfLog.Warning($"sequence {info.Name}: node {senderName} has heard of grants up to {mark}, beyond this owner's watermark, so this owner lost them; it resets");
                _ = Worf.Execute($"UPDATE worf.sequence_range SET heard_watermark = {raised}(coalesce(heard_watermark, $2), $2) WHERE seqoid = $1",
                    seq, SpiParameter.Create(mark));
                SequenceRanges.Reset(seqoid, node, false);
                received = true;
            }

            learned |= Worf.Execute($"""
                UPDATE worf.sequence_range SET watermark = {raised}(coalesce(watermark, $2), $2)
                WHERE seqoid = $1 AND watermark IS DISTINCT FROM {raised}(coalesce(watermark, $2), $2)
                """, seq, SpiParameter.Create(mark)) > 0;
        }

        // Reports of other nodes, relayed toward the owner, each as old as its node made it. A fresher report replaces a
        // relayed one, and the neighbor's own report replaces one it relayed only once that has gone stale. Reports of new
        // nodes are taken in only while this node knows of fewer nodes than shared memory holds, and of fewer that this
        // neighbor relays than shared memory notes from one neighbor, and a report the neighbor no longer relays goes,
        // so that a neighbor can't fill the tables, and every message, with made-up nodes. A node's Snowflake clock never
        // goes back on a report.
        if (entry.TryGetProperty("reports", out JsonElement reports))
        {
            long known = Worf.ReadScalar<long>("SELECT count(*) FROM worf.sequence_range_node WHERE seqoid = $1", seq);
            long relayed = Worf.ReadScalar<long>(
                "SELECT count(*) FROM worf.sequence_range_node WHERE seqoid = $1 AND via_node = $2 AND node_id <> via_node", seq, sender);
            int ignored = 0;
            List<long> still = [];
            foreach (JsonElement report in reports.EnumerateArray())
            {
                // The neighbor's own entry, above, is its live report.
                uint reported = report.GetProperty("node_id").GetUInt32();
                if (reported == node.Node.Id || reported == senderId)
                {
                    continue;
                }

                if (Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_node WHERE seqoid = $1 AND node_id = $2", seq, SpiParameter.Create(reported)).Count == 0)
                {
                    if (known >= SequenceRangeHeard.MaxNodes || relayed >= SequenceRangeHeard.MaxRelayed)
                    {
                        ignored++;
                        continue;
                    }

                    known++;
                    relayed++;
                    learned = true;
                }

                still.Add(reported);

                _ = Worf.Execute("""
                    INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, high_water, watermark, epoch, state, reported_at,
                        holds, clock)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, least(coalesce(pg_catalog.to_timestamp($9), now()), now()), $11, $12)
                    ON CONFLICT (seqoid, node_id) DO UPDATE SET node_name = excluded.node_name, via_node = excluded.via_node,
                        high_water = excluded.high_water, watermark = excluded.watermark, epoch = excluded.epoch, state = excluded.state,
                        reported_at = excluded.reported_at, holds = excluded.holds, clock = greatest(sequence_range_node.clock, excluded.clock)
                    WHERE excluded.reported_at > worf.sequence_range_heard(sequence_range_node.node_id, false, sequence_range_node.reported_at, false)
                        AND (sequence_range_node.via_node <> sequence_range_node.node_id
                            OR worf.sequence_range_heard(sequence_range_node.node_id, true, sequence_range_node.reported_at, false) < now() - $10::interval)
                    """, seq, SpiParameter.Create(reported), SpiParameter.Create(report.GetProperty("node_name").GetWorfString()),
                    sender, SpiParameter.Create(Nullable(report, "high_water")), SpiParameter.Create(Bounded(seqoid, info, report)),
                    SpiParameter.Create(Nullable32(report, "epoch")), SpiParameter.Create(Text(report, "state")),
                    SpiParameter.Create(report.TryGetProperty("reported_at", out JsonElement at) && at.ValueKind == JsonValueKind.Number ? at.GetDouble() : (double?)null),
                    SpiParameter.Create(SequenceRanges.StaleAfter()), SpiParameter.Create(Holds(report)), SpiParameter.Create(Nullable(report, "clock")));
            }

            _ = Worf.Execute("""
                DELETE FROM worf.sequence_range_node WHERE seqoid = $1 AND via_node = $2 AND node_id <> via_node AND NOT node_id = ANY ($3::oid[])
                """, seq, sender, SpiParameter.Create(still.ToArray()));
            if (ignored > 0)
            {
                WorfLog.Warning($"sequence {info.Name}: node {senderName} relayed reports of {ignored} nodes beyond the {SequenceRangeHeard.MaxNodes} this node keeps, or the {SequenceRangeHeard.MaxRelayed} it keeps from one neighbor; they are ignored");
            }
        }

        // Blocks the neighbor holds from this node. A fenced allocator takes back into its ledger the ones it lost.
        bool fenced = Worf.ReadScalar<bool>("SELECT fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = $1", seq);
        if (entry.TryGetProperty("held", out JsonElement heldBlocks))
        {
            foreach (JsonElement block in heldBlocks.EnumerateArray())
            {
                SequenceRanges.RecordHeld(seqoid, info, block.GetProperty("id").GetGuid(), (block.GetProperty("kind").GetWorfString() ?? "n")[0],
                    block.GetProperty("lo").GetInt64(), block.GetProperty("hi").GetInt64(), block.GetProperty("abandoned").GetBoolean(), fenced,
                    senderId, senderName);
            }

            // A grant the neighbor doesn't list among every block it holds from this fenced allocator never reached it, or
            // was left alone, as when it came from the ledger this allocator went back from: its values were never used.
            if (fenced && entry.TryGetProperty("held_complete", out JsonElement complete) && complete.GetBoolean())
            {
                _ = Worf.Execute("UPDATE worf.sequence_range_node SET held_at = now() WHERE seqoid = $1 AND node_id = $2", seq, sender);
                _ = Worf.Execute("""
                    UPDATE worf.sequence_range_grant SET state = 'unclaimed'
                    WHERE seqoid = $1 AND grantee_id = $2 AND state = 'sent' AND kind <> 'x' AND NOT grant_id = ANY ($3)
                    """, seq, sender, SpiParameter.Create(heldBlocks.EnumerateArray().Select(block => block.GetProperty("id").GetGuid()).ToArray()));
            }
        }

        // A neighbor that holds a block this node granted, which this node's ledger has no record of, shows that this node
        // went back and lost the grant: every grant is committed before it is sent. This node resets, which fences its
        // root and pool blocks until the neighbors report what they hold.
        Guid[] acks = [.. entry.GetProperty("acks").EnumerateArray().Select(ack => ack.GetGuid())];
        _ = Worf.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = ANY ($1) AND grantee_id = $2 AND state = 'sent'",
            SpiParameter.Create(acks), sender);
        if (!fenced && !wentBack && Worf.ReadScalar<long>("""
            SELECT count(*) FROM unnest($1::uuid[]) a(id)
            WHERE NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant g WHERE g.grant_id = a.id)
                AND EXISTS (SELECT 1 FROM worf.sequence_range_block b WHERE b.seqoid = $2 AND b.kind IN ('r', 'p') AND b.state = 'open')
            """, SpiParameter.Create(acks), seq) is long unknown and > 0)
        {
            WorfLog.Warning($"sequence {info.Name}: node {senderName} holds {unknown} blocks this node granted and has no record of, so this node went back; it resets");
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
            _ = Worf.Execute("UPDATE worf.sequence_range SET exchanged_at = now() WHERE seqoid = $1", seq);
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
        if (Worf.Execute("UPDATE worf.sequence_range_grant SET state = 'sent' WHERE request_id = $1 AND kind = 'x'", SpiParameter.Create(requestId)) > 0)
        {
            return null;
        }

        WorfRows range = Worf.SelectCatalog("""
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
            _ = Worf.Execute("""
                INSERT INTO worf.sequence_range_grant VALUES (gen_random_uuid(), $1, $2, NULL, $3, $4, 'x', $5, $5, 'sent', now())
                ON CONFLICT (request_id) DO UPDATE SET state = 'sent'
                """, SpiParameter.Create(requestId), SpiParameter.Create(seqoid), SpiParameter.Create(granteeId),
                SpiParameter.Create(granteeName), SpiParameter.Create(h));
            WorfLog.Warning($"sequence {info.Name}: node {granteeName} joined with values up to {h}, which reach into the root; it is refused");
            return null;
        }

        // A Snowflake pool holder grants its pool's numbers only while the owner, which may give them out again a day
        // after it last heard from the pool holder, has been heard from within half a day.
        if (!isOwner && seqoid == SnowflakeIds.NodeSequence() && !SequenceRanges.SnowflakeOwnerHeard(seqoid))
        {
            return false;
        }

        // Another pool holder grants from its pool only once this server has heard from the nodes that granted it, as
        // PoolGrantorsHeard tells; the child waits.
        if (!isOwner && !PoolGrantorsHeard(seqoid, epoch))
        {
            return false;
        }

        string kind = request.TryGetProperty("kind", out JsonElement value) ? value.GetWorfString() ?? "n" : "n";
        return SequenceRanges.Allocate(seqoid, info, epoch, request.GetProperty("size").GetInt64(), granteeId, granteeName, requestId, kind[0]) is not null;
    }

    /// <summary>
    /// Checks whether this node, the owner of a root that has started and isn't fenced, has a watermark short of one a
    /// neighbor reports within the root.
    /// </summary>
    private static bool LostGrants(uint seqoid, SequenceInfo info, WorfLocalNode node, long mark)
        => Worf.ReadScalar<bool>($"""
            SELECT r.owner_node = $2 AND r.fenced_at IS NULL AND r.state NOT IN ('collecting', 'disabled')
                AND $3 {(info.Ascending ? ">=" : "<=")} r.root_start AND (r.watermark IS NULL OR $3 {(info.Ascending ? ">" : "<")} r.watermark)
                AND EXISTS (SELECT 1 FROM worf.sequence_range_block b WHERE b.seqoid = r.seqoid AND b.epoch = r.epoch AND b.kind = 'r')
            FROM worf.sequence_range r WHERE r.seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(node.Node.Id), SpiParameter.Create(mark));

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

        // A pool holder grants exact values from its pool only once this server has heard from the nodes that granted
        // it, as for any other grant; the child waits.
        if (!isOwner && !PoolGrantorsHeard(seqoid, epoch))
        {
            return false;
        }

        if (SequenceRanges.AllocateExact(seqoid, info, epoch, lo, hi, granteeId, granteeName, requestId, kind, isOwner) is not null)
        {
            return true;
        }

        if (isOwner)
        {
            return true;
        }

        SpiParameter seq = SpiParameter.Create(seqoid);
        if (Worf.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND kind = 'f' AND lo = $2 AND hi = $3 AND state = 'refused'",
            seq, SpiParameter.Create(lo), SpiParameter.Create(hi)) > 0)
        {
            _ = Worf.Execute("""
                INSERT INTO worf.sequence_range_grant VALUES (gen_random_uuid(), $1, $2, NULL, $3, $4, 'x', $5, $6, 'sent', now())
                ON CONFLICT (request_id) DO UPDATE SET state = 'sent'
                """, SpiParameter.Create(requestId), seq, SpiParameter.Create(granteeId), SpiParameter.Create(granteeName),
                SpiParameter.Create(lo), SpiParameter.Create(hi));
            return true;
        }

        _ = Worf.Execute("""
            INSERT INTO worf.sequence_range_request (request_id, seqoid, parent_node, kind, size, lo, hi, created_at)
            SELECT gen_random_uuid(), r.seqoid, r.parent_node, 'f', $4, $2, $3, now()
            FROM worf.sequence_range r
            WHERE r.seqoid = $1 AND r.parent_node IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request q WHERE q.seqoid = r.seqoid AND q.kind = 'f' AND q.lo = $2 AND q.hi = $3)
            """, seq, SpiParameter.Create(lo), SpiParameter.Create(hi), SpiParameter.Create(SequenceRanges.Steps(info, lo, hi)));
        return false;
    }

    /// <summary>
    /// Checks whether this server has heard, since it started, from each node that granted this pool holder an open
    /// pool block of a sequence, as it draws from its own blocks only then, since a fence's lift may have given their
    /// values out again meanwhile. One it hasn't is asked at once. A Snowflake pool holder's numbers wait on the owner's
    /// word instead.
    /// </summary>
    private static bool PoolGrantorsHeard(uint seqoid, int epoch)
    {
        if (seqoid == SnowflakeIds.NodeSequence())
        {
            return true;
        }

        foreach (uint grantor in Worf.Read("""
            SELECT DISTINCT b.from_node FROM worf.sequence_range_block b CROSS JOIN worf.local_node l
            WHERE b.seqoid = $1 AND b.epoch = $2 AND b.kind = 'p' AND b.state = 'open' AND b.from_node <> l.node_id
                AND EXISTS (SELECT 1 FROM worf.node n WHERE n.node_id = b.from_node)
            """, 0, SpiParameter.Create(seqoid), SpiParameter.Create(epoch)).Select(static pool => pool.Get<uint>(0)))
        {
            if (!SequenceRangeHeard.Confirmed(grantor))
            {
                SequenceRangeHeard.Want(grantor);
                SequenceRangeExchangeWorker.Wake();
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Asks this node's parent for a pool block sixteen times a child's request, unless one is already open.
    /// </summary>
    private static void RequestPool(uint seqoid, long childSize)
    {
        _ = Worf.Execute("""
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
            string? refused = Worf.ExecuteScalar<string?>(
                "UPDATE worf.sequence_range_request SET state = 'refused' WHERE request_id = $1 AND state = 'open' RETURNING kind::text",
                SpiParameter.Create(requestId));
            if (refused == "n")
            {
                _ = Worf.Execute("""
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

        WorfRows open = Worf.SelectCatalog("SELECT reserved FROM worf.sequence_range_request WHERE request_id = $1 AND parent_node = $2 AND state = 'open'",
            SpiParameter.Create(requestId), SpiParameter.Create(from));
        if (open.Count == 0)
        {
            _ = SequenceRanges.InsertBlock(seqoid, epoch, kind == "i" ? 'i' : kind is "p" or "f" ? 'p' : 'n', lo, hi, "abandoned", from, node, id);
            WorfLog.Log($"sequence {SequenceRanges.GetInfo(seqoid).Name}: block [{lo}, {hi}] from node {fromName} answers no open request, so it is abandoned");
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
            _ = Worf.Execute("INSERT INTO worf.sequence_range_free VALUES ($1, $2, $3)", SpiParameter.Create(id), SpiParameter.Create(lo),
                SpiParameter.Create(hi));
        }

        _ = Worf.Execute("DELETE FROM worf.sequence_range_request WHERE request_id = $1", SpiParameter.Create(requestId));
        _ = Worf.Execute("SET LOCAL synchronous_commit = on");
        return true;
    }

    /// <summary>
    /// Sets this node's distance to the owner, one more than its nearest neighbor's, and its parent, that neighbor,
    /// with ties going to the lowest name. Only neighbors that still exchange count, so that a dropped link's node stops
    /// being the parent, and only those whose routes don't go through this node, so that no ring of nodes counts its
    /// distances up through itself. A request open with another parent is dropped once a new parent is found.
    /// </summary>
    private static void Route(uint seqoid, WorfLocalNode node)
    {
        (uint? parent, int? distance) = Nearest(seqoid, node);
        _ = Worf.Execute("""
            UPDATE worf.sequence_range SET parent_node = $2, distance = $3,
                state = CASE WHEN $2 IS NULL AND state IN ('quarantined', 'no_route') THEN 'no_route'
                    WHEN state = 'no_route' THEN 'quarantined' ELSE state END
            WHERE seqoid = $1
            """, SpiParameter.Create(seqoid), SpiParameter.Create(parent), SpiParameter.Create(distance));
        _ = Worf.Execute("DELETE FROM worf.sequence_range_request WHERE seqoid = $1 AND $2::oid IS NOT NULL AND parent_node <> $2",
            SpiParameter.Create(seqoid), SpiParameter.Create(parent));
    }

    /// <summary>
    /// Finds this node's parent, its nearest neighbor that still exchanges whose route to the owner doesn't go through
    /// this node, and its distance to the owner.
    /// </summary>
    private static (uint? Parent, int? Distance) Nearest(uint seqoid, WorfLocalNode node)
    {
        WorfRows nearest = Worf.SelectCatalog("""
            SELECT n.node_id, n.distance FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid AND r.epoch = n.epoch
            WHERE n.seqoid = $1 AND n.via_node = n.node_id AND n.node_id <> $2 AND n.distance IS NOT NULL AND n.distance < $3
                AND NOT coalesce($2::pg_catalog.int8 = ANY (n.route), false)
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
        WorfRows held = Worf.SelectCatalog("""
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

    // The Snowflake node numbers a report says its node holds, or null for none, or for more than there are, which no
    // node can hold, so that no node stores more than that.
    private static long[]? Holds(JsonElement entry)
        => entry.TryGetProperty("holds", out JsonElement holds) && holds.ValueKind == JsonValueKind.Array && holds.GetArrayLength() <= SnowflakeNodes
            ? [.. holds.EnumerateArray().Select(number => number.GetInt64())]
            : null;

    // A neighbor's distance to the owner, and the nodes from its parent to the owner, as its report gives them, or nulls
    // for a neighbor with no route, or one that reports a route longer than any honest route, which counts as none, so
    // that no node stores, or passes on, more than that.
    private static (int? Distance, long[]? Route) ReportedRoute(JsonElement entry)
    {
        if (!entry.TryGetProperty("route", out JsonElement route) || route.ValueKind != JsonValueKind.Array)
        {
            return (Nullable32(entry, "distance"), null);
        }

        return route.GetArrayLength() > MaxDistance ? (null, null) : (Nullable32(entry, "distance"), [.. route.EnumerateArray().Select(hop => hop.GetInt64())]);
    }

    /// <summary>
    /// Gets when a neighbor's server started, as the message this transaction took in from it says, once it has asked
    /// all it makes of the neighbor's return: the answer to that message, which goes out only if the transaction commits,
    /// says it back, as the messages after do.
    /// </summary>
    /// <param name="neighbor">The neighbor.</param>
    /// <returns>The time, or null.</returns>
    private static long? PendingStart(uint neighbor)
        => s_onCommit.FindLast(noted => noted.Sender == neighbor && noted.StartedAt is not null).StartedAt;

    /// <summary>
    /// Notes, once the transaction that takes in a neighbor's message commits, when the neighbor's server started, which
    /// this server's messages say back from then on, so that one says it only once what this server made of the
    /// neighbor's return, as a request to reset, has committed and goes out with it; and, for a message the neighbor made
    /// knowing this server was back, that this server has heard from it since it started, with what the message asked
    /// done. A savepoint that rolls back forgets what was noted so far.
    /// </summary>
    private static void OnCommit(uint senderId, long? startedAt, bool confirmed)
    {
        if (s_onCommit.Count == 0)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () =>
            {
                foreach ((uint sender, long? started, bool confirmed) in s_onCommit)
                {
                    if (started is long at)
                    {
                        SequenceRangeHeard.RecordStarted(sender, at);
                    }

                    if (confirmed)
                    {
                        SequenceRangeHeard.Confirm(sender);
                    }
                }

                s_onCommit.Clear();
            });
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () => s_onCommit.Clear());
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, static () => s_onCommit.Clear());
            _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (_, _) => s_onCommit.Clear());
        }

        s_onCommit.Add((senderId, startedAt, confirmed));
    }

    /// <summary>
    /// Gets the prepared transaction that holds a sequence's range row, as one that locked or updated it, or null.
    /// </summary>
    private static string? PreparedHolder(uint seqoid) => Worf.ReadScalar<string?>("""
        SELECT x.gid FROM worf.sequence_range r JOIN pg_catalog.pg_prepared_xacts x ON x.transaction = r.xmax WHERE r.seqoid = $1
        """, SpiParameter.Create(seqoid));

    // Whether a request to reset comes from a fence's lift, at a WAL position past any real one.
    private static bool IsLift(string reset) => reset.StartsWith("FFFFFFFF/", StringComparison.Ordinal) || reset.StartsWith("FFFFFFFE/", StringComparison.Ordinal);

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
            writer.WriteString(name, WorfJson.Encode(value));
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

    /// <summary>
    /// Reads the watermark a report gives, bounded as <see cref="SequenceRanges.BoundWatermark"/> bounds it.
    /// </summary>
    private static long? Bounded(uint seqoid, SequenceInfo info, JsonElement report)
        => Nullable(report, "watermark") is long reported ? SequenceRanges.BoundWatermark(seqoid, info, reported).Mark : null;

    private static string? Text(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetWorfString() : null;

    private static long? Nullable(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number ? value.GetInt64() : null;

    private static int? Nullable32(JsonElement entry, string name)
        => entry.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.Number ? value.GetInt32() : null;
}
