using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Reservations ahead of bulk loads: a block large enough for a load that numbers its rows with the sequence, and exact
/// values for rows that already carry their keys. A session can't commit in the middle of its transaction, so it hands
/// its request to the range worker and waits. On the owner the worker allocates the block at once, and elsewhere it
/// commits the request and exchanges it with the node's parent. The worker, not the session, switches to a reserved
/// block, so that the switch commits at once and neither holds the sequence until the session's transaction ends nor
/// leaves the session's guard with blocks no other backend sees. Reservations take values the sequence's owner decides
/// on, so they need its privileges.
/// </summary>
internal static class SequenceRangeReservations
{
    private const int MaxReservations = 64;
    private const int PollInterval = 100;

    // The largest block a reservation asks for, half of bigint's range.
    private const decimal MaxNeeded = 4_611_686_018_427_387_903m;

    /// <summary>
    /// Gets the shared table of requests that sessions hand to their database's range worker.
    /// </summary>
    internal static PgLwLock<WorfReservationTable, WorfReservation> Reservations { get; } = new("worf_sequence_range_reservations");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit() => PgSharedMemory.Initialize(Reservations, MaxReservations, static () => default, static _ => default);

    /// <summary>
    /// Returns once the sequence's active block has at least <paramref name="count"/> values left after the margin,
    /// once the range worker has switched to a block that large if needed.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.reserve_sequence_range(sequence regclass, count bigint, timeout interval DEFAULT '1 minute')
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void ReserveSequenceRange([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long count, PgInterval timeout)
    {
        uint seqoid = RequireOwnership(sequence);
        Elevated.Run(() => ReserveRange(seqoid, count, timeout));
    }

    /// <summary>
    /// Allocates exactly [<paramref name="lo"/>, <paramref name="hi"/>] to this node as an imported block, so that rows
    /// that already carry those keys can be loaded here.
    /// </summary>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.reserve_sequence_values(sequence regclass, lo bigint, hi bigint, timeout interval DEFAULT '1 minute')
        RETURNS void STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static void ReserveSequenceValues([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long lo, long hi, PgInterval timeout)
    {
        uint seqoid = RequireOwnership(sequence);
        Elevated.Run(() => ReserveValues(seqoid, lo, hi, timeout));
    }

    private static void ReserveRange(uint seqoid, long count, PgInterval timeout)
    {
        // The node isn't locked while the session waits, since the manager, which starts the range worker, locks it.
        _ = WorfNode.GetLocalNode(false, false);
        if (count <= 0)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "count must be positive");
        }

        (int epoch, double rate) = RequireUsable(seqoid);
        SequenceInfo info = SequenceRanges.GetInfo(seqoid);

        // Sessions inserting at the same time can't run the load short.
        long needed = (long)Math.Min(count + Math.Ceiling(Math.Max(count / 20m, (decimal)rate * 60)), MaxNeeded);
        long deadline = Worker.GetCurrentTimestamp() + (long)Int128.Min(timeout.ToComparisonMicroseconds(), long.MaxValue / 2);
        Guid requestId = Guid.NewGuid();
        bool requested = false;
        try
        {
            while (true)
            {
                // Whether the worker still had the request, or the node's parent, before this look at the blocks, so that
                // a block granted or switched to between the look and the check isn't missed.
                bool pending = requested && (Pending(requestId) || ReservedNext(seqoid, epoch));
                if (SequenceRanges.NodeBlock(seqoid, epoch, "active") is (Guid, long, long) active &&
                    SequenceRanges.StepsLeft(seqoid, info, active) >= needed)
                {
                    break;
                }

                if (!requested)
                {
                    requested = true;
                    Post(new WorfReservation { RequestId = requestId, Seqoid = seqoid, Kind = (byte)'n', Size = needed });
                }
                else if (!pending)
                {
                    throw new PgException("2200H", $"sequence {info.Name} has no free span of {needed} values left");
                }
                else if (ReservedNext(seqoid, epoch) && HoldsSequence(seqoid))
                {
                    // The worker can't switch while this transaction holds the sequence, as nextval leaves it held.
                    throw new PgException("55P03", $"could not switch sequence {info.Name} to its reserved block while this transaction holds it",
                        hint: "Reserve before the transaction calls nextval on the sequence.");
                }

                Wait(requestId, deadline, $"a block of {needed} values of sequence {info.Name}");
            }
        }
        finally
        {
            // A cancelled session's request doesn't stay behind for the worker.
            Remove(id => id == requestId);
        }

        SequenceRanges.ChangedOnCommit();
    }

    /// <summary>
    /// Checks whether a reserved block waits for the worker to switch to it.
    /// </summary>
    private static bool ReservedNext(uint seqoid, int epoch)
        => Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_block WHERE seqoid = $1 AND epoch = $2 AND kind = 'n' AND state = 'next' AND reserved",
            SpiParameter.Create(seqoid), SpiParameter.Create(epoch)).Count > 0;

    /// <summary>
    /// Checks whether this backend holds a lock on the sequence, which nextval takes until the transaction ends.
    /// </summary>
    private static bool HoldsSequence(uint seqoid)
        => Worf.ReadScalar<bool>("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_locks
                WHERE locktype = 'relation' AND relation = $1 AND pid = pg_catalog.pg_backend_pid() AND granted AND mode <> 'AccessShareLock')
            """, SpiParameter.Create(seqoid));

    private static void ReserveValues(uint seqoid, long lo, long hi, PgInterval timeout)
    {
        _ = WorfNode.GetLocalNode(false, false);
        if (lo > hi)
        {
            throw new PgException(PgSqlStates.InvalidParameterValue, "lo must not be above hi");
        }

        _ = RequireUsable(seqoid);
        SequenceInfo info = SequenceRanges.GetInfo(seqoid);
        Guid requestId = Guid.NewGuid();
        long deadline = Worker.GetCurrentTimestamp() + (long)Int128.Min(timeout.ToComparisonMicroseconds(), long.MaxValue / 2);
        Post(new WorfReservation { RequestId = requestId, Seqoid = seqoid, Kind = (byte)'i', Size = SequenceRanges.Steps(info, lo, hi), Lo = lo, Hi = hi });
        try
        {
            while (true)
            {
                // As for a range, whether the worker still had the request is read before the blocks.
                bool pending = Pending(requestId);
                if (Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_block WHERE seqoid = $1 AND kind = 'i' AND state = 'imported' AND lo = $2 AND hi = $3",
                    SpiParameter.Create(seqoid), SpiParameter.Create(lo), SpiParameter.Create(hi)).Count > 0)
                {
                    break;
                }

                if (!pending)
                {
                    throw new PgException(PgSqlStates.InvalidParameterValue, $"values [{lo}, {hi}] of sequence {info.Name} may already be in use",
                        hint: "Load the rows without their keys so that the sequence numbers them, or load them before enabling range management.");
                }

                Wait(requestId, deadline, $"values [{lo}, {hi}] of sequence {info.Name}");
            }
        }
        finally
        {
            // A cancelled session's request doesn't stay behind for the worker.
            Remove(id => id == requestId);
        }

        SequenceRanges.ChangedOnCommit();
    }

    /// <summary>
    /// Takes the requests that this database's sessions handed over, in a transaction of its own: a next block that is
    /// large enough is reserved for the load, and otherwise, on the owner it allocates them, and elsewhere it records
    /// each with this node's parent. The caller then forgets them with <see cref="Forget"/> once that commits.
    /// </summary>
    /// <param name="node">This node.</param>
    /// <returns>The requests taken.</returns>
    internal static List<Guid> Record(WorfLocalNode node)
    {
        uint dboid = DatabaseId();
        List<WorfReservation> posted = [];
        using (PgLwLockShareGuard<WorfReservationTable, WorfReservation> guard = Reservations.Share())
        {
            for (int i = 0; i < MaxReservations; i++)
            {
                if (guard[i].RequestId != Guid.Empty && guard[i].Dboid == dboid)
                {
                    posted.Add(guard[i]);
                }
            }
        }

        List<Guid> recorded = [];
        foreach (WorfReservation reservation in posted)
        {
            SpiResult range = Spi.Query("SELECT owner_node, epoch FROM worf.sequence_range WHERE seqoid = $1 FOR NO KEY UPDATE", false, 1,
                SpiParameter.Create(reservation.Seqoid));
            if (range.Count > 0 && reservation.Kind == (byte)'n' && Spi.Execute("""
                UPDATE worf.sequence_range_block SET reserved = true
                WHERE block_id = (
                    SELECT b.block_id FROM worf.sequence_range_block b JOIN pg_catalog.pg_sequence s ON s.seqrelid = b.seqoid
                    WHERE b.seqoid = $1 AND b.epoch = $2 AND b.kind = 'n' AND b.state = 'next'
                        AND (b.hi::numeric - b.lo) / abs(s.seqincrement) + 1 >= $3
                    ORDER BY b.received_at LIMIT 1)
                """, SpiParameter.Create(reservation.Seqoid), SpiParameter.Create(range[0].Get<int>(1)), SpiParameter.Create(reservation.Size)) > 0)
            {
                recorded.Add(reservation.RequestId);
                continue;
            }

            if (range.Count > 0 && range[0].Get<uint>(0) == node.Node.Id)
            {
                // An allocation that fails, as for values in use, leaves the session to find no block.
                Allocate(reservation, range[0].Get<int>(1), node);
                recorded.Add(reservation.RequestId);
                continue;
            }

            if (Spi.Execute("""
                INSERT INTO worf.sequence_range_request (request_id, seqoid, parent_node, kind, size, lo, hi, created_at, reserved)
                SELECT $1, r.seqoid, r.parent_node, $3::"char", $4, $5, $6, now(), $3 = 'n'
                FROM worf.sequence_range r
                WHERE r.seqoid = $2 AND r.parent_node IS NOT NULL
                ON CONFLICT (request_id) DO NOTHING
                """, SpiParameter.Create(reservation.RequestId), SpiParameter.Create(reservation.Seqoid),
                SpiParameter.Create(((char)reservation.Kind).ToString()), SpiParameter.Create(reservation.Size),
                SpiParameter.Create(reservation.Kind == (byte)'i' ? reservation.Lo : (long?)null),
                SpiParameter.Create(reservation.Kind == (byte)'i' ? reservation.Hi : (long?)null)) > 0)
            {
                recorded.Add(reservation.RequestId);
            }
        }

        // Refusals stay a minute, so that the sessions waiting on them see them. They are looked for first, so that a
        // worker with none writes nothing.
        if (Worf.ReadScalar<bool>("SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE state = 'refused' AND created_at < now() - interval '1 minute')"))
        {
            _ = Spi.Execute("DELETE FROM worf.sequence_range_request WHERE state = 'refused' AND created_at < now() - interval '1 minute'");
        }

        return recorded;
    }

    /// <summary>
    /// Allocates a reservation on the owner, in the range worker's transaction.
    /// </summary>
    private static void Allocate(WorfReservation reservation, int epoch, WorfLocalNode node)
    {
        uint seqoid = reservation.Seqoid;
        SequenceInfo info = SequenceRanges.GetInfo(seqoid);
        Guid grant;
        long lo;
        long hi;
        if (reservation.Kind == (byte)'i')
        {
            if (SequenceRanges.AllocateExact(seqoid, info, epoch, reservation.Lo, reservation.Hi, node.Node.Id, node.Node.Name, reservation.RequestId, 'i',
                false) is not Guid exact)
            {
                return;
            }

            (grant, lo, hi) = (exact, reservation.Lo, reservation.Hi);
        }
        else if (SequenceRanges.Allocate(seqoid, info, epoch, reservation.Size, node.Node.Id, node.Node.Name, reservation.RequestId) is (Guid block, long start, long end))
        {
            (grant, lo, hi) = (block, start, end);
        }
        else
        {
            return;
        }

        bool imported = reservation.Kind == (byte)'i';
        _ = SequenceRanges.InsertBlock(seqoid, epoch, imported ? 'i' : 'n', lo, hi, imported ? "imported" : "next", node.Node.Id, node, grant, !imported);
        _ = Spi.Execute("UPDATE worf.sequence_range_grant SET state = 'acknowledged' WHERE grant_id = $1", SpiParameter.Create(grant));
    }

    /// <summary>
    /// Forgets requests the range worker has committed.
    /// </summary>
    /// <param name="requests">The requests.</param>
    internal static void Forget(List<Guid> requests)
    {
        if (requests.Count > 0)
        {
            Remove(requests.Contains);
        }
    }

    /// <summary>
    /// Checks that the caller has the privileges of the sequence's owner, as ALTER SEQUENCE needs: a reservation takes
    /// a block of any size from the root, and exact values the guard then admits.
    /// </summary>
    private static uint RequireOwnership(PgDatum sequence)
    {
        // A standby gets no blocks until it is promoted, so a reservation there would only wait out its timeout.
        if (SequenceRanges.Recovering())
        {
            throw new PgException("25006", "cannot reserve sequence values during recovery");
        }

        uint seqoid = SequenceRanges.RequireSequence(sequence);
        return Worf.ReadScalar<bool>("SELECT pg_catalog.pg_has_role(relowner, 'USAGE') FROM pg_catalog.pg_class WHERE oid = $1",
            SpiParameter.Create(seqoid)) ? seqoid
            : throw new PgException("42501", $"must be owner of sequence {SequenceRanges.GetInfo(seqoid).Name}");
    }

    /// <summary>
    /// Checks the sequence's state and returns its epoch and peak insert rate.
    /// </summary>
    private static (int Epoch, double Rate) RequireUsable(uint seqoid)
    {
        SpiResult range = Worf.SelectCatalog("SELECT epoch, owner_node, peak_rate, state, fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = $1",
            SpiParameter.Create(seqoid));
        string? state = range.Count == 0 ? null : range[0].Get<string>(3);
        if (state is null or "collecting" or "refused" or "disabled" or "no_route")
        {
            throw new PgException("55000", $"sequence {SequenceRanges.GetInfo(seqoid).Name} can't draw new blocks while it is {state ?? "not under range management"}");
        }

        return (range[0].Get<int>(0), range[0].Get<double>(2));
    }

    /// <summary>
    /// Checks whether a request still waits: on the worker, or with this node's parent. A request that does neither
    /// and brought no block was refused, or found no free span on the owner.
    /// </summary>
    private static bool Pending(Guid requestId)
        => Posted(requestId) || Worf.SelectCatalog("SELECT 1 FROM worf.sequence_range_request WHERE request_id = $1 AND state = 'open'",
            SpiParameter.Create(requestId)).Count > 0;

    private static uint DatabaseId()
    {
        unsafe
        {
            return NativeGlobals.MyDatabaseId;
        }
    }

    private static void Post(WorfReservation reservation)
    {
        reservation.Dboid = DatabaseId();
        using (PgLwLockExclusiveGuard<WorfReservationTable, WorfReservation> guard = Reservations.Exclusive())
        {
            for (int i = 0; i < MaxReservations; i++)
            {
                if (guard[i].RequestId == Guid.Empty)
                {
                    guard[i] = reservation;
                    WorfReservationTable table = guard.Header;
                    table.Used++;
                    guard.Header = table;
                    _ = PgAtomic.Add(SequenceRanges.ExchangeRequested, 1L);
                    return;
                }
            }
        }

        throw new PgException("53000", "too many sequence range reservations are waiting");
    }

    private static bool Posted(Guid requestId)
    {
        using PgLwLockShareGuard<WorfReservationTable, WorfReservation> guard = Reservations.Share();
        for (int i = 0; i < MaxReservations; i++)
        {
            if (guard[i].RequestId == requestId)
            {
                return true;
            }
        }

        return false;
    }

    private static void Remove(Func<Guid, bool> matches)
    {
        using PgLwLockExclusiveGuard<WorfReservationTable, WorfReservation> guard = Reservations.Exclusive();
        WorfReservationTable table = guard.Header;
        for (int i = 0; i < MaxReservations; i++)
        {
            if (guard[i].RequestId != Guid.Empty && matches(guard[i].RequestId))
            {
                guard[i] = default;
                table.Used--;
            }
        }

        guard.Header = table;
    }

    /// <summary>
    /// Waits a moment for an answer, and gives up, withdrawing the request from the worker, once the time is up.
    /// </summary>
    private static void Wait(Guid requestId, long deadline, string what)
    {
        if (Worker.GetCurrentTimestamp() >= deadline)
        {
            Remove(id => id == requestId);
            throw new PgException("57014", $"timed out waiting for {what}");
        }

        PgInterrupts.Check();
        Worker.WaitLatch(PollInterval);
    }
}
