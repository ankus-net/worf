using System.Globalization;
using System.Text;
using Ankus.Postgres;

[assembly: PgSql("sequence-state-table", """
    CREATE TABLE worf.sequence_state (
    	seqoid oid NOT NULL PRIMARY KEY,
    	cache_size integer NOT NULL,
    	last_value bigint NOT NULL
    ) WITH (user_catalog_table=true);
    """)]

namespace Worf;

/// <summary>
/// worf sequence handling.
/// </summary>
internal static class Sequences
{
    private const int SequenceReplicationMinCache = 1000;
    private const int SequenceReplicationMaxCache = 1000000;

    /// <summary>
    /// Get last value of individual sequence.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The sequence's last value.</returns>
    internal static long SequenceGetLastValue(uint seqoid)
    {
        string seqname;
        unsafe
        {
            seqname = Spi.QuoteQualifiedIdentifier(
                Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(seqoid))),
                Worf.FromServer(NativeMethods.get_rel_name(seqoid)));
        }

        return Spi.ExecuteScalar<long>("SELECT last_value FROM " + seqname);
    }

    /// <summary>
    /// Process sequence updates.
    /// </summary>
    /// <returns>Whether no sequence consumed more than half of its cache.</returns>
    internal static bool SynchronizeSequences()
    {
        bool ret = true;

        PgBackgroundWorker.RunTransaction(() =>
        {
            WorfLocalNode? localNode = WorfNode.GetLocalNode(false, true);
            if (localNode is null)
            {
                return;
            }

            SpiResult states = Spi.Query("SELECT seqoid, cache_size, last_value FROM worf.sequence_state FOR UPDATE", false, 0);

            foreach (SpiRow oldseq in states)
            {
                PgInterrupts.Check();

                uint seqoid = oldseq.Get<uint>(0);
                int cacheSize = oldseq.Get<int>(1);
                long oldLastValue = oldseq.Get<long>(2);

                long lastValue = SequenceGetLastValue(seqoid);

                // Not enough of the sequence was consumed yet for us to care.
                if (oldLastValue >= lastValue + (SequenceReplicationMinCache / 2))
                {
                    continue;
                }

                // Consumed more than half of cache of the sequence.
                if (oldLastValue + (cacheSize / 2) < lastValue)
                {
                    ret = false;
                }

                // The sequence is consumed too fast, increase the buffer cache.
                if (oldLastValue + cacheSize <= lastValue)
                {
                    cacheSize = Math.Min(SequenceReplicationMaxCache, cacheSize * 2);
                }

                long newLastValue = lastValue + cacheSize;

                _ = Spi.Execute("UPDATE worf.sequence_state SET cache_size = $2, last_value = $3 WHERE seqoid = $1",
                    SpiParameter.Create(seqoid), SpiParameter.Create(cacheSize), SpiParameter.Create(newLastValue));

                QueueSequence(localNode, seqoid, newLastValue);
            }
        });

        return ret;
    }

    /// <summary>
    /// Process sequence updates.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void SynchronizeSequence(uint seqoid)
    {
        WorfLocalNode localNode = WorfNode.GetLocalNode(true, false)!;

        // Check if the oid points to actual sequence.
        using PgRelation seqrel = PgRelation.Open(seqoid, PgLockMode.AccessShare);
        if (!seqrel.IsSequence)
        {
            throw new PgException(PgSqlStates.WrongObjectType, $"\"{seqrel.Name}\" is not a sequence");
        }

        // Now search for it in our tracking table.
        SpiResult result = Spi.Query("SELECT cache_size FROM worf.sequence_state WHERE seqoid = $1 FOR UPDATE", false, 0,
            SpiParameter.Create(seqoid));
        if (result.Count == 0)
        {
            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, $"\"{seqrel.Name}\" is not a replicated sequence");
        }

        int cacheSize = result[0].Get<int>(0);
        long lastValue = SequenceGetLastValue(seqoid) + cacheSize;

        _ = Spi.Execute("UPDATE worf.sequence_state SET last_value = $2 WHERE seqoid = $1",
            SpiParameter.Create(seqoid), SpiParameter.Create(lastValue));

        QueueSequence(localNode, seqoid, lastValue);
    }

    /// <summary>
    /// Makes sure there is sequence state record for given sequence.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void CreateSequenceStateRecord(uint seqoid)
    {
        // Check if the state record already exists, and if it doesn't insert it.
        if (Spi.ExecuteScalar<long>("SELECT count(*) FROM worf.sequence_state WHERE seqoid = $1", SpiParameter.Create(seqoid)) == 0)
        {
            _ = Spi.Execute("INSERT INTO worf.sequence_state VALUES ($1, $2, $3)",
                SpiParameter.Create(seqoid), SpiParameter.Create(SequenceReplicationMinCache),
                SpiParameter.Create(SequenceGetLastValue(seqoid)));
        }

        unsafe
        {
            NativeMethods.CommandCounterIncrement();
        }
    }

    /// <summary>
    /// Makes sure there isn't sequence state record for given sequence.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void DropSequenceStateRecord(uint seqoid)
    {
        _ = Spi.Execute("DELETE FROM worf.sequence_state WHERE seqoid = $1", SpiParameter.Create(seqoid));

        unsafe
        {
            NativeMethods.CommandCounterIncrement();
        }
    }

    private static void QueueSequence(WorfLocalNode localNode, uint seqoid, long lastValue)
    {
        List<string> repsetNames = [];
        foreach (WorfRepSet repset in ReplicationSet.GetSeqReplicationSets(localNode.Node.Id, seqoid))
        {
            repsetNames.Add(repset.Name);
        }

        string nspname;
        string relname;
        uint userId;
        unsafe
        {
            nspname = Worf.FromServer(NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(seqoid)));
            relname = Worf.FromServer(NativeMethods.get_rel_name(seqoid));
            userId = NativeMethods.GetUserId();
        }

        StringBuilder json = new("{\"schema_name\": ");
        Executor.EscapeJson(json, nspname);
        json.Append(",\"sequence_name\": ");
        Executor.EscapeJson(json, relname);
        json.Append(CultureInfo.InvariantCulture, $",\"last_value\": \"{lastValue}\"");
        json.Append('}');

        Queue.QueueMessage(repsetNames.Count > 0 ? repsetNames : null, userId, Queue.QueueCommandTypeSequence, json.ToString());
    }
}
