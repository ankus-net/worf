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
    /// <summary>
    /// Drops the state record of a sequence if it exists.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void DropSequenceStateRecord(uint seqoid)
        => Spi.Execute("DELETE FROM worf.sequence_state WHERE seqoid = $1", SpiParameter.Create(seqoid));
}
